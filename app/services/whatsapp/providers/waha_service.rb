require 'base64'
require 'cgi'
require 'uri'

class Whatsapp::Providers::WahaService < Whatsapp::Providers::BaseService
  SUPPORTED_WEBHOOK_EVENTS = %w[message message.any message.edited message.revoked message.ack message.reaction session.status].freeze
  DEFAULT_WEBHOOK_EVENTS = %w[message.any message.edited message.revoked message.ack message.reaction session.status].freeze

  def send_message(phone_number, message)
    @message = message
    @phone_number = phone_number

    if message.attachments.present?
      send_attachment_message(phone_number, message)
    elsif message.content.present?
      send_text_message(phone_number, message)
    else
      mark_message_unsupported(message)
    end
  end

  def send_template(phone_number, template_info, message = nil)
    send_text_message(phone_number, message, build_template_text(template_info))
  end

  def sync_templates
    whatsapp_channel.mark_message_templates_updated
  end

  def validate_provider_config?
    return false if api_base_path.blank?
    return false if admin_token.blank?
    return false if session_name.blank?

    return true if server_status_available?

    sessions_endpoint_available?
  rescue StandardError => e
    Rails.logger.error "Waha API validation error: #{e.message}"
    false
  end

  def api_headers
    {
      'X-Api-Key' => admin_token,
      'Content-Type' => 'application/json',
      'Accept' => 'application/json'
    }
  end

  def media_url(message_id, chat_id = nil)
    return if message_id.blank? || session_name.blank?

    chat_identifier = format_chat_id(chat_id) if chat_id.present?
    chat_identifier ||= format_chat_id(default_chat_id)
    return if chat_identifier.blank?

    "#{api_base_path}/#{session_name}/chats/#{chat_identifier}/messages/#{message_id}?downloadMedia=true"
  end

  def lookup_lid(lid)
    return if lid.blank?

    response = HTTParty.get(
      "#{api_base_path}/#{session_name}/lids/#{CGI.escape(lid)}",
      headers: api_headers,
      timeout: 10
    )

    return unless response.success?

    extract_lid_phone(response.parsed_response)
  rescue StandardError => e
    Rails.logger.warn "Waha API lid lookup failed for #{lid}: #{e.message}"
    nil
  end

  def fetch_group_metadata(group_jid)
    return if group_jid.blank? || session_name.blank?

    response = HTTParty.get(
      "#{api_base_path}/#{session_name}/groups/#{CGI.escape(group_jid)}",
      headers: api_headers,
      timeout: 10
    )

    return unless response.success?

    data = response.parsed_response
    data.is_a?(Hash) ? data : data.try(:first)
  rescue StandardError => e
    Rails.logger.warn "Waha API group metadata fetch failed for #{group_jid}: #{e.message}"
    nil
  end

  def fetch_group_profile_picture(group_jid, refresh: false)
    return if group_jid.blank? || session_name.blank?

    response = HTTParty.get(
      "#{api_base_path}/#{session_name}/groups/#{CGI.escape(group_jid)}/picture",
      headers: api_headers,
      query: { refresh: refresh },
      timeout: 10
    )

    return unless response.success?

    body = response.parsed_response
    body.is_a?(Hash) ? body['url'] : body
  rescue StandardError => e
    Rails.logger.warn "Waha API group picture fetch failed for #{group_jid}: #{e.message}"
    nil
  end

  def fetch_contact_profile_picture(contact_identifier, refresh: false)
    contact_id = normalize_contact_identifier(contact_identifier)
    return if contact_id.blank?

    response = HTTParty.get(
      "#{api_base_path}/contacts/profile-picture",
      headers: api_headers,
      query: { contactId: contact_id, session: session_name, refresh: refresh },
      timeout: 10
    )

    return unless response.success?

    parse_profile_picture_response(response.parsed_response)
  rescue StandardError => e
    Rails.logger.warn "Waha API profile picture fetch failed for #{contact_identifier}: #{e.message}"
    nil
  end

  def configure_webhook(events_override: nil)
    base_path = api_base_path
    name = session_name
    return if base_path.blank? || name.blank?

    webhook = webhook_url
    return { ok: false, error: 'Webhook URL not configured' } if webhook.blank?

    events = self.class.normalize_events(events_override) || effective_webhook_events
    payload = self.class.build_session_payload(
      name: session_name,
      webhook_url: webhook,
      events: events,
      metadata: nil,
      start: true
    )

    body = { config: payload[:config] }

    response = HTTParty.put(
      "#{base_path}/sessions/#{name}",
      headers: api_headers,
      body: body.to_json,
      timeout: 15
    )

    if response.success?
      { ok: true }
    else
      Rails.logger.error "Waha API webhook error: #{response.code} - #{response.body}"
      { ok: false, error: response.body }
    end
  rescue StandardError => e
    Rails.logger.error "Waha API webhook exception: #{e.message}"
    { ok: false, error: e.message }
  end

  def setup_channel_provider
    ensure_provider_defaults!

    base_path = api_base_path
    name = session_name

    if base_path.blank? || admin_token.blank? || name.blank?
      error_message = 'Waha API setup aborted: missing api_url, admin_token, or session_name'
      Rails.logger.error error_message
      return { ok: false, error: error_message }
    end

    remove_existing_session

    payload = session_payload

    response = HTTParty.post(
      "#{base_path}/sessions",
      headers: api_headers,
      body: payload.to_json,
      timeout: 20
    )

    if response.success?
      parsed_response = response.parsed_response
      whatsapp_channel.update_provider_connection!(parsed_response)
      start_session unless payload[:start]
      { ok: true, response: parsed_response }
    else
      error_message = response_error_message(response)
      Rails.logger.error "Waha API session create error: #{response.code} - #{error_message}"
      { ok: false, error: error_message.presence || "HTTP #{response.code}" }
    end
  rescue StandardError => e
    Rails.logger.error "Waha API setup exception: #{e.message}"
    { ok: false, error: e.message }
  end

  def disconnect_channel_provider
    base_path = api_base_path
    name = session_name
    return if base_path.blank? || name.blank?

    response = HTTParty.post(
      "#{base_path}/sessions/logout",
      headers: api_headers,
      body: { name: name }.to_json,
      timeout: 15
    )

    return if response&.success?

    HTTParty.delete(
      "#{base_path}/sessions/#{name}",
      headers: api_headers,
      timeout: 15
    )
  rescue StandardError => e
    Rails.logger.error "Waha API disconnect exception: #{e.message}"
  end

  def delete_message(message)
    chat_id = chat_id_for(message)
    return if chat_id.blank? || message.source_id.blank?

    HTTParty.delete(
      "#{api_base_path}/#{session_name}/chats/#{chat_id}/messages/#{message.source_id}",
      headers: api_headers,
      timeout: 15
    )
  rescue StandardError => e
    Rails.logger.error "Waha API delete exception: #{e.message}"
  end

  def update_message(message, new_text)
    return if new_text.blank?

    chat_id = chat_id_for(message)
    return if chat_id.blank? || message.source_id.blank?

    body = {
      text: new_text,
      linkPreview: true
    }

    response = HTTParty.put(
      "#{api_base_path}/#{session_name}/chats/#{chat_id}/messages/#{message.source_id}",
      headers: api_headers,
      body: body.to_json,
      timeout: 15
    )

    Rails.logger.error "Waha API update error: #{response.code} - #{response.body}" unless response.success?
  rescue StandardError => e
    Rails.logger.error "Waha API update exception: #{e.message}"
  end

  private

  attr_reader :message, :phone_number

  def ensure_provider_defaults!
    config = whatsapp_channel.provider_config
    config['api_url'] ||= ENV.fetch('WAHA_API_URL', nil)
    config['admin_token'] ||= ENV.fetch('WAHA_ADMIN_TOKEN', nil)
    config['session_name'] ||= config['instance_name'] || ENV.fetch('WAHA_SESSION_NAME', nil) || ENV.fetch('WAHA_INSTANCE_NAME', nil)
    whatsapp_channel.provider_config = config
    whatsapp_channel.save! if whatsapp_channel.changed?
  end

  def api_base_path
    base = whatsapp_channel.provider_config['api_url'].presence || ENV.fetch('WAHA_API_URL', nil)
    return '' if base.blank?

    normalized = base.chomp('/')
    normalized.end_with?('/api') ? normalized : "#{normalized}/api"
  end

  def admin_token
    whatsapp_channel.provider_config['admin_token'].presence || ENV.fetch('WAHA_ADMIN_TOKEN', nil)
  end

  def session_name
    config = whatsapp_channel.provider_config
    (config['session_name'].presence || config['instance_name'].presence || ENV.fetch('WAHA_SESSION_NAME', nil) || ENV.fetch('WAHA_INSTANCE_NAME', nil)).to_s
  end

  def send_text_message(phone_number, message, override_text = nil)
    body = {
      chatId: format_chat_id(phone_number),
      text: override_text || outgoing_text(message),
      session: session_name
    }

    reply_to = reply_to_identifier_for(message)
    body[:reply_to] = reply_to if reply_to.present?

    response = HTTParty.post(
      "#{api_base_path}/sendText",
      headers: api_headers,
      body: body.to_json,
      timeout: 20
    )

    handle_delivery_response(response, message)
  end

  def send_attachment_message(phone_number, message)
    attachment = message.attachments.first
    return unless attachment

    response = case attachment.file_type.to_s
               when 'image'
                 send_image_message(phone_number, message, attachment)
               when 'video'
                 send_video_message(phone_number, message, attachment)
               when 'audio'
                 send_voice_message(phone_number, message, attachment)
               else
                 send_document_message(phone_number, message, attachment)
               end

    handle_delivery_response(response, message) if response
  end

  def send_voice_message(phone_number, message, attachment)
    voice_file = voice_file_payload_for(attachment)
    return unless voice_file

    payload = {
      chatId: format_chat_id(phone_number),
      session: session_name,
      convert: true,
      file: voice_file
    }

    reply_to = reply_to_identifier_for(message)
    payload[:reply_to] = reply_to if reply_to.present?

    HTTParty.post(
      "#{api_base_path}/sendVoice",
      headers: api_headers,
      body: payload.to_json,
      timeout: 60
    )
  end

  def send_image_message(phone_number, message, attachment)
    file_payload = media_file_payload_for(attachment, fallback_mime: 'image/jpeg', fallback_extension: '.jpg')
    return send_document_message(phone_number, message, attachment) unless file_payload

    payload = {
      chatId: format_chat_id(phone_number),
      session: session_name,
      caption: message.content.presence,
      file: file_payload
    }.compact

    reply_to = reply_to_identifier_for(message)
    payload[:reply_to] = reply_to if reply_to.present?

    HTTParty.post(
      "#{api_base_path}/sendImage",
      headers: api_headers,
      body: payload.to_json,
      timeout: 60
    )
  end

  def send_video_message(phone_number, message, attachment)
    file_payload = media_file_payload_for(attachment, fallback_mime: 'video/mp4', fallback_extension: '.mp4', ensure_filename: true)
    return send_document_message(phone_number, message, attachment) unless file_payload

    payload = {
      chatId: format_chat_id(phone_number),
      session: session_name,
      caption: message.content.presence,
      convert: true,
      file: file_payload
    }.compact

    reply_to = reply_to_identifier_for(message)
    payload[:reply_to] = reply_to if reply_to.present?

    HTTParty.post(
      "#{api_base_path}/sendVideo",
      headers: api_headers,
      body: payload.to_json,
      timeout: 60
    )
  end

  def send_document_message(phone_number, message, attachment)
    payload = {
      chatId: format_chat_id(phone_number),
      session: session_name,
      caption: message.content.presence,
      file: file_payload_for(attachment)
    }.compact

    reply_to = reply_to_identifier_for(message)
    payload[:reply_to] = reply_to if reply_to.present?

    HTTParty.post(
      "#{api_base_path}/sendFile",
      headers: api_headers,
      body: payload.to_json,
      timeout: 60
    )
  end

  def file_payload_for(attachment)
    if attachment.external_url.present?
      {
        url: attachment.external_url,
        mimetype: attachment.file&.content_type,
        filename: attachment.file&.filename&.to_s
      }.compact
    elsif (url = attachment.download_url.presence)
      {
        url: url,
        mimetype: attachment.file&.content_type,
        filename: attachment.file&.filename&.to_s.presence
      }.compact
    elsif attachment.file.attached?
      {
        data: Base64.strict_encode64(attachment.file.download),
        mimetype: attachment.file.content_type,
        filename: attachment.file.filename.to_s
      }
    end
  end

  def voice_file_payload_for(attachment)
    opus_mime = 'audio/ogg; codecs=opus'

    if attachment.external_url.present?
      {
        url: attachment.external_url,
        mimetype: opus_mime
      }
    elsif (url = attachment.download_url.presence)
      filename = attachment.file&.filename&.to_s
      filename = ensure_opus_filename(filename) if filename.present?

      {
        url: url,
        mimetype: opus_mime,
        filename: filename
      }.compact
    elsif attachment.file.attached?
      {
        data: Base64.strict_encode64(attachment.file.download),
        mimetype: opus_mime,
        filename: ensure_opus_filename(attachment.file.filename.to_s)
      }
    else
      url = attachment.download_url.presence
      return unless url

      {
        url: url,
        mimetype: opus_mime
      }
    end
  end

  def media_file_payload_for(attachment, fallback_mime:, fallback_extension:, ensure_filename: false)
    if attachment.external_url.present?
      filename = attachment.file&.filename&.to_s
      filename ||= filename_from_url(attachment.external_url)
      filename = ensure_extension(filename, fallback_extension) if ensure_filename || filename.present?
      filename ||= generate_default_filename(fallback_extension)

      {
        url: attachment.external_url,
        mimetype: attachment.file&.content_type || fallback_mime,
        filename: filename
      }
    elsif (url = attachment.download_url.presence)
      filename = attachment.file&.filename&.to_s
      filename = filename_from_url(url) if filename.blank?
      filename = ensure_extension(filename, fallback_extension) if ensure_filename || filename.present?
      filename ||= generate_default_filename(fallback_extension)

      {
        url: url,
        mimetype: attachment.file&.content_type || fallback_mime,
        filename: filename
      }
    elsif attachment.file.attached?
      filename = ensure_extension(attachment.file.filename.to_s, fallback_extension)

      {
        data: Base64.strict_encode64(attachment.file.download),
        mimetype: attachment.file.content_type || fallback_mime,
        filename: filename
      }
    else
      url = attachment.download_url.presence
      return unless url

      {
        url: url,
        mimetype: fallback_mime,
        filename: generate_default_filename(fallback_extension)
      }
    end
  end

  def outgoing_text(message)
    return message.outgoing_content if message.respond_to?(:outgoing_content)

    message.to_s
  end

  def reply_to_identifier_for(message)
    return unless message.respond_to?(:content_attributes)

    attrs = message.content_attributes || {}
    value = attrs[:in_reply_to_external_id]
    value = attrs['in_reply_to_external_id'] if value.blank?
    value
  end

  def ensure_opus_filename(filename)
    base = filename.to_s.strip
    return 'voice-message.opus' if base.blank?

    stem = File.basename(base, File.extname(base))
    "#{stem}.opus"
  end

  def ensure_extension(filename, extension)
    base = filename.to_s.strip
    return generate_default_filename(extension) if base.blank?

    File.extname(base).casecmp(extension).zero? ? base : "#{File.basename(base, '.*')}#{extension}"
  end

  def generate_default_filename(extension)
    "chatwoot_media#{extension}"
  end

  def filename_from_url(url)
    URI.parse(url).path.split('/').last
  rescue URI::InvalidURIError
    nil
  end

  def handle_delivery_response(response, message)
    unless response.respond_to?(:success?)
      mark_message_unsupported(message)
      Rails.logger.warn 'Waha API: Voice message send returned no response payload'
      return nil
    end

    if response.success?
      parsed = response.parsed_response
      ids = store_outgoing_waha_ids(message, parsed)
      message_id = ids.first
      mark_delivered_on_send(message)
      return message_id if message_id.present?

      return nil
    end

    handle_error(response, message)
    nil
  end

  def store_outgoing_waha_ids(message, parsed_response)
    return [] unless message && parsed_response.is_a?(Hash)

    primary_id = [
      parsed_response.dig('key', 'id'),
      parsed_response.dig('response', 'key', 'id'),
      parsed_response.dig('data', 'key', 'id'),
      parsed_response['messageId'],
      parsed_response['id'],
      parsed_response['keyId']
    ].find(&:present?)

    extra_ids = [
      parsed_response['messageId'],
      parsed_response['id'],
      parsed_response['keyId'],
      parsed_response.dig('response', 'messageId'),
      parsed_response.dig('data', 'id')
    ]

    ids = ([primary_id] + extra_ids).compact.map(&:to_s).reject(&:blank?).uniq

    remote_candidates = [
      parsed_response.dig('key', 'remoteJid'),
      parsed_response.dig('response', 'key', 'remoteJid'),
      parsed_response.dig('data', 'key', 'remoteJid'),
      chat_id_for(message),
      format_chat_id(message.conversation&.contact_inbox&.source_id)
    ].compact.map(&:to_s).reject(&:blank?).uniq

    core_id = primary_id.to_s.presence || ids.first

    components = parse_waha_identifier(core_id)
    remote_candidates << components[:remote] if components&.[](:remote).present?
    remote_candidates.compact!
    remote_candidates.uniq!

    remote_variants = remote_candidates.flat_map { |remote| waha_remote_variants(remote) }
    remote_variants = remote_candidates if remote_variants.blank?

    base_token = components&.[](:message).presence || core_id

    if base_token.present? && remote_variants.present?
      remote_variants.each do |remote|
        normalized_remote = remote.to_s.strip
        next if normalized_remote.blank?

        ids << "true_#{normalized_remote}_#{base_token}"
        ids << "false_#{normalized_remote}_#{base_token}"
      end
    end

    ids = ids.map(&:to_s).reject(&:blank?).uniq

    return [] if ids.blank?

    ext = (message.external_source_ids || {}).dup
    existing = Array.wrap(ext['waha_message_ids'])
    ext['waha_message_ids'] = (existing + ids).map(&:to_s).reject(&:blank?).uniq
    ext['waha_message_id'] ||= ids.first

    update_attrs = { external_source_ids: ext }
    update_attrs[:source_id] = core_id if core_id.present?

    message.update!(update_attrs)

    ids
  rescue StandardError => e
    Rails.logger.warn "Waha API: Failed to store outgoing message ids for #{message.id}: #{e.message}"
    []
  end

  def mark_delivered_on_send(message)
    return unless message&.outgoing?

    return if message.status.in?(%w[delivered read failed])

    message.update!(status: :delivered)
  rescue StandardError => e
    Rails.logger.warn "Waha API: Could not mark message delivered: #{e.message}"
  end

  def mark_message_unsupported(message)
    message.update!(is_unsupported: true)
  rescue StandardError
    nil
  end

  def effective_webhook_events
    raw = whatsapp_channel.provider_config['webhook_events']
    events = self.class.normalize_events(raw)
    events.presence || DEFAULT_WEBHOOK_EVENTS
  end

  def self.normalize_events(events)
    return if events.blank?

    normalized = Array(events)
                 .map { |event| event.to_s.strip.downcase }
                 .reject(&:blank?)
                 .select { |event| SUPPORTED_WEBHOOK_EVENTS.include?(event) }
                 .uniq

    normalized.delete('message') if normalized.include?('message.any')

    normalized.presence
  end

  def server_status_available?
    response = HTTParty.get(
      "#{api_base_path}/server/status",
      headers: api_headers,
      timeout: 10
    )

    return true if response.success?

    return nil if response.code.to_i == 404

    Rails.logger.warn "Waha API validation warning: server status returned #{response.code}"
    false
  rescue StandardError => e
    Rails.logger.debug { "Waha API server status check failed, will try session endpoint: #{e.message}" }
    nil
  end

  def sessions_endpoint_available?
    response = HTTParty.get(
      "#{api_base_path}/sessions",
      headers: api_headers,
      timeout: 10
    )

    return true if response.success?

    Rails.logger.warn "Waha API validation warning: sessions endpoint returned #{response.code}"
    false
  rescue StandardError => e
    Rails.logger.error "Waha API sessions check failed: #{e.message}"
    false
  end

  def webhook_url
    base = ENV['FRONTEND_URL'].presence || ENV['BACKEND_URL']
    return if base.blank?

    "#{base.chomp('/')}/webhooks/whatsapp/#{whatsapp_channel.phone_number}"
  end

  def session_payload
    self.class.build_session_payload(
      name: session_name,
      webhook_url: webhook_url,
      events: effective_webhook_events,
      metadata: default_metadata,
      start: true
    )
  end

  def default_metadata
    {
      'chatwoot.account_id' => whatsapp_channel.account_id&.to_s,
      'chatwoot.inbox_id' => whatsapp_channel.inbox&.id&.to_s
    }.compact_blank
  end

  def remove_existing_session
    base_path = api_base_path
    name = session_name
    return if base_path.blank? || name.blank?

    HTTParty.delete(
      "#{base_path}/sessions/#{name}",
      headers: api_headers,
      timeout: 15
    )
  rescue StandardError => e
    Rails.logger.warn "Waha API cleanup failed: #{e.message}"
  end

  def chat_id_for(message)
    source = message.conversation&.contact_inbox&.source_id
    format_chat_id(source)
  end

  def default_chat_id
    whatsapp_channel.inbox&.contact_inboxes&.first&.source_id
  end

  def format_chat_id(value)
    return if value.blank?

    val = value.to_s.strip
    return normalize_chat_identifier(val) if val.include?('@')

    digits = val.gsub(/\D/, '')
    return if digits.blank?

    normalize_chat_identifier("#{digits}@s.whatsapp.net")
  end

  def normalize_contact_identifier(value)
    val = value.to_s.strip
    return if val.blank?

    return normalize_chat_identifier(val, prefer_c_us: true) if val.include?('@')

    digits = val.gsub(/\D/, '')
    return if digits.blank?

    normalize_chat_identifier("#{digits}@c.us", prefer_c_us: true)
  end

  def waha_remote_variants(identifier)
    value = identifier.to_s.strip
    return [] if value.blank?

    variants = [value]

    normalized_chat = normalize_chat_identifier(value)
    variants << normalized_chat if normalized_chat.present?

    normalized_contact = normalize_contact_identifier(value)
    variants << normalized_contact if normalized_contact.present?

    variant_source = normalized_chat.presence || normalized_contact.presence || value
    node, domain = variant_source.to_s.split('@', 2)

    if domain.present?
      unless %w[g.us newsletter broadcast lid].include?(domain)
        variants << "#{node}@s.whatsapp.net"
        variants << "#{node}@c.us"
      end
      variants << node
    else
      numeric = variant_source.to_s.gsub(/\D/, '')
      variants << "#{numeric}@s.whatsapp.net" if numeric.present?
      variants << "#{numeric}@c.us" if numeric.present?
    end

    variants.compact.map { |jid| jid.to_s.strip }.reject(&:blank?).uniq
  rescue StandardError
    [value].compact.map { |jid| jid.to_s.strip }.reject(&:blank?).uniq
  end

  def parse_waha_identifier(identifier)
    fragments = identifier.to_s.strip.split('_')
    return {} if fragments.length < 3

    flag = fragments.shift
    remote = fragments.shift
    message_token = fragments.shift
    participants = fragments

    {
      flag: flag.presence,
      remote: remote.presence,
      message: message_token.presence,
      participants: participants.compact.map { |participant| participant.to_s.strip }.reject(&:blank?)
    }
  rescue StandardError
    {}
  end

  def parse_profile_picture_response(payload)
    extract_profile_picture_url(payload)
  end

  def extract_lid_phone(payload)
    case payload
    when Hash
      payload['pn'] || payload[:pn]
    when Array
      extract_lid_phone(payload.first)
    else
      payload
    end
  end

  def extract_profile_picture_url(payload)
    return if payload.blank?

    data = if payload.respond_to?(:to_unsafe_h)
             payload.to_unsafe_h
           elsif payload.is_a?(Hash)
             payload
           elsif payload.is_a?(Array)
             payload.filter_map { |item| extract_profile_picture_url(item) }&.find(&:present?)
           else
             payload
           end

    return data if data.is_a?(String)
    return unless data.is_a?(Hash)

    candidates = collect_profile_picture_candidates(data)

    nested = data['data'] || data[:data]
    if nested.present?
      if nested.is_a?(Array)
        candidates += nested.filter_map { |item| extract_profile_picture_url(item) }
      elsif nested.is_a?(Hash)
        candidates << extract_profile_picture_url(nested)
      end
    end

    Array(candidates).flatten.compact.map(&:to_s).find(&:present?)
  end

  def normalize_chat_identifier(identifier, prefer_c_us: false)
    raw_node, domain = identifier.to_s.split('@', 2)
    return if raw_node.blank?
    return identifier if domain.blank?

    domain = domain.downcase

    case domain
    when 'g.us', 'newsletter', 'broadcast'
      return "#{raw_node}@#{domain}"
    when 'lid'
      return "#{raw_node}@lid"
    end

    digits = raw_node.gsub(/\D/, '')
    node = digits.presence || raw_node

    return "#{node}@c.us" if prefer_c_us

    case domain
    when 's.whatsapp.net', 'c.us'
      "#{node}@s.whatsapp.net"
    else
      "#{node}@#{domain}"
    end
  end

  def collect_profile_picture_candidates(hash, thumb: false)
    normalized = hash.each_with_object({}) do |(key, value), memo|
      memo[key.to_s.downcase] ||= value
    end

    keys = thumb ? PROFILE_PIC_THUMB_KEYS : PROFILE_PIC_LOOKUP_KEYS

    values = keys.map { |lookup_key| normalized[lookup_key] }

    unless thumb
      thumb_block = hash['profilePicThumbObj'] || hash[:profilePicThumbObj] || hash['profilePicThumb'] || hash[:profilePicThumb]
      thumb_block = thumb_block.to_unsafe_h if thumb_block.respond_to?(:to_unsafe_h)
      values += collect_profile_picture_candidates(thumb_block, thumb: true) if thumb_block.is_a?(Hash)
    end

    values
  end

  def start_session
    base_path = api_base_path
    name = session_name
    return if base_path.blank? || name.blank?

    response = HTTParty.post(
      "#{base_path}/sessions/#{name}/start",
      headers: api_headers,
      timeout: 20
    )

    unless response.success?
      Rails.logger.error "Waha API session start error: #{response.code} - #{response.body}"
    end
  rescue StandardError => e
    Rails.logger.error "Waha API session start exception: #{e.message}"
  end

  def build_template_text(template_info)
    text = template_info[:name] || template_info['name'] || 'Template Message'
    parameters = template_info[:parameters] || template_info['parameters'] || []

    Array(parameters).each_with_index do |param, index|
      placeholder = "{{#{index + 1}}}"
      text = text.to_s.gsub(placeholder, param.to_s)
    end

    text
  end

  def handle_error(response, message)
    if response.code.to_i == 403
      Rails.logger.warn "Waha API returned 403 for message #{message&.id}: #{error_message(response)}"
      return
    end

    super(response, message)
  end

  def error_message(response)
    parsed = response.parsed_response
    return parsed['message'] if parsed.is_a?(Hash) && parsed['message'].present?
    return parsed['error'] if parsed.is_a?(Hash) && parsed['error'].present?

    response.body
  end

  def response_error_message(response)
    error_message(response)
  rescue StandardError
    response.body
  end

  def self.build_session_payload(name:, webhook_url:, events:, metadata:, start: true)
    normalized_events = normalize_events(events) || DEFAULT_WEBHOOK_EVENTS

    webhook_config = if webhook_url.present? && normalized_events.present?
                       {
                         url: webhook_url,
                         events: normalized_events,
                         hmac: nil,
                         retries: nil,
                         customHeaders: nil
                       }
                     end

    metadata_payload = metadata.present? ? metadata.compact_blank : nil

    config = {
      proxy: nil,
      debug: false,
      ignore: {
        status: nil,
        groups: nil,
        channels: nil
      },
      noweb: {
        store: {
          enabled: true,
          fullSync: false
        }
      },
      webjs: {
        tagsEventsOn: false
      }
    }

    config[:metadata] = metadata_payload if metadata_payload.present?
    config[:webhooks] = webhook_config.present? ? [webhook_config] : []

    {
      name: name,
      start: start,
      config: config
    }
  end

  public :setup_channel_provider, :configure_webhook, :disconnect_channel_provider, :delete_message, :update_message
end
  PROFILE_PIC_LOOKUP_KEYS = %w[
    profilepicurl profile_picture_url profilepictureurl profilepicture
    avatarurl avatar_url
    profilephoto profilephotourl profile_photo_url
    url
  ].freeze

  PROFILE_PIC_THUMB_KEYS = %w[eurl url link previewurl preview_url].freeze

