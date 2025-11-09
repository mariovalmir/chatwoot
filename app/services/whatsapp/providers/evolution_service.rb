require 'base64'

class Whatsapp::Providers::EvolutionService < Whatsapp::Providers::BaseService
  DEFAULT_URL = ENV.fetch('EVOLUTION_PROVIDER_DEFAULT_URL', nil)
  DEFAULT_API_KEY = ENV.fetch('EVOLUTION_PROVIDER_DEFAULT_API_KEY', nil)

  WEBHOOK_EVENTS = %w[
    QRCODE_UPDATED
    MESSAGES_SET
    MESSAGES_UPSERT
    MESSAGES_UPDATE
    MESSAGES_DELETE
    SEND_MESSAGE
    SEND_MESSAGE_UPDATE
    CONTACTS_UPDATE
    CONNECTION_UPDATE
    MESSAGES_EDITED
    CHATS_UPDATE
    CHATS_UPSERT
    GROUP_UPDATE
    GROUPS_UPSERT
  ].freeze
  def send_message(phone_number, message)
    @message = message
    @phone_number = phone_number

    if message.attachments.present?
      send_attachment_message(phone_number, message)
    elsif message.content.present?
      send_text_message(phone_number, message)
    else
      @message.update!(is_unsupported: true)
      return
    end
  end

  def send_template(phone_number, template_info, message = nil)
    @message = message
    # Evolution API doesn't support template messages in the same way
    Rails.logger.warn "Evolution API doesn't support template messages, sending as text"
    send_text_message(phone_number, message || build_template_text(template_info))
  end

  def sync_templates
    # Evolution API doesn't have template syncing like WhatsApp Cloud
    # Mark as updated to prevent continuous sync attempts
    whatsapp_channel.mark_message_templates_updated
  end

  def received_messages(phone_number, messages)
    @phone_number = phone_number

    # Evolution API uses markMessageAsRead endpoint
    messages.each do |message|
      next unless message.incoming?

      begin
        # Get the remoteJid from contact_inbox source_id
        remote_jid = message.conversation&.contact_inbox&.source_id
        next if remote_jid.blank?

        response = HTTParty.post(
          "#{api_base_path}/chat/markMessageAsRead/#{instance_name}",
          headers: api_headers,
          body: {
            readMessages: [{
              id: message.source_id,
              fromMe: false,
              remoteJid: remote_jid
            }]
          }.to_json,
          timeout: 10
        )

        unless response.success?
          Rails.logger.warn "Evolution API: Failed to mark message as read: #{response.code} - #{response.body}"
        end
      rescue StandardError => e
        Rails.logger.error "Evolution API: Error marking message as read: #{e.message}"
      end
    end

    true
  end

  def validate_provider_config?
    # Check if required configuration is present
    if api_base_path.blank?
      error_message = if whatsapp_channel.provider_config['api_url'].blank? && DEFAULT_URL.blank?
                        'API URL is required. Either provide it in the form or set EVOLUTION_PROVIDER_DEFAULT_URL environment variable'
                      else
                        'API URL is invalid'
                      end
      Rails.logger.warn "Evolution API validation failed: #{error_message}"
      return false
    end

    if admin_token.blank?
      error_message = if whatsapp_channel.provider_config['admin_token'].blank? && DEFAULT_API_KEY.blank?
                        'Admin Token is required. Either provide it in the form or set EVOLUTION_PROVIDER_DEFAULT_API_KEY environment variable'
                      else
                        'Admin Token is invalid'
                      end
      Rails.logger.warn "Evolution API validation failed: #{error_message}"
      return false
    end

    return false if instance_name.blank?

    # Test connection to Evolution API root endpoint
    response = HTTParty.get(
      api_base_path,
      headers: api_headers,
      timeout: 10
    )

    response.success? && response.parsed_response['status'] == 200
  rescue StandardError => e
    Rails.logger.error "Evolution API validation error: #{e.message}"
    false
  end

  def api_headers
    {
      'apikey' => admin_token,
      'Content-Type' => 'application/json'
    }
  end

  def media_url(media_id)
    # Evolution API media endpoint
    "#{api_base_path}/media/#{media_id}"
  end

  def subscribe_to_webhooks
    # Evolution API webhook subscription if needed
  end

  def unsubscribe_from_webhooks
    # Evolution API webhook unsubscription if needed
  end

  private

  def api_base_path
    (whatsapp_channel.provider_config['api_url'].presence || DEFAULT_URL).to_s.chomp('/')
  end

  def admin_token
    whatsapp_channel.provider_config['admin_token'].presence || DEFAULT_API_KEY
  end

  def instance_name
    whatsapp_channel.provider_config['instance_name'].presence || "chatwoot_#{whatsapp_channel.phone_number}"
  end

  def send_delay_ms
    # Evolution expects a delay value it uses internally for processing.
    # Keep this local to the channel config, defaulting to 2000ms; no ENV fallback.
    val = begin
      whatsapp_channel.provider_config['send_delay_ms']
    rescue StandardError
      nil
    end
    (val.present? ? val.to_i : 2000)
  end

  # Selects the webhook events for this channel. If provider_config['webhook_events']
  # is present, uses that; otherwise falls back to the default WEBHOOK_EVENTS.
  def effective_webhook_events
    config_events = begin
      whatsapp_channel.provider_config['webhook_events']
    rescue StandardError
      nil
    end

    raw = config_events.present? ? Array(config_events) : WEBHOOK_EVENTS
    normalized = raw.map { |e| e.to_s.strip.upcase }
    filtered = normalized & WEBHOOK_EVENTS
    filtered.presence || WEBHOOK_EVENTS
  end

  def evolution_reply_context(message)
    reply_to = message.content_attributes[:in_reply_to_external_id]
    return {} if reply_to.blank?

    {
      contextInfo: {
        stanzaId: reply_to,
        quotedMessage: { key: { id: reply_to } }
      },
      context: { id: reply_to },
      quoted: { key: { id: reply_to } },
      quotedMsgId: reply_to
    }
  end

  def send_text_message(phone_number, message)
    body_data = recipient_payload(phone_number).merge(
      delay: send_delay_ms,
      text: message.respond_to?(:outgoing_content) ? message.outgoing_content : message.to_s
    ).merge(evolution_reply_context(message))

    d = send_delay_ms
    body_data[:delay] = d if d.positive?

    response = HTTParty.post(
      "#{api_base_path}/message/sendText/#{instance_name}",
      headers: api_headers,
      body: body_data.to_json
    )

    process_response(response, message)
  end

  def send_attachment_message(phone_number, message)
    attachment = message.attachments.first
    return unless attachment

    case attachment.file_type
    when 'image', 'video', 'file'
      send_media_message(phone_number, message, 'sendMedia')
    when 'audio'
      send_audio_message(phone_number, message)
    else
      # Fallback to text message
      send_text_message(phone_number, message)
    end
  end

  def send_media_message(phone_number, message, endpoint)
    attachment = message.attachments.first

    # Use direct S3 URL for media
    media_url = generate_direct_s3_url(attachment)

    mediatype = attachment.file_type
    mediatype = 'document' if mediatype == 'file'

    body_data = recipient_payload(phone_number).merge(
      mediatype: mediatype,
      delay: send_delay_ms,
      media: media_url,
      caption: message.outgoing_content.to_s,
      fileName: attachment.file.filename.to_s
    ).merge(evolution_reply_context(message))

    d = send_delay_ms
    body_data[:delay] = d if d.positive?

    response = HTTParty.post(
      "#{api_base_path}/message/#{endpoint}/#{instance_name}",
      headers: api_headers,
      body: body_data.to_json
    )

    process_response(response, message)
  end

  def send_audio_message(phone_number, message)
    attachment = message.attachments.first

    # Try direct public URL first (for public S3 buckets)
    result = send_audio_with_direct_url(phone_number, attachment, message)

    # If direct URL fails, try base64
    if !result && attachment.file.attached?
      Rails.logger.warn '[Evolution Audio] Direct URL failed, trying base64'
      result = send_audio_with_base64(phone_number, attachment, message)
    end

    result
  end

  def send_audio_with_direct_url(phone_number, attachment, message)
    # Generate direct public URL for S3 bucket
    audio_url = generate_direct_s3_url(attachment)

    body_data = recipient_payload(phone_number).merge(
      delay: send_delay_ms,
      audio: audio_url
    ).merge(evolution_reply_context(message))

    response = HTTParty.post(
      "#{api_base_path}/message/sendWhatsAppAudio/#{instance_name}",
      headers: api_headers,
      body: body_data.to_json,
      timeout: 60
    )

    process_response(response, message)
  end

  def generate_direct_s3_url(attachment)
    return attachment.file_url unless attachment.file.attached?

    # Extract S3 details from existing signed URL
    signed_url = attachment.download_url
    return signed_url unless ENV['EVOLUTION_PUBLIC_S3'] == 'true'

    # Try to extract bucket and key from the signed URL (flexible regex for different S3 providers)
    if signed_url =~ %r{https://([^/]+)/([^?]+)}
      host = ::Regexp.last_match(1)
      key = ::Regexp.last_match(2)

      # Create direct public URL - just remove query parameters
      direct_url = "https://#{host}/#{key}"
      return direct_url
    end

    # Fallback to original URL if can't parse
    Rails.logger.warn "[Evolution S3] Could not parse S3 URL, using original: #{signed_url}"
    signed_url
  end

  def send_audio_with_base64(phone_number, attachment, message)
    # Convert to base64 - Evolution API expects just the base64 string
    buffer = Base64.strict_encode64(attachment.file.download)

    body_data = recipient_payload(phone_number).merge(
      delay: send_delay_ms,
      audio: buffer # Just the base64 string, no data URI prefix
    ).merge(evolution_reply_context(message))
    response = HTTParty.post(
      "#{api_base_path}/message/sendWhatsAppAudio/#{instance_name}",
      headers: api_headers,
      body: body_data.to_json,
      timeout: 60
    )

    process_response(response, message)
  end

  def build_template_text(template_info)
    # Convert template info to plain text for Evolution API
    text = template_info[:name] || 'Template Message'
    if template_info[:parameters].present?
      template_info[:parameters].each_with_index do |param, index|
        text = text.gsub("{{#{index + 1}}}", param)
      end
    end
    text
  end

  def process_response(response, message)
    if response.success?
      parsed_response = response.parsed_response
      # Try multiple locations to extract the message id
      candidates = [
        parsed_response.dig('key', 'id'),
        parsed_response['messageId'],
        parsed_response['id'],
        parsed_response.dig('response', 'key', 'id'),
        parsed_response.dig('response', 'messageId'),
        parsed_response.dig('data', 'key', 'id'),
        parsed_response.dig('data', 'id'),
        parsed_response['keyId']
      ].compact

      # If sending to a group, optimistically mark as delivered on success
      mark_group_delivered_on_send

      return candidates.first if candidates.any?

      return nil
    end

    handle_error(response, message)
    false
  end

  def handle_error(response, message)
    if response.code.to_i == 403
      Rails.logger.warn "Evolution API returned 403 for message #{message&.id}: #{error_message(response)}"
      return
    end

    super(response, message)
  end

  def error_message(response)
    parsed = response.parsed_response
    return parsed.dig('response', 'message') if parsed.is_a?(Hash) && parsed.dig('response', 'message').present?
    return parsed['message'] if parsed.is_a?(Hash) && parsed['message'].present?
    return parsed['error'] if parsed.is_a?(Hash) && parsed['error'].present?

    response.body
  end

  def setup_channel_provider
    whatsapp_channel.provider_config['api_url'] ||= DEFAULT_URL
    whatsapp_channel.provider_config['admin_token'] ||= DEFAULT_API_KEY
    whatsapp_channel.save! if whatsapp_channel.changed?

    if api_base_path.blank? || admin_token.blank? || instance_name.blank?
      error_message = 'Evolution API setup aborted: missing api_url, admin_token, or instance_name'
      Rails.logger.error error_message
      return { ok: false, error: error_message }
    end

    # Use timeout para evitar travamentos
    remove_existing_instance

    response = HTTParty.post(
      "#{api_base_path}/instance/create",
      headers: api_headers,
      body: instance_payload.to_json,
      timeout: 30  # Timeout de 30 segundos
    )

    if response.success?
      parsed_response = response.parsed_response
      
      # Initialize provider_connection with proper structure
      connection_data = {
        connection: 'connecting',
        qr_data_url: nil,
        error: nil
      }
      
      # Try to fetch QR code de forma assíncrona
      begin
        qr_response = HTTParty.get(
          "#{api_base_path}/instance/connect/#{instance_name}",
          headers: api_headers,
          timeout: 15  # Reduzido para 15 segundos
        )
        
        if qr_response.success? && qr_response.parsed_response.is_a?(Hash)
          qr_data = qr_response.parsed_response
          base64_qr = qr_data['base64'] || qr_data['qrcode'] || qr_data['code']
          
          if base64_qr.present?
            # Check if base64 already has data URI prefix
            if base64_qr.start_with?('data:image')
              connection_data[:qr_data_url] = base64_qr
            else
              connection_data[:qr_data_url] = "data:image/png;base64,#{base64_qr}"
            end
          end
        end
      rescue StandardError => e
        Rails.logger.warn "Evolution API QR fetch warning: #{e.message}"
        # Não falha o setup se QR code não carregar
      end
      
      whatsapp_channel.update_provider_connection!(connection_data)
      
      # Configure webhook de forma assíncrona em background job
      # para não bloquear o setup
      Whatsapp::ConfigureEvolutionWebhookJob.perform_later(whatsapp_channel.id) rescue nil

      { ok: true, response: parsed_response }
    else
      error_message = error_message(response)
      Rails.logger.error "Evolution API setup error: #{response.code} - #{error_message}"
      { ok: false, error: error_message.presence || "HTTP #{response.code}" }
    end
  rescue StandardError => e
    Rails.logger.error "Evolution API setup exception: #{e.message}"
    { ok: false, error: e.message }
  end

  # Build recipient payload supporting 1:1 numbers and group remoteJids (…@g.us)
  def recipient_payload(recipient)
    str = recipient.to_s
    if str.include?('@') && str.end_with?('@g.us')
      # Be maximally compatible with Evolution variants: include both fields
      { number: str, remoteJid: str, groupJid: str }
    else
      { number: str.delete('+') }
    end
  end

  def mark_group_delivered_on_send
    return unless @message&.outgoing?

    begin
      ci_sid = @message.conversation&.contact_inbox&.source_id.to_s
      return unless ci_sid.end_with?('@g.us')
      # Update only if still at 'sent' to avoid conflicts with webhook updates
      return unless @message.status == 'sent'

      @message.update!(status: :delivered)
    rescue StandardError => e
      Rails.logger.warn "Evolution API: Could not mark group message delivered: #{e.message}"
    end
  end

  def remove_existing_instance
    instances = HTTParty.get(
      "#{api_base_path}/instance/fetchInstances",
      headers: api_headers,
      timeout: 15  # Timeout para evitar travamentos
    )
    return unless instances.success?

    names = Array(instances.parsed_response).map { |inst| inst['instanceName'] || inst['name'] }
    return unless names.include?(instance_name)

    HTTParty.delete(
      "#{api_base_path}/instance/delete/#{instance_name}",
      headers: api_headers,
      timeout: 15  # Timeout para deleção
    )
  rescue StandardError => e
    Rails.logger.warn "Evolution API: Failed to remove existing instance: #{e.message}"
    # Não falha o setup se não conseguir remover instância antiga
  end

  def instance_payload
    {
      instanceName: instance_name,
      number: whatsapp_channel.phone_number.delete('+'),
      integration: 'WHATSAPP-BAILEYS',
      qrcode: true,
      webhook: {
        enabled: true,
        url: "#{ENV.fetch('FRONTEND_URL', nil)}/webhooks/whatsapp/#{whatsapp_channel.phone_number}",
        byEvents: false,
        base64: true,
        events: effective_webhook_events
      }
    }
  end

  def configure_webhook(events_override: nil)
    return unless whatsapp_channel.inbox

    webhook_url = "#{ENV.fetch('FRONTEND_URL', nil)}/webhooks/whatsapp/#{whatsapp_channel.phone_number}"
    chosen_events = if events_override.present?
                      Array(events_override).map { |e| e.to_s.strip.upcase } & WEBHOOK_EVENTS
                    else
                      effective_webhook_events
                    end
    body = {
      webhook: {
        enabled: true,
        url: webhook_url,
        byEvents: false,
        base64: true,
        events: chosen_events
      }
    }

    response = HTTParty.post("#{api_base_path}/webhook/set/#{instance_name}", headers: api_headers, body: body.to_json)

    unless response.success?
      Rails.logger.error "Evolution API webhook error: #{response.code} - #{response.body}"
      return { ok: false, error: response.body }
    end

    # Verify by reading settings from the instance
    verify = HTTParty.get("#{api_base_path}/settings/find/#{instance_name}", headers: api_headers)
    if verify.success?
      parsed = verify.parsed_response
      v_events = begin
        parsed.dig('webhook', 'events') ||
          parsed.dig('settings', 'webhook', 'events') ||
          parsed['events'] || []
      rescue StandardError
        []
      end
      Rails.logger.info "Evolution API webhook verified. Events: #{v_events}"
      { ok: true, events: v_events }
    else
      Rails.logger.warn "Evolution API webhook verify failed: #{verify.code} - #{verify.body}"
      { ok: true }
    end
  rescue StandardError => e
    Rails.logger.error "Evolution API webhook exception: #{e.message}"
    { ok: false, error: e.message }
  end

  def disconnect_channel_provider
    return if api_base_path.blank? || admin_token.blank? || instance_name.blank?

    delete_url = "#{api_base_path}/instance/delete/#{instance_name}"

    response = HTTParty.delete(delete_url, headers: api_headers, timeout: 15)

    if response.success?
      # Instance deleted successfully
    else
      Rails.logger.error "Evolution API delete error: #{response.code} - #{response.body}"
    end
  rescue StandardError => e
    Rails.logger.error "Evolution API delete exception: #{e.message}"
  end

  def delete_message(message)
    # Build correct remote JID for 1:1 or group
    ci_source = message.conversation.contact_inbox.source_id.to_s
    remote_jid = if ci_source.include?('@')
                   ci_source
                 else
                   "#{ci_source}@s.whatsapp.net"
                 end

    # Participant JID only for groups
    channel_msisdn = whatsapp_channel.phone_number.to_s.delete('+')
    participant_jid = "#{channel_msisdn}@s.whatsapp.net"

    body_data = {
      id: message.source_id,
      remoteJid: remote_jid,
      fromMe: true
    }
    body_data[:participant] = participant_jid if remote_jid.end_with?('@g.us')

    response = HTTParty.delete(
      "#{api_base_path}/chat/deleteMessageForEveryone/#{instance_name}",
      headers: api_headers,
      body: body_data.to_json
    )

    if response.success?
      # message deleted
    else
      Rails.logger.error "Evolution API delete error: #{response.code} - #{response.body}"
    end
  rescue StandardError => e
    Rails.logger.error "Evolution API delete exception: #{e.message}"
  end

  def update_message(message, new_text)
    return if new_text.blank?

    # Build correct remote JID for 1:1 or group
    ci_source = message.conversation.contact_inbox.source_id.to_s
    remote_jid = if ci_source.include?('@')
                   ci_source
                 else
                   "#{ci_source}@s.whatsapp.net"
                 end

    # Participant JID only for groups
    channel_msisdn = whatsapp_channel.phone_number.to_s.delete('+')
    participant_jid = "#{channel_msisdn}@s.whatsapp.net"

    # Evolution expects number for 1:1 and supports remoteJid for groups
    recipient = ci_source
    body_data = recipient_payload(recipient).merge(
      key: {
        remoteJid: remote_jid,
        fromMe: true,
        id: message.source_id
      },
      text: new_text
    )

    # Some Evolution variants require participant for group edits
    body_data[:participant] = participant_jid if remote_jid.end_with?('@g.us')

    response = HTTParty.post(
      "#{api_base_path}/chat/updateMessage/#{instance_name}",
      headers: api_headers,
      body: body_data.to_json
    )

    Rails.logger.error "Evolution API update error: #{response.code} - #{response.body}" unless response.success?
  rescue StandardError => e
    Rails.logger.error "Evolution API update exception: #{e.message}"
  end

  public :setup_channel_provider, :configure_webhook, :disconnect_channel_provider, :delete_message, :update_message
end
