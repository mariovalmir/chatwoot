module Whatsapp::EvolutionHandlers::Helpers
  include Whatsapp::IncomingMessageServiceHelpers
  include Whatsapp::Waha::HelperMethods

  private

  def remote_jid
    return unless @raw_message.is_a?(Hash)

    @raw_message[:remoteJid] || @raw_message.dig(:key, :remoteJid)
  end

  def remote_jid_alt
    return unless @raw_message.is_a?(Hash)

    @raw_message[:remoteJidAlt] || @raw_message.dig(:key, :remoteJidAlt)
  end

  def participant_jid
    return unless @raw_message.is_a?(Hash)

    @raw_message[:participant] || @raw_message.dig(:key, :participant)
  end

  def participant_alt_jid
    return unless @raw_message.is_a?(Hash)

    @raw_message[:participantAlt] || @raw_message.dig(:key, :participantAlt)
  end

  def resolved_direct_remote_jid(primary_jid = remote_jid, fallback_candidates: nil)
    candidates = Array.wrap(fallback_candidates).compact
    if @raw_message.is_a?(Hash)
      candidates += [
        remote_jid_alt,
        participant_jid,
        participant_alt_jid,
        @raw_message[:chatId],
        @raw_message[:from],
        @raw_message[:to],
        @raw_message[:jid]
      ].compact
    end

    normalized_primary = primary_jid.to_s
    return normalized_primary if normalized_primary.present? &&
                                 !lid_jid?(normalized_primary) &&
                                 !normalized_primary.ends_with?('@g.us')

    candidates.each do |candidate|
      normalized_candidate = normalize_non_lid_jid_candidate(candidate)
      return normalized_candidate if normalized_candidate.present?
    end

    return normalized_primary if normalized_primary.blank?

    mapped = fetch_lid_msisdn(normalized_primary)
    if mapped.blank? && waha_provider?
      mapped = lookup_lid_msisdn_from_provider(normalized_primary)
      store_lid_msisdn_mapping(normalized_primary, mapped) if mapped.present?
    end

    normalize_non_lid_jid_candidate(mapped) || normalized_primary
  end

  def normalize_non_lid_jid_candidate(value)
    return if value.blank?

    str = value.to_s.strip
    return if str.blank?

    if lid_jid?(str)
      mapped = fetch_lid_msisdn(str)
      if mapped.blank? && waha_provider?
        mapped = lookup_lid_msisdn_from_provider(str)
      end
      str = mapped.to_s if mapped.present?
    end

    if str.include?('@')
      return nil if lid_jid?(str)

      return str
    end

    digits = normalize_whatsapp_number(str)
    return if digits.blank?

    "#{digits}@s.whatsapp.net"
  end

  def lid_jid?(jid)
    jid.to_s.ends_with?('@lid')
  end

  def normalize_whatsapp_number(value)
    return if value.blank?

    node = value.to_s
    node = node.split('@').first if node.include?('@')
    node = node.sub(/\A(?:true|false)_/i, '') if node.include?('_')
    node = node.split(':').first if node.include?(':')
    node = node.split('_').first if node.include?('_')

    digits = node.gsub(/\D/, '')
    digits.presence
  end

  def lid_msisdn_cache_key(lid_jid)
    "wa:evo:lid_msisdn:#{inbox.id}:#{lid_jid}"
  end

  def store_lid_msisdn_mapping(lid_jid, phone_value)
    return if lid_jid.blank? || phone_value.blank?
    return unless lid_jid?(lid_jid)

    normalized_phone = normalize_whatsapp_number(phone_value)
    return if normalized_phone.blank?

    Rails.cache.write(lid_msisdn_cache_key(lid_jid), normalized_phone, expires_in: 30.days)
  end

  def fetch_lid_msisdn(lid_jid)
    return if lid_jid.blank?

    Rails.cache.read(lid_msisdn_cache_key(lid_jid))
  end

  def candidate_source_ids_for_jid(jid)
    return [] if jid.blank?

    jid_str = jid.to_s
    return [jid_str] if jid_str.ends_with?('@g.us')

    candidates = [jid_str]
    resolved = resolve_phone_from_any_jid(jid)
    candidates << resolved if resolved.present?

    candidates.compact.uniq
  end

  def contact_source_id_for_jid(jid)
    return if jid.blank?
    return jid if jid.to_s.ends_with?('@g.us')

    resolve_phone_from_any_jid(jid)
  end

  def resolve_phone_from_any_jid(jid)
    return if jid.blank?

    jid_str = jid.to_s
    server = jid_str.split('@')[1]

    if server == 'lid'
      mapped = fetch_lid_msisdn(jid_str)
      return mapped if mapped.present?

      if waha_provider?
        mapped = lookup_lid_msisdn_from_provider(jid_str)
        if mapped.present?
          store_lid_msisdn_mapping(jid_str, mapped)
          return mapped
        end
      end

      alt_candidates = [remote_jid_alt, remote_jid, participant_jid, participant_alt_jid].compact

      alt_candidates.each do |alt|
        next if alt.to_s == jid_str
        next if alt.to_s.ends_with?('@g.us')

        if lid_jid?(alt)
          mapped = fetch_lid_msisdn(alt)
          if mapped.blank? && waha_provider?
            mapped = lookup_lid_msisdn_from_provider(alt)
          end
          next if mapped.blank?

          store_lid_msisdn_mapping(jid_str, mapped)
          return mapped
        end

        msisdn = normalize_whatsapp_number(alt)
        next if msisdn.blank?

        store_lid_msisdn_mapping(jid_str, msisdn)
        return msisdn
      end
    end

    normalized = normalize_whatsapp_number(jid_str)
    return normalized if normalized.present?

    return jid_str if server == 'lid'

    normalized
  end

  def raw_message_id
    # Evolution events sometimes include various id fields. Prefer key id when
    # available, then messageId, and finally id.
    @raw_message[:keyId] || @raw_message[:messageId] || @raw_message.dig(:key, :id) || @raw_message[:id]
  end

  def incoming?
    return @incoming unless @incoming.nil?

    @incoming = if @raw_message[:fromMe].present?
                  !@raw_message[:fromMe]
                elsif @raw_message.dig(:key, :fromMe).present?
                  !@raw_message[:key][:fromMe]
                else
                  # Se não conseguir determinar, assumir que é incoming para evitar erro
                  true
                end
  end

  def message_type
    msg = @raw_message[:message]
    return 'text' unless msg

    if msg[:conversation] || msg.dig(:extendedTextMessage, :text).present?
      'text'
    elsif msg[:imageMessage]
      'image'
    elsif msg[:audioMessage]
      'audio'
    elsif msg[:videoMessage]
      'video'
    elsif msg[:documentMessage] || msg[:documentWithCaptionMessage]
      'file'
    elsif msg[:stickerMessage]
      'sticker'
    elsif msg[:reactionMessage]
      'reaction'
    elsif msg[:locationMessage] || msg[:liveLocationMessage]
      'location'
    elsif msg[:contactMessage] || msg[:contactsArrayMessage]
      'contacts'
    elsif msg[:protocolMessage]
      'protocol'
    else
      'unsupported'
    end
  end

  def message_content
    case message_type
    when 'text'
      @raw_message.dig(:message, :conversation) || @raw_message.dig(:message, :extendedTextMessage, :text)
    when 'image'
      @raw_message.dig(:message, :imageMessage, :caption)
    when 'video'
      @raw_message.dig(:message, :videoMessage, :caption)
    when 'file'
      @raw_message.dig(:message, :documentMessage, :caption) ||
        @raw_message.dig(:message, :documentWithCaptionMessage, :message, :documentMessage, :caption)
    when 'reaction'
      @raw_message.dig(:message, :reactionMessage, :text)
    when 'location'
      # No textual content for location messages. The LocationBubble will be
      # rendered based on the attachment.
      nil
    when 'contacts'
      # Extract contact name for display and include phone numbers
      contact_msg = @raw_message.dig(:message, :contactMessage) ||
                    @raw_message.dig(:message, :contactsArrayMessage, :contacts)&.first
      return 'Contact' if contact_msg.blank?

      name = contact_msg[:displayName] || contact_msg[:vcard]&.match(/FN:(.+)/i)&.[](1) || 'Contact'
      numbers = contact_phones(contact_msg)&.map { |p| p[:phone] } || []

      numbers.present? ? "#{name}\n#{numbers.join("\n")}" : name
    end
  end

  def formatted_group_display_name(subject, remote_jid)
    base = subject.to_s.strip
    base = base.sub(/\s*\(group\)\s*\z/i, '').strip if base.match?(/\(group\)\s*\z/i)
    base = base.presence || group_identifier(remote_jid)
    base = group_identifier(remote_jid) if base.blank?
    return group_identifier(remote_jid) if base.blank?

    base.ends_with?('(GROUP)') ? base : "#{base} (GROUP)"
  end

  def group_identifier(remote_jid)
    remote = remote_jid.to_s
    local_part = remote.split('@').first.to_s
    identifier = local_part.presence || remote
    identifier.presence || 'WhatsApp Group'
  end

  def names_equivalent?(current_name, desired_name)
    current = current_name.to_s.strip
    desired = desired_name.to_s.strip
    return false if current.blank? || desired.blank?

    current.casecmp(desired).zero?
  end

  def file_content_type
    return :image if message_type.in?(%w[image sticker])
    return :video if message_type == 'video'
    return :audio if message_type == 'audio'
    return :location if message_type == 'location'
    return :contact if message_type == 'contacts'

    :file
  end

  def message_mimetype
    case message_type
    when 'image'
      @raw_message.dig(:message, :imageMessage, :mimetype)
    when 'sticker'
      @raw_message.dig(:message, :stickerMessage, :mimetype)
    when 'video'
      @raw_message.dig(:message, :videoMessage, :mimetype)
    when 'audio'
      @raw_message.dig(:message, :audioMessage, :mimetype)
    when 'file'
      @raw_message.dig(:message, :documentMessage, :mimetype) ||
        @raw_message.dig(:message, :documentWithCaptionMessage, :message, :documentMessage, :mimetype)
    end
  end

  def phone_number_from_jid
    resolve_phone_from_any_jid(remote_jid)
  end

  def contact_name
    # Evolution API provides pushName
    name = @raw_message[:pushName].presence
    return name if incoming?

    phone_number_from_jid
  end

  def evolution_extract_message_timestamp(timestamp)
    # Evolution API timestamp is usually in seconds or milliseconds
    timestamp = timestamp.to_i

    # If timestamp looks like it's in milliseconds (> 10^12), convert to seconds
    timestamp /= 1000 if timestamp > 10**12

    # Return Unix timestamp as integer (like baileys_extract_message_timestamp)
    timestamp
  rescue StandardError
    Time.current.to_i
  end

  def filename
    filename = @raw_message.dig(:message, :documentMessage, :fileName) ||
               @raw_message.dig(:message, :documentWithCaptionMessage, :message, :documentMessage, :fileName)
    return filename if filename.present?

    ext = ".#{message_mimetype.split(';').first.split('/').last}" if message_mimetype.present?
    "#{file_content_type}_#{raw_message_id}_#{Time.current.strftime('%Y%m%d')}#{ext}"
  end

  def ignore_message?
    # Skip unsupported message types
    return true if message_type.in?(%w[protocol unsupported])

    # Skip if no content available
    return true if message_content.blank? && !has_media_attachment?

    false
  end

  def has_media_attachment?
    %w[image video audio file sticker location].include?(message_type)
  end

  def self_message?
    # Para messages.update, fromMe pode vir diretamente no root
    @raw_message[:fromMe] || @raw_message.dig(:key, :fromMe) || false
  end

  def jid_type
    # Para messages.update, remoteJid pode vir diretamente no root
    jid = remote_jid.presence || resolved_direct_remote_jid
    return 'unknown' unless jid

    server = jid.split('@').last

    # Based on Evolution API JID patterns
    case server
    when 's.whatsapp.net', 'c.us'
      'user'
    when 'g.us'
      'group'
    when 'lid'
      normalized = resolved_direct_remote_jid
      return 'user' if normalized.present? && !lid_jid?(normalized) && !normalized.ends_with?('@g.us')

      'user'
    when 'broadcast'
      jid.start_with?('status@') ? 'status' : 'broadcast'
    when 'newsletter'
      'newsletter'
    when 'call'
      'call'
    else
      'unknown'
    end
  end

  def group_message?
    jid_type == 'group'
  end

  def participant_phone
    jid = participant_jid
    alt = participant_alt_jid

    primary = resolve_phone_from_any_jid(jid)
    return primary if primary.present?

    fallback = resolve_phone_from_any_jid(alt)
    return fallback if fallback.present?

    normalize_whatsapp_number(alt)
  end

  def formatted_participant_phone
    msisdn = participant_phone
    return '' if msisdn.blank?

    raw = "+#{msisdn.to_s.sub(/^\+/, '')}"
    begin
      TelephoneNumber.parse(raw).international_number
    rescue StandardError
      raw
    end
  end

  def group_prefix_line
    phone = formatted_participant_phone
    name = safe_participant_name

    # If phone is blank and name looks like a JID, convert it to a phone
    if phone.blank? && name.include?('@')
      only_digits = name.split('@').first.to_s.gsub(/\D/, '')
      phone = "+#{only_digits}" if only_digits.present?
      name = '' if phone.present?
    end

    # Prefer showing phone; include name only if it's present and not identical
    prefix_core = if phone.present? && name.present? && name != phone
                    "#{phone} - #{name}"
                  elsif phone.present?
                    phone
                  elsif name.present?
                    name
                  else
                    ''
                  end

    return '' if prefix_core.blank?

    "**#{prefix_core}:**\n"
  end

  def jid_variants(jid)
    value = jid.to_s.strip
    return [] if value.blank?

    variants = [value]

    if value.include?('@')
      node, domain = value.split('@', 2)
      variants << "#{node}@s.whatsapp.net" if domain != 's.whatsapp.net'
      variants << "#{node}@c.us" if domain != 'c.us'
      variants << node if node.present?
    else
      numeric = value.gsub(/\D/, '')
      variants << "#{numeric}@s.whatsapp.net" if numeric.present?
      variants << "#{numeric}@c.us" if numeric.present?
    end

    variants.compact.map { |variant| variant.to_s.strip }.reject(&:blank?).uniq
  end

  def map_provider_status(payload)
    data = ensure_indifferent_hash(payload) || {}

    status_value = data[:status]
    status_value = data[:ackName] if status_value.blank?
    status_value = data[:ack] if status_value.blank?
    status_value = data[:ackStatus] if status_value.blank?

    if status_value.blank? && data[:message].is_a?(Hash)
      message_hash = ensure_indifferent_hash(data[:message])
      status_value = message_hash[:status]
      status_value = message_hash[:ackName] if status_value.blank?
      status_value = message_hash[:ack] if status_value.blank?
    end

    return if status_value.blank?

    normalized = normalize_status_value(status_value)

    if normalized.is_a?(Integer)
      return 'sent' if [0, 1].include?(normalized)
      return 'delivered' if normalized == 2
      return 'read' if [3, 4, 5].include?(normalized)
      return nil
    end

    case normalized
    when 'PENDING', 'PENDING_ACK', 'QUEUED', 'SERVER_ACK', 'SENT', 'ACK', 'ACCEPTED'
      'sent'
    when 'DELIVERY_ACK', 'DELIVERED', 'DELIVERY'
      'delivered'
    when 'READ', 'READ_ACK', 'SEEN', 'VIEWED', 'PLAYED'
      'read'
    when 'ERROR', 'FAILED', 'FAIL'
      'failed'
    else
      nil
    end
  end

  def normalize_status_value(value)
    return value if value.is_a?(Integer)

    str = value.to_s.strip
    return str.to_i if str.match?(/^\d+$/)

    str.upcase
  end

  def evolution_lookup_ids(raw_payload, primary_id = nil)
    payload = ensure_indifferent_hash(raw_payload)
    return [] unless payload.is_a?(Hash)

    ids = []
    ids.concat(Array.wrap(payload[:editLookupIds]))
    ids.concat(Array.wrap(payload[:edit_lookup_ids]))
    ids.concat(Array.wrap(payload[:waha_message_ids]))
    ids << payload[:waha_message_id]
    ids << payload[:id]
    ids << payload[:messageId]
    ids << payload[:keyId]
    ids << payload.dig(:key, :id)

    data = ensure_indifferent_hash(payload[:_data])
    ids.concat(Array.wrap(data&.[](:MessageIDs)))
    ids.concat(Array.wrap(data&.[](:messageIDs)))
    ids.concat(Array.wrap(data&.[](:messageIds)))

    candidates = ids.compact.map(&:to_s).reject(&:blank?).uniq
    return candidates if primary_id.blank?

    candidates.reject { |candidate| candidate.to_s == primary_id.to_s }
  end

  def ensure_indifferent_hash(value)
    case value
    when Hash
      value.respond_to?(:with_indifferent_access) ? value.with_indifferent_access : value
    when ActionController::Parameters
      value.to_unsafe_h.with_indifferent_access
    else
      nil
    end
  end

  # ---- Group subject cache helpers ----
  def group_subject_cache_key(jid)
    "wa:evo:group_subject:#{inbox.id}:#{jid}"
  end

  def cached_group_subject(jid = nil)
    rjid = (jid || remote_jid).to_s
    return if rjid.blank?

    Rails.cache.read(group_subject_cache_key(rjid))
  end

  def store_group_subject(jid, subject)
    return if jid.blank? || subject.blank?

    Rails.cache.write(group_subject_cache_key(jid.to_s), subject.to_s, expires_in: 30.days)
  end

  def fetch_group_subject_from_api(jid)
    return if jid.blank?

    begin
      require 'cgi'
      api_url = (inbox.channel.provider_config['api_url'].presence || ENV.fetch('EVOLUTION_API_URL', nil)).to_s.chomp('/')
      instance = inbox.channel.provider_config['instance_name'].presence || ENV.fetch('EVOLUTION_INSTANCE_NAME', nil)
      return if api_url.blank? || instance.blank?

      url = "#{api_url}/group/findGroupInfos/#{instance}?groupJid=#{CGI.escape(jid.to_s)}"
      response = HTTParty.get(url, headers: inbox.channel.api_headers, timeout: 10)
      return unless response.success?

      data = response.parsed_response
      data = data.first if data.is_a?(Array)
      subject = nil

      if data.is_a?(Hash)
        subject = data['subject'] || data['name'] || data['title'] ||
                  data.dig('response', 'subject') || data.dig('response', 'name')
      end

      if subject.present?
        store_group_subject(jid, subject)

        # Update existing group contact name if present
        if (ci = inbox.contact_inboxes.find_by(source_id: jid))
          ci.contact.update!(name: subject) if ci.contact.name != subject
        end

        subject
      end
    rescue StandardError => e
      Rails.logger.warn "Evolution API: Failed to fetch group subject for #{jid}: #{e.message}"
      nil
    end
  end

  def ensure_group_subject_cached(jid)
    cached_group_subject(jid) || fetch_group_subject_from_api(jid)
  end

  # Returns a participant name safe for display; avoids raw JIDs
  def safe_participant_name
    candidate = @raw_message[:pushName].to_s.strip
    if candidate.blank? && group_message?
      # Fallback to cached participant name when pushName is absent (common on mobile)
      cached = cached_participant_name(participant_jid)
      candidate = cached.to_s.strip if cached.present?
    end

    # If pushName looks like a JID, drop the domain part and keep only digits
    if candidate.include?('@')
      digits = candidate.split('@').first.to_s.gsub(/\D/, '')
      return "+#{digits}" if digits.present?

      return ''
    end

    candidate
  end

  # ---- Participant name cache helpers ----
  def participant_name_cache_key(jid)
    return nil if jid.blank?

    "wa:evo:participant_name:#{inbox.id}:#{jid}"
  end

  def cached_participant_name(jid)
    key = participant_name_cache_key(jid)
    return if key.blank?

    Rails.cache.read(key)
  end

  def store_participant_name(jid, name)
    key = participant_name_cache_key(jid)
    return if key.blank? || name.blank?

    Rails.cache.write(key, name.to_s, expires_in: 30.days)
  end
end
