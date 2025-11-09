require 'base64'
require 'tempfile'

module Whatsapp::EvolutionHandlers::MessagesUpsert
  include Whatsapp::EvolutionHandlers::Helpers
  include Whatsapp::Waha::UpsertMethods

  private

  def process_messages_upsert
    # Evolution API v2.3.1 sends single message data directly in 'data' field
    message_data = processed_params[:data]
    return if message_data.blank?

    @message = nil
    @contact_inbox = nil
    @contact = nil
    @raw_message = message_data

    if incoming?
      handle_message
    else
      # Handle outgoing messages with lock to avoid race conditions
      with_evolution_channel_lock_on_outgoing_message(inbox.channel.id) { handle_message }
    end
  end

  def handle_message
    return unless %w[user group].include?(jid_type)
    return if ignore_message?

    # If this upsert references an existing outgoing message, treat as status update
    if find_message_by_source_id(raw_message_id)
      store_waha_message_ids!(@message, @raw_message)
      update_status_from_upsert if @raw_message[:status].present?
      return
    end

    # Outgoing upserts may arrive before HTTP response contains an id.
    # Try to bind the update's external id to the latest unsourced outgoing
    # message in the same conversation so subsequent updates track correctly.
    if !incoming? && try_bind_outgoing_message_by_conversation(raw_message_id)
      store_waha_message_ids!(@message, @raw_message)
      update_status_from_upsert if @raw_message[:status].present?
      return
    end
    return if message_under_process?

    cache_message_source_id_in_redis
    cache_lid_identifiers
    set_contact

    unless @contact
      clear_message_source_id_from_redis
      Rails.logger.warn "Evolution API: Contact not found for message: #{raw_message_id}"
      return
    end

    set_conversation
    handle_create_message
    clear_message_source_id_from_redis
  end

  # Attempts to find a conversation by remoteJid and bind the upsert's id
  # to the latest unsourced outgoing message so that subsequent updates work.
  def try_bind_outgoing_message_by_conversation(external_id)
    jid = @raw_message[:remoteJid] || @raw_message.dig(:key, :remoteJid)
    return false if jid.blank?

    target_source_id = contact_source_id_for_jid(jid)

    contact_inbox = inbox.contact_inboxes.find_by(source_id: target_source_id)
    return false unless contact_inbox

    conversation = if inbox.lock_to_single_conversation
                     contact_inbox.conversations.last
                   else
                     contact_inbox.conversations.where.not(status: :resolved).last
                   end
    return false unless conversation

    candidate = conversation.messages.outgoing.order(created_at: :desc).limit(5)
                            .detect { |m| m.source_id.blank? && m.created_at > 10.minutes.ago }
    return false unless candidate

    @message = candidate
    @message.update!(source_id: external_id)
    true
  rescue StandardError => e
    Rails.logger.warn "Evolution API: UPSERT fallback bind failed: #{e.message}"
    false
  end

  def set_contact
    if group_message?
      # Groups are identified by full remoteJid (e.g., 5959...-...@g.us)
      rjid = remote_jid.to_s
      subject = ensure_group_subject_cached(rjid)
      fallback_name = formatted_group_display_name(subject, rjid)

      contact_inbox = ::ContactInboxWithContactBuilder.new(
        source_id: rjid,
        inbox: inbox,
        contact_attributes: {
          name: fallback_name,
          phone_number: nil,
          identifier: rjid,
          avatar_url: @raw_message[:profilePicUrl]
        }
      ).perform

      ensure_group_contact_metadata(contact_inbox, remote_jid: rjid)
    else
      push_name = contact_name
      source_id = contact_source_id_for_jid(remote_jid) || normalize_whatsapp_number(remote_jid)

      contact_inbox = ::ContactInboxWithContactBuilder.new(
        source_id: source_id,
        inbox: inbox,
        contact_attributes: {
          name: push_name,
          phone_number: "+#{source_id}",
          avatar_url: @raw_message[:profilePicUrl]
        }
      ).perform
    end

    @contact_inbox = contact_inbox
    @contact = contact_inbox.contact

    # Update contact name if it was just the phone number
    @contact.update!(name: push_name) if !group_message? && @contact.name == source_id && push_name.present?
    update_contact_avatar_from_message
  end

  def handle_create_message
    create_message(attach_media: has_media_attachment?)
  end

  def create_message(attach_media: false)
    # Build content and add group prefix (participant phone and name) when applicable
    base_content = message_content || ''
    # Avoid prefixing for location/contacts to preserve native bubbles
    should_prefix = group_message? && incoming? && %w[location contacts].exclude?(message_type)
    content = should_prefix ? "#{group_prefix_line}#{base_content}" : base_content

    # For outgoing provider-synced events, tag as bot instead of a human user
    outgoing_bot_sender = if incoming?
                            nil
                          else
                            # Prefer a configured inbox agent bot when available
                            @inbox.agent_bot_inbox&.active? ? @inbox.agent_bot : nil
                          end

    @message = @conversation.messages.build(
      content: content,
      account_id: @inbox.account_id,
      inbox_id: @inbox.id,
      source_id: raw_message_id,
      sender: incoming? ? @contact : outgoing_bot_sender,
      sender_type: incoming? ? 'Contact' : 'AgentBot',
      message_type: incoming? ? :incoming : :outgoing,
      content_attributes: message_content_attributes
    )

    handle_attach_media if attach_media
    handle_location if message_type == 'location'
    handle_contacts if message_type == 'contacts'

    @message.save!

    store_waha_message_ids!(@message, @raw_message)

    apply_auto_labels

    inbox.channel.received_messages([@message], @conversation) if incoming?
  end

  def apply_auto_labels
    labels = []
    labels << 'whatsapp-group' if group_message?

    # Tag based on explicit origin param or Evolution source field
    origin = processed_params[:origin].presence || processed_params.dig(:data, :source).presence
    labels << origin if origin.present?

    return if labels.blank?

    @conversation.add_labels(labels.uniq)
  end

  # --- Status updates from UPSERT (some Evolution setups only send upsert) ---
  def update_status_from_upsert
    return unless @message&.outgoing?

    status = map_provider_status(@raw_message)
    return if status.blank?

    update_last_seen_from_upsert(status)

    return unless upsert_status_transition_allowed?(@message.status, status)

    @message.update!(status: status)
  end

  # Use a uniquely named predicate to avoid collisions with MessagesUpdate
  def upsert_status_transition_allowed?(current_status, new_status)
    case current_status
    when 'sent'
      %w[delivered read failed].include?(new_status)
    when 'delivered'
      %w[read].include?(new_status)
    when 'read', 'failed'
      false
    else
      true
    end
  end

  def update_last_seen_from_upsert(status)
    conversation = @message.conversation
    return unless conversation&.contact

    raw_ts = @raw_message[:timestamp] || processed_params[:date_time]
    timestamp = begin
      raw_ts.present? ? Time.zone.parse(raw_ts.to_s) : Time.current
    rescue StandardError
      Time.current
    end

    if status == 'read'
      conversation.update!(contact_last_seen_at: timestamp)
      ::Conversations::UpdateMessageStatusJob.perform_later(conversation.id, timestamp, :read)
    elsif status == 'delivered'
      ::Conversations::UpdateMessageStatusJob.perform_later(conversation.id, timestamp, :delivered)
    end

    conversation.contact.update!(last_activity_at: timestamp)
  end

  def message_content_attributes
    content_attributes = {
      external_created_at: evolution_extract_message_timestamp(@raw_message[:messageTimestamp])
    }

    quoted_id = extract_reply_to_id(@raw_message)
    content_attributes[:in_reply_to_external_id] = quoted_id if quoted_id.present?

    reply_participants = extract_reply_to_participants(@raw_message)
    if reply_participants.present?
      primary_participant = reply_participants.first
      content_attributes[:in_reply_to_participant] ||= primary_participant
      content_attributes[:in_reply_to_participants] = reply_participants
    end

    message_participant = participant_jid
    content_attributes[:participant_jid] = message_participant if message_participant.present?

    message_participant_alt = participant_alt_jid
    content_attributes[:participant_alt_jid] = message_participant_alt if message_participant_alt.present?

    if message_type == 'reaction'
      content_attributes[:in_reply_to_external_id] = @raw_message.dig(:message, :reactionMessage, :key, :id)
      content_attributes[:is_reaction] = true
    elsif message_type == 'unsupported'
      content_attributes[:is_unsupported] = true
    end

    if (ad_reply = Whatsapp::ExternalAdReplyParser.parse(@raw_message))
      content_attributes[:external_ad_reply] = ad_reply
    end

    content_attributes
  end

  def handle_attach_media
    attachment_file = download_attachment_file

    return unless attachment_file

    # Use the enhanced filename and content_type for better reliability
    final_filename = generate_filename_with_extension
    final_content_type = determine_content_type

    attachment = @message.attachments.build(
      account_id: @message.account_id,
      file_type: file_content_type.to_s,
      file: {
        io: attachment_file,
        filename: final_filename,
        content_type: final_content_type
      }
    )

    # Mark audio as recorded if it's a voice note
    attachment.meta = { is_recorded_audio: true } if message_type == 'audio' && @raw_message.dig(:message, :audioMessage, :ptt)

  rescue Down::Error => e
    @message.update!(is_unsupported: true)
    Rails.logger.error "Evolution API: Failed to download attachment for message #{raw_message_id}: #{e.message}"
  rescue StandardError => e
    Rails.logger.error "Evolution API: Failed to create attachment for message #{raw_message_id}: #{e.message}"
    Rails.logger.error "  - Error class: #{e.class}"
    Rails.logger.error "  - Error details: #{e.inspect}"
  end

  def handle_location
    location_msg = @raw_message.dig(:message, :locationMessage) ||
                   @raw_message.dig(:message, :liveLocationMessage)
    return unless location_msg

    @message.content_attributes[:location] = {
      latitude: location_msg[:degreesLatitude],
      longitude: location_msg[:degreesLongitude],
      name: location_msg[:name],
      address: location_msg[:address]
    }

    location_name = if location_msg[:name].present?
                      "#{location_msg[:name]}, #{location_msg[:address]}"
                    else
                      ''
                    end

    @message.attachments.build(
      account_id: @message.account_id,
      file_type: file_content_type.to_s,
      coordinates_lat: location_msg[:degreesLatitude],
      coordinates_long: location_msg[:degreesLongitude],
      fallback_title: location_name,
      external_url: location_msg[:url]
    )
  end

  def handle_contacts
    contact_msg = @raw_message.dig(:message, :contactMessage)
    contacts_array = @raw_message.dig(:message, :contactsArrayMessage, :contacts)

    contacts = if contact_msg
                 [contact_msg]
               elsif contacts_array
                 contacts_array
               else
                 []
               end

    @message.content_attributes[:contacts] = contacts.map do |contact|
      {
        display_name: contact[:displayName],
        vcard: contact[:vcard]
      }
    end

    contacts.each do |contact|
      phones = contact_phones(contact)
      phones = [{ phone: 'Phone number is not available' }] if phones.blank?

      phones.each do |phone|
        @message.attachments.new(
          account_id: @message.account_id,
          file_type: file_content_type,
          fallback_title: phone[:phone].to_s,
          meta: { display_name: contact[:displayName] || contact[:display_name] }
        )
      end
    end
  end

  def download_attachment_file
    # Evolution/Waha may deliver media on different keys depending on the
    # transport. Normalise everything into indifferent hashes so we can check
    # all variants without worrying about string vs symbol keys.
    message = ensure_indifferent_hash(@raw_message[:message]) || {}
    media = ensure_indifferent_hash(@raw_message[:media]) || {}

    # 1. Base64 encoded payload (preferred when available as it avoids another HTTP hop)
    base64_blob = message[:base64] || media[:base64] || media[:data]
    return create_tempfile_from_base64(base64_blob) if base64_blob.present?

    # 2. Direct media URLs, either at message/media root or inside typed payloads
    attachment_url = message[:mediaUrl].presence || media[:mediaUrl].presence || media[:url].presence
    attachment_url ||= locate_typed_media_url(message)
    attachment_url ||= locate_typed_media_url(media)

    if attachment_url.blank?
      direct_path = locate_typed_direct_path(message) || locate_typed_direct_path(media)
      attachment_url = build_direct_path_url(direct_path) if direct_path.present?
    end

    return Down.download(attachment_url, headers: inbox.channel.api_headers) if attachment_url.present?

    # 3. Provider specific download endpoint (e.g. WAHA requires chat id + message id)
    media_identifier = media[:id].presence || @raw_message[:mediaId].presence || raw_message_id
    if media_identifier.present?
      begin
        channel = inbox.channel
        chat_identifier = remote_jid

        media_api_url = if channel.provider == 'waha'
                          channel.media_url(media_identifier, chat_identifier)
                        else
                          channel.media_url(media_identifier)
                        end

        if media_api_url.present?
          return Down.download(media_api_url, headers: channel.api_headers)
        end
      rescue ArgumentError
        # Some providers (legacy implementations) expose arity=1 media_url. Fall back to single arg.
        media_api_url = inbox.channel.media_url(media_identifier)
        return Down.download(media_api_url, headers: inbox.channel.api_headers) if media_api_url.present?
      end
    end

    Rails.logger.warn 'Evolution API: No media source found for attachment download'
    nil
  rescue StandardError => e
    Rails.logger.error "Evolution API: Failed to download media: #{e.message}"
    nil
  end

  def locate_typed_media_url(container)
    container.dig(:imageMessage, :url) ||
      container.dig(:videoMessage, :url) ||
      container.dig(:audioMessage, :url) ||
      container.dig(:documentMessage, :url) ||
      container.dig(:documentWithCaptionMessage, :message, :documentMessage, :url) ||
      container.dig(:stickerMessage, :url)
  end

  def locate_typed_direct_path(container)
    container.dig(:imageMessage, :directPath) ||
      container.dig(:videoMessage, :directPath) ||
      container.dig(:audioMessage, :directPath) ||
      container.dig(:documentMessage, :directPath) ||
      container.dig(:documentWithCaptionMessage, :message, :documentMessage, :directPath) ||
      container.dig(:stickerMessage, :directPath)
  end

  def build_direct_path_url(direct_path)
    return if direct_path.blank?

    base = ensure_indifferent_hash(@raw_message[:media])&.[](:baseUrl)
    base ||= 'https://mmg.whatsapp.net'
    "#{base}#{direct_path}"
  end

  def create_tempfile_from_base64(base64_data)
    # Evolution API pode enviar base64 com ou sem prefixo
    base64_clean = base64_data.gsub(/^data:.*?;base64,/, '')

    # Decodifica o base64
    decoded_data = Base64.decode64(base64_clean)

    # Determine content type and filename
    content_type = determine_content_type
    file_name = generate_filename_with_extension

    # Cria um arquivo temporário
    tempfile = Tempfile.new([raw_message_id, file_extension])
    tempfile.binmode
    tempfile.write(decoded_data)
    tempfile.rewind

    # Simula um objeto Down::File para compatibilidade
    tempfile.define_singleton_method(:original_filename) do
      file_name
    end

    tempfile.define_singleton_method(:content_type) do
      content_type
    end

    # Adiciona método size para compatibilidade
    tempfile.define_singleton_method(:size) do
      File.size(path)
    end

    tempfile
  rescue StandardError => e
    Rails.logger.error "Evolution API: Failed to create file from base64: #{e.message}"
    Rails.logger.error "  - Base64 size: #{base64_data&.length || 0} chars"
    Rails.logger.error "  - Message type: #{message_type}"
    Rails.logger.error "  - Raw mimetype: #{message_mimetype}"
    nil
  end

  def file_extension
    case message_type
    when 'image'
      case message_mimetype
      when /jpeg/
        '.jpg'
      when /png/
        '.png'
      when /gif/
        '.gif'
      when /webp/
        '.webp'
      else
        '.jpg'
      end
    when 'video'
      case message_mimetype
      when /mp4/
        '.mp4'
      when /webm/
        '.webm'
      when /avi/
        '.avi'
      else
        '.mp4'
      end
    when 'audio'
      case message_mimetype
      when /mp3/
        '.mp3'
      when /wav/
        '.wav'
      when /ogg/
        '.ogg'
      when /aac/
        '.aac'
      when /opus/
        '.opus'
      else
        '.mp3'
      end
    when 'file'
      filename_from_message = @raw_message.dig(:message, :documentMessage, :fileName) ||
                              @raw_message.dig(:message, :documentWithCaptionMessage, :message, :documentMessage, :fileName)
      return File.extname(filename_from_message) if filename_from_message.present?

      case message_mimetype
      when /pdf/
        '.pdf'
      when /doc/
        '.doc'
      when /zip/
        '.zip'
      else
        '.bin'
      end
    when 'sticker'
      '.webp'
    else
      '.bin'
    end
  end

  def determine_content_type
    # Primeiro tenta usar o mimetype da mensagem
    mime = message_mimetype
    return mime if mime.present?

    # Fallback baseado no tipo de mensagem
    case message_type
    when 'image'
      'image/jpeg'
    when 'video'
      'video/mp4'
    when 'audio'
      'audio/mpeg'
    when 'file'
      'application/octet-stream'
    when 'sticker'
      'image/webp'
    else
      'application/octet-stream'
    end
  end

  def generate_filename_with_extension
    # Primeiro tenta usar o filename da mensagem
    existing_filename = filename

    # Se já tem extensão, usa como está
    return existing_filename if existing_filename.present? && File.extname(existing_filename).present?

    # Senão, gera um nome com extensão baseada no tipo
    base_name = existing_filename.presence || "#{message_type}_#{raw_message_id}_#{Time.current.strftime('%Y%m%d')}"
    extension = file_extension

    "#{base_name}#{extension}"
  end
end
