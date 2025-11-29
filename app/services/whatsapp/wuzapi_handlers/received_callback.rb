module Whatsapp::WuzapiHandlers::ReceivedCallback # rubocop:disable Metrics/ModuleLength
  include Whatsapp::WuzapiHandlers::Helpers

  private

  def process_received_callback
    @raw_message = processed_params.dig('event', 'Message')
    @message = nil
    @contact_inbox = nil
    @contact = nil

    return unless @raw_message && should_process_message?
    return if find_message_by_source_id(raw_message_id) || message_under_process?

    cache_message_source_id_in_redis

    set_contact

    unless @contact
      Rails.logger.warn "Wuzapi: Contact not found for message: #{raw_message_id}"
      return
    end

    set_conversation
    handle_create_message
  ensure
    clear_message_source_id_from_redis
  end

  def should_process_message?
    info = processed_params.dig('event', 'Info')
    return false unless info

    # Skip group messages, broadcasts, and status updates
    !info['IsGroup'] &&
      !info['IsBroadcast'] &&
      !info['IsNewsletter']
  end

  def message_type # rubocop:disable Metrics/CyclomaticComplexity,Metrics/PerceivedComplexity
    return 'text' if @raw_message.key?('conversation') || @raw_message.dig('extendedTextMessage', 'text')
    return 'image' if @raw_message.key?('imageMessage')
    return 'audio' if @raw_message.key?('audioMessage')
    return 'video' if @raw_message.key?('videoMessage')
    return 'file' if @raw_message.key?('documentMessage')
    return 'sticker' if @raw_message.key?('stickerMessage')
    return 'contact' if @raw_message.key?('contactMessage')
    return 'location' if @raw_message.key?('locationMessage')
    return 'reaction' if @raw_message.key?('reactionMessage')

    'unsupported'
  end

  def message_content
    case message_type
    when 'text'
      @raw_message['conversation'] || @raw_message.dig('extendedTextMessage', 'text')
    when 'image'
      @raw_message.dig('imageMessage', 'caption')
    when 'video'
      @raw_message.dig('videoMessage', 'caption')
    when 'file'
      @raw_message.dig('documentMessage', 'fileName') || @raw_message.dig('documentMessage', 'title')
    when 'contact'
      @raw_message.dig('contactMessage', 'displayName')
    when 'location'
      @raw_message.dig('locationMessage', 'name') || 'Location'
    when 'reaction'
      @raw_message.dig('reactionMessage', 'text')
    end
  end

  def contact_name
    info = processed_params.dig('event', 'Info')
    info&.dig('PushName') || info&.dig('RemoteJid')&.split('@')&.first
  end

  def set_contact
    info = processed_params.dig('event', 'Info')
    return unless info

    remote_jid = info['RemoteJid']
    phone_number = remote_jid&.split('@')&.first
    push_name = contact_name

    return unless phone_number

    contact_attributes = {
      name: push_name || phone_number,
      phone_number: "+#{phone_number}"
    }

    contact_inbox = ::ContactInboxWithContactBuilder.new(
      source_id: phone_number,
      inbox: inbox,
      contact_attributes: contact_attributes
    ).perform

    @contact_inbox = contact_inbox
    @contact = contact_inbox.contact

    @contact.update!(name: push_name) if push_name.present? && @contact.name == phone_number
    try_update_contact_avatar
  end

  def try_update_contact_avatar
    # Wuzapi doesn't provide avatar URLs in webhooks
    # Could be implemented later using /user/avatar endpoint
  end

  def handle_create_message
    if message_type == 'contact'
      create_contact_message
    else
      create_message(attach_media: %w[image sticker file video audio].include?(message_type))
    end
  end

  def create_contact_message
    contact_data = @raw_message['contactMessage']
    vcard = contact_data['vcard']

    build_message
    attach_contact(vcard)
    @message.save!
    notify_channel_of_received_message
  end

  def create_message(attach_media: false)
    build_message
    handle_attach_media if attach_media
    @message.save!
    notify_channel_of_received_message
  end

  def build_message
    info = processed_params.dig('event', 'Info')

    @message = @conversation.messages.build(
      content: message_content,
      account_id: @inbox.account_id,
      inbox_id: @inbox.id,
      source_id: raw_message_id,
      sender: incoming_message? ? @contact : @inbox.account.account_users.first.user,
      sender_type: incoming_message? ? 'Contact' : 'User',
      message_type: incoming_message? ? :incoming : :outgoing,
      content_attributes: message_content_attributes
    )
  end

  def notify_channel_of_received_message
    inbox.channel.received_messages([@message], @conversation) if incoming_message?
  end

  def message_content_attributes
    type = message_type
    info = processed_params.dig('event', 'Info')
    content_attributes = {}

    if info && info['Timestamp']
      content_attributes[:external_created_at] = info['Timestamp']
    end

    if type == 'reaction'
      reaction_msg = @raw_message['reactionMessage']
      content_attributes[:in_reply_to_external_id] = reaction_msg['key']&.dig('id')
      content_attributes[:is_reaction] = true
    elsif type == 'unsupported'
      content_attributes[:is_unsupported] = true
    end

    # Check for quoted message
    context_info = @raw_message.dig('extendedTextMessage', 'contextInfo')
    if context_info && context_info['stanzaId']
      content_attributes[:in_reply_to_external_id] = context_info['stanzaId']
    end

    content_attributes
  end

  def attach_contact(vcard)
    # Parse vcard to extract name
    name = vcard&.match(/FN:(.+)/)&.captures&.first || 'Contact'

    @message.attachments.new(
      account_id: @message.account_id,
      file_type: :contact,
      fallback_title: name,
      meta: { vcard: vcard }.compact_blank
    )
  end

  def handle_attach_media
    # Check if media is provided via S3 or base64
    s3_data = processed_params['s3']
    base64_data = processed_params['base64']

    if s3_data && s3_data['url']
      attach_media_from_url(s3_data['url'], s3_data)
    elsif base64_data
      attach_media_from_base64(base64_data)
    else
      # Try to download from Wuzapi if we have the media keys
      attach_media_from_wuzapi
    end
  rescue StandardError => e
    @message.update!(is_unsupported: true)
    Rails.logger.error "Wuzapi: Failed to attach media for message #{raw_message_id}: #{e.message}"
  end

  def attach_media_from_url(url, metadata)
    attachment_file = Down.download(url)

    @message.attachments.build(
      account_id: @message.account_id,
      file_type: file_content_type.to_s,
      file: {
        io: attachment_file,
        filename: metadata['fileName'] || filename,
        content_type: metadata['mimeType'] || 'application/octet-stream'
      }
    )
  rescue Down::Error => e
    Rails.logger.error "Wuzapi: Failed to download from S3: #{e.message}"
    @message.update!(is_unsupported: true)
  end

  def attach_media_from_base64(base64_str)
    # base64_str format: "data:mime/type;base64,encoded_data"
    match = base64_str.match(%r{^data:(.+);base64,(.+)$})
    return unless match

    mime_type = match[1]
    encoded = match[2]
    decoded = Base64.decode64(encoded)

    @message.attachments.build(
      account_id: @message.account_id,
      file_type: file_content_type.to_s,
      file: {
        io: StringIO.new(decoded),
        filename: filename,
        content_type: mime_type
      }
    )
  rescue StandardError => e
    Rails.logger.error "Wuzapi: Failed to decode base64 media: #{e.message}"
    @message.update!(is_unsupported: true)
  end

  def attach_media_from_wuzapi
    # For now, mark as unsupported if no direct media provided
    # Could implement download via Wuzapi API later
    @message.update!(is_unsupported: true)
    Rails.logger.warn "Wuzapi: No media data provided for message #{raw_message_id}"
  end

  def filename
    mime_type = processed_params['mimeType'] || 'application/octet-stream'
    ext = mime_type.split('/').last&.split(';')&.first || 'bin'

    "#{file_content_type}_#{raw_message_id}_#{Time.current.strftime('%Y%m%d')}.#{ext}"
  end

  def file_content_type
    return :image if %w[image sticker].include?(message_type)
    return :video if message_type == 'video'
    return :audio if message_type == 'audio'

    :file
  end
end
