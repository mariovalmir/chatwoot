module Whatsapp::IncomingMessageServiceHelpers
  def download_attachment_file(attachment_payload)
    attachment_id = attachment_payload[:id] || attachment_payload['id']
    Down.download(inbox.channel.media_url(attachment_id), headers: inbox.channel.api_headers)
  end

  def conversation_params
    {
      account_id: @inbox.account_id,
      inbox_id: @inbox.id,
      contact_id: @contact.id,
      contact_inbox_id: @contact_inbox.id
    }
  end

  def processed_params
    @processed_params ||= params
  end

  def account
    @account ||= inbox.account
  end

  def message_type
    if evolution_api?
      # Evolution API structure: data.messageType
      @processed_params[:data][:messageType]
    else
      payload = current_message_payload
      payload&.dig(:type) || payload&.dig('type')
    end
  end

  def message_content(message)
    # TODO: map interactive messages back to button messages in chatwoot
    message.dig(:text, :body) ||
      message.dig(:button, :text) ||
      message.dig(:interactive, :button_reply, :title) ||
      message.dig(:interactive, :list_reply, :title) ||
      message.dig(:name, :formatted_name)
  end

  def file_content_type(file_type)
    return :image if %w[image sticker].include?(file_type)
    return :audio if %w[audio voice].include?(file_type)
    return :video if ['video'].include?(file_type)
    return :location if ['location'].include?(file_type)
    return :contact if ['contacts'].include?(file_type)

    :file
  end

  def unprocessable_message_type?(message_type)
    %w[reaction ephemeral unsupported request_welcome].include?(message_type)
  end

  def contact_phones(contact)
    phones = contact[:phones]
    if phones.blank? && contact[:vcard].present?
      numbers = contact[:vcard].to_s.scan(/TEL[^:]*:([^\n]+)/i).flatten
      phones = numbers.map { |number| { phone: number.strip } }
    end
    phones
  end

  def processed_waid(waid)
    Whatsapp::PhoneNumberNormalizationService.new(inbox).normalize_and_find_contact(waid)
  end

  def error_webhook_event?(message)
    message.key?('errors')
  end

  def log_error(message)
    Rails.logger.warn "Whatsapp Error: #{message['errors'][0]['title']} - contact: #{message['from']}"
  end

  def process_in_reply_to(message)
    @in_reply_to_external_id = extract_reply_to_id(message)
  end

  def extract_reply_to_id(message)
    return if message.blank?

    if message.is_a?(Hash)
      symbol_reply = message[:replyTo] || message[:reply_to]
      if symbol_reply.present?
        return symbol_reply[:id] || symbol_reply['id'] if symbol_reply.is_a?(Hash)

        return symbol_reply
      end

      context_info = message[:contextInfo] || message['contextInfo']
      if context_info.is_a?(Hash)
        stanza_id = context_info[:stanzaId] || context_info['stanzaId']
        return stanza_id if stanza_id.present?

        quoted = context_info[:quotedMessage] || context_info['quotedMessage']
        if quoted.is_a?(Hash)
          key = quoted[:key] || quoted['key']
          quoted_id = key[:id] if key.is_a?(Hash) && key[:id].present?
          quoted_id ||= key['id'] if key.is_a?(Hash) && key['id'].present?
          return quoted_id if quoted_id.present?
        end
      end

      extended = message[:message]&.dig(:extendedTextMessage, :contextInfo) ||
                 message[:message]&.dig('extendedTextMessage', 'contextInfo')
      if extended.is_a?(Hash)
        stanza_id = extended[:stanzaId] || extended['stanzaId']
        return stanza_id if stanza_id.present?

        quoted = extended[:quotedMessage] || extended['quotedMessage']
        if quoted.is_a?(Hash)
          key = quoted[:key] || quoted['key']
          quoted_id = key[:id] if key.is_a?(Hash) && key[:id].present?
          quoted_id ||= key['id'] if key.is_a?(Hash) && key['id'].present?
          return quoted_id if quoted_id.present?
        end
      end
    end

    message['context']&.[]('id') ||
      message.dig('quoted', 'key', 'id') ||
      message.dig('quotedMsgId') ||
      message.dig('contextInfo', 'stanzaId') ||
      message.dig('contextInfo', 'quotedMessage', 'key', 'id') ||
      message.dig('message', 'extendedTextMessage', 'contextInfo', 'stanzaId') ||
      message.dig('message', 'extendedTextMessage', 'contextInfo', 'quotedMessage', 'key', 'id')
  end

  def extract_reply_to_participants(message)
    return [] if message.blank?

    participants = []

    if message.is_a?(Hash)
      participants.concat(participants_from_reply_hash(message[:replyTo]))
      participants.concat(participants_from_reply_hash(message[:reply_to]))
      participants.concat(participants_from_context_hash(message[:contextInfo]))
      participants.concat(participants_from_context_hash(message['contextInfo']))

      nested_message = message[:message] || message['message']
      if nested_message.is_a?(Hash)
        ext = nested_message[:extendedTextMessage] || nested_message['extendedTextMessage']
        if ext.is_a?(Hash)
          participants.concat(participants_from_context_hash(ext[:contextInfo])) if ext[:contextInfo].is_a?(Hash)
          participants.concat(participants_from_context_hash(ext['contextInfo'])) if ext['contextInfo'].is_a?(Hash)
        end

        reaction = nested_message[:reactionMessage] || nested_message['reactionMessage']
        if reaction.is_a?(Hash)
          participants.concat(participants_from_reply_hash(reaction[:key])) if reaction[:key].is_a?(Hash)
          participants.concat(participants_from_reply_hash(reaction['key'])) if reaction['key'].is_a?(Hash)
        end
      end
    end

    participants << message.dig('replyTo', 'participant')
    participants << message.dig('replyTo', 'participantAlt')
    participants << message.dig('quoted', 'key', 'participant')
    participants << message.dig('quoted', 'key', 'participantAlt')
    participants << message.dig('context', 'participant')
    participants << message.dig('contextInfo', 'participant')
    participants << message.dig('contextInfo', 'quotedMessage', 'key', 'participant')
    participants << message.dig('contextInfo', 'quotedMessage', 'key', 'participantAlt')
    participants << message.dig('message', 'extendedTextMessage', 'contextInfo', 'participant')
    participants << message.dig('message', 'extendedTextMessage', 'contextInfo', 'quotedMessage', 'key', 'participant')

    participants.compact!
    participants.map! { |participant| participant.to_s.strip }
    participants.reject!(&:blank?)
    participants.uniq
  rescue StandardError
    []
  end

  def participants_from_reply_hash(reply_hash)
    return [] unless reply_hash.is_a?(Hash)

    participants = []
    participants << reply_hash[:participant]
    participants << reply_hash['participant']
    participants << reply_hash[:participantAlt]
    participants << reply_hash['participantAlt']

    key = reply_hash[:key] || reply_hash['key']
    if key.is_a?(Hash)
      participants << key[:participant]
      participants << key['participant']
      participants << key[:participantAlt]
      participants << key['participantAlt']
    end

    participants.compact.map { |participant| participant.to_s.strip }.reject(&:blank?)
  end

  def participants_from_context_hash(context_hash)
    return [] unless context_hash.is_a?(Hash)

    participants = []
    participants << context_hash[:participant]
    participants << context_hash['participant']

    quoted = context_hash[:quotedMessage] || context_hash['quotedMessage']
    participants.concat(participants_from_reply_hash(quoted)) if quoted.is_a?(Hash)

    key = context_hash[:key] || context_hash['key']
    participants.concat(participants_from_reply_hash(key)) if key.is_a?(Hash)

    participants.compact.map { |participant| participant.to_s.strip }.reject(&:blank?)
  end

  def find_message_by_source_id(source_id)
    return unless source_id

    @message = Message.find_by(source_id: source_id)
    return @message if @message

    scope = Message.where("external_source_ids ->> 'waha_message_id' = ?", source_id)
    @message = scope.first
    return @message if @message

    @message = Message.where("(external_source_ids -> 'waha_message_ids') ? :identifier", identifier: source_id).first
    return @message if @message

    @message = Message.where('external_source_ids @> ?::jsonb', { 'waha_message_ids' => [source_id] }.to_json).first
  end

  def message_under_process?
    message_id = if evolution_api?
                   # Evolution API structure: data.key.id
                   @processed_params[:data]&.dig(:key, :id)
                 else
                   # Baileys structure: messages.first.id
                   @processed_params[:messages]&.first&.dig(:id)
                 end

    return false unless message_id

    key = format(Redis::RedisKeys::MESSAGE_SOURCE_KEY, id: message_id)
    Redis::Alfred.get(key)
  end

  def cache_message_source_id_in_redis
    message_id = if evolution_api?
                   # Evolution API structure: data.key.id
                   @processed_params[:data]&.dig(:key, :id)
                 else
                   # Baileys structure: messages.first.id
                   return if @processed_params.try(:[], :messages).blank?

                   @processed_params[:messages].first[:id]
                 end

    return unless message_id

    key = format(Redis::RedisKeys::MESSAGE_SOURCE_KEY, id: message_id)
    ::Redis::Alfred.setex(key, true)
  end

  def clear_message_source_id_from_redis
    message_id = if evolution_api?
                   # Evolution API structure: data.key.id
                   @processed_params[:data]&.dig(:key, :id)
                 else
                   # Baileys structure: messages.first.id
                   @processed_params[:messages].first[:id]
                 end

    return unless message_id

    key = format(Redis::RedisKeys::MESSAGE_SOURCE_KEY, id: message_id)
    ::Redis::Alfred.delete(key)
  end

  private

  def evolution_api?
    # Evolution API has data structure with event field, while Baileys has messages array
    @processed_params[:data].present? && @processed_params[:event].present?
  end
end
