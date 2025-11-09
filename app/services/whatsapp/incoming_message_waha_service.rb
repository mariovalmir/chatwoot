class Whatsapp::IncomingMessageWahaService < Whatsapp::IncomingMessageBaseService
  include Whatsapp::EvolutionHandlers::MessagesUpsert
  include Whatsapp::EvolutionHandlers::MessagesUpdate
  include Whatsapp::EvolutionHandlers::MessagesDelete
  include Whatsapp::EvolutionHandlers::Helpers

  EVENT_HANDLERS = {
    'messages.upsert' => :process_messages_upsert,
    'send.message' => :process_messages_upsert,
    # Some Waha setups send outgoing status updates via this event
    'send.message.update' => :process_messages_update,
    'send.message_update' => :process_messages_update,
    'messages.update' => :process_messages_update,
    'messages.edited' => :process_message_edited_event,
    'messages.delete' => :process_messages_delete,
    'contacts.update' => :process_contacts_update,
    'chats.update' => :process_chats_event,
    'chats.upsert' => :process_chats_event,
    'groups.update' => :process_groups_event,
    'groups.upsert' => :process_groups_event,
    'qrcode.updated' => :handle_qrcode_updated,
    'connection.update' => :handle_connection_update,
    'session.status' => :handle_connection_update,
    'message.any' => :process_message_any_event,
    'message' => :process_message_event,
    'message.reaction' => :process_message_reaction_event,
    'message.edited' => :process_message_edited_event,
    'message.revoked' => :process_message_revoked_event,
    'message.ack' => :process_message_ack_event
  }.freeze

  CHAT_EVENTS = %w[chats.update chats.upsert].freeze

  def perform
    event_type = processed_params[:event]

    if EVENT_HANDLERS.key?(event_type)
      send(EVENT_HANDLERS[event_type])
    elsif CHAT_EVENTS.include?(event_type)
      process_chats_event
    else
      Rails.logger.warn "Waha API: Unsupported event type: #{event_type}"
    end
  end

  def handle_qrcode_updated
    qr_base64 = processed_params.dig(:data, :qrcode, :base64)
    
    # Update provider_connection with QR code
    if qr_base64.present?
      current_connection = inbox.channel.provider_connection || {}
      
      # Check if base64 already has data URI prefix
      qr_data_url = if qr_base64.start_with?('data:image')
                      qr_base64
                    else
                      "data:image/png;base64,#{qr_base64}"
                    end
      
      inbox.channel.update_provider_connection!(
        connection: current_connection['connection'] || 'connecting',
        qr_data_url: qr_data_url,
        error: nil
      )
    end
    
    Dispatcher.dispatch(
      Events::Types::WHATSAPP_QRCODE_UPDATED,
      Time.current,
      inbox: inbox,
      qr_code: qr_base64
    )
  end

  def handle_connection_update
    status = processed_params.dig(:data, :state) ||
             processed_params.dig(:data, :connection_status) ||
             processed_params.dig(:data, :connectionStatus) ||
             processed_params.dig(:data, :status)
    status ||= 'close'

    # Prepare connection data - clear QR code when connected
    connection_data = { connection: status }
    if status == 'open'
      connection_data[:qr_data_url] = nil
      connection_data[:error] = nil
    else
      current_connection = inbox.channel.provider_connection || {}
      connection_data[:qr_data_url] = current_connection['qr_data_url']
      connection_data[:error] = current_connection['error']
    end

    inbox.channel.update_provider_connection!(connection_data)

    Dispatcher.dispatch(
      Events::Types::WHATSAPP_CONNECTION_UPDATE,
      Time.current,
      inbox: inbox,
      status: status
    )
  end

  def update_connection_status_from(data)
    data = data.first if data.is_a?(Array)
    return unless data.is_a?(Hash)

    status = data[:state] ||
             data[:connection_status] ||
             data[:connectionStatus] ||
             data[:status]
    return unless status

    # Prepare connection data - clear QR code when connected
    connection_data = { connection: status }
    if status == 'open'
      connection_data[:qr_data_url] = nil
      connection_data[:error] = nil
    else
      current_connection = inbox.channel.provider_connection || {}
      connection_data[:qr_data_url] = current_connection['qr_data_url']
      connection_data[:error] = current_connection['error']
    end

    inbox.channel.update_provider_connection!(connection_data)

    Dispatcher.dispatch(
      Events::Types::WHATSAPP_CONNECTION_UPDATE,
      Time.current,
      inbox: inbox,
      status: status
    )
  end

  private

  def processed_params
    @processed_params ||= params
  end

  def process_contacts_update
    # Waha API sends contact updates when contact info changes (name, profile pic, etc.)
    contacts = processed_params[:data]
    contacts = [contacts] unless contacts.is_a?(Array)

    contacts.each do |contact_data|
      update_contact_info(contact_data)
    end

    # Some contact update events also include connection status information
    update_connection_status_from(processed_params[:data])
  end

  def process_message_any_event
    payload = safe_indifferent_hash(processed_params[:payload]) || {}.with_indifferent_access
    key_from_me = payload.dig(:key, :fromMe)

    if payload[:fromMe].nil? && !key_from_me.nil?
      payload[:fromMe] = key_from_me
    end

    if payload.key?(:fromMe)
      payload[:fromMe] = normalize_waha_from_me(payload[:fromMe])
    end

    if payload[:key].is_a?(Hash) && payload[:key].key?(:fromMe)
      payload[:key][:fromMe] = normalize_waha_from_me(payload[:key][:fromMe])
    end

    processed_params[:origin] ||= payload[:source] if payload[:source].present?
    processed_params[:payload] = payload

    process_message_event
  end

  def process_message_event
    payload = safe_indifferent_hash(processed_params[:payload])
    payload = deep_indifferentize(payload) if payload
    if payload.blank?
      Rails.logger.warn 'Waha API: message received without payload'
      return
    end

    protocol_message = payload.dig(:_data, :Message, :protocolMessage) || payload.dig(:Message, :protocolMessage)
    if protocol_message.present?
      protocol_type = protocol_message[:type]
      case protocol_type
      when 14
        process_message_edited_event
        return
      when 0
        process_message_revoked_event
        return
      end
    end

    message_data = build_waha_message_from_payload(payload)
    if message_data.blank?
      Rails.logger.warn "Waha API: Unable to normalize message payload: #{payload.inspect}"
      return
    end

    processed_params[:data] = message_data
    process_messages_upsert
  rescue StandardError => e
    Rails.logger.error "Waha API: Failed to process message event: #{e.message}"
    Rails.logger.error e.backtrace.first(5).join("\n")
  end

  def process_message_reaction_event
    payload = safe_indifferent_hash(processed_params[:payload])
    if payload.blank?
      Rails.logger.warn 'Waha API: message.reaction received without payload'
      return
    end

    reaction = safe_indifferent_hash(payload[:reaction])
    if reaction.blank? || reaction[:messageId].blank?
      Rails.logger.warn "Waha API: message.reaction missing reaction metadata: #{payload.inspect}"
      return
    end

    payload[:fromMe] = normalize_waha_from_me(payload[:fromMe]) if payload.key?(:fromMe)

    key_data = safe_indifferent_hash(reaction[:key]) || {}.with_indifferent_access
    remote_jid = key_data[:remoteJid].presence || payload[:chatId].presence || payload[:from].presence || payload[:to]
    participant = key_data[:participant].presence || payload[:participant]
    participant_alt = key_data[:participantAlt].presence || payload[:participantAlt]

    key_data[:id] ||= reaction[:messageId]
    key_data[:remoteJid] ||= remote_jid if remote_jid.present?
    key_data[:participant] ||= participant if participant.present?
    key_data[:participantAlt] ||= participant_alt if participant_alt.present?
    key_data[:fromMe] = normalize_waha_from_me(key_data[:fromMe]) if key_data.key?(:fromMe)

    reaction_message = {
      text: reaction[:text],
      key: key_data.compact
    }
    if payload[:timestamp].present? && payload[:timestamp].respond_to?(:to_i)
      reaction_message[:senderTimestampMs] = payload[:timestamp].to_i * 1000
    end

    normalized_payload = payload.merge(
      message: { reactionMessage: reaction_message },
      body: reaction[:text],
      replyTo: { id: reaction[:messageId], key: key_data.compact }.compact,
      quotedMsgId: reaction[:messageId],
      quoted_message_id: reaction[:messageId],
      participant: participant,
      participantAlt: participant_alt
    )

    message_data = build_waha_message_from_payload(normalized_payload)
    if message_data.blank?
      Rails.logger.warn "Waha API: Unable to normalize message.reaction payload: #{payload.inspect}"
      return
    end

    processed_params[:data] = message_data
    process_messages_upsert
  rescue StandardError => e
    Rails.logger.error "Waha API: Failed to process message.reaction event: #{e.message}"
    Rails.logger.error e.backtrace.first(5).join("\n")
  end

  def process_message_edited_event
    if processed_params[:data].present?
      process_messages_update
      return
    end

    payload = safe_indifferent_hash(processed_params[:payload])
    if payload.blank?
      Rails.logger.warn 'Waha API: message.edited received without payload'
      return
    end

    update_payload = build_waha_update_from_edited(payload)
    if update_payload.blank?
      Rails.logger.warn "Waha API: Unable to build update payload for message.edited: #{payload.inspect}"
      return
    end

    processed_params[:data] = update_payload
    process_messages_update
  rescue StandardError => e
    Rails.logger.error "Waha API: Failed to process message.edited event: #{e.message}"
    Rails.logger.error e.backtrace.first(5).join("\n")
  end

  def process_message_revoked_event
    payload = safe_indifferent_hash(processed_params[:payload])
    if payload.blank?
      Rails.logger.warn 'Waha API: message.revoked received without payload'
      return
    end

    delete_payload = build_waha_delete_from_revoked(payload)
    if delete_payload.blank?
      Rails.logger.warn "Waha API: Unable to build delete payload for message.revoked: #{payload.inspect}"
      return
    end

    processed_params[:data] = delete_payload
    process_messages_delete
  rescue StandardError => e
    Rails.logger.error "Waha API: Failed to process message.revoked event: #{e.message}"
    Rails.logger.error e.backtrace.first(5).join("\n")
  end

  def process_message_ack_event
    payload = ensure_indifferent_hash(processed_params[:payload]) || {}
    return if payload.blank?

    message_id = payload[:id] || payload[:messageId] || payload.dig(:key, :id)
    message_id ||= Array.wrap(payload.dig(:_data, :MessageIDs)).first
    return if message_id.blank?

    find_message_by_source_id(message_id)
    if @message.blank?
      Array.wrap(payload.dig(:_data, :MessageIDs)).each do |candidate|
        break if find_message_by_source_id(candidate)
      end
    end
    return unless @message

    store_waha_message_ids!(@message, payload)
    store_participant_metadata_from_ack(@message, payload)

    status = map_provider_status(payload)
    return if status.blank? || !@message.outgoing?

    if status == 'read' && waha_group_ack?(payload)
      Rails.logger.debug { "Waha API: Skipping group read ack for message #{@message.id}" }
      return
    end

    return unless ack_status_transition_allowed?(@message.status, status)

    update_last_seen_from_ack(payload, status)

    @message.update!(status: status)
  rescue StandardError => e
    Rails.logger.error "Waha API: Failed to process message.ack event: #{e.message}"
    Rails.logger.error e.backtrace.first(5).join("\n")
  end

  def build_waha_delete_from_revoked(payload)
    after_payload = safe_indifferent_hash(payload[:after])

    source_id = payload[:id].presence || after_payload&.[](:id).presence
    revoked_id = payload[:revokedMessageId].presence || payload[:revoked_message_id].presence

    remote_jid = after_payload&.dig(:_data, :Info, :Chat)
    remote_jid ||= payload[:chatId] || payload[:from]

    protocol_message = safe_indifferent_hash(payload.dig(:_data, :Message, :protocolMessage))
    protocol_message ||= safe_indifferent_hash(payload.dig(:Message, :protocolMessage))
    protocol_key = safe_indifferent_hash(protocol_message[:key]) if protocol_message

    source_id ||= protocol_key[:id] if protocol_key&.[](:id).present?
    source_id ||= protocol_key[:ID] if protocol_key&.[](:ID).present?

    remote_jid ||= protocol_key[:remoteJid] if protocol_key&.[](:remoteJid).present?
    remote_jid ||= protocol_key[:remoteJID] if protocol_key&.[](:remoteJID).present?

    participant = payload[:participant].presence || after_payload&.[](:participant)
    participant ||= protocol_key[:participant] if protocol_key

    from_me = if payload.key?(:fromMe)
                normalize_waha_from_me(payload[:fromMe])
              elsif after_payload&.key?(:fromMe)
                normalize_waha_from_me(after_payload[:fromMe])
              elsif protocol_key&.key?(:fromMe)
                normalize_waha_from_me(protocol_key[:fromMe])
              end

    normalized_remote = remote_jid.to_s.sub('@s.whatsapp.net', '@c.us') if remote_jid.present?
    prefix = if from_me.nil?
               payload[:id].to_s.split('_').first.presence || 'false'
             else
               waha_boolean_value(from_me) == true ? 'true' : 'false'
             end

    legacy_source_id = if normalized_remote.present? && (revoked_id.present? || protocol_key&.[](:id).present?)
                         identifier = revoked_id.presence || protocol_key[:id] || protocol_key[:ID]
                         "#{prefix}_#{normalized_remote}_#{identifier}"
                       end

    if legacy_source_id.present?
      source_id = legacy_source_id
    elsif source_id.blank? && revoked_id.present? && normalized_remote.present?
      source_id = "#{prefix}_#{normalized_remote}_#{revoked_id}"
    end

    return if source_id.blank? && revoked_id.blank?

    key_id = source_id.presence || revoked_id
    key_id ||= protocol_key[:id] if protocol_key&.[](:id).present?
    key_id ||= protocol_key[:ID] if protocol_key&.[](:ID).present?

    waha_ids = [source_id, revoked_id, after_payload&.[](:id), legacy_source_id]
    waha_ids << protocol_key[:id] if protocol_key&.[](:id).present?
    waha_ids << protocol_key[:ID] if protocol_key&.[](:ID).present?
    waha_ids << payload.dig(:_data, :Info, :ID)

    lookup_seed = {
      id: source_id.presence || revoked_id.presence || after_payload&.[](:id),
      keyId: key_id,
      messageId: key_id,
      key: {
        id: key_id,
        remoteJid: remote_jid,
        remoteJidAlt: after_payload&.[](:remoteJidAlt) || payload[:remoteJidAlt],
        fromMe: from_me,
        participant: participant,
        participantAlt: after_payload&.[](:participantAlt) || payload[:participantAlt]
      }.compact,
      remoteJid: remote_jid,
      remoteJidAlt: after_payload&.[](:remoteJidAlt) || payload[:remoteJidAlt],
      participant: participant,
      participantAlt: after_payload&.[](:participantAlt) || payload[:participantAlt],
      from: payload[:from],
      to: payload[:to],
      chatId: payload[:chatId],
      chat_id: payload[:chat_id],
      revokedMessageId: revoked_id,
      revoked_message_id: revoked_id,
      _data: after_payload&.[](:_data) || payload[:_data]
    }.compact

    generated_ids = extract_waha_message_ids(lookup_seed)
    waha_ids.concat(generated_ids) if generated_ids.present?

    waha_ids = waha_ids.compact.map(&:to_s).reject(&:blank?).uniq

    {
      id: source_id || revoked_id,
      keyId: key_id,
      messageId: key_id,
      key: {
        id: key_id,
        remoteJid: remote_jid,
        fromMe: from_me,
        participant: participant
      }.compact,
      remoteJid: remote_jid,
      participant: participant,
      fromMe: from_me,
      waha_message_id: waha_ids.find { |val| val.include?('_') } || waha_ids.first,
      waha_message_ids: waha_ids.presence,
      editLookupIds: waha_ids.presence,
      _data: after_payload[:_data] || payload[:_data]
    }.compact
  end

  def store_participant_metadata_from_ack(message, payload)
    return unless message && payload.is_a?(Hash)

    participants = []
    participants << payload[:participant]
    participants << payload[:participantAlt]

    info = ensure_indifferent_hash(payload.dig(:_data, :Info))
    if info.present?
      participants << info[:Sender]
      participants << info[:SenderAlt]
    end

    participants << payload[:from]
    participants << payload[:to]

    participants.map! { |val| val.to_s.strip }
    participants.reject!(&:blank?)
    participants.uniq!

    return if participants.blank?

    attrs = message.content_attributes || {}
    attrs = attrs.with_indifferent_access if attrs.respond_to?(:with_indifferent_access)

    existing_group = Array.wrap(attrs[:in_reply_to_participants])
    merged_group = (existing_group + participants).map { |val| val.to_s.strip }.reject(&:blank?).uniq
    attrs[:in_reply_to_participants] = merged_group if merged_group.present?

    primary = attrs[:participant_jid]
    attrs[:participant_jid] = participants.first if primary.blank? && participants.first.present?

    alt = attrs[:participant_alt_jid]
    second_participant = participants[1]
    attrs[:participant_alt_jid] = second_participant if alt.blank? && second_participant.present?

    message.assign_attributes(content_attributes: attrs)
    message.save! if message.changed?
  rescue StandardError => e
    Rails.logger.warn "Waha API: Failed to store participant metadata for message #{message&.id}: #{e.message}"
  end

  def build_waha_update_from_edited(payload)
    payload = safe_indifferent_hash(payload)
    return if payload.blank?

    after_payload = safe_indifferent_hash(payload[:after])
    base_candidates = [after_payload,
                       safe_indifferent_hash(payload[:message]),
                       safe_indifferent_hash(payload[:data]),
                       payload]
    base_payload = base_candidates.compact.find(&:present?)
    return if base_payload.blank?

    merged_payload = ensure_indifferent_hash(base_payload.dup)

    base_candidates.each do |candidate|
      next if candidate.blank?

      indifferent_candidate = ensure_indifferent_hash(candidate)
      next if indifferent_candidate.blank?

      indifferent_candidate.each do |key, value|
        next if merged_payload[key].present?

        merged_payload[key] = value
      end
    end

    %i[id from to body timestamp participant ack ackName editedMessageId].each do |key|
      next if merged_payload[key].present?

      merged_payload[key] = payload[key] if payload.key?(key)
    end

    if merged_payload[:fromMe].blank? && payload.key?(:fromMe)
      merged_payload[:fromMe] = payload[:fromMe]
    end

    if merged_payload[:_data].blank?
      merged_payload[:_data] = after_payload&.[](:_data) if after_payload.present? && after_payload&.[](:_data).present?
      merged_payload[:_data] ||= payload[:_data]
    end

    protocol_message = ensure_indifferent_hash(merged_payload.dig(:_data, :Message, :protocolMessage))
    protocol_message ||= ensure_indifferent_hash(payload.dig(:_data, :Message, :protocolMessage))
    protocol_message ||= ensure_indifferent_hash(payload.dig(:Message, :protocolMessage))
    protocol_key = ensure_indifferent_hash(protocol_message[:key]) if protocol_message.is_a?(Hash)

    message_data = build_waha_message_from_payload(merged_payload)
    return if message_data.blank?

    bool_from_me = if merged_payload.key?(:fromMe)
                     waha_boolean_value(merged_payload[:fromMe])
                   elsif payload.key?(:fromMe)
                     waha_boolean_value(payload[:fromMe])
                   end

    unless bool_from_me.nil?
      message_data[:fromMe] = bool_from_me
      message_data[:key] = ensure_indifferent_hash(message_data[:key] || {})
      message_data[:key][:fromMe] = bool_from_me
    end

    remote_jid = message_data[:remoteJid].presence
    remote_jid ||= merged_payload[:remoteJid]
    remote_jid ||= after_payload&.[](:remoteJid) if after_payload.present?
    remote_jid ||= protocol_key[:remoteJid] if protocol_key&.[](:remoteJid).present?
    remote_jid ||= protocol_key[:remoteJID] if protocol_key&.[](:remoteJID).present?
    remote_jid ||= protocol_message[:remoteJid] if protocol_message&.[](:remoteJid).present?
    remote_jid ||= protocol_message[:remoteJID] if protocol_message&.[](:remoteJID).present?
    remote_jid ||= if bool_from_me == false
                     merged_payload[:from]
                   elsif bool_from_me == true
                     merged_payload[:to]
                   end
    remote_jid ||= payload[:from]
    remote_jid ||= payload[:to]

    if remote_jid.present?
      message_data[:remoteJid] = remote_jid
      message_data[:key] = ensure_indifferent_hash(message_data[:key] || {})
      message_data[:key][:remoteJid] ||= remote_jid
    end

    participant = merged_payload[:participant].presence || payload[:participant]
    participant ||= protocol_key[:participant] if protocol_key&.[](:participant).present?
    if participant.present?
      message_data[:participant] = participant
      message_data[:key] = ensure_indifferent_hash(message_data[:key] || {})
      message_data[:key][:participant] ||= participant
    end

    normalized_remote = remote_jid.to_s.sub('@s.whatsapp.net', '@c.us') if remote_jid.present?

    edited_message_id = merged_payload[:editedMessageId].presence ||
                        payload[:editedMessageId].presence ||
                        after_payload&.[](:editedMessageId).presence ||
                        protocol_message&.[](:editedMessageId)
    edited_message_id ||= protocol_message&.dig(:editedMessage, :key, :id)
    edited_message_id ||= protocol_message&.dig(:editedMessage, :key, :ID)
    edited_message_id ||= protocol_key[:id] if protocol_key&.[](:id).present? && protocol_key[:id].to_s != message_data[:id].to_s
    edited_message_id ||= protocol_key[:ID] if protocol_key&.[](:ID).present? && protocol_key[:ID].to_s != message_data[:id].to_s

    source_prefix = if bool_from_me.nil?
                      payload[:id].to_s.split('_').first.presence || 'false'
                    else
                      bool_from_me ? 'true' : 'false'
                    end

    legacy_source_id = if normalized_remote.present? && edited_message_id.present?
                         "#{source_prefix}_#{normalized_remote}_#{edited_message_id}"
                       end

    original_message_id = legacy_source_id.presence ||
                          payload[:id].presence ||
                          merged_payload[:id].presence ||
                          after_payload&.[](:id).presence ||
                          merged_payload[:keyId].presence ||
                          merged_payload[:messageId].presence ||
                          protocol_key&.[](:id).presence ||
                          protocol_key&.[](:ID).presence ||
                          message_data[:id].presence

    if original_message_id.present?
      message_data[:id] = original_message_id
      message_data[:keyId] = original_message_id
      message_data[:messageId] = original_message_id
      key_hash = ensure_indifferent_hash(message_data[:key] || {})
      key_hash[:id] = original_message_id
      message_data[:key] = key_hash
    end

    new_body = merged_payload[:body].presence || payload[:body]
    if new_body.present?
      message_data[:editedMessage] = { conversation: new_body }
      if message_data[:message].is_a?(Hash)
        message_data[:message][:conversation] ||= new_body
      else
        message_data[:message] = { conversation: new_body }
      end
      message_data[:body] = new_body
    elsif message_data[:message].present?
      message_data[:editedMessage] ||= ensure_indifferent_hash(message_data[:message])
    end

    if edited_message_id.present?
      message_data[:editedMessageId] ||= edited_message_id
      message_data[:editedMessage] ||= ensure_indifferent_hash(message_data[:message])
    end

    ids = Array.wrap(message_data[:waha_message_ids])
    ids << message_data[:waha_message_id]
    ids << merged_payload[:id]
    ids << payload[:id]
    ids << merged_payload[:editedMessageId]
    ids << payload[:editedMessageId]
    ids << edited_message_id
    ids << original_message_id
    ids << legacy_source_id
    ids << protocol_key[:id] if protocol_key&.[](:id).present?
    ids << protocol_key[:ID] if protocol_key&.[](:ID).present?
    ids << protocol_message[:id] if protocol_message&.[](:id).present?
    ids << protocol_message[:ID] if protocol_message&.[](:ID).present?
    ids << merged_payload.dig(:_data, :Info, :ID)
    ids << payload.dig(:_data, :Info, :ID)
    ids << after_payload&.dig(:_data, :Info, :ID) if after_payload.present?
    ids = ids.compact.map(&:to_s).reject(&:blank?).uniq

    if ids.present?
      remote_alt = message_data[:remoteJidAlt] || merged_payload[:remoteJidAlt] ||
                   after_payload&.[](:remoteJidAlt) || payload[:remoteJidAlt]
      participant_alt = message_data[:participantAlt] || merged_payload[:participantAlt] ||
                        after_payload&.[](:participantAlt) || payload[:participantAlt]
      from_me_value = bool_from_me.nil? ? message_data[:fromMe] : bool_from_me

      key_data = ensure_indifferent_hash(message_data[:key] || {})
      key_data[:remoteJid] ||= remote_jid if remote_jid.present?
      key_data[:participant] ||= participant if participant.present?
      key_data[:participantAlt] ||= participant_alt if participant_alt.present?
      key_data[:fromMe] = from_me_value unless from_me_value.nil?

      lookup_seed = {
        waha_message_ids: ids,
        waha_message_id: ids.first,
        editLookupIds: ids,
        id: original_message_id || message_data[:id],
        messageId: original_message_id || message_data[:messageId],
        keyId: original_message_id || message_data[:keyId],
        key: key_data,
        remoteJid: remote_jid,
        remoteJidAlt: remote_alt,
        participant: participant,
        participantAlt: participant_alt,
        from: payload[:from],
        to: payload[:to],
        chatId: merged_payload[:chatId] || payload[:chatId],
        chat_id: merged_payload[:chat_id] || payload[:chat_id],
        fromMe: from_me_value,
        editedMessageId: edited_message_id,
        protocolMessage: protocol_message,
        _data: merged_payload[:_data] || after_payload&.[](:_data) || payload[:_data]
      }.compact

      generated_ids = extract_waha_message_ids(lookup_seed)
      ids |= generated_ids if generated_ids.present?

      message_data[:key] = key_data
      message_data[:remoteJidAlt] ||= remote_alt if remote_alt.present?
      message_data[:participantAlt] ||= participant_alt if participant_alt.present?
      message_data[:fromMe] = from_me_value unless from_me_value.nil?

      message_data[:waha_message_ids] = ids
      message_data[:waha_message_id] ||= ids.first
      message_data[:editLookupIds] = ids
    end

    message_data
  end

  def build_waha_message_from_payload(payload)
    raw_data = deep_indifferentize(payload[:_data]) || {}.with_indifferent_access
    info = deep_indifferentize(raw_data[:Info]) || {}.with_indifferent_access

    message_block = raw_data[:Message] || raw_data[:message] || raw_data[:RawMessage] || raw_data[:rawMessage]
    message_block = deep_indifferentize(message_block) || {}.with_indifferent_access

    if message_block.blank? && payload[:body].present?
      message_block = { conversation: payload[:body] }.with_indifferent_access
    end

    location_payload = extract_waha_location(payload, message_block, raw_data)
    location_hash = nil

    if location_payload.present?
      message_block[:locationMessage] = location_payload
      location_hash = {
        latitude: location_payload[:degreesLatitude],
        longitude: location_payload[:degreesLongitude],
        name: location_payload[:name],
        address: location_payload[:address],
        url: location_payload[:url]
      }.compact
    end

    message_type = determine_waha_message_type(payload, message_block, info, location_payload)

    remote_jid = info[:Chat].presence || payload[:chatId].presence || payload[:jid].presence
    remote_jid ||= payload.dig(:key, :remoteJid).presence
    remote_jid ||= payload.dig(:key, :remoteJID).presence
    remote_jid ||= payload[:from].presence || payload[:to].presence
    remote_jid ||= payload.dig(:replyTo, :participant)
    remote_jid ||= payload.dig(:replyTo, :participantAlt)
    remote_jid = remote_jid.to_s if remote_jid
    return if remote_jid.blank?

    remote_jid_alt = info[:RecipientAlt].presence || info[:SenderAlt].presence
    participant = payload[:participant].presence || info[:Sender]
    participant_alt = payload[:participantAlt].presence || info[:SenderAlt]

    from_me = normalize_waha_from_me(payload[:fromMe])

    raw_timestamp = payload[:timestamp]
    message_timestamp = if raw_timestamp.respond_to?(:to_i)
                          raw_timestamp.to_i
                        else
                          raw_timestamp
                        end

    reply_reference = extract_waha_reply_reference(payload, raw_data, message_block)
    reply_id = extract_reply_identifier(reply_reference, payload, message_block, raw_data)
    reply_reference = { id: reply_id } if reply_reference.blank? && reply_id.present?
    reply_reference = deep_indifferentize(reply_reference) if reply_reference.is_a?(Hash)

    waha_ids = []
    waha_ids << payload[:id]
    waha_ids << info[:ID]
    waha_ids.concat(Array.wrap(raw_data[:MessageIDs]))
    waha_ids.concat(Array.wrap(raw_data[:messageIDs]))
    waha_ids.concat(Array.wrap(raw_data[:messageIds]))
    waha_ids << reply_id
    waha_ids = waha_ids.compact.map(&:to_s).reject(&:blank?).uniq

    key_id = payload[:id].presence || info[:ID]
    key_id ||= waha_ids.first
    key_id = key_id.to_s if key_id

    key_from_me = payload.dig(:key, :fromMe)
    key_from_me = normalize_waha_from_me(key_from_me) unless key_from_me.nil?

    key_hash = {
      id: key_id,
      remoteJid: remote_jid,
      fromMe: from_me,
      participant: participant
    }.compact
    key_hash[:fromMe] = key_from_me unless key_from_me.nil?

    message_data = {
      id: key_id,
      keyId: key_id,
      messageId: key_id,
      key: key_hash,
      remoteJid: remote_jid,
      remoteJidAlt: remote_jid_alt,
      participant: participant,
      participantAlt: participant_alt,
      fromMe: from_me,
      hasMedia: payload[:hasMedia],
      media: payload[:media],
      pushName: info[:PushName] || payload[:pushName],
      status: payload[:status] || payload[:ackName] || payload[:ack],
      ack: payload[:ack],
      ackName: payload[:ackName],
      messageTimestamp: message_timestamp,
      timestamp: message_timestamp,
      message: message_block,
      body: payload[:body],
      replyTo: reply_reference,
      source: payload[:source],
      waha_message_id: waha_ids.first,
      waha_message_ids: waha_ids.presence,
      _data: raw_data,
      messageType: message_type,
      location: location_hash
    }.compact

    message_data.with_indifferent_access
  end

  def determine_waha_message_type(payload, message_block, info, location_payload)
    explicit_type = payload[:type] || payload[:messageType] || info[:MediaType] || info[:mediaType] || info[:Type]
    normalized_explicit = explicit_type.to_s.downcase

    case normalized_explicit
    when 'conversation', 'text'
      return 'text'
    when 'image'
      return 'image'
    when 'audio', 'ptt'
      return 'audio'
    when 'video'
      return 'video'
    when 'document', 'file'
      return 'file'
    when 'sticker'
      return 'sticker'
    when 'reaction'
      return 'reaction'
    when 'protocol'
      return 'protocol'
    when 'location'
      return 'location'
    when 'contacts', 'contact'
      return 'contacts'
    end

    return 'location' if location_payload.present? || message_block[:liveLocationMessage].present?

    contacts = message_block[:contactMessage].presence || message_block.dig(:contactsArrayMessage, :contacts)
    return 'contacts' if contacts.present?

    if message_block[:conversation].present? || message_block.dig(:extendedTextMessage, :text).present?
      return 'text'
    end

    return 'image' if message_block[:imageMessage].present?
    return 'audio' if message_block[:audioMessage].present?
    return 'video' if message_block[:videoMessage].present?
    if message_block[:documentMessage].present? || message_block[:documentWithCaptionMessage].present?
      return 'file'
    end
    return 'sticker' if message_block[:stickerMessage].present?
    return 'reaction' if message_block[:reactionMessage].present?
    return 'protocol' if message_block[:protocolMessage].present?

    'unsupported'
  end

  def extract_waha_location(payload, message_block, raw_data)
    candidates = []
    candidates << message_block[:locationMessage]
    candidates << message_block[:liveLocationMessage]
    candidates << payload[:location]
    candidates << payload[:locationMessage]
    candidates << payload[:liveLocationMessage]
    candidates << payload.dig(:message, :locationMessage)
    candidates << payload.dig(:message, :liveLocationMessage)
    candidates << payload.dig(:Message, :locationMessage)
    candidates << payload.dig(:Message, :liveLocationMessage)
    candidates << payload.dig(:RawMessage, :locationMessage)
    candidates << payload.dig(:RawMessage, :liveLocationMessage)
    candidates << raw_data[:Location]
    candidates << raw_data[:location]
    candidates << raw_data.dig(:Message, :locationMessage)
    candidates << raw_data.dig(:Message, :liveLocationMessage)
    candidates << raw_data.dig(:message, :locationMessage)
    candidates << raw_data.dig(:message, :liveLocationMessage)
    candidates << raw_data.dig(:RawMessage, :locationMessage)
    candidates << raw_data.dig(:RawMessage, :liveLocationMessage)

    candidates.compact.each do |candidate|
      normalized = normalize_waha_location(candidate)
      return normalized if normalized.present?
    end

    nil
  end

  def extract_waha_reply_reference(payload, raw_data, message_block)
    primary = payload[:replyTo] || payload[:reply_to] || payload[:quoted]
    primary ||= payload[:quotedMessage] || payload[:quoted_message]
    primary ||= payload[:reply] || payload[:inReplyTo]

    if primary.present?
      return primary.is_a?(Array) ? primary.first : primary
    end

    candidates = []
    candidates << payload[:context]
    candidates << payload[:contextInfo]
    candidates << payload[:context_info]
    candidates << payload[:quotedInfo]
    candidates << payload[:quoted_info]
    candidates << message_block[:contextInfo] if message_block.is_a?(Hash)
    candidates << message_block.dig(:extendedTextMessage, :contextInfo) if message_block.is_a?(Hash)
    candidates << raw_data[:ContextInfo]
    candidates << raw_data.dig(:Message, :contextInfo)
    candidates << raw_data.dig(:message, :contextInfo)

    candidates.each do |ctx|
      next if ctx.blank?

      normalized = ctx.is_a?(Array) ? ctx.first : ctx
      return normalized if normalized.present?
    end

    nil
  end

  def extract_reply_identifier(reply_reference, payload, message_block, raw_data)
    refs = []

    if reply_reference.is_a?(Hash)
      refs << reply_reference[:id]
      refs << reply_reference[:messageId]
      refs << reply_reference[:message_id]
      refs << reply_reference[:keyId]
      refs << reply_reference[:key_id]
      refs << reply_reference.dig(:key, :id)
      refs << reply_reference[:stanzaId]
      refs << reply_reference[:stanza_id]
      refs << reply_reference[:quotedMessageId]
      refs << reply_reference[:quoted_message_id]
      refs << reply_reference.dig(:quotedMessage, :key, :id)
    elsif reply_reference.present?
      refs << reply_reference
    end

    refs << payload[:quotedMsgId]
    refs << payload[:quoted_message_id]
    refs << payload[:quotedMessageId]
    refs << payload[:quotedStanzaId]
    refs << payload[:referencedMessageId]
    refs << payload[:referenced_message_id]

    if message_block.is_a?(Hash)
      refs << message_block.dig(:contextInfo, :stanzaId)
      refs << message_block.dig(:extendedTextMessage, :contextInfo, :stanzaId)
      refs << message_block.dig(:extendedTextMessage, :contextInfo, :quotedMessage, :key, :id)
    end

    refs << raw_data.dig(:ContextInfo, :stanzaId)
    refs << raw_data.dig(:Message, :contextInfo, :stanzaId)
    refs << raw_data.dig(:Message, :contextInfo, :quotedMessage, :key, :id)

    refs.compact.map(&:to_s).find(&:present?)
  end

  def handle_attach_media
    if message_type == 'location' && waha_location_message_present?
      # WAHA already embeds normalized coordinates in the message payload. Skipping
      # the generic media downloader prevents a duplicate, metadata-only location
      # attachment from being created (which would hide the proper map bubble).
      return
    end

    super
  end

  def waha_location_message_present?
    return false unless defined?(@raw_message) && message_type == 'location'

    message = ensure_indifferent_hash(@raw_message&.[](:message)) || {}
    message[:locationMessage].present? || message[:liveLocationMessage].present?
  end

  def safe_indifferent_hash(data)
    return if data.blank?

    hash = if data.respond_to?(:to_unsafe_h)
             data.to_unsafe_h
           else
             data
           end

    return unless hash.is_a?(Hash)

    hash.with_indifferent_access
  end

  def deep_indifferentize(value)
    case value
    when Hash
      value.each_with_object({}) do |(k, v), memo|
        memo[k] = deep_indifferentize(v)
      end.with_indifferent_access
    when Array
      value.map { |item| deep_indifferentize(item) }
    else
      value
    end
  end

  def normalize_waha_location(candidate)
    data = if candidate.respond_to?(:to_unsafe_h)
             candidate.to_unsafe_h
           elsif candidate.respond_to?(:to_hash)
             candidate.to_hash
           else
             candidate
           end

    return unless data.is_a?(Hash)

    normalized = deep_indifferentize(data)

    latitude = normalized[:degreesLatitude]
    latitude ||= normalized[:latitude] || normalized[:lat]
    longitude = normalized[:degreesLongitude]
    longitude ||= normalized[:longitude] || normalized[:lng] || normalized[:lon]

    return if latitude.blank? || longitude.blank?

    latitude_f = latitude.to_f
    longitude_f = longitude.to_f

    name = normalized[:name]
    name ||= normalized[:title] || normalized[:label]

    address = normalized[:address]
    address ||= normalized[:description]
    address ||= normalized[:addressLine] || normalized[:address_line]

    if name.blank? && address.blank?
      name = format('%<lat>.6f, %<lng>.6f', lat: latitude_f, lng: longitude_f)
    end

    url = normalized[:url]
    url ||= normalized[:mapUrl] || normalized[:map_url]
    url ||= normalized[:externalUrl] || normalized[:external_url]
    url ||= normalized[:link] || normalized[:googleMapsUri] || normalized[:google_maps_uri]
    url ||= "https://maps.google.com/?q=#{latitude_f},#{longitude_f}"

    jpeg_thumbnail = normalized[:jpegThumbnail] || normalized[:JPEGThumbnail]

    {
      degreesLatitude: latitude_f,
      degreesLongitude: longitude_f,
      name: name&.to_s&.strip.presence,
      address: address&.to_s&.strip.presence,
      url: url&.to_s&.strip.presence,
      accuracyInMeters: normalized[:accuracyInMeters] || normalized[:accuracy],
      speedMetersPerSecond: normalized[:speedMetersPerSecond],
      degreesClockwiseFromMagneticNorth: normalized[:degreesClockwiseFromMagneticNorth],
      jpegThumbnail: jpeg_thumbnail
    }.compact
  end

  def ack_status_transition_allowed?(current_status, new_status)
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

  def update_last_seen_from_ack(payload, status)
    conversation = @message.conversation
    return unless conversation&.contact

    raw_ts = payload[:timestamp] || payload.dig(:_data, :Timestamp)
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

  def waha_group_ack?(payload)
    data = ensure_indifferent_hash(payload) || {}

    remote_candidates = [
      data[:from],
      data[:remoteJid],
      data[:remote_jid],
      data[:chatId],
      data[:chat_id],
      data.dig(:key, :remoteJid),
      data.dig(:key, :remoteJID),
      data.dig(:_data, :Chat),
      data.dig(:_data, :Info, :Chat),
      @message&.conversation&.contact_inbox&.source_id
    ]

    remote_candidates.compact.any? { |jid| jid.to_s.ends_with?('@g.us') }
  end

  def update_contact_info(contact_data)
    contact_data = contact_data.with_indifferent_access

    raw_remote_jid = contact_data[:remoteJid]
    return unless raw_remote_jid

    fallback_candidates = [
      contact_data[:remoteJidAlt],
      contact_data[:participant],
      contact_data[:participantAlt],
      contact_data[:waid]
    ]
    normalized_remote_jid = resolved_direct_remote_jid(raw_remote_jid, fallback_candidates: fallback_candidates)
    remote_jid_for_lookup = normalized_remote_jid || raw_remote_jid

    push_name = contact_data[:pushName]
    profile_pic_url = extract_waha_profile_picture_url(contact_data)

    lid_variants = [contact_data[:remoteJid], contact_data[:remoteJidAlt], contact_data[:jid],
                    contact_data[:userJid], contact_data[:lidJid]]
                   .select { |value| lid_jid?(value) }
                   .compact
                   .uniq

    unless lid_variants.empty?
      plain_variants = [contact_data[:remoteJid], contact_data[:remoteJidAlt],
                        contact_data[:participant], contact_data[:participantAlt],
                        contact_data[:waid]]
                       .reject { |value| value.blank? || lid_jid?(value) }
                       .uniq

      plain_variants.each do |candidate|
        candidate_str = candidate.to_s
        next if candidate_str.ends_with?('@g.us')

        msisdn = normalize_whatsapp_number(candidate)
        next if msisdn.blank?

        lid_variants.each { |lid| store_lid_msisdn_mapping(lid, msisdn) }
      end

      if plain_variants.blank? && waha_provider?
        lid_variants.each do |lid|
          msisdn = lookup_lid_msisdn_from_provider(lid)
          next if msisdn.blank?

          store_lid_msisdn_mapping(lid, msisdn)
        end
      end
    end

    # Cache participant name for direct JIDs so group messages from mobile can show it
    if push_name.present? && remote_jid_for_lookup.to_s.match?(/@s\.whatsapp\.net|@c\.us/)
      store_participant_name(remote_jid_for_lookup, push_name)
    end

    # Find existing contact
    contact_inbox = nil
    candidate_source_ids_for_jid(remote_jid_for_lookup).each do |source_id|
      contact_inbox = inbox.contact_inboxes.find_by(source_id: source_id)
      break if contact_inbox
    end
    return unless contact_inbox

    contact = contact_inbox.contact

    # Update contact name if changed
    contact.update!(name: push_name) if push_name.present? && !remote_jid_for_lookup.to_s.ends_with?('@g.us') && (contact.name != push_name)

    # Update profile picture, fetching from WAHA if necessary
    additional = contact.additional_attributes || {}
    stored_url = additional['social_whatsapp_profile_pic_url']
    additional_changed = false

    if profile_pic_url.blank?
      if should_refresh_profile_picture?(contact: contact, stored_url: stored_url, additional: additional)
        force_refresh = force_profile_picture_refresh?(stored_url: stored_url, additional: additional)
        fetched_url = fetch_waha_profile_picture(contact_inbox.source_id, refresh: force_refresh)
        additional['waha_profile_pic_checked_at'] = Time.current.iso8601
        profile_pic_url = fetched_url if fetched_url.present?
        additional_changed = true
      end
    else
      if additional['waha_profile_pic_checked_at'].blank?
        additional['waha_profile_pic_checked_at'] = Time.current.iso8601
        additional_changed = true
      end
    end

    if profile_pic_url.present?
      if stored_url != profile_pic_url || !contact.avatar.attached?
        additional['social_whatsapp_profile_pic_url'] = profile_pic_url
        contact.update!(additional_attributes: additional)
        Avatar::AvatarFromUrlJob.perform_later(contact, profile_pic_url)
      elsif additional_changed
        contact.update!(additional_attributes: additional)
      end
    elsif additional_changed
      contact.update!(additional_attributes: additional)
    end
  rescue StandardError => e
    Rails.logger.error "Waha API: Failed to update contact info: #{e.message}"
  end

  # Chats update/upsert may include display names/subjects for groups
  def process_chats_event
    chats = processed_params[:data]
    chats = [chats] unless chats.is_a?(Array)

    chats.each do |chat|
      handle_chat_metadata(chat)
    rescue StandardError => e
      Rails.logger.warn "Waha API: Failed to process chat metadata: #{e.message}"
    end
  end

  def handle_chat_metadata(chat)
    remote_jid = chat[:id] || chat[:jid] || chat[:remoteJid]
    return if remote_jid.blank?

    contact_inbox = inbox.contact_inboxes.find_by(source_id: remote_jid)
    return unless contact_inbox

    contact = contact_inbox.contact

    # Prefer explicit group/chat subject fields
    subject = chat[:name] || chat[:subject] || chat[:title]

    if remote_jid.to_s.ends_with?('@g.us')
      if subject.present?
        store_group_subject(remote_jid, subject)
        display_name = formatted_group_display_name(subject, remote_jid)
        contact.update!(name: display_name) unless names_equivalent?(contact.name, display_name)
      end
    elsif subject.present? && contact.name != subject
      # For 1:1 chats, name might be present as :name
      contact.update!(name: subject)
    end

    # Update avatar if provided
    profile_pic_url = chat[:profilePicUrl] || chat[:picUrl] || chat[:imgUrl]
    return unless profile_pic_url.present?

    additional = contact.additional_attributes || {}
    stored_url = additional['social_whatsapp_profile_pic_url']

    return unless stored_url != profile_pic_url || !contact.avatar.attached?

    contact.update!(additional_attributes: additional.merge('social_whatsapp_profile_pic_url' => profile_pic_url))
    Avatar::AvatarFromUrlJob.perform_later(contact, profile_pic_url)
  end

  # GROUPS event usually carries the canonical subject for group JIDs
  def process_groups_event
    groups = processed_params[:data]
    groups = [groups] unless groups.is_a?(Array)

    groups.each do |grp|
      remote_jid = grp[:id] || grp[:jid] || grp[:remoteJid]
      next if remote_jid.blank?

      subject = grp[:subject] || grp[:name] || grp[:title]
      store_group_subject(remote_jid, subject) if subject.present?

      contact_inbox = inbox.contact_inboxes.find_by(source_id: remote_jid)
      next unless contact_inbox && subject.present?

      contact = contact_inbox.contact
      display_name = formatted_group_display_name(subject, remote_jid)
      contact.update!(name: display_name) unless names_equivalent?(contact.name, display_name)
    rescue StandardError => e
      Rails.logger.warn "Waha API: Failed to process group metadata: #{e.message}"
    end
  end

  WAHA_TRUE_STRINGS = %w[true t 1 yes y on].freeze
  WAHA_FALSE_STRINGS = %w[false f 0 no n off].freeze

  def normalize_waha_from_me(value)
    boolean = waha_boolean_value(value)
    boolean.nil? ? value : boolean
  end

  def waha_boolean_value(value)
    case value
    when true, false
      value
    when NilClass
      nil
    when String
      normalized = value.strip.downcase
      return true if WAHA_TRUE_STRINGS.include?(normalized)
      return false if WAHA_FALSE_STRINGS.include?(normalized)
      nil
    when Numeric
      !value.zero?
    else
      nil
    end
  end
end
