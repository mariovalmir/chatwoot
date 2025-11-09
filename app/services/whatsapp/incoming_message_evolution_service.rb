class Whatsapp::IncomingMessageEvolutionService < Whatsapp::IncomingMessageBaseService
  include Whatsapp::EvolutionHandlers::MessagesUpsert
  include Whatsapp::EvolutionHandlers::MessagesUpdate
  include Whatsapp::EvolutionHandlers::MessagesDelete
  include Whatsapp::EvolutionHandlers::Helpers

  EVENT_HANDLERS = {
    'messages.upsert' => :process_messages_upsert,
    'send.message' => :process_messages_upsert,
    # Some Evolution setups send outgoing status updates via this event
    'send.message.update' => :process_messages_update,
    'send.message_update' => :process_messages_update,
    'messages.update' => :process_messages_update,
    'messages.edited' => :process_messages_update,
    'messages.delete' => :process_messages_delete,
    'contacts.update' => :process_contacts_update,
    'chats.update' => :process_chats_event,
    'chats.upsert' => :process_chats_event,
    'groups.update' => :process_groups_event,
    'groups.upsert' => :process_groups_event,
    'qrcode.updated' => :handle_qrcode_updated,
    'connection.update' => :handle_connection_update
  }.freeze

  CHAT_EVENTS = %w[chats.update chats.upsert].freeze

  def perform
    event_type = processed_params[:event]

    if EVENT_HANDLERS.key?(event_type)
      send(EVENT_HANDLERS[event_type])
    elsif CHAT_EVENTS.include?(event_type)
      process_chats_event
    else
      Rails.logger.warn "Evolution API: Unsupported event type: #{event_type}"
    end
  end

  def handle_qrcode_updated
    Dispatcher.dispatch(
      Events::Types::WHATSAPP_QRCODE_UPDATED,
      Time.current,
      inbox: inbox,
      qr_code: processed_params.dig(:data, :qrcode, :base64)
    )
  end

  def handle_connection_update
    status = processed_params.dig(:data, :state) ||
             processed_params.dig(:data, :connection_status) ||
             processed_params.dig(:data, :connectionStatus) ||
             processed_params.dig(:data, :status)
    status ||= 'close'

    inbox.channel.update_provider_connection!(connection: status)

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

    inbox.channel.update_provider_connection!(connection: status)

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
    # Evolution API sends contact updates when contact info changes (name, profile pic, etc.)
    contacts = processed_params[:data]
    contacts = [contacts] unless contacts.is_a?(Array)

    contacts.each do |contact_data|
      update_contact_info(contact_data)
    end

    # Some contact update events also include connection status information
    update_connection_status_from(processed_params[:data])
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
    profile_pic_url = contact_data[:profilePicUrl]

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

    # Update profile picture if url provided
    if profile_pic_url.present?
      additional = contact.additional_attributes || {}
      stored_url = additional['social_whatsapp_profile_pic_url']

      if stored_url != profile_pic_url || !contact.avatar.attached?
        contact.update!(additional_attributes: additional.merge('social_whatsapp_profile_pic_url' => profile_pic_url))
        Avatar::AvatarFromUrlJob.perform_later(contact, profile_pic_url)
      end
    end
  rescue StandardError => e
    Rails.logger.error "Evolution API: Failed to update contact info: #{e.message}"
  end

  # Chats update/upsert may include display names/subjects for groups
  def process_chats_event
    chats = processed_params[:data]
    chats = [chats] unless chats.is_a?(Array)

    chats.each do |chat|
      handle_chat_metadata(chat)
    rescue StandardError => e
      Rails.logger.warn "Evolution API: Failed to process chat metadata: #{e.message}"
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
      store_group_subject(remote_jid, subject) if subject.present?
      if subject.present?
        desired_name = "#{subject} (GROUP)"
        contact.update!(name: desired_name) if contact.name != desired_name
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
      desired_name = "#{subject} (GROUP)"
      contact.update!(name: desired_name) if contact.name != desired_name
    rescue StandardError => e
      Rails.logger.warn "Evolution API: Failed to process group metadata: #{e.message}"
    end
  end
end
