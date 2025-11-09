module Whatsapp::EvolutionHandlers::MessagesUpdate
  include Whatsapp::EvolutionHandlers::Helpers

  class MessageNotFoundError < StandardError; end

  private

  def process_messages_update
    # Evolution API v2.3.1 sends update data directly in 'data' field
    update_data = processed_params[:data]
    return if update_data.blank?

    # Handle both single update and array of updates
    updates = update_data.is_a?(Array) ? update_data : [update_data]

    updates.each do |update|
      @message = nil
      @raw_message = update

      begin
        if incoming?
          handle_update
        else
          # Handle outgoing message status updates
          handle_outgoing_update
        end
      rescue StandardError => e
        Rails.logger.error "Evolution API: Error processing message update: #{e.message}"
        Rails.logger.error "Evolution API: Update data: #{update.inspect}"
        Rails.logger.error e.backtrace.first(5).join("\n")
      end
    end
  end

  def handle_update
    unless find_message_by_source_id(raw_message_id)
      lookup_ids = evolution_lookup_ids(@raw_message, raw_message_id)
      unless lookup_ids.any? { |candidate| find_message_by_source_id(candidate) }
        Rails.logger.warn "Evolution API: Message not found for update: #{raw_message_id}"
        return
      end
    end

    store_waha_message_ids!(@message, @raw_message)

    update_status if @raw_message[:status].present?
    return unless @raw_message[:editedMessage].present? || @raw_message[:message].present?

    handle_edited_content
  end

  def handle_outgoing_update
    unless find_message_by_source_id(raw_message_id)
      lookup_ids = evolution_lookup_ids(@raw_message, raw_message_id)
      found_via_lookup = lookup_ids.any? { |candidate| find_message_by_source_id(candidate) }

      unless found_via_lookup
        # Fallback: locate the most recent outgoing message in the same conversation
        # and bind the external id, then proceed with status update.
        if try_bind_outgoing_message_by_conversation(raw_message_id)
          Rails.logger.info "Evolution API: Bound outgoing message #{raw_message_id} to latest conversation message"
        else
          Rails.logger.warn "Evolution API: Outgoing message not found for update: #{raw_message_id}"
          return
        end
      end
    end

    store_waha_message_ids!(@message, @raw_message)

    update_status if @raw_message[:status].present?
    return unless @raw_message[:editedMessage].present? || @raw_message[:message].present?

    handle_edited_content
  end

  def update_status
    status = map_provider_status(@raw_message)
    return if status.blank?

    update_last_seen_at(status) if %w[read delivered].include?(status)

    if status_transition_allowed?(status)
      @message.update!(status: status)
    else
      Rails.logger.warn "Evolution API: Status transition not allowed: #{@message.status} -> #{status}"
    end
  end

  def handle_edited_content
    new_content = extract_edited_content
    return if new_content.blank?

    # Store the edit information
    content_attributes = @message.content_attributes || {}
    content_attributes[:edited] = true
    content_attributes[:edit_timestamp] = Time.current.to_i
    content_attributes[:original_content] = @message.content

    edited_text = "\u270D\uFE0F #{I18n.t('conversations.messages.edited')}\n#{new_content}"

    @message.update!(
      content: edited_text,
      content_attributes: content_attributes
    )
  end

  def extract_edited_content
    msg = @raw_message[:editedMessage] || @raw_message[:message]
    return unless msg

    # Extract content from edited message following same pattern as original messages
    msg[:conversation] ||
      msg.dig(:extendedTextMessage, :text) ||
      msg.dig(:imageMessage, :caption) ||
      msg.dig(:videoMessage, :caption) ||
      msg.dig(:documentMessage, :caption)
  end

  def status_transition_allowed?(new_status)
    # Define allowed status transitions to prevent invalid updates
    current_status = @message.status

    case current_status
    when 'sent'
      %w[delivered read failed].include?(new_status)
    when 'delivered'
      %w[read].include?(new_status)
    when 'read'
      false # Read is final status
    when 'failed'
      false # Failed is final status
    else
      true # Allow any transition from unknown/nil status
    end
  end

  # Attempts to find a conversation by remoteJid and bind the update's id
  # to the latest unsourced outgoing message so that subsequent updates work.
  def try_bind_outgoing_message_by_conversation(external_id)
    jid = @raw_message[:remoteJid] || @raw_message.dig(:key, :remoteJid)
    return false if jid.blank?

    # Determine contact_inbox by jid type
    target_source_id = if jid.to_s.ends_with?('@g.us')
                         jid
                       else
                         jid.split('@').first
                       end

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
    store_waha_message_ids!(@message, @raw_message)
    true
  rescue StandardError => e
    Rails.logger.warn "Evolution API: Fallback bind failed: #{e.message}"
    false
  end

  def update_last_seen_at(status)
    conversation = @message.conversation
    return unless conversation&.contact

    raw_ts = @raw_message[:timestamp] || processed_params[:date_time]
    timestamp = if raw_ts
                  begin
                    Time.zone.parse(raw_ts.to_s)
                  rescue StandardError
                    Time.current
                  end
                else
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
end
