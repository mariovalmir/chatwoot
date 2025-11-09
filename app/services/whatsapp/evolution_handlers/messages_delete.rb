module Whatsapp::EvolutionHandlers::MessagesDelete
  include Whatsapp::EvolutionHandlers::Helpers

  private

  def process_messages_delete
    delete_data = processed_params[:data]
    return if delete_data.blank?

    deletes = delete_data.is_a?(Array) ? delete_data : [delete_data]

    deletes.each do |del|
      @message = nil
      @raw_message = del
      handle_delete
    rescue StandardError => e
      Rails.logger.error "Evolution API: Error processing message delete: #{e.message}"
      Rails.logger.error "Evolution API: Delete data: #{del.inspect}"
      Rails.logger.error e.backtrace.first(5).join("\n")
    end
  end

  def handle_delete
    unless find_message_by_source_id(raw_message_id)
      lookup_ids = evolution_lookup_ids(@raw_message, raw_message_id)
      unless lookup_ids.any? { |candidate| find_message_by_source_id(candidate) }
        Rails.logger.warn "Evolution API: Message not found for delete: #{raw_message_id}"
        return
      end
    end

    # Avoid duplicating deleted prefix/content if already processed
    attrs = (@message.content_attributes || {})
    already_deleted = attrs['deleted'] || attrs[:deleted]
    return if already_deleted

    deleted_prefix = "\u26D4 #{I18n.t('conversations.messages.deleted')}"
    original_content = @message.content
    content_attrs = @message.content_attributes || {}
    content_attrs[:deleted] = true
    @message.attachments.destroy_all
    show_original = @message.inbox&.show_deleted_message_placeholder
    new_content = show_original ? "#{deleted_prefix}\n#{original_content}" : deleted_prefix
    @message.update!(content: new_content,
                     content_type: :text,
                     content_attributes: content_attrs)
  end
end
