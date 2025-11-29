module Whatsapp::WuzapiHandlers::MessageStatusCallback
  include Whatsapp::WuzapiHandlers::Helpers

  private

  def process_message_status_callback
    # Handle message status updates (sent, delivered, read, failed)
    status_data = processed_params.dig('event')
    return unless status_data

    message_id = status_data['Id'] || status_data['messageId']
    return unless message_id

    message = inbox.messages.find_by(source_id: message_id)
    return unless message

    # Update status based on Wuzapi status
    status = status_data['Status']&.downcase
    case status
    when 'sent'
      message.update!(status: :sent)
    when 'delivered'
      message.update!(status: :delivered)
    when 'read'
      message.update!(status: :read)
    when 'failed', 'error'
      message.update!(status: :failed, external_error: status_data['Error'] || 'Message failed')
    end
  end
end
