module Whatsapp::WuzapiHandlers::DeliveryCallback
  include Whatsapp::WuzapiHandlers::Helpers

  private

  def process_delivery_callback
    # Wuzapi sends ReadReceipt events when messages are delivered/read
    # Update message status based on receipt type
    receipt_type = processed_params.dig('type')
    return unless receipt_type == 'ReadReceipt'

    message_ids = processed_params.dig('event', 'MessageIds') || []
    return if message_ids.empty?

    message_ids.each do |message_id|
      message = inbox.messages.find_by(source_id: message_id)
      next unless message

      # Mark as delivered (Wuzapi doesn't distinguish between delivered and read in basic webhook)
      message.update!(status: :delivered) if message.status == 'sent'
    end
  end
end
