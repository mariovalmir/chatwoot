class Whatsapp::IncomingMessageWuzapiService < Whatsapp::IncomingMessageBaseService
  include Events::Types
  include Whatsapp::WuzapiHandlers::ConnectedCallback
  include Whatsapp::WuzapiHandlers::DisconnectedCallback
  include Whatsapp::WuzapiHandlers::ReceivedCallback
  include Whatsapp::WuzapiHandlers::DeliveryCallback
  include Whatsapp::WuzapiHandlers::MessageStatusCallback

  def perform
    event_type = processed_params[:type] || processed_params['type']

    return if event_type.blank?

    Rails.configuration.dispatcher.dispatch(
      PROVIDER_EVENT_RECEIVED,
      Time.zone.now,
      inbox: inbox,
      event: event_type,
      payload: processed_params
    )

    # Route to appropriate handler
    case event_type
    when 'Message'
      process_received_callback
    when 'ReadReceipt'
      process_delivery_callback
    when 'ChatPresence'
      # Handle presence updates (typing, online, etc) - optional
      Rails.logger.info "Wuzapi: ChatPresence event received"
    when 'HistorySync'
      # Handle history sync - optional
      Rails.logger.info "Wuzapi: HistorySync event received"
    when 'Connected'
      process_connected_callback
    when 'Disconnected'
      process_disconnected_callback
    when 'MessageStatus'
      process_message_status_callback
    else
      Rails.logger.warn "Wuzapi: Unsupported event type: #{event_type}"
    end
  end
end
