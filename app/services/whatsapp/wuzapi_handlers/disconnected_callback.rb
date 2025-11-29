module Whatsapp::WuzapiHandlers::DisconnectedCallback
  include Whatsapp::WuzapiHandlers::Helpers

  private

  def process_disconnected_callback
    inbox.channel.update_provider_connection!(connection: 'close')
  end
end
