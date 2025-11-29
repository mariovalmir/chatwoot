module Whatsapp::WuzapiHandlers::ConnectedCallback
  include Whatsapp::WuzapiHandlers::Helpers

  private

  def process_connected_callback
    # Wuzapi sends status updates via webhook
    # Update connection status when logged in
    if processed_params.dig('data', 'LoggedIn')
      inbox.channel.update_provider_connection!(connection: 'open')
    end
  end
end
