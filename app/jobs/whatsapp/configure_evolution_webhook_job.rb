class Whatsapp::ConfigureEvolutionWebhookJob < ApplicationJob
  queue_as :default

  def perform(channel_id)
    channel = Channel::Whatsapp.find_by(id: channel_id)
    return unless channel&.provider == 'evolution'

    service = channel.provider_service
    webhook_result = service.send(:configure_webhook)
    
    unless webhook_result.is_a?(Hash) && webhook_result[:ok]
      Rails.logger.warn "Evolution API webhook configuration warning: #{webhook_result[:error]}"
    end
  rescue StandardError => e
    Rails.logger.error "Evolution API webhook job error: #{e.message}"
  end
end
