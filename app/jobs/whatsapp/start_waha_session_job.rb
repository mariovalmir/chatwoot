class Whatsapp::StartWahaSessionJob < ApplicationJob
  queue_as :default

  def perform(channel_id)
    channel = Channel::Whatsapp.find_by(id: channel_id)
    return unless channel&.provider == 'waha'

    service = channel.provider_service
    service.send(:start_session)
  rescue StandardError => e
    Rails.logger.error "WAHA session start job error: #{e.message}"
  end
end
