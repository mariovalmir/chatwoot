class Whatsapp::Providers::WhatsappWuzapiService < Whatsapp::Providers::BaseService # rubocop:disable Metrics/ClassLength
  class ProviderUnavailableError < StandardError; end

  DEFAULT_URL = ENV.fetch('WUZAPI_PROVIDER_DEFAULT_URL', nil)
  DEFAULT_ADMIN_TOKEN = ENV.fetch('WUZAPI_ADMIN_TOKEN', nil)

  def send_template(phone_number, template_info); end

  def sync_templates; end

  def send_message(phone, message)
    phone = phone.delete('+')
    params = message.content_attributes[:wuzapi_args].presence || {}

    if message.content_attributes[:is_reaction]
      send_reaction_message(phone, message, **params)
    elsif message.attachments.present?
      handle_message_with_attachment(message, phone, **params)
    elsif message.content.present?
      send_text_message(phone, message, **params)
    else
      message.update!(is_unsupported: true)
      nil
    end
  end

  def validate_provider_config?
    # Se só tem admin token, criar usuário automaticamente
    ensure_user_created if needs_user_creation?

    response = HTTParty.get(
      "#{api_base_url}/session/status",
      headers: api_headers,
      timeout: 15
    )

    process_response(response)
  rescue HTTParty::Error, Net::OpenTimeout => e
    Rails.logger.error "Wuzapi validation error: #{e.message}"
    false
  end

  def setup_channel_provider
    # Subscribe to events and connect
    response = HTTParty.post(
      "#{api_base_url}/session/connect",
      headers: api_headers,
      body: {
        Subscribe: %w[Message ReadReceipt ChatPresence],
        Immediate: false
      }.to_json,
      timeout: 30
    )

    # Verifica se teve sucesso ou se já está conectado
    already_connected = response.parsed_response.dig('error')&.include?('already connected')
    
    unless process_response(response) || already_connected
      Rails.logger.error "Wuzapi connect failed: #{response.code} - #{response.body}"
      raise ProviderUnavailableError
    end

    # Configure webhook (sempre tenta configurar)
    configure_webhook

    # Check status and QR code
    status_response = HTTParty.get(
      "#{api_base_url}/session/status",
      headers: api_headers,
      timeout: 15
    )

    if status_response.success?
      data = status_response.parsed_response.dig('data')
      Rails.logger.info "Wuzapi raw status response: #{status_response.parsed_response.inspect}"
      Rails.logger.info "Wuzapi session data: #{data.inspect}"
      Rails.logger.info "Wuzapi session status: connected=#{data&.dig('connected')}, loggedIn=#{data&.dig('loggedIn')}"
      
      if data && data['connected'] && !data['loggedIn']
        # Precisa escanear QR code
        Rails.logger.info "Wuzapi: Enqueuing QR code job for channel #{whatsapp_channel.id}"
        Channels::Whatsapp::WuzapiQrCodeJob.perform_later(whatsapp_channel)
      elsif data && data['loggedIn']
        # Já está autenticado
        Rails.logger.info "Wuzapi: Channel #{whatsapp_channel.id} already logged in"
        whatsapp_channel.update_provider_connection!(connection: 'open')
      elsif !data || !data['connected']
        # Precisa conectar primeiro, enfileira job do QR
        Rails.logger.info "Wuzapi: Not connected, enqueuing QR code job for channel #{whatsapp_channel.id}"
        Channels::Whatsapp::WuzapiQrCodeJob.perform_later(whatsapp_channel)
      end
    end

    true
  rescue HTTParty::Error, Net::OpenTimeout => e
    Rails.logger.error "Wuzapi setup error: #{e.message}"
    raise ProviderUnavailableError
  end

  def disconnect_channel_provider
    response = HTTParty.post(
      "#{api_base_url}/session/disconnect",
      headers: api_headers,
      timeout: 15
    )

    raise ProviderUnavailableError unless process_response(response)

    true
  rescue HTTParty::Error, Net::OpenTimeout => e
    Rails.logger.error "Wuzapi disconnect error: #{e.message}"
    raise ProviderUnavailableError
  end

  def qr_code_image
    response = HTTParty.get(
      "#{api_base_url}/session/qr",
      headers: api_headers,
      timeout: 15
    )

    return unless process_response(response)

    qr_data = response.parsed_response.dig('data', 'QRCode')

    # Check if already logged in
    if qr_data.blank?
      status_response = HTTParty.get(
        "#{api_base_url}/session/status",
        headers: api_headers,
        timeout: 15
      )

      if status_response.success? && status_response.parsed_response.dig('data', 'loggedIn')
        whatsapp_channel.update_provider_connection!(connection: 'open')
        return
      end
    end

    qr_data
  rescue HTTParty::Error, Net::OpenTimeout => e
    Rails.logger.error "Wuzapi QR code error: #{e.message}"
    nil
  end

  def read_messages(messages, phone_number:, **)
    message_ids = messages.map(&:source_id).compact

    return true if message_ids.empty?

    response = HTTParty.post(
      "#{api_base_url}/chat/markread",
      headers: api_headers,
      body: {
        Id: message_ids,
        ChatPhone: phone_number.delete('+')
      }.to_json,
      timeout: 15
    )

    raise ProviderUnavailableError unless process_response(response)

    true
  rescue HTTParty::Error, Net::OpenTimeout => e
    Rails.logger.error "Wuzapi mark read error: #{e.message}"
    raise ProviderUnavailableError
  end

  def on_whatsapp(phone_number)
    response = HTTParty.post(
      "#{api_base_url}/user/check",
      headers: api_headers,
      body: {
        Phone: [phone_number.delete('+')]
      }.to_json,
      timeout: 15
    )

    raise ProviderUnavailableError unless process_response(response)

    users = response.parsed_response.dig('data', 'Users') || []
    user = users.first || {}

    {
      'exists' => user['IsInWhatsapp'] || false,
      'phone' => user['JID'],
      'verified_name' => user['VerifiedName']
    }
  rescue HTTParty::Error, Net::OpenTimeout => e
    Rails.logger.error "Wuzapi on_whatsapp error: #{e.message}"
    { 'exists' => false, 'phone' => nil, 'verified_name' => nil }
  end

  private

  def api_base_url
    url = DEFAULT_URL || 'http://localhost:8080'
    url = "https://#{url}" unless url.match?(%r{^https?://})
    url
  end

  def api_headers
    token = user_token
    { 'Content-Type' => 'application/json', 'Token' => token }
  end

  def admin_token
    DEFAULT_ADMIN_TOKEN
  end

  def user_token
    whatsapp_channel.provider_config['user_token'].presence
  end

  def needs_user_creation?
    # Se tem admin token mas não tem user token, precisa criar
    admin_token.present? && user_token.blank?
  end

  def ensure_user_created
    return if user_token.present?
    return unless admin_token.present?

    # Gera token único para o usuário (usa timestamp já que inbox_id ainda não existe)
    generated_token = "chatwoot_#{Time.now.to_i}_#{SecureRandom.hex(8)}"
    
    # Cria usuário no Wuzapi
    response = HTTParty.post(
      "#{api_base_url}/admin/users",
      headers: {
        'Content-Type' => 'application/json',
        'Authorization' => admin_token
      },
      body: {
        name: "Chatwoot #{whatsapp_channel.phone_number || 'Inbox'}",
        token: generated_token
      }.to_json,
      timeout: 15
    )

    if response.success?
      # Salva o user token gerado no provider_config
      whatsapp_channel.provider_config['user_token'] = generated_token
      whatsapp_channel.save!
      Rails.logger.info "Wuzapi user created: #{generated_token}"
    else
      Rails.logger.error "Failed to create Wuzapi user: #{response.code} - #{response.body}"
      raise ProviderUnavailableError, "Failed to create Wuzapi user"
    end
  rescue HTTParty::Error, Net::OpenTimeout => e
    Rails.logger.error "Wuzapi user creation error: #{e.message}"
    raise ProviderUnavailableError, "Failed to create Wuzapi user: #{e.message}"
  end

  def process_response(response)
    unless response.success?
      Rails.logger.error "Wuzapi API error: #{response.code} - #{response.body}"
      return false
    end

    parsed = response.parsed_response
    parsed.is_a?(Hash) && (parsed['success'] == true || parsed['code'] == 200)
  end

  def configure_webhook
    # Use custom webhook base URL if provided, otherwise use default
    base_url = ENV.fetch('WUZAPI_WEBHOOK_BASE_URL', nil) || whatsapp_channel.inbox.account.domain
    webhook_path = Rails.application.routes.url_helpers.webhooks_whatsapp_path(
      phone_number: whatsapp_channel.phone_number
    )
    webhook_url = "#{base_url}#{webhook_path}"

    Rails.logger.info "Wuzapi: Configuring webhook URL: #{webhook_url}"

    response = HTTParty.post(
      "#{api_base_url}/webhook",
      headers: api_headers,
      body: {
        webhookURL: webhook_url
      }.to_json,
      timeout: 15
    )

    raise ProviderUnavailableError unless process_response(response)
  end

  def send_text_message(phone, message, **params)
    response = HTTParty.post(
      "#{api_base_url}/chat/send/text",
      headers: api_headers,
      body: {
        Phone: phone,
        Body: message.content,
        **params
      }.compact.to_json,
      timeout: 30
    )

    unless process_response(response)
      message.update!(status: :failed, external_error: response.parsed_response&.dig('error'))
      raise ProviderUnavailableError
    end

    response.parsed_response&.dig('data', 'Id')
  rescue HTTParty::Error, Net::OpenTimeout => e
    message.update!(status: :failed, external_error: e.message)
    Rails.logger.error "Wuzapi send text error: #{e.message}"
    raise ProviderUnavailableError
  end

  def handle_message_with_attachment(message, phone, **params)
    attachment = message.attachments.first

    if attachment.file.byte_size > max_size(attachment)
      message.update!(status: :failed, external_error: 'File too large')
      return
    end

    base64_data = Base64.strict_encode64(attachment.file.download)
    buffer = "data:#{attachment.file.content_type};base64,#{base64_data}"

    case attachment.file_type
    when 'image'
      send_image_message(phone, message, buffer, **params)
    when 'audio'
      send_audio_message(phone, message, buffer, **params)
    when 'file'
      send_document_message(phone, message, attachment, buffer, **params)
    when 'video'
      send_video_message(phone, message, buffer, **params)
    end
  rescue StandardError => e
    message.update!(status: :failed, external_error: e.message)
    Rails.logger.error "Wuzapi attachment error: #{e.message}"
    raise ProviderUnavailableError
  end

  def max_size(attachment)
    case attachment.file_type
    when 'image'
      5.megabytes
    when 'audio', 'video'
      16.megabytes
    else
      100.megabytes
    end
  end

  def send_image_message(phone, message, buffer, **params)
    response = HTTParty.post(
      "#{api_base_url}/chat/send/image",
      headers: api_headers,
      body: {
        Phone: phone,
        Image: buffer,
        Caption: message.content,
        **params
      }.compact.to_json,
      timeout: 30
    )

    raise ProviderUnavailableError unless process_response(response)

    response.parsed_response&.dig('data', 'Id')
  end

  def send_audio_message(phone, _message, buffer, **params)
    response = HTTParty.post(
      "#{api_base_url}/chat/send/audio",
      headers: api_headers,
      body: {
        Phone: phone,
        Audio: buffer,
        **params
      }.compact.to_json,
      timeout: 30
    )

    raise ProviderUnavailableError unless process_response(response)

    response.parsed_response&.dig('data', 'Id')
  end

  def send_document_message(phone, message, attachment, buffer, **params)
    file_name = attachment.file.filename.to_s
    file_name = 'document.bin' if file_name.blank?

    response = HTTParty.post(
      "#{api_base_url}/chat/send/document",
      headers: api_headers,
      body: {
        Phone: phone,
        Document: buffer,
        FileName: file_name,
        Caption: message.content,
        **params
      }.compact.to_json,
      timeout: 30
    )

    raise ProviderUnavailableError unless process_response(response)

    response.parsed_response&.dig('data', 'Id')
  end

  def send_video_message(phone, message, buffer, **params)
    response = HTTParty.post(
      "#{api_base_url}/chat/send/video",
      headers: api_headers,
      body: {
        Phone: phone,
        Video: buffer,
        Caption: message.content,
        **params
      }.compact.to_json,
      timeout: 30
    )

    raise ProviderUnavailableError unless process_response(response)

    response.parsed_response&.dig('data', 'Id')
  end

  def send_reaction_message(phone, message, **params)
    response = HTTParty.post(
      "#{api_base_url}/chat/react",
      headers: api_headers,
      body: {
        Phone: phone,
        Body: message.content,
        Id: message.in_reply_to_external_id,
        **params
      }.compact.to_json,
      timeout: 15
    )

    raise ProviderUnavailableError unless process_response(response)

    response.parsed_response&.dig('data', 'Id')
  end
end
