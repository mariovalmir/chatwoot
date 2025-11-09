module Whatsapp
  module Waha
    module HelperMethods
      PROFILE_PIC_LOOKUP_KEYS = %w[
        profilepicurl profile_picture_url profilepictureurl profilepicture
        avatarurl avatar_url
        profilephoto profilephotourl profile_photo_url
        url
      ].freeze

      PROFILE_PIC_THUMB_KEYS = %w[eurl url link previewurl preview_url].freeze

      private

      def waha_provider?
        inbox&.channel&.provider == 'waha'
      end

      def lookup_lid_msisdn_from_provider(lid_jid)
        return unless waha_provider?

        provider = inbox.channel.provider_service
        mapped = provider.lookup_lid(lid_jid)
        normalize_whatsapp_number(mapped)
      rescue StandardError => e
        Rails.logger.warn "Waha lid lookup failed for #{lid_jid}: #{e.message}"
        nil
      end

      def fetch_waha_group_metadata(group_jid)
        return unless waha_provider?

        provider = inbox.channel.provider_service
        provider.fetch_group_metadata(group_jid)
      rescue StandardError => e
        Rails.logger.warn "Waha group metadata fetch failed for #{group_jid}: #{e.message}"
        nil
      end

      def fetch_waha_group_profile_picture(group_jid, refresh: false)
        return unless waha_provider?

        provider = inbox.channel.provider_service
        provider.fetch_group_profile_picture(group_jid, refresh: refresh)
      rescue StandardError => e
        Rails.logger.warn "Waha group picture fetch failed for #{group_jid}: #{e.message}"
        nil
      end

      def extract_waha_profile_picture_url(payload)
        return if payload.blank?

        data = payload.respond_to?(:to_unsafe_h) ? payload.to_unsafe_h : payload
        return extract_profile_picture_from_hash(data) unless data.is_a?(Array)

        data.filter_map do |item|
          extract_waha_profile_picture_url(item.respond_to?(:to_unsafe_h) ? item.to_unsafe_h : item)
        end&.find(&:present?)
      end

      def extract_profile_picture_from_hash(data)
        return if data.blank? || !data.is_a?(Hash)

        candidates = collect_profile_picture_candidates(data)

        thumb = data['profilePicThumbObj'] || data[:profilePicThumbObj] || data['profilePicThumb'] || data[:profilePicThumb]
        thumb = thumb.to_unsafe_h if thumb.respond_to?(:to_unsafe_h)
        candidates += collect_profile_picture_candidates(thumb, thumb: true) if thumb.is_a?(Hash)

        nested = data['data'] || data[:data]
        candidates << extract_waha_profile_picture_url(nested) if nested.present?

        Array(candidates).flatten.compact.map(&:to_s).find(&:present?)
      end

      def collect_profile_picture_candidates(hash, thumb: false)
        normalized = hash.each_with_object({}) do |(key, value), memo|
          memo[key.to_s.downcase] ||= value
        end

        keys = thumb ? PROFILE_PIC_THUMB_KEYS : PROFILE_PIC_LOOKUP_KEYS

        keys.map { |lookup_key| normalized[lookup_key] }
      end

      def store_waha_message_ids!(message, raw_payload = nil)
        return unless message

        payload = raw_payload || @raw_message
        ids = Array.wrap(extract_waha_message_ids(payload))
        return if ids.blank?

        external_ids = (message.external_source_ids || {}).dup
        existing = Array.wrap(external_ids['waha_message_ids']).map(&:to_s)
        merged = (existing + ids.map(&:to_s)).reject(&:blank?).uniq

        return if merged == existing && external_ids['waha_message_id'].present?

        external_ids['waha_message_ids'] = merged
        external_ids['waha_message_id'] ||= merged.first

        if external_ids != message.external_source_ids
          message.update_column(:external_source_ids, external_ids)
        end
      rescue StandardError => e
        Rails.logger.warn "Waha API: Failed to store message ids for #{message&.id}: #{e.message}"
      end

      def extract_waha_message_ids(raw_payload)
        payload = ensure_indifferent_hash(raw_payload)
        return [] unless payload.is_a?(Hash)

        ids = []
        ids.concat(Array.wrap(payload[:waha_message_ids]))
        ids << payload[:waha_message_id]
        ids << payload[:messageId]
        ids << payload[:id]
        ids << payload[:keyId]
        ids << payload.dig(:key, :id)
        ids << payload.dig(:key, :ID)

        protocol_message = ensure_indifferent_hash(payload[:message])
        if protocol_message.present?
          ids << protocol_message[:stanzaId]
          ids << protocol_message[:stanzaID]
          ids << protocol_message[:id]
          ids << protocol_message.dig(:key, :id)
        end

        protocol_key = ensure_indifferent_hash(payload[:key])
        if protocol_key.present?
          ids << protocol_key[:id]
          ids << protocol_key[:ID]
          ids << protocol_key[:stanzaId]
          ids << protocol_key[:stanzaID]
          ids << protocol_key[:editedMessageId]
          ids << protocol_key[:edited_message_id]
        end

        if protocol_message.present?
          ids << protocol_message[:editedMessageId]
          ids << protocol_message[:edited_message_id]
          ids.concat(Array.wrap(protocol_message[:editedMessageIds]))
          ids.concat(Array.wrap(protocol_message[:edited_message_ids]))
          ids << protocol_message.dig(:editedMessage, :key, :id)
          ids << protocol_message.dig(:editedMessage, :key, :ID)
        end

        data = ensure_indifferent_hash(payload[:_data])
        ids.concat(Array.wrap(data&.[](:MessageIDs)))
        ids.concat(Array.wrap(data&.[](:messageIDs)))
        ids.concat(Array.wrap(data&.[](:messageIds)))

        info = ensure_indifferent_hash(data&.[](:Info))
        if info.present?
          ids << info[:ID]
          ids << info[:Id]
          ids << info[:id]
          ids << info[:MessageID]
          ids << info[:messageId]
          ids << info[:ServerID]
        end

        ids.compact!
        ids.map!(&:to_s)
        ids.reject!(&:blank?)

        remote_candidates = []
        remote_candidates << payload[:remoteJid]
        remote_candidates << payload[:remoteJidAlt]
        remote_candidates << payload[:remoteJID]
        remote_candidates << payload[:chatId]
        remote_candidates << payload[:chat_id]
        remote_candidates << payload[:chat]
        remote_candidates << payload[:chatID]
        remote_candidates << payload[:from]
        remote_candidates << payload[:to]
        remote_candidates << payload.dig(:key, :remoteJid)
        remote_candidates << payload.dig(:key, :remoteJID)
        remote_candidates << protocol_key[:remoteJid] if protocol_key&.[](:remoteJid).present?
        remote_candidates << protocol_key[:remoteJID] if protocol_key&.[](:remoteJID).present?
        remote_candidates << payload.dig(:_data, :remoteJid)
        remote_candidates << payload.dig(:_data, :Info, :Chat)
        remote_candidates << protocol_message[:remoteJid] if protocol_message&.[](:remoteJid).present?
        remote_candidates << protocol_message[:remoteJID] if protocol_message&.[](:remoteJID).present?
        remote_candidates << info[:Chat] if info.present?

        participant_candidates = []
        participant_candidates << payload[:participant]
        participant_candidates << payload[:participantAlt]
        participant_candidates << payload[:author]
        participant_candidates << payload[:sender]
        participant_candidates << payload[:remoteParticipant]
        participant_candidates << payload.dig(:key, :participant)
        participant_candidates << payload.dig(:key, :participantAlt)
        participant_candidates << payload.dig(:_data, :Participant)
        participant_candidates << payload.dig(:_data, :participant)
        participant_candidates << payload.dig(:_data, :Info, :Participant)
        participant_candidates << payload.dig(:_data, :Info, :participant)
        participant_candidates << payload.dig(:_data, :Info, :Sender)
        participant_candidates << payload.dig(:_data, :Info, :SenderAlt)
        participant_candidates << protocol_key[:participant] if protocol_key&.[](:participant).present?
        participant_candidates << protocol_key[:participantAlt] if protocol_key&.[](:participantAlt).present?

        prefix_candidates = []
        message_tokens = ids.select { |identifier| !identifier.include?('_') }
        message_tokens << payload[:messageToken]
        message_tokens << payload[:message_token]
        message_tokens << payload[:token]
        message_tokens << payload.dig(:key, :token)
        message_tokens << payload.dig(:_data, :messageToken)
        message_tokens << payload.dig(:_data, :MessageToken)
        message_tokens << payload[:revokedMessageId]
        message_tokens << payload[:revoked_message_id]

        ids.dup.each do |identifier|
          next if identifier.blank?

          fragments = identifier.split('_')
          next if fragments.length < 3

          flag = fragments.shift
          next unless %w[true false].include?(flag)

          remote = fragments.shift
          message_token = fragments.shift

          prefix_candidates << flag
          remote_candidates << remote if remote.present?
          message_tokens << message_token if message_token.present?

          if fragments.present?
            participant_candidates.concat(fragments.compact)
            ids << [flag, remote, message_token].join('_') if remote.present? && message_token.present?
          end
        end

        remote_variants = remote_candidates.compact.flat_map { |remote| jid_variants(remote) }
        remote_variants.map!(&:to_s)
        remote_variants.reject!(&:blank?)
        remote_variants.uniq!

        data_from_me = payload[:fromMe]
        data_from_me = payload.dig(:key, :fromMe) if data_from_me.nil?

        prefix_variants = prefix_candidates.compact.map(&:to_s).select { |val| %w[true false].include?(val) }
        if data_from_me.nil?
          prefix_variants |= %w[true false]
        else
          bool_from_me = ActiveModel::Type::Boolean.new.cast(data_from_me)
          prefix_variants << (bool_from_me ? 'true' : 'false')
        end
        prefix_variants.uniq!

        participant_variants = participant_candidates.compact.flat_map { |participant| jid_variants(participant) }
        participant_variants.map!(&:to_s)
        participant_variants.reject!(&:blank?)
        participant_variants.uniq!

        message_tokens = message_tokens.compact.map(&:to_s).reject(&:blank?).uniq

        if prefix_variants.present? && remote_variants.present? && message_tokens.present?
          prefix_variants.each do |prefix|
            remote_variants.each do |remote|
              message_tokens.each do |token|
                ids << [prefix, remote, token].join('_')

                participant_variants.each do |participant|
                  ids << [prefix, remote, token, participant].join('_')
                end
              end
            end
          end
        end

        ids.uniq
      end
    end
  end
end
