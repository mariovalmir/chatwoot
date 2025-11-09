module Whatsapp
  module Waha
    module UpsertMethods
      PROFILE_PICTURE_REFRESH_INTERVAL = 12.hours

      private

      def cache_lid_identifiers
        lid_variants = [remote_jid, remote_jid_alt, participant_jid, participant_alt_jid]
                       .select { |jid| lid_jid?(jid) }
                       .compact
                       .uniq

        return if lid_variants.empty?

        plain_variants = [remote_jid, remote_jid_alt, participant_jid, participant_alt_jid]
                         .reject { |jid| jid.blank? || lid_jid?(jid) }
                         .uniq

        plain_variants.each do |plain|
          plain_str = plain.to_s
          next if plain_str.ends_with?('@g.us')

          msisdn = normalize_whatsapp_number(plain)
          next if msisdn.blank?

          lid_variants.each { |lid| store_lid_msisdn_mapping(lid, msisdn) }
        end

        if plain_variants.empty? && waha_provider?
          lid_variants.each do |lid|
            msisdn = lookup_lid_msisdn_from_provider(lid)
            next if msisdn.blank?

            store_lid_msisdn_mapping(lid, msisdn)
          end
        end
      end

      def update_contact_avatar_from_message
        additional = @contact.additional_attributes || {}
        stored_url = additional['social_whatsapp_profile_pic_url']
        additional_changed = false

        profile_pic_url = extract_waha_profile_picture_url(@raw_message)

        if profile_pic_url.blank?
          if should_refresh_profile_picture?(contact: @contact, stored_url: stored_url, additional: additional)
            force_refresh = force_profile_picture_refresh?(stored_url: stored_url, additional: additional)
            fetched_url = fetch_waha_profile_picture(@contact_inbox&.source_id, refresh: force_refresh)
            additional['waha_profile_pic_checked_at'] = Time.current.iso8601
            profile_pic_url = fetched_url if fetched_url.present?
            additional_changed = true
          end
        elsif additional['waha_profile_pic_checked_at'].blank?
          additional['waha_profile_pic_checked_at'] = Time.current.iso8601
          additional_changed = true
        end

        return unless profile_pic_url.present? || additional_changed

        if profile_pic_url.present? && (stored_url != profile_pic_url || !@contact.avatar.attached?)
          additional['social_whatsapp_profile_pic_url'] = profile_pic_url
          @contact.update!(additional_attributes: additional)
          ::Avatar::AvatarFromUrlJob.perform_later(@contact, profile_pic_url)
        elsif additional_changed
          @contact.update!(additional_attributes: additional)
        end
      end

      def ensure_group_contact_metadata(contact_inbox, remote_jid:)
        return unless contact_inbox
        return unless waha_provider?

        contact = contact_inbox.contact

        cached_subject = ensure_group_subject_cached(remote_jid)
        desired_name = formatted_group_display_name(cached_subject, remote_jid)

        needs_metadata = cached_subject.blank? || !names_equivalent?(contact.name, desired_name)

        if needs_metadata
          metadata = normalize_group_metadata(fetch_waha_group_metadata(remote_jid))
          resolved_subject = metadata['name'] || metadata['subject'] || metadata['title']

          if resolved_subject.present?
            store_group_subject(remote_jid, resolved_subject)
            desired_name = formatted_group_display_name(resolved_subject, remote_jid)
          end
        end

        if desired_name.present? && !names_equivalent?(contact.name, desired_name)
          contact.update!(name: desired_name)
        end

        ensure_group_contact_avatar(contact, remote_jid)
      end

      def ensure_group_contact_avatar(contact, remote_jid)
        additional = contact.additional_attributes || {}
        stored_url = additional['social_whatsapp_profile_pic_url']
        needs_avatar = stored_url.blank? || !contact.avatar.attached?
        return unless needs_avatar

        last_checked = group_profile_picture_last_checked_at(additional)
        return if stored_url.blank? && last_checked.present? && last_checked > PROFILE_PICTURE_REFRESH_INTERVAL.ago

        picture_url = fetch_waha_group_profile_picture(remote_jid, refresh: false)
        additional_changed = false
        timestamp = Time.current.iso8601

        if additional['waha_group_profile_pic_checked_at'] != timestamp
          additional['waha_group_profile_pic_checked_at'] = timestamp
          additional_changed = true
        end

        if picture_url.present? && (stored_url != picture_url || !contact.avatar.attached?)
          additional['social_whatsapp_profile_pic_url'] = picture_url
          contact.update!(additional_attributes: additional)
          ::Avatar::AvatarFromUrlJob.perform_later(contact, picture_url)
        elsif additional_changed
          contact.update!(additional_attributes: additional)
        end
      end

      def normalize_group_metadata(data)
        return {} unless data.is_a?(Hash)

        data.each_with_object({}) do |(key, value), memo|
          memo[key.to_s.downcase] = value
        end
      end

      def group_profile_picture_last_checked_at(additional)
        raw_value = additional&.dig('waha_group_profile_pic_checked_at') || additional&.dig(:waha_group_profile_pic_checked_at)
        return if raw_value.blank?

        Time.zone.parse(raw_value.to_s)
      rescue StandardError
        nil
      end

      def should_refresh_profile_picture?(contact:, stored_url:, additional:)
        return false unless waha_provider?
        return false if group_message?

        missing_avatar = stored_url.blank? || !contact.avatar.attached?
        return true if missing_avatar

        last_checked = profile_picture_last_checked_at(additional)
        return true if last_checked.blank?

        last_checked < PROFILE_PICTURE_REFRESH_INTERVAL.ago
      end

      def force_profile_picture_refresh?(stored_url:, additional:)
        return false unless waha_provider?
        return false if stored_url.blank?

        last_checked = profile_picture_last_checked_at(additional)
        return false unless last_checked

        last_checked < PROFILE_PICTURE_REFRESH_INTERVAL.ago
      end

      def profile_picture_last_checked_at(additional)
        raw_value = additional&.dig('waha_profile_pic_checked_at') || additional&.dig(:waha_profile_pic_checked_at)
        return if raw_value.blank?

        Time.zone.parse(raw_value.to_s)
      rescue StandardError
        nil
      end

      def fetch_waha_profile_picture(source_id, refresh: false)
        return unless waha_provider?

        identifier = waha_contact_identifier(source_id)
        return if identifier.blank?

        provider = inbox.channel.provider_service
        provider.fetch_contact_profile_picture(identifier, refresh: refresh)
      rescue StandardError => e
        Rails.logger.warn "Waha profile picture fetch failed for #{source_id}: #{e.message}"
        nil
      end

      def waha_contact_identifier(source_id)
        return if source_id.blank?

        str = source_id.to_s
        return if str.ends_with?('@g.us')

        if str.ends_with?('@lid')
          mapped = lookup_lid_msisdn_from_provider(str)
          return if mapped.blank?

          str = mapped
        end
        return str if str.include?('@')

        digits = normalize_whatsapp_number(str)
        return if digits.blank?

        "#{digits}@s.whatsapp.net"
      end
    end
  end
end
