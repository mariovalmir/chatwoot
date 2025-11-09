<script setup>
import { computed, ref } from 'vue';
import { useRouter } from 'vue-router';
import { useStore } from 'vuex';
import { useI18n } from 'vue-i18n';
import { useVuelidate } from '@vuelidate/core';
import { useAlert } from 'dashboard/composables';
import { required } from '@vuelidate/validators';
import { isPhoneE164OrEmpty } from 'shared/helpers/Validators';
import { isValidURL } from '../../../../../helper/URLHelper';

import NextButton from 'dashboard/components-next/button/Button.vue';

const router = useRouter();
const store = useStore();
const { t } = useI18n();

const inboxName = ref('');
const phoneNumber = ref('');
const apiUrl = ref('');
const adminToken = ref('');
const instanceName = ref('');

const uiFlags = computed(() => store.getters['inboxes/getUIFlags']);

const rules = computed(() => ({
  inboxName: { required },
  phoneNumber: { required, isPhoneE164OrEmpty },
  apiUrl: { required, isValidURL },
  adminToken: { required },
  instanceName: { required },
}));

const v$ = useVuelidate(rules, {
  inboxName,
  phoneNumber,
  apiUrl,
  adminToken,
  instanceName,
});

const createChannel = async () => {
  v$.value.$touch();
  if (v$.value.$invalid) {
    return;
  }

  try {
    const whatsappChannel = await store.dispatch('inboxes/createChannel', {
      name: inboxName.value,
      channel: {
        type: 'whatsapp',
        phone_number: phoneNumber.value,
        provider: 'evolution',
        provider_config: {
          api_url: apiUrl.value,
          admin_token: adminToken.value,
          instance_name: instanceName.value,
        },
      },
    });

    router.replace({
      name: 'settings_inboxes_add_agents',
      params: {
        page: 'new',
        inbox_id: whatsappChannel.id,
      },
    });
  } catch (error) {
    useAlert(error.message || t('INBOX_MGMT.ADD.WHATSAPP.EVOLUTION.API.ERROR_MESSAGE'));
  }
};
</script>

<template>
  <form class="flex flex-wrap mx-0" @submit.prevent="createChannel()">
    <div class="w-[65%] flex-shrink-0 flex-grow-0 max-w-[65%]">
      <label :class="{ error: v$.inboxName.$error }">
        {{ $t('INBOX_MGMT.ADD.WHATSAPP.INBOX_NAME.LABEL') }}
        <input
          v-model="inboxName"
          type="text"
          :placeholder="$t('INBOX_MGMT.ADD.WHATSAPP.INBOX_NAME.PLACEHOLDER')"
          @blur="v$.inboxName.$touch"
        />
        <span v-if="v$.inboxName.$error" class="message">
          {{ $t('INBOX_MGMT.ADD.WHATSAPP.INBOX_NAME.ERROR') }}
        </span>
      </label>
    </div>

    <div class="w-[65%] flex-shrink-0 flex-grow-0 max-w-[65%]">
      <label :class="{ error: v$.phoneNumber.$error }">
        {{ $t('INBOX_MGMT.ADD.WHATSAPP.PHONE_NUMBER.LABEL') }}
        <input
          v-model="phoneNumber"
          type="text"
          :placeholder="$t('INBOX_MGMT.ADD.WHATSAPP.PHONE_NUMBER.PLACEHOLDER')"
          @blur="v$.phoneNumber.$touch"
        />
        <span v-if="v$.phoneNumber.$error" class="message">
          {{ $t('INBOX_MGMT.ADD.WHATSAPP.PHONE_NUMBER.ERROR') }}
        </span>
      </label>
    </div>

    <div class="w-[65%] flex-shrink-0 flex-grow-0 max-w-[65%]">
      <label :class="{ error: v$.apiUrl.$error }">
        {{ $t('INBOX_MGMT.ADD.WHATSAPP.EVOLUTION.API_URL.LABEL') }}
        <input
          v-model="apiUrl"
          type="text"
          :placeholder="$t('INBOX_MGMT.ADD.WHATSAPP.EVOLUTION.API_URL.PLACEHOLDER')"
          @blur="v$.apiUrl.$touch"
        />
        <span v-if="v$.apiUrl.$error" class="message">
          {{ $t('INBOX_MGMT.ADD.WHATSAPP.EVOLUTION.API_URL.ERROR') }}
        </span>
      </label>
    </div>

    <div class="w-[65%] flex-shrink-0 flex-grow-0 max-w-[65%]">
      <label :class="{ error: v$.adminToken.$error }">
        {{ $t('INBOX_MGMT.ADD.WHATSAPP.EVOLUTION.ADMIN_TOKEN.LABEL') }}
        <input
          v-model="adminToken"
          type="password"
          :placeholder="$t('INBOX_MGMT.ADD.WHATSAPP.EVOLUTION.ADMIN_TOKEN.PLACEHOLDER')"
          @blur="v$.adminToken.$touch"
        />
        <span v-if="v$.adminToken.$error" class="message">
          {{ $t('INBOX_MGMT.ADD.WHATSAPP.EVOLUTION.ADMIN_TOKEN.ERROR') }}
        </span>
      </label>
    </div>

    <div class="w-[65%] flex-shrink-0 flex-grow-0 max-w-[65%]">
      <label :class="{ error: v$.instanceName.$error }">
        {{ $t('INBOX_MGMT.ADD.WHATSAPP.EVOLUTION.INSTANCE_NAME.LABEL') }}
        <input
          v-model="instanceName"
          type="text"
          :placeholder="$t('INBOX_MGMT.ADD.WHATSAPP.EVOLUTION.INSTANCE_NAME.PLACEHOLDER')"
          @blur="v$.instanceName.$touch"
        />
        <span v-if="v$.instanceName.$error" class="message">
          {{ $t('INBOX_MGMT.ADD.WHATSAPP.EVOLUTION.INSTANCE_NAME.ERROR') }}
        </span>
      </label>
    </div>

    <div class="w-full">
      <NextButton
        :is-loading="uiFlags.isCreating"
        type="submit"
        solid
        blue
        :label="$t('INBOX_MGMT.ADD.WHATSAPP.SUBMIT_BUTTON')"
      />
    </div>
  </form>
</template>
