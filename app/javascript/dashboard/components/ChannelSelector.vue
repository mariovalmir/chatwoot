<script setup>
import Icon from 'next/icon/Icon.vue';

defineProps({
  title: {
    type: String,
    required: true,
  },
  description: {
    type: String,
    default: '',
  },
  icon: {
    type: String,
    required: true,
  },
  isComingSoon: {
    type: Boolean,
    default: false,
  },
});
</script>

<template>
  <button
    class="relative bg-n-solid-1 cursor-pointer rounded-2xl flex flex-col transition-all duration-200 ease-in p-6 items-start border border-solid border-n-weak w-full hover:enabled:scale-[1.02]"
    :class="{
      'hover:enabled:border-n-blue-9 hover:enabled:shadow-lg disabled:opacity-60 disabled:cursor-not-allowed':
        !isComingSoon,
      'cursor-not-allowed disabled:opacity-80': isComingSoon,
    }"
  >
    <div
      class="flex size-14 items-center justify-center rounded-xl bg-n-alpha-2 mb-4"
    >
      <Icon :icon="icon" class="text-n-slate-10 size-8" />
    </div>

    <div class="flex flex-col items-start gap-2 w-full">
      <h3 class="text-n-slate-12 text-lg text-start font-semibold">
        {{ title }}
      </h3>
      <p class="text-n-slate-11 text-start text-sm leading-relaxed">
        {{ description }}
      </p>
    </div>

    <div
      v-if="isComingSoon"
      class="absolute inset-0 flex items-center justify-center backdrop-blur-[2px] rounded-2xl bg-gradient-to-br from-n-background/90 via-n-background/70 to-n-background/95 cursor-not-allowed"
    >
      <span class="text-n-slate-12 font-medium text-sm">
        {{ $t('CHANNEL_SELECTOR.COMING_SOON') }} 🚀
      </span>
    </div>
  </button>
</template>
