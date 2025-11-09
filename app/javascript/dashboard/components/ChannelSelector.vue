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
    class="relative bg-n-solid-1 gap-6 cursor-pointer rounded-2xl flex flex-col justify-start transition-all duration-200 ease-in -m-px py-8 px-6 items-start border border-solid border-n-weak min-w-[280px] flex-1 max-w-[320px]"
    :class="{
      'hover:enabled:border-n-blue-9 hover:enabled:shadow-md disabled:opacity-60 disabled:cursor-not-allowed':
        !isComingSoon,
      'cursor-not-allowed disabled:opacity-80': isComingSoon,
    }"
  >
    <div
      class="flex size-12 items-center justify-center rounded-full bg-n-alpha-2"
    >
      <Icon :icon="icon" class="text-n-slate-10 size-7" />
    </div>

    <div class="flex flex-col items-start gap-2 w-full">
      <h3 class="text-n-slate-12 text-base text-start font-semibold">
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
