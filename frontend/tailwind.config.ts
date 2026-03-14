import type { Config } from 'tailwindcss';

export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      fontFamily: {
        display: ['"Manrope"', 'system-ui', 'sans-serif'],
        body: ['"Manrope"', 'system-ui', 'sans-serif'],
      },
      colors: {
        canvas: 'var(--ui-canvas)',
        ink: 'var(--ui-ink)',
        subtext: '#5f6f82',
        surface: 'var(--ui-surface)',
        border: 'var(--ui-border)',
      },
      boxShadow: {
        card: '0 6px 20px rgba(15, 23, 42, 0.06)',
      },
    },
  },
  plugins: [],
} satisfies Config;
