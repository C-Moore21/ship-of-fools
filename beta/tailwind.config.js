export default {
  content: [
  './index.html',
  './src/**/*.{js,ts,jsx,tsx}'
],
  theme: {
    extend: {
      colors: {
        bg: '#080c14',
        surface: '#0d1220',
        surface2: '#141a2e',
        line: '#1e2a45',
        ink: '#dce6f5',
        muted: '#5a6e8a',
        chalk: '#f0f4ff',
        accent: '#e8332a',
        'accent-hover': '#ff4a40',
        royal: '#1a4bbf',
        'royal-light': '#2d65e0',
        'royal-bright': '#6090ff',
        gold: '#c8a84b',
        'gold-light': '#e8c86a',
        moss: '#2a9d6f',
        violet: '#9c6fff',
        amber: '#f5a623',
      },
      fontFamily: {
        display: ['"Playfair Display"', 'Georgia', 'serif'],
        mono: ['"Space Mono"', 'ui-monospace', 'monospace'],
      },
      transitionTimingFunction: {
        archive: 'cubic-bezier(0.23, 1, 0.32, 1)',
      },
    },
  },
  plugins: [],
}
