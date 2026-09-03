import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Builds into ../static/beta so Flask can serve the bundle at /static/beta/*
// and render an entry HTML at /beta. During dev, Vite runs on :5173 and
// proxies /api → Flask on :5000 so backend calls Just Work.
export default defineConfig({
  plugins: [react()],
  base: '/static/beta/',
  build: {
    outDir: '../static/beta',
    emptyOutDir: true,
    assetsDir: 'assets',
    manifest: true,
  },
  server: {
    port: 5173,
    proxy: {
      '/api': 'http://localhost:5000',
    },
  },
})
