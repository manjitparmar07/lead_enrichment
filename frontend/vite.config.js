import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const backendUrl = env.VITE_BACKEND_URL || 'http://localhost:8020'

  return {
    plugins: [react()],

    server: {
      host: true,
      port: 5174,
      proxy: {
        '/api': {
          target: backendUrl,
          changeOrigin: true,
        },
      },
    },

    build: {
      // Target modern browsers — smaller, faster output
      target: 'esnext',

      // Raise warning threshold (Ably alone is 181 KB)
      chunkSizeWarningLimit: 600,

      rollupOptions: {
        output: {
          manualChunks: {
            // Core framework — cached long-term, rarely changes
            'vendor-react': ['react', 'react-dom', 'react-router-dom'],

            // Ably is 181 KB — split so it doesn't block initial paint
            // (already dynamically imported in LeadEnrichmentPage, but this
            //  ensures rollup doesn't inline it into the main chunk)
            'vendor-ably': ['ably'],

            // Small UI libs
            'vendor-ui': ['react-hot-toast'],
          },
        },
      },

      // Inline tiny assets (<4 KB) instead of separate files
      assetsInlineLimit: 4096,
    },
  }
})
