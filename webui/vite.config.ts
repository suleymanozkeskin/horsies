/// <reference types="vitest/config" />
import { fileURLToPath, URL } from 'node:url';

import tailwindcss from '@tailwindcss/vite';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

/**
 * The SPA is served from an arbitrary mount path (`/`, `/monitoring`, …), so
 * asset URLs are emitted relative and resolve against the `<base href>` the
 * server injects alongside `window.__HORSIES_UI__`.
 */
export default defineConfig({
  base: './',
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  build: {
    // The wheel ships this directory as package data; CI builds it before the
    // wheel build. It lives outside the Vite root, hence the explicit empty.
    outDir: '../horsies/web/static',
    emptyOutDir: true,
  },
  server: {
    port: 5273,
    proxy: {
      // `horsies web --database-url …` running alongside the dev server.
      '/api': {
        target: 'http://127.0.0.1:8600',
        changeOrigin: true,
      },
    },
  },
  test: {
    environment: 'jsdom',
    globals: false,
    setupFiles: ['./src/test/setup.ts'],
    include: [
      'src/**/*.test.ts',
      'src/**/*.test.tsx',
      'scripts/**/*.test.mjs',
    ],
  },
});
