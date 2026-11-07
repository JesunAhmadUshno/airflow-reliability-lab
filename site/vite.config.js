import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import fs from 'node:fs';
import { repoDocsPlugin } from './src/docs.mjs';
// The site reads ../results/* and repo sources at build time, so every number on the page is generated, never typed.
const read = (p) => fs.readFileSync(new URL(`../${p}`, import.meta.url), 'utf8');
export default defineConfig({
  base: '/airflow-reliability-lab/',
  plugins: [react(), repoDocsPlugin(read)],
  server: { fs: { allow: ['..'] } },
});
