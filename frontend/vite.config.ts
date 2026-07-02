/// <reference types="vitest/config" />
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { execSync } from 'child_process'

function safeExec(command: string, fallback: string): string {
  try {
    return execSync(command).toString().trim()
  } catch {
    return fallback
  }
}

const commitHash = safeExec('git rev-parse --short HEAD', 'unknown')
const commitDate = safeExec('git log -1 --format=%ci', 'unknown').slice(0, 10)
const buildRef = (() => {
  try {
    return execSync('git describe --tags --exact-match 2>/dev/null').toString().trim()
  } catch {
    try {
      return execSync('git rev-parse --abbrev-ref HEAD').toString().trim()
    } catch {
      return 'unknown'
    }
  }
})()

export default defineConfig({
  base: process.env.GITHUB_ACTIONS ? '/consensus-landscape/' : '/',
  plugins: [react()],
  define: {
    __BUILD_HASH__: JSON.stringify(commitHash),
    __BUILD_DATE__: JSON.stringify(commitDate),
    __BUILD_REF__: JSON.stringify(buildRef),
  },
  test: {
    globals: true,
  },
})
