#!/usr/bin/env bun
/**
 * HTTP Server Entry Point
 * Starts only the HTTP server for web interface and API
 */

import { connectMongo, closeMongo } from './src/db/mongo';
import { startHTTPServer } from './src/servers/http-server';
import { startMemoryManager, onShutdown } from './src/utils/memory-manager';

// Start memory manager FIRST (handles SIGTERM/SIGINT with graceful shutdown)
startMemoryManager();

// Register real shutdown before exit
onShutdown(async () => {
  console.log('[HTTP] Closing MongoDB...');
  await closeMongo();
  console.log('[HTTP] Shutdown complete.');
});

async function startHTTP() {
  console.log('Starting HTTP Server...\n');
  await connectMongo();
  startHTTPServer();
  console.log('\n✓ HTTP Server running');
}

startHTTP().catch((error) => {
  console.error('Failed to start HTTP server:', error);
  process.exit(1);
});