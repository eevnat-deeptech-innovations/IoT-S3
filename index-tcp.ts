#!/usr/bin/env bun
/**
 * TCP Server Entry Point
 * Starts only the TCP server for device communication
 */

import { connectMongo, closeMongo } from './src/db/mongo';
import { startTCPServer } from './src/servers/tcp-server';
import { startMemoryManager, onShutdown } from './src/utils/memory-manager';
import { flushAllBuffers } from './src/services/s3-buffer';

// Start memory manager FIRST (handles SIGTERM/SIGINT with graceful flush)
startMemoryManager();

// Register real flush before shutdown
onShutdown(async () => {
  console.log('[TCP] Flushing S3 buffers...');
  await flushAllBuffers();

  console.log('[TCP] Closing MongoDB...');
  await closeMongo();

  console.log('[TCP] Shutdown complete.');
});

async function startTCP() {
  console.log('Starting TCP Server...\n');
  await connectMongo();
  startTCPServer();
  console.log('\n✓ TCP Server running');
}

startTCP().catch((error) => {
  console.error('Failed to start TCP server:', error);
  process.exit(1);
});