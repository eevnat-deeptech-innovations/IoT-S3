#!/usr/bin/env bun
/**
 * TCP Server Entry Point
 * Starts only the TCP server for device communication
 */

import { connectMongo } from './src/db/mongo';
import { startTCPServer } from './src/servers/tcp-server';
import { startMemoryManager, onShutdown } from './src/utils/memory-manager';

// Start memory manager FIRST (handles SIGTERM/SIGINT with graceful flush)
startMemoryManager();

// Register flush before shutdown
onShutdown(async () => {
  console.log('[TCP] Flushing pending data to MongoDB and S3...');
  await new Promise(resolve => setTimeout(resolve, 3000));
  console.log('[TCP] Flush complete.');
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