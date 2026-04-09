/**
 * Packet Processing Service
 * Handles parsing, authorization, and storage of device packets
 */

import { parseTS101Packet } from '../devices/ts101/parser';
import { isAuthorized } from '../config/auth';
import { saveToMongo } from '../db/mongo';

/**
 * Process incoming packet: parse, authorize, and save
 */
export async function processPacket(rawPayload: string): Promise<void> {
  const parsedData = parseTS101Packet(rawPayload);

  if (!parsedData) {
    console.warn("Failed to parse packet");
    return;
  }

  if (!isAuthorized(parsedData.imei)) {
    console.error(`Unauthorized IMEI: ${parsedData.imei}`);
    return;
  }

  await saveToMongo(parsedData);
  console.log(`✓ Saved ${parsedData.packetType} from ${parsedData.imei}`);
}
