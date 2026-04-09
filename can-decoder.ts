/**
 * CAN Data Decoder Router for TS101 Device
 * Routes CAN decoding to vendor-specific decoders based on vendor ID
 * Supports METS (EEVNAT) and NDS (iTriangle) vendors
 */

import { decodeMetsCANData } from './mets';
import { decodeNdsCANData } from './nds';

/**
 * Decoded CAN Signal Value (unified interface)
 */
export interface DecodedSignal {
  name: string;
  rawValue: number;
  value: number;
  unit: string;
  description?: string;
}

/**
 * Decoded CAN Message (unified interface)
 */
export interface DecodedCANMessage {
  messageId: string;
  messageName: string;
  signals: DecodedSignal[];
}

/**
 * Decode CAN data based on vendor ID
 * @param canData - CAN data string from TS101 packet
 * @param vendorID - Vendor ID from packet (e.g., "eevnat", "EEVNAT", "iTriangle")
 */
export function decodeCANData(canData: string, vendorID: string = "eevnat"): {
  canType: string;
  eventKey: string;
  eventValue: string;
  messages: DecodedCANMessage[];
} {
  // Normalize vendor ID
  const normalizedVendor = vendorID.toLowerCase();

  // Route to appropriate decoder
  if (normalizedVendor === "itriangle") {
    return decodeNdsCANData(canData);
  } else {
    // Default to METS (EEVNAT) decoder for "eevnat" and others
    return decodeMetsCANData(canData);
  }
}
