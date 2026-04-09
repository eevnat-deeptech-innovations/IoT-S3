/**
 * MongoDB Connection and Data Persistence
 */

import { MongoClient, Db } from 'mongodb';

const MONGO_URI = process.env.MONGO_URI || "mongodb://localhost:27015";
const DB_NAME = process.env.DB_NAME || "telematics";

let mongoClient: MongoClient | null = null;
let db: Db | null = null;

/**
 * Ensure required indexes exist on the logs collection
 */
async function ensureIndexes(): Promise<void> {
  const database = getDb();
  const collection = database.collection("logs");

  console.log("Creating indexes...");

  // 1. Compound index for IMEI + Timestamp (primary query pattern)
  await collection.createIndex(
    { imei: 1, timestamp: -1 },
    { name: "idx_imei_timestamp", background: true }
  );

  // 2. Timestamp index for date range queries and sorting
  await collection.createIndex(
    { timestamp: -1 },
    { name: "idx_timestamp", background: true }
  );

  // 3. IMEI index for filtering and distinct queries
  await collection.createIndex(
    { imei: 1 },
    { name: "idx_imei", background: true }
  );

  // 4. Compound index for CSV export optimization
  await collection.createIndex(
    { imei: 1, timestamp: 1 },
    { name: "idx_imei_timestamp_asc", background: true }
  );

  console.log("✓ Indexes created/verified");
}

/**
 * Initialize MongoDB connection
 */
export async function connectMongo(): Promise<void> {
  if (!mongoClient) {
    mongoClient = new MongoClient(MONGO_URI);
    await mongoClient.connect();
    db = mongoClient.db(DB_NAME);
    console.log(`Connected to MongoDB: ${DB_NAME}`);
    
    // Ensure indexes are created (idempotent - safe to run multiple times)
    // await ensureIndexes();
  }
}

/**
 * Get MongoDB database instance
 */
export function getDb(): Db {
  if (!db) {
    throw new Error("MongoDB not connected. Call connectMongo() first.");
  }
  return db;
}

/**
 * Save parsed packet data to MongoDB
 */
export async function saveToMongo(data: any): Promise<void> {
  const database = getDb();
  await database.collection("logs").insertOne(data);
}

/**
 * Close MongoDB connection
 */
export async function closeMongo(): Promise<void> {
  if (mongoClient) {
    await mongoClient.close();
    mongoClient = null;
    db = null;
    console.log("MongoDB connection closed");
  }
}
