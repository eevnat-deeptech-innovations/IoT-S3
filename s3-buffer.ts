import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const S3_BUCKET = process.env.S3_BUCKET_NAME!;
const S3_REGION = process.env.AWS_REGION!;
const S3_PREFIX = process.env.S3_KEY_PREFIX || "raw";

const s3 = new S3Client({
  region: S3_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
  },
});

interface BufferEntry {
  records: string[];
  timer: ReturnType<typeof setTimeout> | null;
  startTime: Date;
  lastActivity: Date;
}

const imeiBuffers = new Map<string, BufferEntry>();
const WINDOW_MS = 10 * 60 * 1000;
const IDLE_CLEANUP_MS = 30 * 60 * 1000;

setInterval(() => {
  const now = Date.now();
  for (const [imei, entry] of imeiBuffers.entries()) {
    const idleMs = now - entry.lastActivity.getTime();
    if (idleMs > IDLE_CLEANUP_MS && entry.records.length === 0) {
      if (entry.timer) clearTimeout(entry.timer);
      imeiBuffers.delete(imei);
    }
  }
}, IDLE_CLEANUP_MS);

async function flushBuffer(imei: string): Promise<void> {
  const entry = imeiBuffers.get(imei);
  if (!entry || entry.records.length === 0) {
    if (entry?.timer) clearTimeout(entry.timer);
    imeiBuffers.delete(imei);
    return;
  }

  const records = [...entry.records];
  const windowStart = entry.startTime.toISOString().replace(/[:.]/g, "-");
  entry.records = [];
  entry.timer = null;
  entry.startTime = new Date();

  const fileContent = records.join("\n");
  const s3Key = `${S3_PREFIX}/${imei}/${imei}_${windowStart}.txt`;

  try {
    await s3.send(new PutObjectCommand({
      Bucket: S3_BUCKET,
      Key: s3Key,
      Body: fileContent,
      ContentType: "text/plain",
    }));
    console.log(`[S3] Uploaded ${records.length} records to ${s3Key}`);
  } catch (err) {
    console.error(`[S3] Upload failed for IMEI ${imei}:`, err);
    entry.records = [...records, ...entry.records];
  }

  const current = imeiBuffers.get(imei);
  if (current) {
    current.timer = setTimeout(() => flushBuffer(imei), WINDOW_MS);
  }
}

export function addToS3Buffer(imei: string, rawPayload: string): void {
  if (!imei) return;

  let entry = imeiBuffers.get(imei);

  if (!entry) {
    entry = { records: [], timer: null, startTime: new Date(), lastActivity: new Date() };
    imeiBuffers.set(imei, entry);
    entry.timer = setTimeout(() => flushBuffer(imei), WINDOW_MS);
    console.log(`[S3] Started 10-min buffer for IMEI: ${imei}`);
  }

  entry.records.push(rawPayload);
  entry.lastActivity = new Date();
}

export async function flushAllBuffers(): Promise<void> {
  const flushPromises: Promise<void>[] = [];
  for (const imei of imeiBuffers.keys()) {
    flushPromises.push(flushBuffer(imei));
  }
  await Promise.all(flushPromises);
  imeiBuffers.clear();
}