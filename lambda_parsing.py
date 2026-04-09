import json
import io
import re
import uuid
import boto3
import cantools
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone

s3 = boto3.client('s3')
glue = boto3.client('glue', region_name='ap-south-1')

mapping_cache = {}
dbc_cache = {}
allowlist_cache = {}

def get_imei_mapping(bucket):
    if 'mapping' in mapping_cache:
        return mapping_cache['mapping']
    obj = s3.get_object(Bucket=bucket, Key='config/imei-mapping.json')
    mapping = json.loads(obj['Body'].read().decode('utf-8'))
    mapping_cache['mapping'] = mapping
    print(f"Loaded IMEI mapping: {len(mapping)} IMEIs")
    return mapping

def get_signal_allowlist(bucket):
    if 'allowlist' in allowlist_cache:
        return allowlist_cache['allowlist']
    try:
        obj = s3.get_object(Bucket=bucket, Key='config/signal-allowlist.json')
        raw = json.loads(obj['Body'].read().decode('utf-8'))
        allowed = set()
        for entry in raw.get('allowed_can_ids', []):
            if isinstance(entry, str):
                allowed.add(int(entry, 16))
            else:
                allowed.add(int(entry))
        allowlist_cache['allowlist'] = allowed
        print(f"Loaded CAN ID allowlist: {len(allowed)} IDs")
        return allowed
    except Exception as e:
        print(f"Could not load allowlist: {e} — decoding all CAN IDs")
        allowlist_cache['allowlist'] = None
        return None

def load_dbc(bucket, dbc_key):
    if dbc_key in dbc_cache:
        return dbc_cache[dbc_key]
    print(f"Loading DBC: {dbc_key}")
    obj = s3.get_object(Bucket=bucket, Key=dbc_key)
    dbc_text = obj['Body'].read().decode('cp1252', errors='ignore')
    db = cantools.database.Database()
    db.add_dbc_string(dbc_text)
    dbc_cache[dbc_key] = db
    print(f"  → {len(db.messages)} messages loaded from {dbc_key}")
    return db

def get_dbc_list_for_imei(bucket, imei):
    mapping = get_imei_mapping(bucket)
    if imei not in mapping:
        raise ValueError(f"IMEI {imei} not found in mapping")
    dbc_keys = mapping[imei]  # this is now a list of 4 DBC paths
    return [load_dbc(bucket, key) for key in dbc_keys]

def resolve_message_multi_dbc(db_list, can_id):
    for db in db_list:
        try:
            msg = db.get_message_by_frame_id(can_id)
            return db, msg
        except KeyError:
            continue
    return None, None


def decode_frame_to_signal_rows(db_list, metadata, can_id, data_hex, allowlist):
    # Skip this frame entirely if CAN ID is not in the allowlist
    if allowlist is not None and can_id not in allowlist:
        return []

    data_bytes = bytes.fromhex(data_hex)

    db, msg = resolve_message_multi_dbc(db_list, can_id)
    if not msg:
        return []

    try:
        signals = msg.decode(
            data_bytes,
            decode_choices=False,
            allow_truncated=False
        )
    except Exception as e:
        print(f"Error decoding CAN {hex(can_id)} ({msg.name}): {e}")
        return []

    rows = []
    for signal_name, signal_value in signals.items():
        if signal_value is None:
            continue    # Only skip true decode failures, NOT zeros
        rows.append({
            **metadata,
            'can_id':       can_id,
            'can_id_hex':   f"0x{can_id:08X}",
            'message_name': msg.name,
            'signal_name':  signal_name,
            'signal_value': float(signal_value),   # 0.0 is kept
        })
    return rows
def parse_itriangle_packet(raw_line):
    raw_line = re.sub(r'\*[0-9A-Fa-f]+$', '', raw_line.strip())
    can_pattern = re.compile(r'([0-9A-Fa-f]{8}):([0-9A-Fa-f]{16})')
    first_match = can_pattern.search(raw_line)
    if not first_match:
        raise ValueError("No CAN frames found in packet")
    header_section = raw_line[:first_match.start()]
    can_section    = raw_line[first_match.start():]
    fields = header_section.rstrip('|, ').split(',')
    imei      = fields[6].strip()
    date_str  = fields[9].strip()
    time_str  = fields[10].strip()
    latitude  = float(fields[11].strip())
    lat_dir   = fields[12].strip()
    longitude = float(fields[13].strip())
    lon_dir   = fields[14].strip()
    speed     = float(fields[15].strip())
    heading   = float(fields[16].strip())
    if lat_dir == 'S': latitude  = -latitude
    if lon_dir == 'W': longitude = -longitude
    day    = int(date_str[0:2])
    month  = int(date_str[2:4])
    year   = int(date_str[4:8])
    hour   = int(time_str[0:2])
    minute = int(time_str[2:4])
    second = int(time_str[4:6])
    ts = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
    metadata = {
        'imei':        imei,
        'timestamp':   ts.isoformat(),
        'latitude':    latitude,
        'longitude':   longitude,
        'speed_kmh':   speed,
        'heading_deg': heading,
    }
    can_frames = [
        {'can_id': int(m.group(1), 16), 'data_hex': m.group(2)}
        for m in can_pattern.finditer(can_section)
    ]
    return metadata, can_frames

def register_athena_partition(bucket, imei, year, month, day):
    location = (
        f"s3://{bucket}/processed/"
        f"imei={imei}/year={year}/month={month}/day={day}/"
    )
    try:
        glue.create_partition(
            DatabaseName='telematics',
            TableName='processed_signals',        # ← CORRECT TABLE
            PartitionInput={
                'Values': [str(imei), str(year), str(month), str(day)],
                'StorageDescriptor': {
                    'Location': location,
                    'InputFormat':  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    }
                }
            }
        )
        print(f"Partition registered: imei={imei}/year={year}/month={month}/day={day}")
    except glue.exceptions.AlreadyExistsException:
        pass  # Already registered — completely fine, skip silently
    except Exception as e:
        print(f"Partition registration failed (non-fatal): {e}")

def lambda_handler(event, context):
    all_rows = []
    for s3_record in event['Records']:
        bucket = s3_record['s3']['bucket']['name']
        key    = s3_record['s3']['object']['key']
        print(f"Processing: s3://{bucket}/{key}")
        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj['Body'].read().decode('utf-8', errors='ignore')
        for line in content.splitlines():
            line = line.strip()
            if not line or not line.startswith('$'):
                continue
            try:
                metadata, can_frames = parse_itriangle_packet(line)
            except Exception as e:
                print(f"Bad packet: {e}")
                continue
            imei = metadata['imei']
            try:
                db_list = get_dbc_list_for_imei(bucket, imei)
            except ValueError as e:
                print(str(e))
                continue
            allowlist = get_signal_allowlist(bucket)
            for frame in can_frames:
                signal_rows = decode_frame_to_signal_rows(
                    db_list,
                    metadata,
                    frame['can_id'],
                    frame['data_hex'],
                    allowlist
                )
                all_rows.extend(signal_rows)
    if not all_rows:
        print("No rows decoded.")
        return {'statusCode': 200, 'body': 'No data decoded'}
    df = pd.DataFrame(all_rows)
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
    df['year']  = df['timestamp'].dt.year
    df['month'] = df['timestamp'].dt.month
    df['day']   = df['timestamp'].dt.day
    for (imei, year, month, day), group in df.groupby(['imei','year','month','day']):
        table = pa.Table.from_pandas(group, preserve_index=False)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression='snappy')
        buf.seek(0)
        out_key = (
            f"processed/imei={imei}/"
            f"year={year}/month={month}/day={day}/"
            f"{uuid.uuid4()}.parquet"
        )
        s3.put_object(Bucket=bucket, Key=out_key, Body=buf.getvalue())
        print(f"Written {len(group)} rows → {out_key}")
        register_athena_partition(bucket, imei, year, month, day)
    return {'statusCode': 200, 'body': f'Processed {len(all_rows)} rows'}
