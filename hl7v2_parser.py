import json
import re
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from collections import defaultdict
from datetime import datetime

# HL7 message split pattern
HL7_SPLIT_PATTERN = r'MSH\|'

# Load schema from S3
def load_schema_from_s3():
    s3 = boto3.client('s3')
    schema_bucket = 'hl7v2autoct'
    schema_key = 'config/schema/hl7_segment_schema.json'
    obj = s3.get_object(Bucket=schema_bucket, Key=schema_key)
    schema_data = obj['Body'].read().decode('utf-8')
    return json.loads(schema_data)

SEGMENT_SCHEMA = load_schema_from_s3()

def clear_s3_folder(bucket, prefix):
    """
    Delete all objects under a specific S3 prefix (folder).
    
    Args:
        bucket: S3 bucket name
        prefix: S3 prefix/folder path (without leading slash, with trailing slash)
    """
    s3 = boto3.client('s3')
    try:
        print(f"Clearing S3 folder: s3://{bucket}/{prefix}")
        
        # List all objects under the prefix
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        delete_count = 0
        for page in pages:
            if 'Contents' not in page:
                print(f"No objects found under {prefix}")
                continue
            
            # Prepare objects for deletion (max 1000 per batch)
            objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
            
            if objects_to_delete:
                # Delete objects in batch
                response = s3.delete_objects(
                    Bucket=bucket,
                    Delete={'Objects': objects_to_delete}
                )
                deleted = len(response.get('Deleted', []))
                delete_count += deleted
                print(f"Deleted {deleted} objects from {prefix}")
                
                # Check for errors
                if 'Errors' in response:
                    for error in response['Errors']:
                        print(f"Error deleting {error['Key']}: {error['Message']}")
        
        print(f"Total objects deleted: {delete_count}")
        return delete_count
        
    except Exception as e:
        print(f"Error clearing S3 folder: {str(e)}")
        raise

def getSegments(msg):
    return re.split(r'\r|\n', msg.strip())

# Parse HL7 field into nested list structure
def parse_field(field):
    repetitions = field.split('~')
    if len(repetitions) == 1:
        components = repetitions[0].split('^')
        comp_list = []
        for comp in components:
            if '&' in comp:
                subcomps = comp.split('&')
                comp_list.append(subcomps)
            else:
                comp_list.append(comp)
        return comp_list if len(comp_list) > 1 else comp_list[0]
    else:
        parsed = []
        for rep in repetitions:
            components = rep.split('^')
            comp_list = []
            for comp in components:
                if '&' in comp:
                    subcomps = comp.split('&')
                    comp_list.append(subcomps)
                else:
                    comp_list.append(comp)
            parsed.append(comp_list)
        return parsed

# Convert HL7 message to structured JSON
def hl7_to_custom_json(HL7Message):
    result = defaultdict(list)
    segments = getSegments(HL7Message)

    for segment in segments:
        if not segment.strip():
            continue

        parts = segment.strip().split('|')
        seg_name = parts[0]
        seg_data = {}

        if seg_name == "MSH":
            seg_data["1"] = "|"
            seg_data["2"] = parts[1]
            fields = parts[2:]
            field_offset = 3
        else:
            fields = parts[1:]
            field_offset = 1

        for i, field in enumerate(fields, start=field_offset):
            if not field:
                continue
            seg_data[str(i)] = parse_field(field)

        result[seg_name].append(seg_data)

    for key in result:
        if len(result[key]) == 1:
            result[key] = result[key][0]

    return dict(result)

# Split multiple HL7 messages
def split_hl7_messages(raw_data):
    chunks = re.split(HL7_SPLIT_PATTERN, raw_data.strip())
    return [chunk if chunk.startswith("MSH|") else "MSH|" + chunk for chunk in chunks if chunk.strip()]

def parse_multiple_hl7_messages(raw_data):
    messages = split_hl7_messages(raw_data)
    return [hl7_to_custom_json(msg) for msg in messages]

# Serialize complex fields for Parquet compatibility
def serialize_field(value):
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    return value

# Convert parsed messages into Parquet-compatible tables
def convert_to_parquet_tables(parsed_messages, schema):
    tables = defaultdict(list)

    for msg in parsed_messages:
        message_id = msg.get("MSH", {}).get("10", "")  # MSH-10

        for segment, entries in msg.items():
            if segment not in schema:
                continue

            if isinstance(entries, dict):
                entries = [entries]

            for entry in entries:
                row = {"message_control_id": message_id}
                for key, value in entry.items():
                    field_name = f"{segment}-{key}"
                    row[field_name] = serialize_field(value)
                tables[segment].append(row)

    return tables

# Store Parquet files in S3
def store_parquet_to_s3(tables, bucket):
    s3 = boto3.client('s3')
    output_keys = []
    for segment, rows in tables.items():
        df = pd.DataFrame(rows)
        table = pa.Table.from_pandas(df)
        buffer = BytesIO()
        pq.write_table(table, buffer)

        output_key = f"output/parsed_hl7_segments/segment={segment}/hl7_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.parquet"
        s3.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=buffer.getvalue(),
            ContentType='application/octet-stream'
        )

        output_keys.append({
            "segment": segment,
            "s3_key": output_key
        })

    return output_keys

# Lambda entry point
def lambda_handler(event, context):
    try:
        print("Lambda triggered")
        print("Event:", event)

        # Check for raw HL7 message
        if 'body' in event:
            try:
                body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
                hl7_raw = body.get('hl7_message')
            except Exception as parse_error:
                raise ValueError(f"Failed to parse request body: {str(parse_error)}")
        else:
            hl7_raw = event.get('hl7_message')

        if not hl7_raw:
            raise ValueError("Missing 'hl7_message' in event payload")

        # Target S3 bucket
        input_bucket = 'hl7v2autoct'
        input_key = f"input/raw_hl7_messages/hl7_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.txt"

        # Upload HL7 message to S3
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket=input_bucket,
            Key=input_key,
            Body=hl7_raw.encode('utf-8'),
            ContentType='text/plain'
        )
        print(f"Uploaded HL7 message to s3://{input_bucket}/{input_key}")

        # Cleanup parsed HL7 segments folder
        try:
            output_prefix = "output/parsed_hl7_segments/"
            print(f"Cleaning up output folder before parsing: s3://{input_bucket}/{output_prefix}")
            clear_s3_folder(input_bucket, output_prefix)
        except Exception as cleanup_error:
            print(f"Warning: Failed to cleanup output folder: {str(cleanup_error)}")

        # Parse and store HL7 segments
        obj = s3.get_object(Bucket=input_bucket, Key=input_key)
        hl7_raw = obj['Body'].read().decode('utf-8')

        parsed_messages = parse_multiple_hl7_messages(hl7_raw)
        tables = convert_to_parquet_tables(parsed_messages, SEGMENT_SCHEMA)
        output_keys = store_parquet_to_s3(tables, input_bucket)
        print("output_keys:", output_keys)

        # Start Step Function
        stepfunctions = boto3.client('stepfunctions')
        step_response = stepfunctions.start_execution(
            stateMachineArn="arn:aws:states:us-east-1:238845559334:stateMachine:HL7v2AutoCT",
            input=json.dumps({
                "input_key": input_key,
                "parsed_segments": output_keys
            })
        )

        execution_arn = step_response['executionArn']
        print("Step Function started:", execution_arn)

        # Return response to user
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "message": (
                    "Pipeline started successfully.\n"
                    "It may take up to 6 to 8 minutes to complete.\n\n"
                    "To retrieve the final validation report, use the URL below:"
                ),
                "execution_arn": execution_arn,
                "check_status_url": f"https://4p593pb2bf.execute-api.us-east-1.amazonaws.com/prod/get-hl7autoct-report?executionArn={execution_arn}",
                "get_specification_file_url": f"https://4p593pb2bf.execute-api.us-east-1.amazonaws.com/prod/get-hl7autoct-report?executionArn={execution_arn}&type=specification",
                "get_validation_report_url": f"https://4p593pb2bf.execute-api.us-east-1.amazonaws.com/prod/get-hl7autoct-report?executionArn={execution_arn}&type=validation",
                "timestamp": datetime.utcnow().isoformat() + "Z"
            })
        }


    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json"
            },
            "body": json.dumps({"error": str(e)})
        }
