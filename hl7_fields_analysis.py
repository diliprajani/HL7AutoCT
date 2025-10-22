import json
import boto3
import time
from collections import defaultdict

ATHENA_DATABASE = "hl7v2_db"
ATHENA_OUTPUT = "s3://hl7v2autoct/output/analyzed_hl7v2_data/"
RULESET_BUCKET = "hl7v2autoct"
RULESET_KEY = "config/ruleset/hl7_ruleset.json"

s3 = boto3.client("s3")
athena = boto3.client("athena")

def clear_s3_folder(bucket, prefix):
    """
    Delete all objects under a specific S3 prefix (folder).
    
    Args:
        bucket: S3 bucket name
        prefix: S3 prefix/folder path (without leading slash)
    """
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

def run_athena_query(query):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
    )
    query_execution_id = response["QueryExecutionId"]

    for _ in range(30):  # Wait up to 30 seconds
        status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(1)

    if state != "SUCCEEDED":
        print(f"Query failed or timed out: {state}")
        return None

    result = athena.get_query_results(QueryExecutionId=query_execution_id)
    return result

def parse_athena_result(result):
    rows = result["ResultSet"]["Rows"]
    if len(rows) < 2:
        return {}
    headers = [col.get("VarCharValue", "") for col in rows[0]["Data"]]
    values = [col.get("VarCharValue", "") for col in rows[1]["Data"]]
    return dict(zip(headers, values))

def lambda_handler(event, context):
    print("Lambda triggered with input:")
    print(json.dumps(event))

    # **CLEANUP ATHENA OUTPUT FOLDER AT START**
    try:
        # Extract bucket and prefix from ATHENA_OUTPUT
        # Format: s3://bucket-name/prefix/path/
        athena_output_parts = ATHENA_OUTPUT.replace("s3://", "").split("/", 1)
        output_bucket = athena_output_parts[0]
        output_prefix = athena_output_parts[1] if len(athena_output_parts) > 1 else ""
        
        # Ensure prefix doesn't start with '/' but ends with '/'
        output_prefix = output_prefix.lstrip('/').rstrip('/') + '/'
        
        print(f"Cleaning up Athena output: s3://{output_bucket}/{output_prefix}")
        clear_s3_folder(output_bucket, output_prefix)
        
    except Exception as e:
        print(f"Warning: Failed to cleanup Athena output folder: {str(e)}")
        # Continue execution even if cleanup fails

    # Load HL7 ruleset from S3
    ruleset_obj = s3.get_object(Bucket=RULESET_BUCKET, Key=RULESET_KEY)
    ruleset = json.loads(ruleset_obj["Body"].read().decode("utf-8"))

    # Filter enabled fields
    enabled_fields = []
    for segment, fields in ruleset.items():
        for field_id, field_info in fields.items():
            if field_info.get("IsEnabled", False):
                enabled_fields.append({
                    "segment": segment,
                    "field_id": field_id,
                    "label": field_info.get("name", ""),
                    "usage": field_info.get("usage", ""),
                    "ruleset": field_info.get("ruleset", [])
                })

    # Enrich fields using Athena
    enriched_fields = []
    for field in enabled_fields:
        segment = field["segment"]
        field_id = field["field_id"]
        table_name = f"hl7_segment_{segment.lower()}"
        column_name = f'"{field_id}"'

        query = f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT({column_name}) AS filled_rows,
            ROUND(COUNT({column_name}) * 100.0 / COUNT(*), 2) AS fill_rate,
            MIN(length({column_name})) AS min_length,
            MAX(length({column_name})) AS max_length,
            CAST(ARRAY_AGG(DISTINCT {column_name}) AS JSON) AS all_values
        FROM {table_name}
        WHERE {column_name} IS NOT NULL;
        """

        print(f"Running Athena query for {segment}-{field_id}")
        result = run_athena_query(query)
        stats = parse_athena_result(result) if result else {}
        print(f"Stats for {segment}-{field_id}: {json.dumps(stats)}")

        enriched_fields.append({
            **field,
            "stats": stats
        })

    # Group enriched fields by segment
    grouped = defaultdict(list)
    for field in enriched_fields:
        grouped[field["segment"]].append(field)

    fields_by_segment = [
        {
            "segment": segment,
            "fields": fields
        }
        for segment, fields in grouped.items()
    ]

    print("Returning fields_by_segment:")
    print(json.dumps(fields_by_segment))

    return {
        "fields_by_segment": fields_by_segment
    }
