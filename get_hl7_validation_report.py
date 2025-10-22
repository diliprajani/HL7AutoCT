import json
import boto3

stepfunctions = boto3.client("stepfunctions")

pipeline_steps = {
    "ParseHL7Messages": "Parsing HL7 Messages",
    "AnalyzeHL7Fields": "Analyzing HL7 Fields",
    "EvaluateTransformationRulesPerSegment": "Evaluating Transformation Rules",
    "GenerateHL7Specification": "Generating HL7 Specification",
    "GenerateMirthJSCode": "Generating Mirth JS Code",
    "ExportMirthXMLJSCode": "Exporting Mirth XML + JS",
    "ValidateJSLogic": "Validating JS Logic",
    "ExportJSValidationReport": "Exporting Validation Report",
    "AggregateResults": "Finalizing Results"
}

def lambda_handler(event, context):
    cors_headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type"
    }

    # ✅ Handle OPTIONS preflight request
    if event.get("httpMethod") == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": cors_headers,
            "body": json.dumps({"message": "CORS preflight OK"})
        }

    query_params = event.get("queryStringParameters", {})
    execution_arn = query_params.get("executionArn")
    report_type = query_params.get("type")

    if not execution_arn:
        return {
            "statusCode": 400,
            "headers": cors_headers,
            "body": json.dumps({"error": "Missing 'executionArn' query parameter"})
        }

    try:
        response = stepfunctions.describe_execution(executionArn=execution_arn)
        status = response.get("status")
        output_raw = response.get("output", "{}")

        try:
            output = json.loads(output_raw)
        except Exception:
            output = {"raw_output": output_raw}

        if status == "SUCCEEDED" and report_type:
            final = output.get("final_status", {})
            report_type = report_type.lower()

            if report_type == "specification":
                download_url = final.get("specification_download_url")
            elif report_type == "validation":
                download_url = final.get("validation_report_download_url")
            else:
                return {
                    "statusCode": 400,
                    "headers": cors_headers,
                    "body": json.dumps({"error": f"Invalid report type '{report_type}'. Use 'specification' or 'validation'."})
                }

            if download_url:
                return {
                    "statusCode": 302,
                    "headers": {
                        **cors_headers,
                        "Location": download_url
                    }
                }

        history = stepfunctions.get_execution_history(
            executionArn=execution_arn,
            maxResults=1000,
            reverseOrder=False
        )

        completed_states = {
            event["stateExitedEventDetails"]["name"]
            for event in history.get("events", [])
            if event["type"] == "TaskStateExited"
        }

        all_steps = list(pipeline_steps.keys())
        current_index = max(
            (i for i, step in enumerate(all_steps) if step in completed_states),
            default=-1
        )
        current_step_key = all_steps[current_index + 1] if current_index + 1 < len(all_steps) else "Completed"
        current_step = pipeline_steps.get(current_step_key, current_step_key)

        progress_percent = "100%" if status == "SUCCEEDED" else f"{int(((current_index + 1) / len(all_steps)) * 100)}%"

        result = {
            "executionArn": execution_arn,
            "status": status,
            "message": "Pipeline is still running or download URL not available yet.",
            "progress": progress_percent,
            "current_step": current_step,
            "final_status": output.get("final_status", {})
        }

        return {
            "statusCode": 200,
            "headers": cors_headers,
            "body": json.dumps(result, indent=2)
        }

    except stepfunctions.exceptions.ExecutionDoesNotExist as e:
        return {
            "statusCode": 404,
            "headers": cors_headers,
            "body": json.dumps({"error": "Execution not found", "details": str(e)})
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": cors_headers,
            "body": json.dumps({"error": "Failed to retrieve execution", "details": str(e)})
        }
