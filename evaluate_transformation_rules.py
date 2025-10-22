import json
import boto3
import time
import random

bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")

def invoke_with_backoff(payload, max_retries=5):
    for attempt in range(max_retries):
        try:
            response = bedrock.invoke_model(**payload)
            return response
        except bedrock.exceptions.ThrottlingException as e:
            wait = 2 ** attempt + random.uniform(0, 1)
            print(f"Throttled. Retrying in {wait:.2f} seconds...")
            time.sleep(wait)
        except Exception as e:
            print(f"Non-throttling error: {str(e)}")
            raise e
    raise Exception("Max retries exceeded due to throttling.")

def lambda_handler(event, context):
    segment = event.get("segment")
    print("segment:", segment)
    fields = event.get("fields", [])
    print("fields:", fields)

    if not segment or not fields:
        return {
            "statusCode": 400,
            "error": "Missing 'segment' or 'fields' in request payload."
        }

    prompt = f"""
    You are an HL7 transformation assistant. Your task is to evaluate HL7 field values from the {segment} segment against transformation rules and produce a clean, structured JSON response.

    Each field includes:
    - Field ID (e.g., PID-5)
    - Sample values (from HL7 data)
    - Ruleset (if any)

    Fields:
    {json.dumps(fields, indent=2)}

    **Instructions:**

    WHEN A TRANSFORMATION RULESET EXISTS:
    - Apply the transformation logic to each sample value
    - Set `"Rule Triggered"` to `"Yes"`
    - Return ONLY the transformed values in `"Expected Output"` (no commentary)

    WHEN NO TRANSFORMATION RULESET EXISTS:
    - Set `"Rule Triggered"` to `"No"`
    - Return sample values as-is in `"Expected Output"`

    **CRITICAL LOGIC**
    - Always include the transformation logic in "Transformation Rules" if provided.
    - Return ONLY the transformed values in `"Expected Output"` (no commentary)

    ---

    **HL7 Structure Guidance:**

    HL7 fields may contain:
    - **Components**: e.g., `"ICU^101^1^HospitalA"` → `["ICU", "101", "1", "HospitalA"]`. Here "^" acts as Component separator
    - **Sub-components**: e.g., `"LN&GLU&1234"` → `["LN", "GLU", "1234"]`. Here "&" acts as Sub-component separator
    - **Field repetitions**: e.g., ICU^101^1^HospitalA~ER^102^2^HospitalB → `[ICU^101^1^HospitalA, ER^102^2^HospitalB, ...]`. Here "~" acts as repetitions separator

    Sample values will be provided as **nested arrays** to reflect this structure.

    ---

    **Transformation Examples:**

    - `"Copy PV1-3.1"` → Extract component 1 from each PV1-3 value → `["ICU", "ER", "41PMCER"]`
    - `"Copy PV1-7"` → Return full field value as-is → `["1234^Smith^Jane", "105927^VINCENT^ANDREW^L"]`
    - `"Copy first non-null iteration of PV1-7 where PV1-7.13 = 'NPI'"` → Return matching iteration of PV1-7 according to where condition
    - `"OBR-4"` → Combine components and sub-components into HL7 string format:
      - `["GLU", "Glucose", ["LN", "GLU", "1234"]]` → `"GLU^Glucose^LN&GLU&1234"`
      - Multiple values → Join with `~` → `"GLU^Glucose^LN&GLU&1234~A1C^Test^LN1&GLU1&12341"`

    ---

    **Output Format (JSON only):**

    Return a JSON object where each key is a field ID and the value is an object with.
    Please see below examples for reference. Similarly, apply the transformation logic to 

    {{
      "PV1-3": {{
        "Rule Triggered": "Yes",
        "Transformation Rules": "Copy PV1-3.1", 
        "Sample Values": ["ICU", "101", "1", "HospitalA"], ["ER", "01", "01"], ["41PMCER", "19", "19", "41PMC"],
        "Expected Output": ["ICU", "ER", "41PMCER"]
      }},
      "PV1-7": {{
        "Rule Trigered": "Yes",
        "Transformation Rules": "iteration": "Copy first non-null iteration of PV1-7 if PV1-7.13 equals 'National Provider Identifier' or 'NPI'; otherwise copy first non-null iteration of PV1-7.", 
        "Sample Values": "[["1234", "Smith", "Jane", "", "", "", "", "", "", "", "", "", "NPI"], ["105927", "VINCENT", "ANDREW", "L"]]",
        "Expected Output": ["1234^Smith^Jane^^^^^^^^^^NPI"]
      }},
      "OBR-4": {{
        "Rule Triggered": "No",
        "Transformation Rules": "", 
        "Sample Values": [["GLU", "Glucose", ["LN", "GLU", "1234"]], ["A1C", "Test", ["LN1", "GLU1", "12341"]]],
        "Expected Output": ["GLU^Glucose^LN&GLU&1234~A1C^Test^LN1&GLU1&12341"]
      }}
    }}
    """


    payload = {
        "modelId": "anthropic.claude-3-sonnet-20240229-v1:0",
        "contentType": "application/json",
        "accept": "application/json",
        "body": json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 4000,
            "temperature": 0.5
        })
    }

    try:
        response = invoke_with_backoff(payload)
        result = json.loads(response["body"].read())
        claude_text = result["content"][0]["text"]
        print("claude_text:", claude_text)

        
        try:
            # Remove Markdown code block if present
            if claude_text.strip().startswith("```json"):
                claude_text = claude_text.strip().removeprefix("```json").removesuffix("```").strip()
            elif claude_text.strip().startswith("```"):
                claude_text = claude_text.strip().removeprefix("```").removesuffix("```").strip()

            parsed = json.loads(claude_text)
        except json.JSONDecodeError:
            parsed = {
                "error": "Claude response could not be parsed as JSON",
                "raw_response": claude_text
            }

        return {
            "statusCode": 200,
            "segment": segment,
            "evaluations": parsed
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "error": str(e)
        }
