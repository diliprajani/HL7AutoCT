import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

bedrock = boto3.client("bedrock-runtime")

def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)[:1000]}")
    
    js_specs = event.get("js_specs", [])
    logger.info(f"js_specs length: {len(js_specs)}")
    
    validation_report = []

    for i, segment_spec in enumerate(js_specs):
        logger.info(f"Processing segment {i+1}/{len(js_specs)}")
        
        segment_name = segment_spec.get("segment")
        
        if "js_result" in segment_spec:
            js_result = segment_spec.get("js_result")
            logger.info(f"Found js_result, type: {type(js_result)}")
            
            if isinstance(js_result, list) and len(js_result) > 0:
                fields = js_result[0].get("fields", []) if isinstance(js_result[0], dict) else []
            elif isinstance(js_result, dict):
                fields = js_result.get("fields", [])
            else:
                fields = []
        else:
            fields = segment_spec.get("fields", [])
        
        logger.info(f"Segment: {segment_name}, Found {len(fields)} fields")
        
        if not fields:
            logger.warning(f"No fields to process for segment {segment_name}, skipping")
            continue

        prompt = build_prompt(segment_name, fields)
        logger.info(f"Built prompt for {segment_name}, length: {len(prompt)} chars")
        
        try:
            logger.info(f"Calling Bedrock for segment {segment_name}")
            
            response = bedrock.invoke_model(
                modelId="anthropic.claude-3-sonnet-20240229-v1:0",
                contentType="application/json",
                accept="application/json",
                body=json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",    
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 4000,
                    "temperature": 0
                })
            )

            response_body = json.loads(response["body"].read())
            result_text = response_body["content"][0]["text"]
            
            logger.info(f"Received response from Claude for {segment_name}")
            
            parsed_fields = parse_response(result_text)
            logger.info(f"Parsed {len(parsed_fields)} validation results for {segment_name}")
            
            validation_report.append({
                "segment": segment_name,
                "fields": parsed_fields
            })
            
        except Exception as e:
            logger.error(f"Error processing segment {segment_name}: {str(e)}", exc_info=True)
            validation_report.append({
                "segment": segment_name,
                "error": str(e)
            })

    logger.info(f"Total validation report segments: {len(validation_report)}")

    return {
        "statusCode": 200,
        "body": json.dumps({"validation_report": validation_report}, indent=2)
    }

def build_prompt(segment_name, fields):
    lines = [
        f"You are an HL7 transformation assistant. Your task is to evaluate HL7 field values from the {segment_name} segment against transformation rules and produce a clean, structured JSON response.",
        "",
        "**Input Structure:**",
        "- Each field includes:",
        "  - Field ID (e.g., PV1-3)",
        "  - Label (field description)",
        "  - Sample Values (actual HL7 data as nested arrays)",
        "  - Statistics (fill rate, total rows, etc.)",
        "  - Ruleset (transformation rule to apply, if specified)",
        "",
        "**Instructions:**",
        "- WHEN a transformation rule exists:",
        "  - Apply the rule to each sample value",
        "  - Set 'Rule Triggered' to 'Yes'",
        "  - Include the rule text in 'Transformation Rules'",
        "  - Return ONLY the transformed values in 'Expected Output'",
        "- WHEN no rule exists:",
        "  - Set 'Rule Triggered' to 'No'",
        "  - Return sample values as-is in 'Expected Output'",
        "",
        "**HL7 Structure Guidance:**",
        "- Sample values may contain nested arrays representing HL7 components, sub-components, or repetitions",
        "- Reconstruct HL7 strings using:",
        "  - '^' to join components",
        "  - '&' to join sub-components",
        "  - '~' to join repetitions",
        "- Example: ['GLU', 'Glucose', ['LN', 'GLU', '1234']] → 'GLU^Glucose^LN&GLU&1234'",
        "- Example: [['ICU', '101'], ['ER', '01']] → 'ICU^101~ER^01'",
        "",
        "**Output Format (JSON only):**",
        "Return a JSON array of objects, each representing one field:",
        '[{',
        '  "Data Element": "...",',
        '  "Canonical Field": "...",',
        '  "Source Field": "...",',
        '  "Sample Input": [...],',
        '  "Expected Output": [...],',
        '  "Actual Output": [...],',
        '  "Validation Status": "Pass or Fail",',
        '  "Validation Comments": "Details if failed, e.g., \'[1] expected ER^01^01, got ER\'",',
        '  "Transformation Rules": "...",',
        '  "Source Field JS Code": "..."',
        '}]',
        "",
        "**Critical Instructions:**",
        "- ONE row per field",
        "- Sample Input, Expected Output, and Actual Output must be arrays",
        "- Escape special characters properly",
        "- Return ONLY valid JSON — no markdown, no commentary",
        "",
        "**Fields to Validate:**",
        ""
    ]
    
    fields_added = 0
    for field in fields:
        if not field.get("Source Field JS Code") or not field.get("Sample Input Values"):
            logger.warning(f"Skipping field {field.get('Data Element')} - missing required data")
            continue
        
        trans_rules = field.get('Transformation Rules', [])
        trans_rules_str = '; '.join(trans_rules) if isinstance(trans_rules, list) else str(trans_rules)
        
        logger.info(f"Transformation rules for {field.get('Canonical Field')}: {trans_rules_str}")
        
        sample_inputs = field.get('Sample Input Values')
        expected_outputs = field.get('Expected Output')
        
        lines.append("---")
        lines.append(f"Data Element: {field.get('Data Element')}")
        lines.append(f"Canonical Field: {field.get('Canonical Field')}")
        lines.append(f"Source Field: {field.get('Source Field')}")
        lines.append(f"Sample Input Values: {json.dumps(sample_inputs)}")
        lines.append(f"Expected Output: {json.dumps(expected_outputs)}")
        lines.append(f"Transformation Rules: {trans_rules_str}")
        lines.append(f"Source Field JS Code: {field.get('Source Field JS Code')}")
        lines.append("")
        fields_added += 1
    
    logger.info(f"Added {fields_added} fields to prompt for {segment_name}")
    
    lines.append("---")
    lines.append("Return ONLY the JSON array with one object per field.")
    
    return "\n".join(lines)

def parse_response(text):
    try:
        cleaned = text.strip()
        
        if "```" in cleaned:
            start = cleaned.find("```") + 3
            if cleaned[start:start+4] == "json":
                start += 4
            end = cleaned.find("```", start)
            cleaned = cleaned[start:end].strip()
        
        if not cleaned.startswith('['):
            start = cleaned.find('[')
            end = cleaned.rfind(']')
            if start != -1 and end != -1:
                cleaned = cleaned[start:end+1]
        
        parsed = json.loads(cleaned)
        for item in parsed:
            if not all(k in item for k in ["Canonical Field", "Expected Output", "Actual Output"]):
                logger.warning(f"Incomplete field data: {item}")

        logger.info(f"Successfully parsed {len(parsed)} items")
        return parsed
        
    except Exception as e:
        logger.error(f"Parse error: {e}")
        logger.error(f"Text to parse: {text[:500]}")
        return [{"error": "Failed to parse", "raw": text[:200]}]
