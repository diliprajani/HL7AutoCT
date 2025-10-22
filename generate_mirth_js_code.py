import boto3
import json
import time
import random

bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")

def get_canonical_js_code(field_id):
    segment, path = field_id.split("-")
    parts = path.split(".")
    field = parts[0]
    sub = parts[1] if len(parts) > 1 else "1"
    return f"tmp['{segment}']['{segment}.{field}']['{segment}.{field}.{sub}']"

def build_claude_prompt(segment, fields):
    prompt = f"""You are an expert in HL7 v2, JavaScript, and Mirth Connect. Your task is to generate JavaScript logic for HL7 fields using Mirth Connect conventions.

Template Reference:
tmp['SEG']['SEG.X']['SEG.X.Y'] = msg['SEG']['SEG.A']['SEG.A.B'].toString();
Where:

SEG = HL7 segment name (e.g., PID, OBX, MSH)
X and A = HL7 field numbers (e.g., 5 for PID-5)
Y and B = HL7 component numbers (e.g., 1 for PID-5.1)
The left-hand side must always use the tmp object
The right-hand side must always use the msg object

CRITICAL: HANDLING FIELDS WITH NA OR MISSING SOURCE
- If Source Field is "NA", "N/A", empty, or null, this means the field is not present in the source message
- For these fields, assign an empty string "" to the canonical field
- DO NOT reference msg['NA'] or msg['N/A']
- Example for NA field:
  Canonical Field JS Code: tmp['PV1']['PV1.8']['PV1.8.1'];
  Source Field JS Code: ""
  Canonical-Source Field JS Code: tmp['PV1']['PV1.8']['PV1.8.1'] = "";

Output Requirements:
For each HL7 field, return exactly three JavaScript code blocks:

1. Canonical Field JS Code:
Return only the left-hand side of the assignment (the target field reference)
Use only the tmp object
Do not include msg, .toString(), or assignment operators (=)
Always default to component 1 unless a specific component is mentioned
Example:
Input: PID-3
Output: tmp['PID']['PID.3']['PID.3.1'];

2. Source Field JS Code:

WHEN SOURCE FIELD IS "NA", "N/A", EMPTY, OR NULL:
- Return only: ""
- Do NOT use msg object
- Do NOT use .toString()

WHEN TRANSFORMATION RULES ARE PROVIDED:
- Use the msg object
- Always append .toString() to msg references
- Apply the transformation logic using standard JavaScript
- Store the result in a temporary variable (e.g., let tmpPID5 = '';)
- Example:
let tmpPID5 = '';
const pid5Values = msg['PID']['PID.5'];
for (let i = 0; i < pid5Values.length; i++) {{
    const pid5Iteration = pid5Values[i];
    const pid5_7 = pid5Iteration['PID.5.7'].toString();
    if (pid5_7 === 'L' || pid5_7 === 'Current' || pid5_7 === 'Legal') {{
        tmpPID5 = pid5Iteration['PID.5.1'].toString();
        break;
    }} else if (tmpPID5 === '') {{
        tmpPID5 = pid5Iteration['PID.5.1'].toString();
    }}
}}

WHEN NO TRANSFORMATION RULES ARE PROVIDED:
- Return ONLY the msg reference with .toString()
- Do NOT use temporary variables
- Do NOT include assignment operators
- Example:
msg['PID']['PID.8']['PID.8.1'].toString()

3. Canonical-Source Field JS Code:
This block contains the COMPLETE executable JavaScript code.

WHEN SOURCE FIELD IS "NA", "N/A", EMPTY, OR NULL:
- Assign empty string to the canonical field
- Example:
tmp['PV1']['PV1.8']['PV1.8.1'] = "";

WHEN TRANSFORMATION RULES ARE PROVIDED:
- Include the full logic from "Source Field JS Code" (variable declaration, transformation logic, etc.)
- End with the assignment to the tmp object
- Example:
let tmpPID5 = '';
const pid5Values = msg['PID']['PID.5'];
for (let i = 0; i < pid5Values.length; i++) {{
    const pid5Iteration = pid5Values[i];
    const pid5_7 = pid5Iteration['PID.5.7'].toString();
    if (pid5_7 === 'L' || pid5_7 === 'Current' || pid5_7 === 'Legal') {{
        tmpPID5 = pid5Iteration['PID.5.1'].toString();
        break;
    }} else if (tmpPID5 === '') {{
        tmpPID5 = pid5Iteration['PID.5.1'].toString();
    }}
}}
tmp['PID']['PID.5']['PID.5.1'] = tmpPID5;

WHEN NO TRANSFORMATION RULES ARE PROVIDED (direct mapping):
- Use ONLY a single direct assignment statement
- Do NOT use temporary variables
- Directly assign msg to tmp in one line
- Example:
tmp['PID']['PID.8']['PID.8.1'] = msg['PID']['PID.8']['PID.8.1'].toString();

Formatting Instructions:
Always specify fields down to the first component unless otherwise stated
Use consistent formatting with clear, readable code structure
Do not include any explanatory text — only return the JavaScript code blocks
Always maintain proper JavaScript syntax

CRITICAL Rules for Source Field JS Code:
DO NOT use functions - No function declarations or implementations (no function(), no arrow functions =>)
Write inline sequential code only - Use straightforward variable assignments and control structures
One operation per line - Do not cluster multiple operations on a single line
Keep logic simple and readable - Use basic if/else, loops, and variable assignments
Use temporary variables ONLY when transformation rules are provided
No reusable function wrappers - Code should be direct transformation logic only
NEVER reference msg['NA'] or msg['N/A'] - assign empty string "" instead

CRITICAL: YOU MUST GENERATE SEPARATE CODE BLOCKS FOR EACH FIELD
- Each field must have its own set of three code blocks (Canonical, Source, and Canonical-Source)
- Do NOT combine multiple fields into one block
- Always maintain the exact block structure for every field
- Process fields one at a time in the order provided

Output Format (REPEAT THIS STRUCTURE FOR EVERY FIELD):

=== FIELD: <Canonical Field ID> ===

*** Canonical Field JS Code: ***
** Start Canonical Code **
<canonical code for THIS field only>
** End Canonical Code **

*** Source Field JS Code: ***
** Start Source Code **
<source code for THIS field only - use "" if source is NA/empty, NO variable assignment if no transformation rules>
** End Source Code **

*** Canonical-Source Field JS Code: ***
** Start Canonical-Source Code **
<complete executable code for THIS field only - use = ""; if source is NA/empty>
** End Canonical-Source Code **

Input Format I Will Provide:
Canonical Field: Target field location (e.g., PID-5)
Source Field: Source field location (e.g., PID-5) - may be "NA" or empty if field not present in source
Transformation Rule: Logic description (e.g., "If PID-5.7 is 'L', 'Current', or 'Legal', copy that iteration. Otherwise, copy first non-null iteration.")

Segment: {segment}

FIELDS TO PROCESS:
"""

    for i, field in enumerate(fields, 1):
        canonical = field["Canonical Field"]
        source = field.get("Source Field", "")
        rule = field.get("Transformation Rules", "")
        
        # Make it explicit when source is NA or empty
        source_display = source if source and source not in ["NA", "N/A"] else "NA (field not present in source)"
        
        prompt += f"\n\nField #{i}:\n- Canonical Field: {canonical}\n- Source Field: {source_display}\n- Transformation Rule: {rule if rule else '(No transformation - direct mapping)'}\n"
    
    return prompt


def invoke_with_backoff(payload, max_retries=8):
    for attempt in range(max_retries):
        try:
            response = bedrock.invoke_model(**payload)
            return response
        except bedrock.exceptions.ThrottlingException:
            wait = 2 ** attempt + random.uniform(0, 2)
            print(f"Throttled. Retrying in {wait:.2f} seconds...")
            time.sleep(wait)
        except Exception as e:
            print(f"Non-throttling error: {str(e)}")
            raise e
    raise Exception("Max retries exceeded due to throttling.")

def call_claude(prompt):
    payload = {
        "modelId": "anthropic.claude-3-sonnet-20240229-v1:0",
        "contentType": "application/json",
        "accept": "application/json",
        "body": json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 4000,
            "temperature": 0.2
        })
    }

    response = invoke_with_backoff(payload)
    
    result = json.loads(response["body"].read())
    return result["content"][0]["text"].strip()

def parse_claude_response(response_text, fields):
    print("@@@@@@@@@@@@@@@Claude raw response:\n", response_text)
    
    field_map = {}
    
    # Split response by field markers
    field_sections = response_text.split("=== FIELD:")
    
    for section in field_sections[1:]:  # Skip the first empty split
        lines = section.split("\n")
        field_id = lines[0].strip().replace("===", "").strip()
        
        canonical_code = ""
        source_code = ""
        combined_code = ""
        
        mode = None
        inside_block = False
        current_block = []
        
        for line in lines[1:]:
            line_stripped = line.strip()
            
            if line_stripped == "** Start Canonical Code **":
                mode = "canonical"
                inside_block = True
                current_block = []
                continue
            elif line_stripped == "** End Canonical Code **":
                canonical_code = "\n".join(current_block).strip()
                inside_block = False
                continue
            elif line_stripped == "** Start Source Code **":
                mode = "source"
                inside_block = True
                current_block = []
                continue
            elif line_stripped == "** End Source Code **":
                source_code = "\n".join(current_block).strip()
                inside_block = False
                continue
            elif line_stripped == "** Start Canonical-Source Code **":
                mode = "combined"
                inside_block = True
                current_block = []
                continue
            elif line_stripped == "** End Canonical-Source Code **":
                combined_code = "\n".join(current_block).strip()
                inside_block = False
                continue
            
            if inside_block:
                current_block.append(line)
        
        field_map[field_id] = {
            "Canonical Field JS Code": canonical_code,
            "Source Field JS Code": source_code,
            "Canonical-Source Field JS Code": combined_code
        }
    
    return field_map



def lambda_handler(event, context):
    segment_list = event if isinstance(event, list) else [event]
    updated_segments = []

    for segment_obj in segment_list:
        segment = segment_obj.get("segment")
        fields = segment_obj.get("fields", [])

        if not segment or not fields:
            continue

        for field in fields:
            field["Canonical Field JS Code"] = get_canonical_js_code(field["Canonical Field"])

        prompt = build_claude_prompt(segment, fields)
        response_text = call_claude(prompt)
        js_map = parse_claude_response(response_text, fields)

        for field in fields:
            field_id = field["Canonical Field"]
            field["Canonical Field JS Code"] = js_map.get(field_id, {}).get("Canonical Field JS Code", "")
            field["Source Field JS Code"] = js_map.get(field_id, {}).get("Source Field JS Code", "")
            field["Canonical-Source Field JS Code"] = js_map.get(field_id, {}).get("Canonical-Source Field JS Code", "")

        updated_segments.append({
            "segment": segment,
            "fields": fields
        })

    return updated_segments
