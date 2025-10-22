import json
import xml.etree.ElementTree as ET
from xml.dom import minidom
import boto3
import re

s3 = boto3.client("s3")
bucket_name = "hl7v2autoct"
template_key = "config/mirth_templates/mirth_orm_template.xml"
output_key_prefix = "output/mirth_js_xml_code/"
output_key = f"{output_key_prefix}mirth_js_xml_code.xml"

def prettify_xml(elem):
    """Return a pretty-printed XML string for the Element without XML declaration."""
    rough_string = ET.tostring(elem, encoding='unicode')
    reparsed = minidom.parseString(rough_string)
    pretty_xml = reparsed.toprettyxml(indent="  ")
    # Remove XML declaration line
    return '\n'.join(pretty_xml.split('\n')[1:])

def extract_javascript_from_template(template_content):
    """Extract JavaScript code between START and END markers."""
    lines = template_content.split('\n')
    js_lines = []
    capture = False
    
    for line in lines:
        if '// START_JAVASCRIPT_CODE' in line:
            capture = True
            continue
        elif '// END_JAVASCRIPT_CODE' in line:
            capture = False
            continue
        
        if capture:
            js_lines.append(line)
    
    return '\n'.join(js_lines)

def replace_javascript_in_template(template_content, new_js_code):
    """Replace JavaScript code between START and END markers with new code."""
    lines = template_content.split('\n')
    result_lines = []
    capture = False
    
    for line in lines:
        if '// START_JAVASCRIPT_CODE' in line:
            result_lines.append(line)
            result_lines.append(new_js_code)
            capture = True
            continue
        elif '// END_JAVASCRIPT_CODE' in line:
            result_lines.append(line)
            capture = False
            continue
        
        if not capture:
            result_lines.append(line)
    
    return '\n'.join(result_lines)

def indent_js_code(js_code):
    """
    Properly indent JavaScript code based on braces and control structures.
    """
    lines = js_code.split('\n')
    indented_lines = []
    indent_level = 0
    base_indent = 1  # Base indentation for code inside try block
    
    for line in lines:
        stripped = line.strip()
        
        # Skip empty lines (will be added back without indent)
        if not stripped:
            indented_lines.append('')
            continue
        
        # Skip comment lines (add base indent + current level)
        if stripped.startswith('//'):
            indented_lines.append('  ' * (base_indent + indent_level) + stripped)
            continue
        
        # Decrease indent for closing braces
        if stripped.startswith('}'):
            indent_level = max(0, indent_level - 1)
        
        # Add the line with proper indentation (base + current level)
        indented_lines.append('  ' * (base_indent + indent_level) + stripped)
        
        # Increase indent for opening braces
        if stripped.endswith('{'):
            indent_level += 1
    
    # Join lines and replace single quotes with XML entity
    result = '\n'.join(indented_lines)
    result = result.replace("'", "&apos;")
    
    return result

def lambda_handler(event, context):
    print("event:", event)
    js_specs = event.get("js_specs", [])
    print("js_specs:", js_specs)
    
    # Read the template file from S3
    try:
        template_response = s3.get_object(Bucket=bucket_name, Key=template_key)
        template_content = template_response['Body'].read().decode('utf-8')
        print("Template file loaded successfully")
    except Exception as e:
        print(f"Error reading template file: {str(e)}")
        return {
            "status": "error",
            "message": f"Failed to read template: {str(e)}"
        }
    
    js_content = []
    current_segment = None

    for spec in js_specs:
        print("spec:", spec)
        js_result = spec.get("js_result", [])
        print("js_result:", js_result)
        if not js_result:
            continue

        segment = js_result[0].get("segment", "")
        fields = js_result[0].get("fields", [])
        print("fields:", fields)
        
        # Add segment comment header
        if segment and segment != current_segment:
            if current_segment is not None:
                js_content.append("")  # Blank line between segments
            js_content.append(f"// {segment} Segment")
            current_segment = segment
        
        for field in fields:
            print("field:", field)
            combined_js = field.get("Canonical-Source Field JS Code", "").strip()
            print("combined_js:", combined_js)
            if combined_js:
                lines = combined_js.splitlines()
                
                # Check if this is a multi-line block (has transformation logic)
                is_complex = len(lines) > 1
                
                if is_complex:
                    js_content.append("")  # Add blank line before complex logic
                
                # Add the entire block as-is (will be indented later)
                js_content.append(combined_js)
                
                # Add blank line after each field
                js_content.append("")

    # Remove trailing blank line if exists
    if js_content and js_content[-1] == "":
        js_content.pop()

    # Generate the new JavaScript code (unindented first)
    raw_js_code = "\n".join(js_content)
    
    # Apply proper indentation
    new_js_code = indent_js_code(raw_js_code)
    
    print("Generated JavaScript:\n", new_js_code)

    # Replace JavaScript in template
    updated_template = replace_javascript_in_template(template_content, new_js_code)
    print("JavaScript replaced in template")

    # Save the updated template to S3
    s3.put_object(
        Bucket=bucket_name,
        Key=output_key,
        Body=updated_template,
        ContentType="application/xml"
    )

    return {
        "status": "combined XML saved",
        "s3_path": f"s3://{bucket_name}/{output_key}",
        "template_used": f"s3://{bucket_name}/{template_key}"
    }