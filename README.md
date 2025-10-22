# HL7v2 AutoCT Report API

This API allows you to run an HL7v2 AutoCT pipeline that analyzes HL7 messages, applies transformation rules, and generates two downloadable reports:
- HL7v2 Specification
- JS Code Validation Report

**Please note: All the config files are already uploaded in S3 for all the Lambdas to function appropriately.**

---

## Step-by-Step Usage

### 1. Trigger the Pipeline (`POST`)

**Endpoint:**
```
POST https://4p593pb2bf.execute-api.us-east-1.amazonaws.com/prod/run-hl7autoct-pipeline?
```

**Example with cURL:**
```bash
curl -X POST https://4p593pb2bf.execute-api.us-east-1.amazonaws.com/prod/run-hl7autoct-pipeline -H "Content-Type: application/json" --data @sample_hl7_json.json
```

**Sample contents sample_hl7_json.json:**
{
  "hl7_message": "MSH|^~\\&|LABAPP|LAB|HOSPAPP|HOSP|202510071600||ORM^O01|123456|P|2.5\nPID|1||78901^^^HOSP^MR||Smith^Jane||19751225|F|||456 Elm St^^Orlando^FL^32801\nPV1|1|O|ER^01^01||||1234^Jones^Mark|||||||||||123456\nORC|NW|ORD4488|||||202510071600|||1234^Jones^Mark\nOBR|1|ORD4488|LAB9988|GLU^Glucose^LN&GLU&1234~A1C^Test^LN1&GLU1&12341|R|202510071605|||||||1234^Jones^Mark\nOBX|1|ST|GLU^Glucose^LN||95|mg/dL|70-110|N|||F\nOBX|2|ST|WBC^White Blood Cell Count^LN||5.4~5.6~5.8|10^3/uL&microL|4.0-10.0|N|||F\nOBX|3|ST|HGB^Hemoglobin^LN||13.5~13.7|g/dL|12.0-16.0|N|||F\n\nMSH|^~\\&|ADTApp|HospitalA|EHRSystem|HospitalB|20251008||ADT^A01|123456|P|2.5\nEVN|A01|20251008\nPID|1||123456^^^HospitalA^MR||Doe^John||19800101|M|||123 Main St^^Miami^FL^33101||555-1234|||M||123456789\nPV1|1|I|ICU^101^1^HospitalA||||1234^Smith^Jane|||||||||||1234567|||||||||||||||||||||||||20251008\nDG1|1||I10|Essential (primary) hypertension|ICD-10|20251001|A\nDG1|2||E11.9|Type 2 diabetes mellitus without complications|ICD-10|20251001|A\nDG1|3||J45.909|Unspecified asthma, uncomplicated|ICD-10|20251001|A"
}

**Example with Postman:**
- Method: `POST`
- URL: `https://4p593pb2bf.execute-api.us-east-1.amazonaws.com/prod/run-hl7autoct-pipeline?`
- Header: Key -> `Content-Type`; Value -> application/json
- Body: Raw -> JSON
```json
{
  "hl7_message": "MSH|^~\\&|LABAPP|LAB|HOSPAPP|HOSP|202510071600||ORM^O01|123456|P|2.5\nPID|1||78901^^^HOSP^MR||Smith^Jane||19751225|F|||456 Elm St^^Orlando^FL^32801\nPV1|1|O|ER^01^01||||1234^Jones^Mark|||||||||||123456\nORC|NW|ORD4488|||||202510071600|||1234^Jones^Mark\nOBR|1|ORD4488|LAB9988|GLU^Glucose^LN&GLU&1234~A1C^Test^LN1&GLU1&12341|R|202510071605|||||||1234^Jones^Mark\nOBX|1|ST|GLU^Glucose^LN||95|mg/dL|70-110|N|||F\nOBX|2|ST|WBC^White Blood Cell Count^LN||5.4~5.6~5.8|10^3/uL&microL|4.0-10.0|N|||F\nOBX|3|ST|HGB^Hemoglobin^LN||13.5~13.7|g/dL|12.0-16.0|N|||F\n\nMSH|^~\\&|ADTApp|HospitalA|EHRSystem|HospitalB|20251008||ADT^A01|123456|P|2.5\nEVN|A01|20251008\nPID|1||123456^^^HospitalA^MR||Doe^John||19800101|M|||123 Main St^^Miami^FL^33101||555-1234|||M||123456789\nPV1|1|I|ICU^101^1^HospitalA||||1234^Smith^Jane|||||||||||1234567|||||||||||||||||||||||||20251008\nDG1|1||I10|Essential (primary) hypertension|ICD-10|20251001|A\nDG1|2||E11.9|Type 2 diabetes mellitus without complications|ICD-10|20251001|A\nDG1|3||J45.909|Unspecified asthma, uncomplicated|ICD-10|20251001|A"
}

```

**Sample Response:**
```json
{
    "message": "Pipeline started successfully.\nIt may take up to 7 to 8 minutes to complete.\n\nTo retrieve the final validation report, use the URL below:",
    "execution_arn": "arn:aws:states:us-east-1:238845559334:execution:HL7v2AutoCT:6cdd7c81-0487-450d-8ca9-6efaa6ecfe73",
    "check_status_url": "https://4p593pb2bf.execute-api.us-east-1.amazonaws.com/prod/get-hl7autoct-report?executionArn=arn:aws:states:us-east-1:238845559334:execution:HL7v2AutoCT:6cdd7c81-0487-450d-8ca9-6efaa6ecfe73",
    "get_specification_file_url": "https://4p593pb2bf.execute-api.us-east-1.amazonaws.com/prod/get-hl7autoct-report?executionArn=arn:aws:states:us-east-1:238845559334:execution:HL7v2AutoCT:6cdd7c81-0487-450d-8ca9-6efaa6ecfe73&type=specification",
    "get_validation_report_url": "https://4p593pb2bf.execute-api.us-east-1.amazonaws.com/prod/get-hl7autoct-report?executionArn=arn:aws:states:us-east-1:238845559334:execution:HL7v2AutoCT:6cdd7c81-0487-450d-8ca9-6efaa6ecfe73&type=validation",
    "get_xmljs_file_url": "https://4p593pb2bf.execute-api.us-east-1.amazonaws.com/prod/get-hl7autoct-report?executionArn=arn:aws:states:us-east-1:238845559334:execution:HL7v2AutoCT:6cdd7c81-0487-450d-8ca9-6efaa6ecfe73&type=xmljs",
    "timestamp": "2025-10-22T15:47:45.222228Z"
}

```

Save the `executionArn` — you'll need it to check progress or download results.

---


### 2. Interpretation of Response through Browser or cURL

**Check status:**  "https://4p593pb2bf.execute-api.us-east-1.amazonaws.com/prod/get-hl7autoct-report?executionArn=arn:aws:states:us-east-1:238845559334:execution:HL7v2AutoCT:6cdd7c81-0487-450d-8ca9-6efaa6ecfe73"

```bash
curl "https://your-api-url.com/get-hl7autoct-report?executionArn=arn:aws:states:us-east-1:123456789012:execution:HL7v2AutoCT:abc123"
```

**Download specification:type=specification** "https://4p593pb2bf.execute-api.us-east-1.amazonaws.com/prod/get-hl7autoct-report?executionArn=arn:aws:states:us-east-1:238845559334:execution:HL7v2AutoCT:6cdd7c81-0487-450d-8ca9-6efaa6ecfe73&type=specification"

```bash
curl -L "https://4p593pb2bf.execute-api.us-east-1.amazonaws.com/prod/get-hl7autoct-report?executionArn=arn:aws:states:us-east-1:238845559334:execution:HL7v2AutoCT:6cdd7c81-0487-450d-8ca9-6efaa6ecfe73&type=specification"
```

**Download validation report:type=validation** "https://4p593pb2bf.execute-api.us-east-1.amazonaws.com/prod/get-hl7autoct-report?executionArn=arn:aws:states:us-east-1:238845559334:execution:HL7v2AutoCT:6cdd7c81-0487-450d-8ca9-6efaa6ecfe73&type=validation"

```bash
curl -L "https://4p593pb2bf.execute-api.us-east-1.amazonaws.com/prod/get-hl7autoct-report?executionArn=arn:aws:states:us-east-1:238845559334:execution:HL7v2AutoCT:6cdd7c81-0487-450d-8ca9-6efaa6ecfe73&type=validation"
```

**Download Mirth XML + JS Code:type=xmljs**
"https://4p593pb2bf.execute-api.us-east-1.amazonaws.com/prod/get-hl7autoct-report?executionArn=arn:aws:states:us-east-1:238845559334:execution:HL7v2AutoCT:6cdd7c81-0487-450d-8ca9-6efaa6ecfe73&type=xmljs"

```bash
curl -L "https://4p593pb2bf.execute-api.us-east-1.amazonaws.com/prod/get-hl7autoct-report?executionArn=arn:aws:states:us-east-1:238845559334:execution:HL7v2AutoCT:6cdd7c81-0487-450d-8ca9-6efaa6ecfe73&type=xmljs"
```

**`-L` follows the redirect to download the file.**

---

## Response Format

### 1. If pipeline is still running:
```json
{
  "executionArn": "...",
  "status": "RUNNING",
  "message": "Pipeline is still running or download URL not available yet.",
  "progress": "44%",
  "current_step": "Generating Mirth JS Code",
  "final_status": {}
}
```

### 2. If pipeline is complete:
```json
{
  "executionArn": "...",
  "status": "SUCCEEDED",
  "message": "Pipeline is still running or download URL not available yet.",
  "progress": "100%",
  "current_step": "Completed",
  "final_status": {
    "specification_download_url": "...",
    "validation_report_download_url": "...",
    "xmljs_download_url": "...",
    "total_segments": 7,
    "total_rows": 40
  }
}

```

---

### 3. How to Interpret the Response

| Field | Meaning |
|-------|---------|
| `status` | Current pipeline status (`RUNNING`, `SUCCEEDED`, `FAILED`, etc.) |
| `progress` | Estimated progress percentage |
| `current_step` | Human-readable name of the current pipeline step |
| `final_status` | Contains download URLs and summary once completed |

---

## Notes

- JSON responses are pretty-printed for readability
- Redirects only occur when `type` is provided and pipeline is complete
- Progress is estimated based on pipeline step index
- You can run this from browser, Postman, or any HTTP client

---
