# amplify-einsteinRepLibMetaCard Lambda

This repository contains the AWS Lambda function that powers metadata processing and preview handling for Workday report library in the AgentForce Amplify GPT integration. It handles prompt filtering based on Workday security groups, secure report metadata transformation, and retry-safe preview data delivery.

## 📦 Folder Structure
```
.
├── lambda_function.py
├── requirements.txt
├── tiktoken_cache/
├── test_events/
│   ├── event_valid.json
│   ├── event_uuid_pending.json
│   └── event_missing_fields.json
└── README.md
```

## 🚀 Deployment Guide

### ✅ Prerequisites
- Python 3.11+ installed (`python3 --version`)
- AWS CLI configured (`aws configure`)
- IAM permissions to upload to S3 and update Lambda
- GitHub repo access (for code backup)

### 1. Clone the Repository
```bash
git clone https://github.com/Shreya3199/lambda-amplify-einsteinRepLibMetaCard.git
cd lambda-amplify-einsteinRepLibMetaCard
```

### 2. Set Up Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Package Lambda Code
```bash
zip -r ../lambda-amplify-einsteinRepLibMetaCard-DEV_043_18JUL25.zip .
```

### 4. Upload to S3
```bash
aws s3 cp ../lambda-amplify-einsteinRepLibMetaCard-DEV_043_18JUL25.zip \
  s3://workday-wcp-ffmmnb-new-us-west-2/einsteinGPTPromptBuilder/
```

### 5. Update Lambda Function
**Make sure the region is set explicitly if not default:**
```bash
aws lambda update-function-code \
  --region us-west-2 \
  --function-name amplify-einsteinRepLibMetaCard-DEV \
  --s3-bucket workday-wcp-ffmmnb-new-us-west-2 \
  --s3-key einsteinGPTPromptBuilder/lambda-amplify-einsteinRepLibMetaCard-DEV_043_18JUL25.zip
```
🔁 Repeat with -IMPL, -SB, or PROD function names when promoting.

---

## 🧪 Local Setup & Testing Guide

### ✅ Requirements
- Python 3.11 or higher
- `python-lambda-local` for testing
- AWS CLI configured with correct region

### 🔹 Install Dependencies
```bash
pip install -r requirements.txt -t .
```
Make sure your `lambda_function.py` and the installed packages are all in the same directory (as required by AWS Lambda).

### 🔹 Local Testing with Sample Event
```bash
python-lambda-local -f lambda_handler lambda_function.py test_events/event_valid.json -t 600000
```

### 🔹 Sample Event File (test_events/event_valid.json)
```json
{
  "wd_ext_operation": "agentforcePreviewDataPull",
  "uuid": "sample-uuid-1234",
  "wd_tenant_alias": "salesforce11",
  "_aws_s3_bucket": "workday-wcp-ffmmnb-new-us-west-2"
}
```

### 🔹 Sample requirements.txt
```
boto3
tiktoken
jsonpickle
```

---

## 🛠️ Tips & Troubleshooting
- Region issues: If you encounter region errors, pass `--region us-west-2` explicitly.
- Ensure you **zip your Lambda code from the root** directory (not nested inside folders).
- Use CloudWatch for debugging if your Lambda fails silently.
- Maintain error codes and logs in the expected format to support Splunk/PagerDuty triaging.

---

Let us know if you need support with environment variables, IAM setup, or testing previews.
