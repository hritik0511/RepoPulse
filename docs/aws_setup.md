# AWS setup (console, ~30 min)

Do this after the Databricks pipeline works from your laptop-loaded files.
Use an IAM user, not root. Set a billing budget alert first (Billing > Budgets).

## 1. S3 bucket (raw archive)
S3 > Create bucket > unique name (e.g. `repopulse-raw-<yourname>`), keep "Block all public access" ON.

## 2. Store the Databricks token
Systems Manager > Parameter Store > Create parameter
- Name: `/repopulse/databricks-token`
- Type: SecureString
- Value: your Databricks personal access token

## 3. IAM role for the Lambda
IAM > Roles > Create role > AWS service > Lambda.
- Attach `AWSLambdaBasicExecutionRole`.
- Name it `repopulse-ingest`, create it, then add an inline policy (JSON), replacing the placeholders:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": "s3:PutObject", "Resource": "arn:aws:s3:::<your-bucket>/*"},
    {"Effect": "Allow", "Action": "ssm:GetParameter", "Resource": "arn:aws:ssm:<region>:<account-id>:parameter/repopulse/databricks-token"}
  ]
}
```

## 4. Lambda function
Lambda > Create function > Author from scratch
- Name `repopulse-ingest`, runtime Python 3.12, role `repopulse-ingest`.
- Open the code editor, replace the contents of `lambda_function.py` with the contents of
  `ingestion/gharchive.py`, then click Deploy.
- Configuration > General configuration: timeout 10 min, memory 1024 MB, ephemeral storage 1024 MB.
- Configuration > Environment variables:
  - `DATABRICKS_HOST` = https://<workspace>.cloud.databricks.com
  - `DATABRICKS_TOKEN_PARAM` = /repopulse/databricks-token
  - `VOLUME_DIR` = /Volumes/workspace/gh_lakehouse/raw/landing
  - `S3_BUCKET` = your bucket name
- Code tab > Runtime settings > Edit: Handler = `lambda_function.handler`.

## 5. Test it once
Test tab > event JSON `{"hour": "2024-01-01T07"}` > Test.
Check: the file is in S3 under `raw/gharchive/` and in the Databricks volume's `landing` folder.

## 6. Schedule it
EventBridge > Rules > Create rule > Schedule > `rate(1 hour)` > target: your Lambda.
(The scheduled run ingests the hour from 3 hours ago, since GH Archive publishes with a delay.)

## 7. Clean up when you're done demoing
Disable or delete the EventBridge rule (otherwise it keeps uploading and uses your Databricks quota),
and revoke the Databricks token if you no longer need it.
