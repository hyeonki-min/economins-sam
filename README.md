# economins-sam

`economins-sam` is a serverless data ingestion project that fetches macroeconomic data from public Open APIs and stores the results in AWS S3 for further processing and analysis.

This repository is part of the [Economins](https://github.com/hyeonkimin/economins) ecosystem, which aims to provide accessible macroeconomic insights through visualization and contextual information.

- ecos - Code for the application's Lambda function, which fetches data from the Bank of Korea Open API and stores it in S3.
- events - Invocation events that you can use to invoke the function.
- krx - Code for the application's Lambda function, which fetches data from the Korea Exchange Open API and stores it in S3.
- reb - Code for the application's Lambda function, which fetches data from the Korea Real Estate Board Open API and stores it in S3.
- tests - Unit tests for the application code. 
- template.yaml - A template that defines the application's AWS resources.

## ✨ Features

- Scheduled data fetching using AWS Lambda and EventBridge
- Integration with public Open APIs
- S3 storage for raw or transformed datasets
- Easily deployable using AWS SAM (Serverless Application Model)
- Environment-variable-based configuration

## 🏗️ Architecture

> EventBridge -> Lambda (Fetch) -> S3 Bucket

## Data Format
Each dataset collected via Lambda and stored in S3 is saved as a JSON array. The structure follows a simple time-series
```json
[
  { "x": "2023-01", "y": 101.2 },
  { "x": "2023-02", "y": 102.3 },
  { "x": "2023-03", "y": 103.0 }
]
```
### 📉 Quarterly Data Handling
```json
[
  { "x": "2023-01", "y": null },
  { "x": "2023-02", "y": null },
  { "x": "2023-03", "y": 98.1 },
  { "x": "2023-04", "y": null },
  { "x": "2023-05", "y": null },
  { "x": "2023-06", "y": 101.7 },
  ...
]
```

## ⚙️ Requirements

- AWS CLI configured
- AWS SAM CLI
- Python 3.9+
- An S3 bucket to store the data
- API keys or credentials for the target public APIs

## 🔐 Required AWS Permissions

To successfully deploy and operate `economins-sam`, your IAM user or role must have the following permissions:

### 📦 Core AWS Services

These permissions are required to deploy and manage the SAM application using AWS CloudFormation, Lambda, and associated services.

| Service                  | Required Permissions (Examples)                                                                                                                                 |
|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| AWS Lambda               | `lambda:*` — Full access to manage Lambda functions                                                                                                             |
| AWS CloudFormation       | `cloudformation:CreateChangeSet`, `cloudformation:ExecuteChangeSet`, `cloudformation:DescribeStacks`, `cloudformation:DeleteStack`, etc.                       |
| IAM                      | `iam:PassRole` — To allow Lambda functions to assume execution roles                                                                                            |
| Amazon EventBridge       | `events:PutRule`, `events:PutTargets`, `events:DeleteRule`, etc. — To schedule periodic data fetch tasks                                                        |
| AWS Scheduler            | `scheduler:*` — If using AWS Scheduler instead of EventBridge for timed execution                                                                               |
| AWS Resource Groups & Tags | `resource-groups:*`, `tag:GetResources`                                                                                                                        |
| Amazon Application Insights | `applicationinsights:DescribeApplication` (optional)                                                                                                         |

### 🗂️ Amazon S3 Access

The Lambda function needs read/write access to the target S3 bucket for storing the fetched datasets.

| S3 Permission                  | Description                                  |
|-------------------------------|----------------------------------------------|
| `s3:CreateBucket`, `s3:DeleteBucket`         | Manage S3 buckets if the app creates new ones |
| `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject` | Upload and retrieve data to/from S3            |
| `s3:GetBucketPolicy`, `s3:PutBucketPolicy`  | Configure access control if needed             |
| `s3:GetBucketLocation`, `s3:ListBucket`      | Discover bucket location and contents          |


## 🚀 Getting Started

1. **Clone the repository**
   ```bash
   git clone https://github.com/your-org/economins-sam.git
   cd economins-sam
2. Configure AWS CLI
   
   Make sure your AWS credentials are configured. You can set them up by running:
   ```bash
   aws configure
3. Build the application
   ```bash
   sam build
   ```
4. Deploy to AWS
   ```
   sam deploy --guided --parameter-overrides ParameterKey=RebApiKey,ParameterValue=<RebApiKey> ParameterKey=EcosApiKey,ParameterValue=<EcosApiKey> ParameterKey=KrxApiKey,ParameterValue=<KrxApiKey> ParameterKey=LambdaRoleName,ParameterValue=<LambdaRoleName> ParameterKey=AccountId,ParameterValue=<AccountId>
   ```
5. Check your S3 bucket

   After deployment, the Lambda function will periodically fetch data and store it in the specified S3 bucket.