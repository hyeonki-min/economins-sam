import json
import boto3
from datetime import datetime
import os

s3 = boto3.client('s3')
base_url = ''


BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
OUTPUT_KEY = os.environ.get("S3_OUTPUT_KEY")
REB_API_KEY = os.environ.get("REB_API_KEY")
STATBL_ID = os.environ.get("STATBL_ID")
CLS_ID = os.environ.get("CLS_ID")
GRP_ID = os.environ.get("GRP_ID")
ITM_ID = os.environ.get("ITM_ID")

params = {
    'KEY': REB_API_KEY,
    'Type': 'json',
    'STATBL_ID': STATBL_ID,
    'DTACYCLE_CD': 'MM',
    'CLS_ID': CLS_ID,
    'pSize': 1000
}

if GRP_ID:
    params['GRP_ID'] = GRP_ID
if ITM_ID:
    params['ITM_ID'] = ITM_ID

def transform_data(data):
    result = []
    rows = data.get("SttsApiTblData", [])[1].get("row", [])

    for item in rows:
        date_str = item.get("WRTTIME_DESC", "").replace(" ", "").replace("년", "-").replace("월", "")
        try:
            date_fmt = datetime.strptime(date_str, "%Y-%m")
            date_key = date_fmt.strftime("%Y-%m")
        except ValueError:
            continue

        value = round(float(item.get("DTA_VAL", 0)), 1)
        result.append({date_key: value})
    
    return result

def lambda_handler(event, context):
    try:
        x = requests.get(base_url, params=params)
        x.raise_for_status()
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error fetching data from API: {str(e)}'
        }
    transformed = transform_data(x.json())

    json_string = json.dumps(transformed, ensure_ascii=False)

    # S3 업로드
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=OUTPUT_KEY,
            Body=json_string.encode('utf-8'),
            ContentType='application/json'
        )
        return {
            'statusCode': 200,
            'body': f'File uploaded to s3://{BUCKET_NAME}/{OUTPUT_KEY}'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error uploading to S3: {str(e)}'
        }
