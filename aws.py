import json


def upload_dict(bucket, data_dict: dict, key: str):
    response = bucket.put_object(
        Key=key,
        Body=json.dumps(data_dict).encode("utf-8")
    )
    return response


def get_object(s3, bucket_name: str, key: str):
    obj = s3.Object(
        bucket_name=bucket_name,
        key=key
    )
    return json.loads(obj.get()['Body'].read().decode("utf-8"))
