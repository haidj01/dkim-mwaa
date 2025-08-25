from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

def s3_sensor(task_id, bucket, key):
    return S3KeySensor(
        task_id=task_id,
        bucket_name=bucket,
        bucket_key=key,
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=60 * 5,
        mode="poke",
        wildcard_match=False
    )