FROM public.ecr.aws/lambda/python:3.12

# Lambda container dependencies.
COPY requirements-lambda-v2.txt /var/task/requirements-lambda-v2.txt
RUN pip install --no-cache-dir -r /var/task/requirements-lambda-v2.txt

# Pipeline sources needed by the Lambda handler.
COPY pipeline.py /var/task/pipeline.py
COPY pipeline_recording_id_v2.py /var/task/pipeline_recording_id_v2.py
COPY utils.py /var/task/utils.py
COPY lambda_s3_pipeline_v2.py /var/task/lambda_s3_pipeline_v2.py

# handler = <module>.<function>
CMD ["lambda_s3_pipeline_v2.lambda_handler"]
