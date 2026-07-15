FROM public.ecr.aws/lambda/python:3.11

COPY lambda/requirements.txt /tmp/lambda-requirements.txt

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /tmp/lambda-requirements.txt

COPY src/ ${LAMBDA_TASK_ROOT}/src/
COPY config/ ${LAMBDA_TASK_ROOT}/config/
COPY lambda/common.py ${LAMBDA_TASK_ROOT}/common.py
COPY lambda/generate_handler.py ${LAMBDA_TASK_ROOT}/generate_handler.py
COPY lambda/validate_handler.py ${LAMBDA_TASK_ROOT}/validate_handler.py
COPY lambda/sync_handler.py ${LAMBDA_TASK_ROOT}/sync_handler.py
COPY lambda/dlq_handler.py ${LAMBDA_TASK_ROOT}/dlq_handler.py

ENV PYTHONPATH=${LAMBDA_TASK_ROOT}

WORKDIR ${LAMBDA_TASK_ROOT}
