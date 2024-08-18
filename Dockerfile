FROM prefecthq/prefect:2.18.3-python3.10

ARG TOPIC_NAME
ARG KAFKA_URL
ARG DB_URL

ENV TOPIC_NAME=${TOPIC_NAME}
ENV KAFKA_URL=${KAFKA_URL}
ENV DB_URL=${DB_URL}

COPY requirements.txt .

RUN python -m pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . /opt/prefect/flows

WORKDIR /opt/prefect/flows

CMD ["python", "./flows/flow.py"]