FROM prefecthq/prefect:2.18.3-python3.10

ARG LOGGING_LEVEL
ARG TOPIC_NAME
ARG KAFKA_URL
ARG DB_URL
ARG VERSION

ENV VERSION=$VERSION
ENV LOGGING_LEVEL=${LOGGING_LEVEL}
ENV PREFECT_LOGGING_LEVEL=${LOGGING_LEVEL}
ENV TOPIC_NAME=${TOPIC_NAME}
ENV KAFKA_URL=${KAFKA_URL}
ENV DB_URL=${DB_URL}

COPY pyproject.toml poetry.lock* ./

RUN python -m pip install --upgrade pip\
    && pip install --no-cache-dir poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-root \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY . /opt/prefect/flows

WORKDIR /opt/prefect/flows

CMD ["python", "./flow.py"]