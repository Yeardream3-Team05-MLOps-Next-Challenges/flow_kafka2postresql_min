import os
from prefect import flow, task

from src.logger import get_logger, setup_logging
from src.logic import read_kafka_logic, write_db_logic, commit_kafka_offsets

@task
def read_kafka(topic_name, kafka_url):
    """Kafka에서 데이터를 읽어오는 Prefect 태스크."""
    return read_kafka_logic(topic_name, kafka_url)

@task
def write_db(data_source, db_url):
    """데이터베이스에 데이터를 쓰는 Prefect 태스크."""
    write_db_logic(data_source, db_url)

@task
def commit_offsets(consumer, write_result):
    """Kafka 오프셋을 커밋하는 Prefect 태스크."""
    return commit_kafka_offsets(consumer, write_result)

@flow
def hun_min_kafka2postgresql_flow():
    """전체 데이터 처리 플로우를 정의하는 Prefect 플로우."""
    setup_logging()
    
    topic_name = os.getenv("TOPIC_NAME")
    kafka_url = os.getenv("KAFKA_URL")
    db_url = os.getenv("DB_URL")

    df, consumer = read_kafka(topic_name, kafka_url)
    write_result = write_db(df, db_url)
    commit_offsets(consumer, write_result)


if __name__ == "__main__":
    hun_min_kafka2postgresql_flow()