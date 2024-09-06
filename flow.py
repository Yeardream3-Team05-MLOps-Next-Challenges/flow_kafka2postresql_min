from confluent_kafka import Consumer, TopicPartition, KafkaException
import os
import pandas as pd
import json
from sqlalchemy import create_engine
import logging

from prefect import flow, task, get_run_logger

# 로깅 설정
def get_logger():
    try:
        return get_run_logger()
    except Exception:
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(__name__)
logger = get_logger()


@task
def read_kafka(topic_name, kafka_url):
    global logger

    logger.info(f"Attempting to read from Kafka topic: {topic_name}")
    
    # Consumer 설정
    conf = {
        'bootstrap.servers': kafka_url,
        'group.id': 'min_kafka2postgresql_flow',
        'auto.offset.reset': 'earliest'
    }
    
    try:
        consumer = Consumer(**conf)
        # 토픽의 모든 파티션 조회
        partitions = consumer.list_topics(topic_name).topics[topic_name].partitions.keys()
        topic_partitions = [TopicPartition(topic_name, p) for p in partitions]
        
        data = []
        for partition in topic_partitions:
            # 각 파티션의 시작과 끝 오프셋 조회
            start_offset, end_offset = consumer.get_watermark_offsets(partition)
            partition.offset = start_offset  # 시작 오프셋부터 읽기 시작
            
            logger.info(f"Reading partition {partition.partition} from offset {start_offset} to {end_offset}")
            
            consumer.assign([partition])
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                # 메시지 처리
                value = json.loads(msg.value().decode('utf-8'))
                data.append({
                    'topic': msg.topic(),
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                    'timestamp': msg.timestamp()[1],
                    'window_start': value['window']['start'],
                    'window_end': value['window']['end'],
                    'stock_code': value['종목코드'],
                    'open': value['open'],
                    'high': value['high'],
                    'low': value['low'],
                    'close': value['close'],
                    'candle': value['candle'],
                })
                
                if msg.offset() + 1 >= end_offset:
                    break
        
        # 데이터프레임으로 변환
        df = pd.DataFrame(data)
        logger.info(f"Successfully read {len(df)} records from Kafka")
        return df
    
    except KafkaException as e:
        logger.error(f"Kafka error: {e}")
        raise
    finally:
        consumer.close()

@task
def write_db(data_source, db_url):
    global logger

    logger.info("Attempting to write data to PostgreSQL")
    try:
        engine = create_engine(db_url)
        data_source.to_sql('flow_kafka2postresql_min', engine, if_exists='append', index=False)
        logger.info(f"Successfully wrote {len(data_source)} records to PostgreSQL")
    except Exception as e:
        logger.error(f"Database error: {e}")
        raise

@flow
def hun_min_kafka2postgresql_flow():

    topic_name = os.getenv("TOPIC_NAME")
    kafka_url = os.getenv("KAFKA_URL")
    db_url = os.getenv("DB_URL")

    kafka_data = read_kafka(topic_name, kafka_url)
    write_db(kafka_data, db_url)

if __name__ == "__main__":

    hun_min_kafka2postgresql_flow()
