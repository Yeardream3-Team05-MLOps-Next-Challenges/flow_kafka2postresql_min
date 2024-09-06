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
    
    conf = {
        'bootstrap.servers': kafka_url,
        'group.id': 'min_kafka2postgresql_flow',
        'enable.auto.commit': False,
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([topic_name])
    
    try:
        # 파티션 할당 대기
        while True:
            partitions = consumer.assignment()
            if partitions:
                break
            consumer.poll(1.0)
        
        data = []
        final_offsets = {}
        for partition in partitions:
            start_offset, end_offset = consumer.get_watermark_offsets(partition)
            committed = consumer.committed([partition])
            current_offset = committed[0].offset if committed[0].offset > -1 else start_offset
            
            logger.info(f"Partition {partition.partition}: start_offset={start_offset}, "
                        f"end_offset={end_offset}, current_offset={current_offset}")
            
            if current_offset < end_offset:
                tp = TopicPartition(topic_name, partition.partition, current_offset)
                consumer.seek(tp)
                
                while current_offset < end_offset:
                    msg = consumer.poll(1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                    
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
                    
                    current_offset = msg.offset() + 1
                
                final_offsets[partition.partition] = current_offset - 1
        
        df = pd.DataFrame(data)
        logger.info(f"Successfully read {len(df)} records from Kafka")
        return df, consumer, final_offsets
    
    except Exception as e:
        logger.error(f"Error reading from Kafka: {e}")
        consumer.close()
        raise

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

    df, consumer, final_offsets = read_kafka(topic_name, kafka_url)
    
    try:
        write_db(df, db_url)
        
        # 데이터베이스 쓰기가 성공한 후에만 오프셋 커밋
        for partition, offset in final_offsets.items():
            consumer.commit(offsets=[TopicPartition(topic_name, partition, offset + 1)])
        logger.info("Final offsets committed successfully")
    
    finally:
        consumer.close()

if __name__ == "__main__":

    hun_min_kafka2postgresql_flow()
