from confluent_kafka import Consumer, TopicPartition
import pandas as pd
import json
from sqlalchemy import create_engine

def read_kafka_logic(topic_name, kafka_url):
    """Kafka에서 데이터를 읽어오는 순수 함수."""
    # Consumer 설정
    conf = {
        'bootstrap.servers': kafka_url,
        'group.id': 'min_kafka2postgresql_flow',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(**conf)

    # 토픽의 모든 파티션 조회
    partitions = consumer.list_topics(topic_name).topics[topic_name].partitions.keys()
    topic_partitions = [TopicPartition(topic_name, p) for p in partitions]

    # 각 파티션의 최신 offset 조회
    end_offsets = consumer.get_watermark_offsets(topic_partitions[0])

    # 최신 offset으로 파티션 설정
    for i in range(len(topic_partitions)):
        topic_partitions[i].offset = end_offsets[1]

    # 모든 파티션을 구독
    consumer.assign(topic_partitions)

    data = []
    try:
        for partition in topic_partitions:
            # 각 파티션별로 최신 offset까지 메시지 읽기
            consumer.seek(partition)
            while True:
                msg = consumer.poll(1.0)
                if msg is None or msg.offset() >= partition.offset:
                    break 
                if msg.error():
                    print(msg.error())
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
                    'stock_code': value['종목코드'],  # '종목코드'에 맞게 수정
                    'open': value['open'],
                    'high': value['high'],
                    'low': value['low'],
                    'close': value['close'],
                    'candle': value['candle'],
                })
    finally:
        consumer.close()

    # 데이터프레임으로 변환
    df = pd.DataFrame(data)

    return df

def write_db_logic(data_source, db_url):
    """데이터베이스에 데이터를 쓰는 순수 함수."""
    engine = create_engine(db_url)
    data_source.to_sql('kafka_data', engine, if_exists='append', index=False)