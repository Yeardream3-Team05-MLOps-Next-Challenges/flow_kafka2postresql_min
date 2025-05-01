from confluent_kafka import Consumer, TopicPartition, KafkaError, KafkaException
import pandas as pd
import json
from sqlalchemy import create_engine, Table, MetaData, Column, String, Integer, Float, DateTime, UniqueConstraint
from sqlalchemy.dialects.postgresql import insert as pg_insert
import time
from src.logger import get_logger

def read_kafka_logic(topic_name, kafka_url, group_id='min_kafka2postgresql_flow', poll_timeout = 5.0, max_poll_duration_sec = 30):
    """Kafka에서 데이터를 읽어오는 순수 함수."""
    # Consumer 설정
    logger = get_logger()
    conf = {
        'bootstrap.servers': kafka_url,
        'group.id': group_id,
        'auto.offset.reset': 'earlist',
        'enable.auto.commit': False 
    }
    consumer = Consumer(**conf)

    data = []
    messages_processed_count = 0
    start_time = time.time()

    try:
        consumer.subscribe([topic_name])
        logger.info(f"토픽 구독 시작: {topic_name}")

        while True:
            # 시간 제한 체크
            if (time.time() - start_time) > max_poll_duration_sec:
                logger.info(f"최대 폴링 시간({max_poll_duration_sec}s) 도달. 폴링 중지.")
                break

            msg = consumer.poll(timeout=poll_timeout)

            if msg is None:
                # 지정된 시간 동안 메시지 없음
                logger.info(f"{poll_timeout}초 동안 새 메시지 없음. 폴링 종료.")
                break # 루프 종료

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # 파티션 끝 도달은 정상적인 상황. 계속 폴링 가능.
                    logger.debug(f"파티션 끝 도달: {msg.topic()} [{msg.partition()}]")
                    continue
                else:
                    # 실제 에러 발생
                    logger.error(f"Kafka Consume 오류: {msg.error()}")
                    raise KafkaException(msg.error()) # Prefect 태스크 실패 처리

            # 메시지 처리
            try:
                value = json.loads(msg.value().decode('utf-8'))
                # min_topic의 실제 데이터 스키마에 맞게 컬럼 추출
                record = {
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
                }
                data.append(record)
                messages_processed_count += 1
            except json.JSONDecodeError as e:
                logger.warning(f"JSON 파싱 오류 (오프셋 {msg.offset()}): {e} - 메시지 건너<0xEB><0x9A><0x8D>")
            except Exception as e:
                logger.error(f"메시지 처리 중 오류 (오프셋 {msg.offset()}): {e}", exc_info=True)
                # 오류 발생 시 해당 메시지 건너뛰고 계속 진행할지, 아니면 중단할지 결정 필요
                # 여기서는 일단 건너뛰는 것으로 가정

        logger.info(f"총 {messages_processed_count}개의 새 메시지 처리 완료.")

        if not data:
            logger.info("처리할 새 데이터 없음.")
            # 데이터가 없어도 consumer 객체는 반환하여 finally에서 close 되도록 함
            return pd.DataFrame(), consumer

        df = pd.DataFrame(data)
        logger.info("Pandas DataFrame으로 변환 완료.")
        return df, consumer # 데이터프레임과 consumer 객체 반환

    except Exception as e:
        logger.error(f"Kafka 읽기 중 예외 발생: {e}", exc_info=True)
        # 예외 발생 시 consumer를 닫고 예외를 다시 발생시켜 태스크 실패 처리
        if 'consumer' in locals() and consumer:
            consumer.close()
            logger.info("에러로 인해 Kafka Consumer 닫힘.")
        raise e

def write_db_logic(data_source, db_url, table_name='kafka2postgre'):
    """데이터베이스에 데이터를 쓰는 순수 함수."""
    logger = get_logger()
    if data_source is None or data_source.empty:
        logger.info("DB에 쓸 데이터가 없습니다.")
        return False

    engine = create_engine(db_url)
    metadata = MetaData()

    try:
        kafka_table = Table(table_name, metadata,
            Column('message_id', String, primary_key=True), # message_id를 기본 키로 설정 (자동으로 Unique + Not Null)
            Column('topic', String),
            Column('partition', Integer),
            Column('offset', Integer),
            Column('timestamp', String),
            Column('window_start', String), 
            Column('window_end', String),   
            Column('stock_code', String),
            Column('open', Float),
            Column('high', Float),
            Column('low', Float),
            Column('close', Float),
            Column('candle', String),
        )

        # 테이블 존재 확인 및 생성
        with engine.connect() as conn:
            if not engine.dialect.has_table(conn, table_name):
                logger.info(f"테이블 '{table_name}'이(가) 존재하지 않아 새로 생성합니다.")
                metadata.create_all(engine)
                logger.info(f"테이블 '{table_name}' 생성 완료.")

        # 데이터 준비 및 UPSERT
        if not data_source.empty:
            # message_id 생성 (stock_code + window_start)
            data_source['message_id'] = data_source.apply(
                lambda r: f"{r['stock_code']}_{r['window_start']}", axis=1
            )

            insert_data = data_source.to_dict(orient='records')

            if not insert_data:
                logger.info("No data to insert after processing.")
                return False

            stmt = pg_insert(kafka_table).values(insert_data)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=['message_id'] 
            )

            with engine.connect() as conn:
                conn.execute(stmt)
                conn.commit()
                logger.info(f"{len(insert_data)} 건의 데이터 DB 저장/무시 완료.")
            return True

    except Exception as e:
        logger.error(f"DB 쓰기 작업 중 오류 발생: {e}", exc_info=True)
        raise



def commit_kafka_offsets(consumer: Consumer, write_successful: bool):
    """DB 쓰기 성공 시 Kafka 오프셋을 커밋하고 Consumer를 닫는 태스크."""
    logger = get_logger()
    if consumer is None:
        logger.info("커밋할 Kafka Consumer 없음.")
        return

    try:
        if write_successful:
            logger.info("DB 쓰기 성공. Kafka 오프셋 커밋 시도...")
            consumer.commit(asynchronous=False)
            logger.info("Kafka 오프셋 커밋 완료.")
        else:
            logger.warning("DB 쓰기 실패 또는 데이터 없음. Kafka 오프셋 커밋하지 않음.")
    except Exception as e:
        logger.error(f"Kafka 오프셋 커밋 중 오류 발생: {e}", exc_info=True)
        raise 
    finally:
        logger.info("Kafka Consumer 닫기 시도...")
        consumer.close()
        logger.info("Kafka Consumer 닫힘.")