import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import json
from src.logic import read_kafka_logic, write_db_logic

# read_kafka_logic 테스트
@patch('src.logic.Consumer')
def test_read_kafka_logic(MockConsumer):
    # Mock 설정
    mock_consumer = MockConsumer.return_value
    mock_consumer.list_topics.return_value.topics = {
        "test_topic": MagicMock(partitions={0: MagicMock(), 1:MagicMock()})
    }
    mock_consumer.get_watermark_offsets.return_value = [0, 1]  # 테스트 offset
    
    test_data_0 = {
        "window": {"start": "20240702", "end": "20240703"},
        "종목코드": "005930",
        "open": 100,
        "high": 110,
        "low": 90,
        "close": 105,
        "candle": 1
    }

    test_data_1 = {
        "window": {"start": "20240702", "end": "20240703"},
        "종목코드": "005930",
        "open": 100,
        "high": 110,
        "low": 90,
        "close": 105,
        "candle": 1
    }

    mock_consumer.poll.side_effect = [
         MagicMock(error=lambda : None, value=lambda : json.dumps(test_data_0).encode('utf-8'), topic=lambda: "test_topic", partition=lambda: 0, offset=lambda: 0, timestamp=lambda: (1,1000)),
         None,
         MagicMock(error=lambda : None, value=lambda : json.dumps(test_data_1).encode('utf-8'), topic=lambda: "test_topic", partition=lambda: 1, offset=lambda: 0, timestamp=lambda: (1,1000)),
         None
    ]

    # 테스트 실행
    topic_name = "test_topic"
    kafka_url = "test_kafka_url"
    df = read_kafka_logic(topic_name, kafka_url)
    
    # 검증
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert 'topic' in df.columns
    assert 'partition' in df.columns
    assert 'offset' in df.columns
    assert 'timestamp' in df.columns
    assert 'window_start' in df.columns
    assert 'window_end' in df.columns
    assert 'stock_code' in df.columns
    assert 'open' in df.columns
    assert 'high' in df.columns
    assert 'low' in df.columns
    assert 'close' in df.columns
    assert 'candle' in df.columns

# write_db_logic 테스트
@patch('src.logic.create_engine')
def test_write_db_logic(MockCreateEngine):
  
    # Mock 설정
    mock_engine = MockCreateEngine.return_value
    mock_df = MagicMock()

    # 테스트 실행
    db_url = "test_db_url"
    write_db_logic(mock_df, db_url)
    
    # 검증
    MockCreateEngine.assert_called_once_with(db_url)
    mock_df.to_sql.assert_called_once_with('kafka_data', mock_engine, if_exists='append', index=False)