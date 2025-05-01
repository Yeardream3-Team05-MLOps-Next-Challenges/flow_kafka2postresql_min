import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import json
from src.logic import commit_kafka_offsets, read_kafka_logic, write_db_logic

# read_kafka_logic 테스트
@patch('src.logic.Consumer')
def test_read_kafka_logic(MockConsumer):
    # Mock 설정
    mock_consumer = MockConsumer.return_value
    mock_consumer.list_topics.return_value.topics = {
        "test_topic": MagicMock(partitions={0: MagicMock(), 1:MagicMock()})
    }
    mock_consumer.get_watermark_offsets.return_value = [0, 1]  # 테스트 offset
    
    # 테스트 데이터
    test_data = {
        "window": {"start": "20240702", "end": "20240703"},
        "종목코드": "005930", "open": 100, "high": 110, 
        "low": 90, "close": 105, "candle": 1
    }
    
    # poll 메서드가 메시지 하나를 반환 후 None 반환
    mock_msg = MagicMock()
    mock_msg.error.return_value = None
    mock_msg.value.return_value = json.dumps(test_data).encode('utf-8')
    mock_msg.topic.return_value = "test_topic"
    mock_msg.partition.return_value = 0
    mock_msg.offset.return_value = 100
    mock_msg.timestamp.return_value = (1, 1000)
    
    mock_consumer.poll.side_effect = [mock_msg, None]
    
    # 함수 실행
    df, consumer = read_kafka_logic("test_topic", "test_kafka_url")
    
    # 간단한 검증
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert consumer == mock_consumer

# write_db_logic 테스트
@patch('src.logic.create_engine')
def test_write_db_logic(MockCreateEngine):
  
    # Mock 설정
    mock_engine = MagicMock()
    MockCreateEngine.return_value = mock_engine
    
    # 테스트 데이터
    test_data = {
        'stock_code': ['005930'],
        'window_start': ['20240702'],
        'window_end': ['20240703'],
        'open': [100.0], 'high': [110.0], 
        'low': [90.0], 'close': [105.0],
        'candle': [1]
    }
    df = pd.DataFrame(test_data)
    
    # 함수 실행
    result = write_db_logic(df, "test_db_url")
    
    # 성공 여부만 확인
    assert result is True

def test_commit_kafka_offsets():
    # 간단한 Consumer 모킹
    mock_consumer = MagicMock()
    
    # 함수 실행 및 검증
    commit_kafka_offsets(mock_consumer, True)
    mock_consumer.commit.assert_called_once()
    
    # 실패 케이스
    mock_consumer.reset_mock()
    commit_kafka_offsets(mock_consumer, False)
    mock_consumer.commit.assert_not_called()