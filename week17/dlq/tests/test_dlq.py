import json
from unittest.mock import Mock, patch

import consumer


class FakeMessage:
    def __init__(self, value):
        self._value = value

    def error(self):
        return None

    def value(self):
        return self._value


def test_valid_message():
    message = FakeMessage(
        json.dumps({"sentence": "Hello world"}).encode("utf-8")
    )

    producer = Mock()

    result = consumer.process_message(message, producer)

    assert result is True


@patch("consumer.send_to_dlq")
def test_invalid_json_sent_to_dlq(mock_send):
    message = FakeMessage(b"invalid json")
    producer = Mock()

    result = consumer.process_message(message, producer)

    assert result is True
    mock_send.assert_called_once()


@patch("consumer.send_to_dlq")
def test_missing_sentence_sent_to_dlq(mock_send):
    message = FakeMessage(
        json.dumps({"text": "Hello world"}).encode("utf-8")
    )
    producer = Mock()

    result = consumer.process_message(message, producer)

    assert result is True
    mock_send.assert_called_once()


@patch("consumer.send_to_dlq")
def test_empty_sentence_sent_to_dlq(mock_send):
    message = FakeMessage(
        json.dumps({"sentence": ""}).encode("utf-8")
    )
    producer = Mock()

    result = consumer.process_message(message, producer)

    assert result is True
    mock_send.assert_called_once()


@patch("consumer.send_to_dlq")
def test_invalid_encoding_sent_to_dlq(mock_send):
    message = FakeMessage(b"\xff\xfe")
    producer = Mock()

    result = consumer.process_message(message, producer)

    assert result is True
    mock_send.assert_called_once()