import json
from unittest.mock import MagicMock

import pytest

import producer.cotahist_producer as cotahist_module
from producer.cotahist_producer import CotahistKafkaProducer


@pytest.fixture
def producer_context(monkeypatch):
    kafka_mock = MagicMock()
    kafka_mock.flush.return_value = 0

    producer_class_mock = MagicMock(
        return_value=kafka_mock,
    )

    logger_mock = MagicMock()

    application_logger_mock = MagicMock()
    application_logger_mock.get_logger.return_value = logger_mock

    monkeypatch.setattr(
        cotahist_module,
        "Producer",
        producer_class_mock,
    )

    monkeypatch.setattr(
        cotahist_module,
        "ApplicationLogger",
        MagicMock(
            return_value=application_logger_mock,
        ),
    )

    service = CotahistKafkaProducer()

    return {
        "service": service,
        "kafka": kafka_mock,
        "producer_class": producer_class_mock,
        "logger": logger_mock,
    }


def test_should_create_kafka_producer_with_expected_config(
    producer_context,
):
    producer_class = producer_context["producer_class"]

    config = producer_class.call_args.args[0]

    assert config["acks"] == "all"
    assert config["enable.idempotence"] is True
    assert "bootstrap.servers" in config
    assert "client.id" in config


def test_should_extract_date_from_daily_file_name():
    result = CotahistKafkaProducer._extract_file_date(
        "COTAHIST_D09092026.TXT"
    )

    assert result == "2026-09-09"


def test_should_return_none_for_non_daily_file():
    result = CotahistKafkaProducer._extract_file_date(
        "COTAHIST_A2026.TXT"
    )

    assert result is None


def test_should_build_cotahist_event(
    producer_context,
):
    service = producer_context["service"]

    event = service._build_event(
        file_name="COTAHIST_D09092026.TXT",
        line_number=2,
        raw_record="01TESTE     ",
        file_date="2026-09-09",
    )

    assert event["schema_version"] == 1

    assert event["event_id"] == (
        "COTAHIST_D09092026.TXT:2"
    )

    assert event["source"] == "B3"
    assert event["dataset"] == "COTAHIST"

    assert event["file_name"] == (
        "COTAHIST_D09092026.TXT"
    )

    assert event["file_date"] == "2026-09-09"
    assert event["line_number"] == 2

    assert event["record_type"] == "01"

    assert event["raw_record"] == "01TESTE     "

    assert event["ingested_at"].endswith("Z")


@pytest.mark.parametrize(
    ("raw_record", "expected_type"),
    [
        ("00HEADER", "00"),
        ("01DETAIL", "01"),
        ("99TRAILER", "99"),
    ],
)
def test_should_detect_record_type(
    producer_context,
    raw_record,
    expected_type,
):
    service = producer_context["service"]

    event = service._build_event(
        file_name="COTAHIST_D09092026.TXT",
        line_number=1,
        raw_record=raw_record,
        file_date="2026-09-09",
    )

    assert event["record_type"] == expected_type


def test_should_produce_event_to_kafka(
    producer_context,
):
    service = producer_context["service"]
    kafka = producer_context["kafka"]

    event = {
        "event_id": "file.txt:1",
        "raw_record": "01TEST",
    }

    service._produce_event(
        key="file.txt",
        event=event,
    )

    kafka.produce.assert_called_once()

    call = kafka.produce.call_args

    assert call.kwargs["topic"] == service.topic
    assert call.kwargs["key"] == b"file.txt"

    payload = json.loads(
        call.kwargs["value"].decode("utf-8")
    )

    assert payload == event

    assert (
        call.kwargs["on_delivery"]
        == service._delivery_report
    )


def test_should_retry_when_producer_buffer_is_full(
    producer_context,
):
    service = producer_context["service"]
    kafka = producer_context["kafka"]

    kafka.produce.side_effect = [
        BufferError(),
        None,
    ]

    service._produce_event(
        key="file.txt",
        event={
            "event_id": "file.txt:1",
        },
    )

    assert kafka.produce.call_count == 2

    kafka.poll.assert_any_call(1)
    kafka.poll.assert_any_call(0)


def test_should_count_successful_delivery(
    producer_context,
):
    service = producer_context["service"]

    service._delivery_report(
        None,
        MagicMock(),
    )

    assert service.delivered_messages == 1
    assert service.failed_messages == 0


def test_should_count_failed_delivery(
    producer_context,
):
    service = producer_context["service"]
    logger = producer_context["logger"]

    error = Exception("Kafka unavailable")

    service._delivery_report(
        error,
        MagicMock(),
    )

    assert service.failed_messages == 1
    assert service.delivered_messages == 0

    logger.error.assert_called_once()


def test_should_raise_error_when_file_does_not_exist(
    producer_context,
    tmp_path,
):
    service = producer_context["service"]

    file_path = (
        tmp_path
        / "COTAHIST_D09092026.TXT"
    )

    with pytest.raises(FileNotFoundError):
        service.publish_file(file_path)


def test_should_preserve_fixed_width_record(
    producer_context,
    tmp_path,
):
    service = producer_context["service"]
    kafka = producer_context["kafka"]

    file_path = (
        tmp_path
        / "COTAHIST_D09092026.TXT"
    )

    file_path.write_bytes(
        b"00HEADER   \r\n"
        b"01DIMED        \r\n"
        b"99TRAILER     \r\n"
    )

    service._produce_event = MagicMock()

    service.publish_file(file_path)

    assert service._produce_event.call_count == 3

    calls = service._produce_event.call_args_list

    first_event = calls[0].kwargs["event"]
    second_event = calls[1].kwargs["event"]
    third_event = calls[2].kwargs["event"]

    assert first_event["raw_record"] == "00HEADER   "
    assert second_event["raw_record"] == "01DIMED        "
    assert third_event["raw_record"] == "99TRAILER     "

    assert first_event["line_number"] == 1
    assert second_event["line_number"] == 2
    assert third_event["line_number"] == 3

    assert first_event["event_id"] == (
        "COTAHIST_D09092026.TXT:1"
    )

    assert second_event["event_id"] == (
        "COTAHIST_D09092026.TXT:2"
    )

    assert calls[0].kwargs["key"] == (
        "COTAHIST_D09092026.TXT"
    )

    kafka.flush.assert_called_once_with(30)


def test_should_raise_error_when_messages_remain_after_flush(
    producer_context,
    tmp_path,
):
    service = producer_context["service"]
    kafka = producer_context["kafka"]

    kafka.flush.return_value = 2

    file_path = (
        tmp_path
        / "COTAHIST_D09092026.TXT"
    )

    file_path.write_bytes(
        b"01TEST\n"
    )

    service._produce_event = MagicMock()

    with pytest.raises(
        RuntimeError,
        match="2 mensagens não foram entregues",
    ):
        service.publish_file(file_path)