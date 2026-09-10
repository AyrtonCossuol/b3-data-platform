import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CLIENT_ID,
    KAFKA_COTAHIST_RAW_TOPIC,
)
from shared.logging.application_logger import ApplicationLogger


class CotahistKafkaProducer:
    """Publica registros brutos COTAHIST no Kafka."""

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = KAFKA_COTAHIST_RAW_TOPIC,
    ) -> None:
        self.topic = topic

        self.application_logger = ApplicationLogger()
        self.logger = self.application_logger.get_logger()

        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": KAFKA_CLIENT_ID,
                "acks": "all",
                "enable.idempotence": True,
            }
        )

        self.delivered_messages = 0
        self.failed_messages = 0

    def publish_file(
        self,
        file_path: Path,
    ) -> None:
        """Publica todas as linhas de um arquivo COTAHIST."""

        if not file_path.exists():
            raise FileNotFoundError(
                f"Arquivo não encontrado: {file_path}"
            )

        file_name = file_path.name
        file_date = self._extract_file_date(file_name)

        self.logger.info(
            "Iniciando publicação no Kafka. Arquivo: %s",
            file_name,
        )

        with file_path.open("rb") as input_file:

            for line_number, raw_line in enumerate(
                input_file,
                start=1,
            ):
                raw_record = (
                    raw_line
                    .rstrip(b"\r\n")
                    .decode("latin-1")
                )

                event = self._build_event(
                    file_name=file_name,
                    line_number=line_number,
                    raw_record=raw_record,
                    file_date=file_date,
                )

                self._produce_event(
                    key=file_name,
                    event=event,
                )

        remaining_messages = self.producer.flush(30)

        if remaining_messages > 0:
            raise RuntimeError(
                f"{remaining_messages} mensagens não foram entregues."
            )

        self.logger.info(
            (
                "Publicação concluída. "
                "Entregues: %s | Falhas: %s"
            ),
            self.delivered_messages,
            self.failed_messages,
        )

    def _build_event(
        self,
        file_name: str,
        line_number: int,
        raw_record: str,
        file_date: str | None,
    ) -> dict:
        """Cria o envelope Kafka sem interpretar COTAHIST."""

        return {
            "schema_version": 1,
            "event_id": f"{file_name}:{line_number}",
            "source": "B3",
            "dataset": "COTAHIST",
            "file_name": file_name,
            "file_date": file_date,
            "line_number": line_number,
            "record_type": raw_record[:2],
            "raw_record": raw_record,
            "ingested_at": (
                datetime.now(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            ),
        }

    def _produce_event(
        self,
        key: str,
        event: dict,
    ) -> None:
        """Adiciona um evento à fila do Kafka Producer."""

        payload = json.dumps(
            event,
            ensure_ascii=False,
            separators=(",", ":"),
        )

        while True:
            try:
                self.producer.produce(
                    topic=self.topic,
                    key=key.encode("utf-8"),
                    value=payload.encode("utf-8"),
                    on_delivery=self._delivery_report,
                )

                break

            except BufferError:
                self.producer.poll(1)

        self.producer.poll(0)

    def _delivery_report(
        self,
        error,
        message,
    ) -> None:
        """Recebe a confirmação de entrega."""

        if error is not None:
            self.failed_messages += 1

            self.logger.error(
                "Erro ao publicar mensagem no Kafka: %s",
                error,
            )
            return

        self.delivered_messages += 1

    @staticmethod
    def _extract_file_date(
        file_name: str,
    ) -> str | None:
        """Extrai a data do nome de um arquivo diário."""

        match = re.match(
            r"^COTAHIST_D(\d{2})(\d{2})(\d{4})\.TXT$",
            file_name,
        )

        if not match:
            return None

        day, month, year = match.groups()

        return f"{year}-{month}-{day}"


def parse_arguments() -> argparse.Namespace:
    """Lê os argumentos da linha de comando."""

    parser = argparse.ArgumentParser(
        description=(
            "Publica arquivos COTAHIST brutos no Kafka."
        ),
    )

    parser.add_argument(
        "--file",
        type=Path,
        required=True,
        help="Caminho do arquivo COTAHIST TXT.",
    )

    return parser.parse_args()


def main() -> None:
    """Executa o producer COTAHIST."""

    arguments = parse_arguments()

    producer = CotahistKafkaProducer()

    producer.publish_file(
        file_path=arguments.file,
    )


if __name__ == "__main__":
    main()