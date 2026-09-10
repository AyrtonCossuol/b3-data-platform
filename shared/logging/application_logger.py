import logging
import uuid
from datetime import datetime
from pathlib import Path

from config.settings import (
    LOG_DIR,
    LOG_FILE_PREFIX,
    LOG_TIMESTAMP_FORMAT,
)


class ApplicationLogger:
    """
    Configura um logger exclusivo para cada execução da aplicação.

    Cada instância gera um UUID próprio e cria um arquivo de log
    separado, permitindo rastrear uma execução específica.
    """

    LOGGER_NAME_PREFIX = "b3_data_platform"
    LOG_FORMAT = (
        "%(asctime)s | %(levelname)s | "
        "%(name)s | %(message)s"
    )
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(self) -> None:
        """Inicializa o logger com um identificador único."""
        self.execution_id = self._generate_execution_id()
        self.execution_timestamp = self._generate_execution_timestamp()
        self.logger_name = self._build_logger_name()
        self.log_file_name = self._build_log_file_name()
        self._logger = self._create_logger()

    @staticmethod
    def _generate_execution_id() -> str:
        """Gera um identificador único para a execução."""
        return str(uuid.uuid4())

    @staticmethod
    def _generate_execution_timestamp() -> str:
        """Gera o timestamp no formato YYYYMMDDHHMMSS."""
        return datetime.now().strftime(LOG_TIMESTAMP_FORMAT)

    def _build_logger_name(self) -> str:
        """Cria um nome único para o logger."""
        short_id = self.execution_id[:8]
        return f"{self.LOGGER_NAME_PREFIX}_{short_id}"

    def _build_log_file_name(self) -> str:
        """Cria o nome do arquivo de log da execução."""
        short_id = self.execution_id[:8]

        return (
            f"{LOG_FILE_PREFIX}_"
            f"{short_id}_"
            f"{self.execution_timestamp}.log"
        )

    def _create_logger(self) -> logging.Logger:
        """Cria e configura o logger da execução atual."""
        logger = logging.getLogger(self.logger_name)

        if logger.handlers:
            return logger

        logger.setLevel(logging.INFO)
        logger.propagate = False

        formatter = self._create_formatter()

        logger.addHandler(self._create_file_handler(formatter))
        logger.addHandler(self._create_console_handler(formatter))

        return logger

    def _create_formatter(self) -> logging.Formatter:
        """Cria o formato padrão das mensagens de log."""
        return logging.Formatter(
            fmt=self.LOG_FORMAT,
            datefmt=self.DATE_FORMAT,
        )

    def _create_file_handler(
        self,
        formatter: logging.Formatter,
    ) -> logging.FileHandler:
        """Cria o handler que grava os logs em arquivo."""
        Path(LOG_DIR).mkdir(
            parents=True,
            exist_ok=True,
        )

        log_path = Path(LOG_DIR) / self.log_file_name

        handler = logging.FileHandler(
            filename=log_path,
            encoding="utf-8",
        )
        handler.setFormatter(formatter)

        return handler

    @staticmethod
    def _create_console_handler(
        formatter: logging.Formatter,
    ) -> logging.StreamHandler:
        """Cria o handler que exibe os logs no console."""
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)

        return handler

    def get_logger(self) -> logging.Logger:
        """Retorna o logger da execução atual."""
        return self._logger

    def get_execution_id(self) -> str:
        """Retorna o UUID completo da execução."""
        return self.execution_id

    def get_log_file_name(self) -> str:
        """Retorna o nome do arquivo de log criado."""
        return self.log_file_name
