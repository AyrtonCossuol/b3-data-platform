import argparse
import zipfile
from datetime import date
from pathlib import Path

import requests
from tqdm import tqdm

from config.settings import (
    ANNUAL_FILE_PATTERN,
    B3_HISTORICAL_DATA_URL,
    DAILY_FILE_PATTERN,
    DATA_DIR,
    DOWNLOAD_CHUNK_SIZE,
    REQUEST_TIMEOUT_SECONDS,
)
from shared.logging.application_logger import ApplicationLogger


class CotahistDownloader:
    """Baixa e extrai arquivos históricos COTAHIST da B3."""

    def __init__(
        self,
        base_url: str = B3_HISTORICAL_DATA_URL,
        output_dir: Path = DATA_DIR,
        timeout: int = REQUEST_TIMEOUT_SECONDS,
    ) -> None:
        """Inicializa o downloader da COTAHIST."""
        self.base_url = base_url
        self.output_dir = output_dir
        self.timeout = timeout

        self.application_logger = ApplicationLogger()
        self.logger = self.application_logger.get_logger()

        self.logger.info(
            "Execução iniciada. ID: %s",
            self.application_logger.get_execution_id(),
        )

    def download_annual_file(
        self,
        year: int,
        extract_file: bool = True,
    ) -> Path:
        """Baixa o arquivo anual da COTAHIST."""
        file_name = ANNUAL_FILE_PATTERN.format(year=year)
        return self._download_and_process(file_name, extract_file)

    def download_daily_file(
        self,
        trading_date: date,
        extract_file: bool = True,
    ) -> Path:
        """Baixa o arquivo diário da COTAHIST."""
        date_string = trading_date.strftime("%d%m%Y")
        file_name = DAILY_FILE_PATTERN.format(date=date_string)

        return self._download_and_process(file_name, extract_file)

    def _download_and_process(
        self,
        file_name: str,
        extract_file: bool,
    ) -> Path:
        """Baixa, valida e opcionalmente extrai um arquivo."""
        file_url = self._build_file_url(file_name)
        file_path = self._prepare_file_path(file_name)

        if file_path.exists():
            self.logger.warning(
                "O arquivo já existe e não será baixado novamente: %s",
                file_path,
            )
            return file_path

        self.logger.info("Iniciando download: %s", file_name)

        try:
            self._download_file(file_url, file_path)

            if extract_file:
                self._extract_file(file_path)

            self.logger.info(
                "Processo concluído com sucesso: %s",
                file_name,
            )

        except requests.HTTPError as error:
            self._remove_incomplete_file(file_path)

            status_code = (
                error.response.status_code
                if error.response is not None
                else "desconhecido"
            )

            self.logger.error(
                "Arquivo não disponível. Status HTTP: %s",
                status_code,
            )
            raise

        except requests.RequestException:
            self._remove_incomplete_file(file_path)
            self.logger.exception(
                "Erro de comunicação ao acessar a B3.",
            )
            raise

        except zipfile.BadZipFile:
            self._remove_incomplete_file(file_path)
            self.logger.exception(
                "O arquivo baixado não é um ZIP válido.",
            )
            raise

        return file_path

    def _build_file_url(self, file_name: str) -> str:
        """Monta a URL completa do arquivo."""
        return f"{self.base_url}/{file_name}"

    def _prepare_file_path(self, file_name: str) -> Path:
        """Cria o diretório de saída e retorna o caminho do arquivo."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        return self.output_dir / file_name

    def _download_file(
        self,
        file_url: str,
        file_path: Path,
    ) -> None:
        """Faz o download do arquivo em blocos."""
        with requests.get(
            file_url,
            stream=True,
            timeout=self.timeout,
        ) as response:
            response.raise_for_status()

            total_size = int(
                response.headers.get("content-length", 0),
            )

            with file_path.open("wb") as output_file:
                self._write_response_to_file(
                    response=response,
                    output_file=output_file,
                    total_size=total_size,
                    file_name=file_path.name,
                )

    def _write_response_to_file(
        self,
        response: requests.Response,
        output_file,
        total_size: int,
        file_name: str,
    ) -> None:
        """Grava a resposta HTTP no arquivo de destino."""
        progress_bar = tqdm(
            total=total_size,
            unit="B",
            unit_scale=True,
            desc=file_name,
        )

        with progress_bar:
            for chunk in response.iter_content(
                chunk_size=DOWNLOAD_CHUNK_SIZE,
            ):
                if not chunk:
                    continue

                output_file.write(chunk)
                progress_bar.update(len(chunk))

        self.logger.info("Download finalizado: %s", file_name)

    def _extract_file(self, zip_path: Path) -> None:
        """Extrai o conteúdo do arquivo ZIP."""
        self.logger.info("Extraindo arquivo: %s", zip_path.name)

        with zipfile.ZipFile(zip_path, "r") as zip_file:
            zip_file.extractall(self.output_dir)

        self.logger.info(
            "Arquivo extraído no diretório: %s",
            self.output_dir,
        )

    def _remove_incomplete_file(self, file_path: Path) -> None:
        """Remove um arquivo incompleto ou inválido."""
        if file_path.exists():
            file_path.unlink()
            self.logger.info(
                "Arquivo incompleto removido: %s",
                file_path,
            )


def parse_arguments() -> argparse.Namespace:
    """Lê os argumentos informados na linha de comando."""
    parser = argparse.ArgumentParser(
        description="Downloader de arquivos históricos COTAHIST da B3.",
    )

    parser.add_argument(
        "--year",
        type=int,
        help="Ano do arquivo anual. Exemplo: 2025.",
    )

    parser.add_argument(
        "--date",
        type=lambda value: date.fromisoformat(value),
        help=(
            "Data do pregão no formato YYYY-MM-DD. "
            "Exemplo: 2025-09-10."
        ),
    )

    parser.add_argument(
        "--no-extract",
        action="store_true",
        help="Não extrair o conteúdo do arquivo ZIP.",
    )

    return parser.parse_args()


def main() -> None:
    """Executa o processo principal do downloader."""
    arguments = parse_arguments()

    if not arguments.year and not arguments.date:
        raise ValueError("Informe --year ou --date.")

    if arguments.year and arguments.date:
        raise ValueError(
            "Informe apenas um dos argumentos: --year ou --date.",
        )

    downloader = CotahistDownloader()

    if arguments.date:
        downloader.download_daily_file(
            trading_date=arguments.date,
            extract_file=not arguments.no_extract,
        )
        return

    downloader.download_annual_file(
        year=arguments.year,
        extract_file=not arguments.no_extract,
    )


if __name__ == "__main__":
    main()
