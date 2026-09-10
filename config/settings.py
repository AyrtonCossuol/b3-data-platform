import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "downloader" / "data"

LOG_DIR = BASE_DIR / "logs"

LOG_FILE_PREFIX = "b3_downloader"

B3_HISTORICAL_DATA_URL = (
    "https://bvmf.bmfbovespa.com.br/InstDados/SerHist"
)

REQUEST_TIMEOUT_SECONDS = 60

DOWNLOAD_CHUNK_SIZE = 1024 * 1024

ANNUAL_FILE_PATTERN = "COTAHIST_A{year}.ZIP"

DAILY_FILE_PATTERN = "COTAHIST_D{date}.ZIP"

LOG_TIMESTAMP_FORMAT = "%Y%m%d%H%M%S"

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_COTAHIST_RAW_TOPIC = os.getenv(
    "KAFKA_COTAHIST_RAW_TOPIC",
    "b3.cotahist.raw.v1",
)

KAFKA_CLIENT_ID = "b3-cotahist-producer"