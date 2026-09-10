# B3 Data Platform

Projeto inicial para download de arquivos COTAHIST da B3.

## Instalação

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

No Windows PowerShell:

```powershell
.venv\Scripts\activate
pip install -r requirements.txt
```

## Download anual

```bash
python -m downloader.download_cotahist --year 2025
```

## Download diário

```bash
python -m downloader.download_cotahist --date 2025-09-09
```

## Download sem extrair o ZIP

```bash
python -m downloader.download_cotahist --year 2025 --no-extract
```

Os arquivos baixados ficam em `downloader/data/`.

Os logs de cada execução ficam em `logs/`, com UUID e timestamp no nome do arquivo.
