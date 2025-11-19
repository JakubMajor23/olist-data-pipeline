# Olist Data Pipeline

Ten projekt to transformacja danych Olist przy użyciu **dbt** (data build tool) i **PostgreSQL**.

## Struktura projektu

- `models/`: Modele dbt (SQL).
- `seeds/`: Pliki CSV z danymi statycznymi.
- `tests/`: Testy jakości danych.
- `analyses/`: Analizy ad-hoc.
- `docker-compose.yml`: Konfiguracja środowiska Docker (bazy danych).

## Wymagania

- [Docker](https://www.docker.com/)
- [Python](https://www.python.org/) (zalecana wersja 3.10+)

## Uruchomienie środowiska

### 1. Konfiguracja zmiennych środowiskowych

Skopiuj plik `.env.example` do `.env`:

```bash
cp .env.example .env
```

### 2. Uruchomienie baz danych

Uruchom kontenery Docker (baza źródłowa i hurtownia danych):

```bash
docker-compose up -d
```

### 3. Instalacja zależności Python

Zaleca się użycie wirtualnego środowiska:

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# lub
.venv\Scripts\activate     # Windows
```

Zainstaluj wymagane pakiety:

```bash
pip install -r requirements.txt
```

### 4. Konfiguracja dbt

Zainstaluj zależności dbt:

```bash
dbt deps
```

Sprawdź połączenie:

```bash
dbt debug
```

### 5. Uruchomienie projektu

Załaduj dane (seeds):

```bash
dbt seed
```

Uruchom modele:

```bash
dbt run
```

Uruchom testy:

```bash
dbt test
```
