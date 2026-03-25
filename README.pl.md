<div align="center">

<p>
  <a href="README.md">English</a> | <strong>Polski</strong>
</p>

# Olist Data Pipeline

![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10.2-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt--postgres-1.8.2-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/Python-kontrole%20CI-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker%20Compose-stack-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![SQLFluff](https://img.shields.io/badge/SQLFluff-Postgres%20%2B%20dbt-00C7B7?style=for-the-badge)
![Power BI](https://img.shields.io/badge/Power%20BI-report-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)

Projekt portfolio ELT oparty o dataset Olist. Repozytorium symuluje miesięczny napływ danych do źródłowego PostgreSQL, ładuje warstwę surową hurtowni przez Apache Airflow i buduje modele analityczne w dbt.

**Live dbt docs:** <https://jakubmajor23.github.io/olist-data-pipeline/>

</div>

---

## Co Jest W Repozytorium

- Stack Docker Compose z Apache Airflow, symulowaną bazą źródłową PostgreSQL, bazą hurtowni PostgreSQL oraz oddzielną bazą metadanych Airflow.
- Dwa DAG-i Airflow:
  - `simulate_source_system`
  - `olist_elt_pipeline`
- Projekt dbt z 20 modelami SQL:
  - 9 modeli staging
  - 5 wymiarów
  - 2 wymiary słownikowe
  - 4 tabele faktów
- Trzy pliki seed dbt:
  - `order_statuses.csv`
  - `payments_types.csv`
  - `product_category_name_translation.csv`
- Trzy własne testy dbt w SQL:
  - `assert_delivered_orders_are_approved.sql`
  - `assert_delivered_orders_have_dates.sql`
  - `assert_fallback_members_exist.sql`
- Workflowy GitHub Actions do lintowania Pythona (Ruff), lintowania SQL (SQLFluff) oraz wdrażania dokumentacji dbt.
- Raport Power BI zapisany w repo jako `report.pbix`.

## Architektura

### Komponenty runtime

| Komponent | Aktualna implementacja |
| :--- | :--- |
| Orkiestracja | Apache Airflow `LocalExecutor` w Dockerze |
| Warstwa źródłowa | PostgreSQL `postgres-olist-source` |
| Warstwa surowa hurtowni | PostgreSQL `postgres-dwh`, schema `raw_data` |
| Schema docelowa dbt | `main` w Dockerze, `dwh_main_dev` przy lokalnym uruchamianiu dbt |
| Baza metadanych | PostgreSQL `postgres-airflow-meta` |
| Transformacje | dbt Core z `dbt-postgres` |
| Lintowanie SQL | SQLFluff z dbt templaterem i dialektem Postgres |
| Kontrole Pythona | Linter Ruff w CI |

### Przepływ pipeline'u

> **Uwaga:** DAG `simulate_source_system` nie jest elementem potoku analitycznego, to jedynie infrastruktura służąca do mockowania naturalnego zachowania użytkowników sklepu i symulacji czasu.

1. `simulate_source_system` działa miesięcznie od `2016-09-01` do `2018-12-01`, z `catchup=True`, `depends_on_past=True` i `max_active_runs=1`.
2. Tylko dla pierwszego logicznego miesiąca ładuje do bazy źródłowej dane referencyjne:
   - produkty
   - sprzedawców
   - geolokalizację
3. Dla każdego logicznego miesiąca ładuje do bazy źródłowej dane transakcyjne:
   - klientów
   - zamówienia
   - pozycje zamówień
   - płatności
   - recenzje
4. Następnie wyzwala `olist_elt_pipeline` z tym samym `logical_date` i czeka na zakończenie.
5. `olist_elt_pipeline` tworzy schemę `raw_data`, odświeża w całości surowe tabele statyczne, usuwa dane dla wybranego miesiąca z tabel transakcyjnych, ładuje je ponownie, a potem uruchamia:
   - `dbt seed --target docker`
   - `dbt run --target docker`
   - `dbt test --target docker`


## Model dbt

Projekt dbt czyta surowe tabele Olist ze schemy `raw_data` i buduje model hurtowniany w dwóch warstwach.

### Warstwa staging

- `stg__customers`
  - normalizuje nazwę miasta (lowercase, trimmed) i kod stanu (uppercase, trimmed)
- `stg__orders`
  - normalizuje zamówienia oznaczone jako `delivered`, ale bez pełnych timestampów dostawy, do statusu `shipped`
  - uzupełnia brakujące `order_approved_at` dla dostarczonych zamówień timestampem zakupu
  - zapisuje flagę audytową `is_approval_date_imputed`
- `stg__order_items`
  - buduje deterministyczny klucz zastępczy z `order_id` i `order_item_id`
  - ładuje się przyrostowo przez join do nowo załadowanych zamówień
- `stg__payments`
  - buduje klucz zastępczy z `order_id` i `payment_sequential`
  - przenosi `order_purchase_timestamp` na potrzeby logiki incremental
- `stg__reviews`
  - deduplikuje po `review_id`
  - zachowuje najnowszy `review_answer_timestamp`
  - uzupełnia brakujący tekst wartościami `No Title` i `No Comment`
- `stg__products`
  - tłumaczy kategorie produktów z portugalskiego na angielski przez join do seeda
  - odrzuca wiersze bez kompletu wymiarów fizycznych lub wagi
- `stg__sellers`
  - rzutuje kolumny na jawne typy dla spójności
- `stg__geolocation`
  - usuwa wiersze bez współrzędnych
  - dodaje flagę `is_valid_brazilian_location`

### Warstwa marts

Fakty:

- `fact_orders`
- `fact_sales_items`
- `fact_payments`
- `fact_reviews`

Wymiary i lookupi:

- `dim_customers`
- `dim_date`
- `dim_geolocation`
- `dim_products`
- `dim_sellers`
- `dim_order_status`
- `dim_payment_type`

W aktualnym SQL widać też następujące wzorce modelowania:

- `dim_customers` zachowuje jeden rekord na `customer_unique_id`, wybierając najnowszy adres według czasu zamówienia.
- `dim_geolocation` agreguje dane źródłowe do jednego rekordu na `geolocation_zip_code_prefix`.
- Kilka wymiarów dodaje rekordy fallback, np. `MD5('unknown')` lub `MD5('not_defined')`.
- Własny test dbt `assert_fallback_members_exist.sql` sprawdza obecność tych rekordów.

<div align="center">
  <img src="readme_images/dwh.png" alt="Diagram hurtowni danych" width="100%">
</div>

## Szybki Start

### Wymagania

- Docker Desktop albo Docker Engine z Compose v2

### 1. Utwórz `.env`

```bash
# Windows PowerShell
Copy-Item .env.example .env

# Linux / macOS
cp .env.example .env
```

Przykładowy plik ustawia domyślnie następujące porty hosta:

| Usługa | Port hosta |
| :--- | :--- |
| Airflow web UI | `8080` |
| Source PostgreSQL | `5433` |
| DWH PostgreSQL | `5434` |

### 2. Uruchom stack

```bash
docker compose up -d --build
```

### 3. Otwórz Airflow

- URL: <http://localhost:8080>
- Domyślne dane logowania z `docker-compose.yml`:
  - login: `airflow`
  - hasło: `airflow`

### 4. Uruchom pełny backfill historyczny

Głównym punktem wejścia w Airflow jest `simulate_source_system`.

- Odpauzuj `simulate_source_system` w UI Airflow.
- Ponieważ DAG ma `catchup=True`, Airflow utworzy miesięczne runy od września 2016 do grudnia 2018.
- Każdy run ładuje jeden miesiąc do source DB, a potem wyzwala `olist_elt_pipeline` dla tego samego logicznego miesiąca.

## CI/CD i Dokumentacja

### CI: `.github/workflows/ci_checks.yml`

Uruchamia się dla:

- pull requestów do `main`
- pushy do `main`

Aktualne kontrole:

- Ruff na `dags/` i `scripts/` z `--select E9,F63,F7,F82`
- SQLFluff lint dla modeli dbt z commitowaną konfiguracją `.sqlfluff`
  - workflow uruchamia najpierw `dbt deps`, aby makra z paczek rozwiązały się poprawnie

### Deployment docs: `.github/workflows/deploy_docs.yml`

Uruchamia się dla:

- pushy do `main`
- ręcznego `workflow_dispatch`

Aktualnie workflow:

- uruchamia tymczasową usługę PostgreSQL 16 w GitHub Actions
- instaluje zależności z commitowanego `requirements.txt`
- używa commitowanego profilu `olist_dbt/profiles.yml`
- uruchamia `dbt deps`
- uruchamia `dbt docs generate --target docker`
- wdraża `olist_dbt/target` do brancha `gh-pages`

Live site: <https://jakubmajor23.github.io/olist-data-pipeline/>

## Struktura Repozytorium

```text
.
|-- .env.example
|-- .github/
|   `-- workflows/
|       |-- ci_checks.yml
|       `-- deploy_docs.yml
|-- .gitignore
|-- .sqlfluff
|-- dags/
|   |-- olist_elt_pipeline_dag.py
|   `-- simulate_source_system_dag.py
|-- data/
|-- docker-compose.yml
|-- Dockerfile
|-- docs/
|-- init-scripts-olist/
|-- olist_dbt/
|   |-- dbt_project.yml
|   |-- models/
|   |-- packages.yml
|   |-- profiles.yml
|   |-- seeds/
|   `-- tests/
|-- readme_images/
|-- README.md
|-- README.pl.md
|-- report.pbix
|-- requirements.txt
`-- scripts/
    |-- db_config.py
    |-- simulate_production.py
    `-- validate_data.py
```

## Power BI

Repo zawiera:

- `report.pbix`
- trzy zrzuty dashboardów w `readme_images/`

<div align="center">
  <img src="readme_images/D1.png" alt="Dashboard Power BI 1" width="100%">
  <img src="readme_images/D2.png" alt="Dashboard Power BI 2" width="100%">
  <img src="readme_images/D3.png" alt="Dashboard Power BI 3" width="100%">
</div>

<div align="center">

Autor: Jakub Major

</div>
