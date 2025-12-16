<div align="center">

# Olist E-commerce Data Pipeline

![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7%2B-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt--Core-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker--Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)

<br>

**Kompletny, skalowalny system ELT (Extract, Load, Transform) symulujący środowisko produkcyjne e-commerce.**

[Kontekst](#-kontekst-i-cele) •
[Technologie](#-zastosowane-wzorce-i-technologie) •
[Architektura](#-architektura-i-przepływ-danych) •
[Uruchomienie](#-instrukcja-uruchomienia)

</div>

---

## 💡 Kontekst i Cele

**Problem:** Surowe dane Olist to rozproszone logi transakcyjne – analiza przychodu, opóźnień czy retencji wymaga łączenia wielu tabel i jest nieefektywna w czasie rzeczywistym.

**Rozwiązanie:** Zautomatyzowany potok danych przekształcający surowe logi w czysty model **Galaxy Schema** (Konstelacja Faktów) w Hurtowni Danych.

### Główne Cele
- **Single Source of Truth:** Centralizacja danych o Zamówieniach, Płatnościach i Produktach.
- **Skalowalność:** Ładowanie przyrostowe (Incremental Loading) dla obsługi rosnącego wolumenu danych.
- **Jakość Danych:** Integralność referencyjna, brak duplikatów (Idempotentność) i testy `dbt`.

---

## Zastosowane Wzorce i Technologie

Projekt realizuje zasady inżynierii danych (**Modern Data Stack**) poprzez:

| Obszar | Implementacja |
| :--- | :--- |
| **Orkiestracja** | **Event-Driven Airflow**: Wyzwalanie DAG-ów przez REST API zaraz po pojawieniu się nowych danych (symulacja). |
| **Modelowanie** | **Galaxy Schema**: Architektura Konstelacji Faktów (3 tabele faktów) eliminująca problem *Fan-out* i iloczynu kartezjańskiego. |
| **Transformacja** | **dbt Core**: Modele zmaterializowane jako `incremental` oraz `table`, makra Jinja (DRY), testy jakości danych. |
| **Jakość Kodu** | **SQLFluff**: Linter SQL zapewniający spójny styl kodu (zgodnie z plikiem `.sqlfluff`). |
| **Infrastruktura** | **Docker & Docker Compose**: Pełna konteneryzacja Airflow (z dbt) oraz bazy danych Postgres. |

---

## Architektura i Przepływ Danych

System zaprojektowano modułowo, oddzielając warstwę symulacji od właściwego przetwarzania.

### Cykl Życia Danych (End-to-End Flow)

---

## Model Danych i Wyzwania

### Model Galaxy Schema
Projekt wykorzystuje architekturę **Konstelacji Faktów**, gdzie trzy tabele faktów współdzielą wymiary (*conformed dimensions*).

<div align="center">
  <img src="readme_images/dwh.png" alt="Architektura systemu" width="100%">
</div>

---

## Struktura Projektu

```bash
.
├── dags/                   # Definicje DAG-ów Airflow
│   └── olist_elt_dump_dag.py
├── olist_dbt/              # Projekt transformacji dbt
│   ├── models/
│   │   ├── staging/        # Modele pośrednie (Source & Cleaning)
│   │   └── marts/          # Modele biznesowe (Galaxy Schema)
│   ├── seeds/              # Pliki statyczne (CSV)
│   └── dbt_project.yml
├── scripts/                # Skrypty pomocnicze
│   ├── run_demo.py         # Orkiestrator symulacji
│   ├── simulate_production.py
│   └── validate_data.py
├── docker-compose.yml      # Definicja infrastruktury
├── requirements.txt        # Zależności Python
└── README.md
```

---

## Instrukcja Uruchomienia

### Wymagania
* Docker & Docker Compose
* Python 3.10+

### Szybki Start

**1. Konfiguracja Środowiska**
Skopiuj przykładowy plik konfiguracyjny (zawiera domyślne hasła dla środowiska deweloperskiego).
```bash
cp .env.example .env
```

**2. Start Infrastruktury**
Uruchom kontenery bazy danych i Airflow.
```bash
docker-compose up -d --build
```

**3. Uruchomienie Symulacji (Demo)**
Skrypt `run_demo.py` automatycznie:
1. Utworzy wirtualne środowisko (opcjonalnie).
2. Symuluje napływ danych historycznych (miesiąc po miesiącu).
3. Wyzwoli odpowiednie procesy w Airflow.

```bash
# Przygotowanie środowiska Python
python -m venv .venv
# Windows: .venv\Scripts\activate | Linux/Mac: source .venv/bin/activate
source .venv/bin/activate
pip install -r requirements.txt

# Start demo
python scripts/run_demo.py
```

---

## Roadmapa i Status

- [x] **Infrastruktura**: Dockerized Airflow & Postgres.
- [x] **Logika ELT**: Custom Python Operators z transakcyjną spójnością.
- [x] **Transformacja**: Modele dbt Incremental & implementacja Galaxy Schema.
- [x] **Orkiestracja**: Architektura Event-driven poprzez Airflow API.
- [x] **QA**: Automatyczna weryfikacja danych (`validate_data.py`).
- [ ] **Dokumentacja**: Hosting `dbt docs`.
- [ ] **BI**: Dashboardy w Metabase/Superset.

<br>

<div align="center">

**Autor:** Jakub Major

</div>