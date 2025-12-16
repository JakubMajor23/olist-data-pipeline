<div align="center">

# Olist E-commerce Data Pipeline

![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7%2B-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt--Core-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker--Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![SQLFluff](https://img.shields.io/badge/SQLFluff-Expected_Quality-00C7B7?style=for-the-badge&logo=sql&logoColor=white)

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

Proces symuluje rzeczywiste działanie hurtowni danych w trybie przyrostowym (Incremental Load):

1.  **Symulacja Transakcji (`simulate_production.py`):**
    * Skrypt pobiera dane z plików CSV odpowiadające konkretnemu miesiącowi (np. styczeń 2017).
    * Dane są ładowane do operacyjnej bazy danych (`postgres-olist-source`), zachowując więzy integralności (najpierw Klienci, potem Zamówienia, na końcu Płatności/Recenzje).

2.  **Trigger API (`run_demo.py`):**
    * Natychmiast po załadowaniu danych, orkiestrator wysyła zapytanie POST do REST API Airflow.
    * Przekazuje parametr `logical_date`, co pozwala na precyzyjne przetworzenie tylko nowego wycinka czasu.

3.  **Extract & Load (Airflow DAG):**
    * **Idempotentność:** Przed załadowaniem, DAG usuwa z warstwy `raw_data` wszelkie dane dla przetwarzanego miesiąca. Zapobiega to duplikatom w przypadku ponownego uruchomienia.
    * **Transfer:** Dane są przenoszone z bazy źródłowej do hurtowni (Raw Layer) przy użyciu wydajnych silników SQLAlchemy.

4.  **Transformacja (dbt):**
    * Airflow uruchamia kontener z dbt (`dbt run`).
    * Dane surowe są czyszczone (Staging) i modelowane do postaci tabel faktów i wymiarów (Marts).

5.  **Walidacja:**
    * Na końcu `run_demo.py` uruchamiany jest skrypt weryfikujący zgodność liczby wierszy między źródłem a hurtownią.
---
## 🛠️ Szczegóły Transformacji (dbt)

Warstwa transformacji została podzielona na dwa etapy zgodnie z dobrymi praktykami Analytics Engineering:

### Warstwa Staging (Raw -> Staging)
* Materializacja jako `incremental` dla dużych tabel (Zamówienia, Płatności) i `table` dla słowników.
* Logika **Fail Fast**: Plik `dbt_project.yml` wymusza testy unikalności kluczy podstawowych.

### Warstwa Marts (Staging -> Facts/Dims)
Model **Galaxy Schema** łączy procesy biznesowe przez wspólne wymiary (*Conformed Dimensions*).

| Tabela Faktów | Opis i Logika |
| :--- | :--- |
| **fact_orders** | Centralna tabela transakcyjna. Agreguje wartości koszyka (`SUM(price)`), koszty dostawy oraz łączy statusy zamówień i recenzje w jeden widok analityczny. |
| **fact_sales_items** | Najbardziej granularna tabela (poziom produktu w koszyku). Pozwala na analizę sprzedaży per Produkt (`product_id`) i Sprzedawca (`seller_id`). |
| **fact_payments** | Analiza przepływów pieniężnych, typów płatności (karta, voucher) oraz rat (`payment_installments`). |

---

## 🌟 Wyróżniające Rozwiązania Techniczne

Projekt implementuje zaawansowane wzorce inżynieryjne, wykraczające poza standardowe kursy ETL:

### 1. Zaawansowane Modelowanie (Ghost Records)
W tabelach wymiarów (np. `dim_products`, `dim_reviews`) zastosowano tzw. **Ghost Records**.
* **Problem:** Brak spójności referencyjnej (np. zamówienie produktu, którego nie ma w bazie produktów) powoduje utratę wierszy przy `INNER JOIN`.
* **Rozwiązanie:** Sztuczny rekord z kluczem `MD5('unknown')`. Błędne klucze obce są mapowane do kategorii "Unknown" zamiast być odrzucane, co gwarantuje kompletność raportów finansowych.

### 2. Jakość Danych Geograficznych (Data Cleaning)
Surowe dane logistyczne zawierają wiele błędów (np. koordynaty poza granicami Brazylii) oraz duplikatów (wiele odczytów GPS dla jednego kodu pocztowego).
* **Walidacja:** Filtrowanie koordynatów w warstwie Staging.
* **Agregacja:** Wyliczanie centroidu (średnia szerokość/długość) dla każdego `zip_code` w celu stworzenia unikalnego słownika lokalizacji.

### 3. Obsługa Historii Klientów (Deduplication)
Klienci w systemie Olist mogą zmieniać adresy.
* **Logika:** Wymiar `dim_customers` wykorzystuje funkcję okna `ROW_NUMBER() ... ORDER BY order_purchase_timestamp DESC`, aby przypisać do klienta zawsze **aktualny adres** (na podstawie ostatniego zamówienia), tworząc spójny "Golden Record".

### 4. Ciągłość Czasowa (Date Spine)
Wymiar czasu `dim_date` nie powstał z danych transakcyjnych (co powodowałoby luki w dniach bez sprzedaży), lecz został wygenerowany algorytmicznie za pomocą pakietu `dbt_utils`. Gwarantuje to poprawność analiz typu "Running Total" czy "Year-over-Year".

---

## Model Danych

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