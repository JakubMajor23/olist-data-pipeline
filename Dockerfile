# Używamy oficjalnego obrazu Airflow jako bazy
FROM apache/airflow:2.10.2

# Przełączamy się na roota, żeby zainstalować systemowe dodatki (opcjonalne, ale git przydaje się do dbt)
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         git \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Wracamy na użytkownika airflow, żeby instalować pakiety Pythona
USER airflow

# Kopiujemy nasz plik z wymaganiami do kontenera
COPY requirements.txt /requirements.txt

# Instalujemy biblioteki
RUN pip install --no-cache-dir -r /requirements.txt