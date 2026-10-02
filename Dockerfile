FROM astrocrpublic.azurecr.io/runtime:3.3-8
# Install dbt in its OWN virtual environment inside the image.
# Airflow and dbt depend on different versions of some libraries;
# a separate venv keeps them from conflicting with each other.
RUN python -m venv dbt_venv && \
    dbt_venv/bin/pip install --no-cache-dir dbt-core==1.12.5 dbt-duckdb==1.11.0
