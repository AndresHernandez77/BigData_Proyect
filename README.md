# Data Pipeline with Apache Airflow, PostgreSQL and dbt

## Overview

This project implements an end-to-end data pipeline using Apache Airflow, PostgreSQL, and dbt. The pipeline automatically generates synthetic network usage data, stores it in a raw data layer, transforms and cleans the data through staging models, and finally publishes trusted datasets ready for analytics and reporting.

The workflow follows a modern Data Engineering architecture based on layered data processing:

* Raw Layer
* Staging Layer
* Trusted Layer

## Technologies Used

* Apache Airflow
* PostgreSQL
* dbt (Data Build Tool)
* Docker & Docker Compose
* Python
* Faker
* Polars

## Running the Project

### Start Docker Containers

```bash
docker compose up -d
```

### Access Airflow

```text
http://localhost:8080
```

### Execute DAG

1. Open Airflow UI.
2. Enable the DAG.
3. Trigger the DAG manually or wait for scheduled execution.
4. Monitor task execution through logs.

## Database Access

PostgreSQL configuration:

```text
Host: localhost
Port: 5433
Database: airflow
Username: airflow
Password: airflow
```
## Author

Andrés Hernández Sánchez

Computer Engineering Student

Data Engineering & Big Data Project
