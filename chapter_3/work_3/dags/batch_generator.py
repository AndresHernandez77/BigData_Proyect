import random
import csv
import logging
import uuid
import polars as pl

from faker import Faker
from datetime import date, datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler()]
)
def create_data(locale:str)->Faker:
    logging.info(f"Creted synthetic data for {locale.split('_')[-1]}contry code.")
    return Faker(locale)

def generate_record(fake:Faker)->list:
    person_name = fake.name()
    user_name = person_name.replace(" ","").lower()
    email = f"{user_name}@{fake.free_email_domain()}"
    personal_number= fake.ssn()
    birth_date = fake.date_of_birth()
    address = fake.address().replace("\n"," , ")
    phone_number=fake.phone_number()
    mac_address=fake.mac_address()
    ip_address= fake.ipv4()
    clabe = fake.iban()
    accessed_at= fake.date_time_between("-1y")
    session_duration= random.randint(0,36_000)
    download_speed= random.randint(0,1_000)
    upload_speed=random.randint(0, 900)
    consumed_trafic = random.randint(0, 2_000_000)
    return[
    person_name,user_name,email,personal_number,birth_date,address,phone_number, mac_address,ip_address,clabe,accessed_at,session_duration
    ,download_speed, upload_speed,consumed_trafic
    ]


def write_to_csv()->None:
    fake=create_data("es_MX")
    headers=[
    "person_name",
    "user_name",
    "email",
    "personal_number",
    "birth_date",
    "address",
    "phone_number",
    "mac_address",
    "ip_address",
    "clabe",
    "accessed_at",
    "session_duration",
    "download_speed",
    "upload_speed",
    "consumed_trafic",
    ]
    if str(date.today()) == "2026-06-09":
        rows = random.randint(100_372,100_372)
    else:
        rows = random.randint(0,1_1001)

    with open("/opt/airflow/data/raw_data.csv",mode="w",encoding="utf-8",newline="") as file:
        writer=csv.writer(file)
        writer.writerow(headers)

        for _ in range(rows):
            writer.writerow(generate_record(fake))
    logging.info("written{rows} records to the CSV file")

def add_id()->None:
    df=pl.read_csv("/opt/airflow/data/raw_data.csv")
    uuid_list=[str(uuid.uuid4()) for _ in range(df.height)]
    df=df.with_columns(pl.Series("unique_id",uuid_list))
    df.write_csv("/opt/airflow/data/raw_data.csv")
    logging.info("Added UUID to the daraset.")

def update_datetime(run_type)->None:
    if run_type == 'next':
        current_time = datetime.now().replace(microsecond=0)
        yesterday_time=str(current_time-timedelta(days=1))
        df=pl.read_csv("/opt/airflow/data/raw_data.csv")
        df=df.with_columns(pl.lit(yesterday_time).alias("accessed_at"))
        df.write_csv("/opt/airflow/data/raw_data.csv")
        logging.info("Updated accessed timestamp")

def save_raw_data():


#if __name__ == "__main__":

    # Logging starting of the process.

    logging.info(f"Started batch processing for {date.today()}.")

    # Define the output file name with today's date.

    #output_file = f"/work_2/data_2/batch_{date.today()}.csv"

    #output_file = f"C:/Users/Andres/Desktop/Big Data/batch_{date.today()}.csv"#Aplica cuando tengo el archivo en el directorio BD_DrivenPath\chapter_2\work_2 
    write_to_csv()
    add_id()
    update_datetime('next')
    logging.info(f"finish bash processing {date.today()}")
    #y un nivel abajo esta data_2

    

    # Define number of records: first run - 10_372; next runs random number.

    if str(date.today()) == "2026-05-21":

        records = random.randint(100_372, 100_372)

        run_type = "first"

    else:

        records = random.randint(0, 1_101)

        run_type = "next"


    # Generate and write records to the CSV.

    #write_to_csv(f"{output_file}", records)

    # Add UUID to dataset.

    #add_id(output_file)

    # Update the timestamp.

   # update_datetime(output_file, run_type)

    # Logging ending of the process.

    logging.info(f"Finished batch processing {date.today()}.")

