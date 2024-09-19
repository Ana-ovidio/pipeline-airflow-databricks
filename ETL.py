# Databricks notebook source
import requests
import logging
from datetime import date
from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %md
# MAGIC ##**Extract Data**

# COMMAND ----------

def extract_data(dt: date, base: str = "BRL") -> dict:
    url = f"https://api.apilayer.com/exchangerates_data/{dt}&base={base}"
    headers= {
    "apikey": "mY3EijY9WmJnMyPM9QNtRD3Log4f2LSE"
    }
    response = requests.request("GET", url, headers=headers)
    if response.status_code != 200:
        logging.error("Não foi possível extrair os dados")
        raise
    else: 
        return response.json()

# COMMAND ----------

# MAGIC %md
# MAGIC ##**Tranformation Data**

# COMMAND ----------

def insert_dt_import_column(df: object, column_date: str) -> object:
    return df.withColumn("dt_import", lit(column_date))

def export_data_to_df(data_json: dict, columns: list, **kwargs: dict) -> object:
    data = [(exchange, float(rate)) for exchange, rate in data_json.items()]
    df = spark.createDataFrame(data, schema=columns)
    df = insert_dt_import_column(df, kwargs["dt_import"])
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##**Load Data**

# COMMAND ----------

def save(df: object, dt_import: str) -> None:
    year, mounth, day = dt_import.split("-")
    path = f"dbfs:/databricks-results/bronze/{year}/{mounth}/{day}"
    df.write.format("parquet")\
        .mode("overwrite")\
        .save(path)
    logging.info(f"Data load with sucess to {path}")

# COMMAND ----------

result = extract_data(date(2021, 1, 1))
kwargs = {"dt_import": result["date"]}
df = export_data_to_df(result["rates"], columns=["exchange", "rate"], **kwargs)
save(df, dt_import=result["date"])

