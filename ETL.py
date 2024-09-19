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

result = extract_data(date(2021, 1, 1))

# COMMAND ----------

# MAGIC %md
# MAGIC ##**Tranformation Data**

# COMMAND ----------

def list_of_tuples(data_json: dict) -> list:
    data = [(exchange, float(rate)) for exchange, rate in data_json.items()]
    return data 

data = list_of_tuples(result["rates"])
df = spark.createDataFrame(data, schema=["exchange", "rate"])

# COMMAND ----------

display(df.head(5))

# COMMAND ----------


df= df.withColumn("dt_import", lit(result["date"]))

# COMMAND ----------

display(df.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ##**Load Data**

# COMMAND ----------

year, mounth, day = result["date"].split("-")
print(year, mounth, day)

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/"))

# COMMAND ----------

path = f"dbfs:/databricks-results/bronze/{year}/{mounth}/{day}"
df.write.format("parquet")\
    .mode("overwrite")\
    .save(path)
