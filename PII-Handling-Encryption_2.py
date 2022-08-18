# Databricks notebook source
dfDecrypted = df
colList = ['Email','PhoneNo']
for i in colList:
    dfDecrypted = dfDecrypted.withColumn(i, decrypt_udf(i,lit(dbutils.secrets.get(secretscope,'encryptionKey'))))

# COMMAND ----------

display(dfDecrypted)

# COMMAND ----------

print("Adding Chcking In New Updates to the Notebook Code")
