# Databricks notebook source
# MAGIC %md ## To Perform Encryption on Azure Databricks. The idea is to perform Symmetric Encryption i.e. generate a Key and store it in the Key Vault, based on the Key, the consumers will encrypt decrypt the sensitive datacolumn. To do this install Cryptography on the ADB Cluster https://pypi.org/project/cryptography/ perform AES Advanced Encryption Standard -128 Byte encryption. Refer for details https://cryptography.io/en/latest/fernet/ Below is the demo for Encrypting and Decrypting Email and PhoneNumber of the Employees. The Key is Stored in the KeyVault

# COMMAND ----------

# DBTITLE 1,Encryption On ADB Demo
import sys
import time
import datetime
import os
import re

from pyspark import SparkConf
from pyspark import SparkContext 
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import cryptography
from cryptography.fernet import Fernet
from pyspark.sql.functions import lit

secretscope = 'secret'

def encrypt(ValString,SecKey):
  ValString = ValString.encode()
  ValString = bytes(ValString)
  f = Fernet(SecKey)
  encrypted = f.encrypt(ValString)
  encrypted = encrypted.decode("utf-8")
  return (encrypted)

encrypt_udf = udf(lambda z,SecKey: encrypt(z,SecKey), StringType())
spark.udf.register("encrypt_udf", encrypt_udf)


def decrypt(ValString,SecKey):
  ValString = ValString.encode()
  ValString = bytes(ValString)
  f = Fernet(SecKey)
  decrypted = f.decrypt(ValString)
  decryptedString = decrypted.decode()
  return (decryptedString)

decrypt_udf = udf(lambda z,SecKey: decrypt(z,SecKey), StringType())
spark.udf.register("decrypt_udf", decrypt_udf)

# COMMAND ----------

df = spark.sql("""SELECT 'E10' AS EmpId, 'Ryan' AS EmpName,'ryan@email.com' AS Email, '999-121-1111' AS PhoneNo, '2019-04-15' AS ExtractDate 
               UNION  SELECT 'E11' AS EmpId, 'Jose' AS EmpName,'jose@email.com' AS Email, '999-121-1111' AS PhoneNo, '2019-04-14' AS ExtractDate 
               UNION  SELECT 'E12' AS EmpId, 'Sam' AS EmpName,'sam@email.com' AS Email, '999-121-1111' AS PhoneNo, '2019-04-16' AS ExtractDate
               UNION SELECT 'E16' AS EmpId, 'Keshav' AS EmpName,null AS Email,'999-121-1111' AS PhoneNo, '2019-04-15' AS ExtractDate""")

# COMMAND ----------

display(df)

# COMMAND ----------

colList = ['Email','PhoneNo']
for i in colList:
  df = df.withColumn(i, coalesce(i,lit(" ")))
  df = df.withColumn(i, encrypt_udf(i,lit(dbutils.secrets.get(secretscope,'encryptionKey'))))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %run ./PII-Handling-Encryption_2

# COMMAND ----------

#dfDecrypted = df
#colList = ['Email','PhoneNo']
#for i in colList:
#  dfDecrypted = dfDecrypted.withColumn(i, decrypt_udf(i,lit(dbutils.secrets.get(secretscope,'encryptionKey'))))

# COMMAND ----------

#display(dfDecrypted)

# COMMAND ----------

# DBTITLE 1,Generate Key To Store in KeyVault
#key = Fernet.generate_key()
#print(key)

# COMMAND ----------


