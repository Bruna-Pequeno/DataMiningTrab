#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import findspark
import os

findspark.init(os.environ.get('SPARK_HOME'))

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as functions
from decimal import Decimal


# In[ ]:


spark = SparkSession.builder.appName("GastoMinisterio").getOrCreate()


# In[ ]:


df = spark.read.format("csv").option("header", True).option("delimiter", ";").option('encoding', 'windows-1252').csv('C:/Spark/bin/2019_Viagem.csv')


# In[ ]:


df.printSchema


# In[ ]:


#converte decimal
to_value = lambda v : Decimal(v.replace(",", "."))


# In[ ]:


udf_to_value = functions.udf(to_value)


# In[ ]:


df2 = df.withColumn("Max_por_org_sup",    udf_to_value(df["Valor passagens"]))
df2 = df2.withColumn("Media_por_org_sup", udf_to_value(df2["Valor passagens"]))
df2 = df2.withColumn("Min_por_org_sup",   udf_to_value(df2["Valor passagens"]))
df2 = df2.withColumn("Total_por_org_sup", udf_to_value(df2["Valor passagens"]))

df2 = df2.withColumn("Max_por_destinos", udf_to_value(df2["Valor passagens"]))
df2 = df2.withColumn("Media_por_destinos", udf_to_value(df2["Valor passagens"]))
df2 = df2.withColumn("Min_por_destinos", udf_to_value(df2["Valor passagens"]))
df2 = df2.withColumn("Total_por_destinos", udf_to_value(df2["Valor passagens"]))

df2 = df2.withColumn("Max_por_cargos", udf_to_value(df2["Valor passagens"]))
df2 = df2.withColumn("Media_por_cargos", udf_to_value(df2["Valor passagens"]))
df2 = df2.withColumn("Min_por_cargos", udf_to_value(df2["Valor passagens"]))
df2 = df2.withColumn("Total_por_cargos", udf_to_value(df2["Valor passagens"]))

df2 = df2.withColumn("Max_por_solicitante", udf_to_value(df2["Valor passagens"]))
df2 = df2.withColumn("Media_por_solicitante", udf_to_value(df2["Valor passagens"]))
df2 = df2.withColumn("Min_por_solicitante", udf_to_value(df2["Valor passagens"]))
df2 = df2.withColumn("Total_por_solicitante", udf_to_value(df2["Valor passagens"]))

df2 = df.withColumn("Max_por_org_sup_diaria",    udf_to_value(df["Valor diárias"]))
df2 = df2.withColumn("Media_por_org_sup_diaria", udf_to_value(df2["Valor diárias"]))
df2 = df2.withColumn("Min_por_org_sup_diaria",   udf_to_value(df2["Valor diárias"]))
df2 = df2.withColumn("Total_por_org_sup_diaria", udf_to_value(df2["Valor diárias"]))


# In[ ]:


from pyspark.sql import functions as F


# In[ ]:


df2.groupBy("Nome do órgão superior").agg(F.max("Max_por_org_sup"), 
                                          F.avg("Media_por_org_sup"), 
                                          F.min("Min_por_org_sup"), 
                                          F.sum("Total_por_org_sup")).sort('Nome do órgão superior').show( truncate=True)


# In[ ]:


df2.groupBy("Destinos").agg(F.max("Max_por_destinos"), 
                                          F.avg("Media_por_destinos"), 
                                          F.min("Min_por_destinos"), 
                                          F.sum("Total_por_destinos")).sort('Destinos').show( truncate=True)


# In[ ]:


df2.groupBy("Cargo").agg(F.max("Max_por_cargos"), 
                                          F.avg("Media_por_cargos"), 
                                          F.min("Min_por_cargos"), 
                                          F.sum("Total_por_cargos")).sort('Cargo').show( truncate=False)


# In[ ]:


df2.groupBy("Solicitante").agg(F.max("Max_por_solicitante"), 
                                          F.avg("Media_por_solicitante"), 
                                          F.min("Min_por_solicitante"), 
                                          F.sum("Total_por_solicitante")).sort('Solicitante').show( truncate=False)


# In[ ]:


df2.groupBy("Sup_Diaria").agg(F.max("Max_por_sup_diaria"), 
                                          F.avg("Media_por_sup_diaria"), 
                                          F.min("Min_por_sup_diaria"), 
                                          F.sum("Total_por_sup_diaria")).sort('Sup_Diaria').show( truncate=False)


# In[ ]:


tabela_aggnm_sup = df2.groupBy("Nome do órgão superior").agg(F.max("Max_por_org_sup"), 
                                          F.avg("Media_por_org_sup"), 
                                          F.min("Min_por_org_sup"), 
                                          F.sum("Total_por_org_sup")).sort('Nome do órgão superior')


# In[ ]:


export_tabela_aggnm_sup = tabela_aggnm_sup.rdd


# In[ ]:


tabela_aggnm_destino =df2.groupBy("Destinos").agg(F.max("Max_por_destinos"), 
                                          F.avg("Media_por_destinos"), 
                                          F.min("Min_por_destinos"), 
                                          F.sum("Total_por_destinos")).sort('Destinos')


# In[ ]:


export_tabela_aggnm_destino = tabela_aggnm_destino.rdd


# In[ ]:


tabela_aggnm_cargo =df2.groupBy("Cargo").agg(F.max("Max_por_cargos"), 
                                          F.avg("Media_por_cargos"), 
                                          F.min("Min_por_cargos"), 
                                          F.sum("Total_por_cargos")).sort('Cargo')


# In[ ]:


export_tabela_aggnm_cargo = tabela_aggnm_cargo.rdd


# In[ ]:


tabela_aggnm_solicitante =df2.groupBy("Solicitante").agg(F.max("Max_por_solicitante"), 
                                          F.avg("Media_por_solicitante"), 
                                          F.min("Min_por_solicitante"), 
                                          F.sum("Total_por_solicitante")).sort('Solicitante')


# In[ ]:


export_tabela_aggnm_solicitante = tabela_aggnm_solicitante.rdd


# In[ ]:


tabela_aggnm_solicitante =df2.groupBy("Sup_Diaria").agg(F.max("Max_por_sup_diaria"), 
                                          F.avg("Media_por_sup_diaria"), 
                                          F.min("Min_por_sup_diaria"), 
                                          F.sum("Total_por_sup_diaria")).sort('Sup_Diaria')


# In[ ]:


export_tabela_aggnm_sup_diaria = tabela_aggnm_sup_diaria.rdd


# In[ ]:


tabela_aggnm_sup.to_Pandas()


# In[ ]:


tabela_aggnm_sup.to_csv('C:/Spark/bin/tabela_aggnm_sup.csv',encoding='windows-1252',index=False)


# In[ ]:


tabela_aggnm_destino.to_Pandas()


# In[ ]:


tabela_aggnm_destino.to_csv('C:/Spark/bin/tabela_aggnm_destino.csv',encoding='windows-1252',index=False)


# In[ ]:


tabela_aggnm_cargo.to_Pandas()


# In[ ]:


tabela_aggnm_cargo.to_csv('C:/Spark/bin/tabela_aggnm_cargo.csv',encoding='windows-1252',index=False)


# In[ ]:


tabela_aggnm_solicitante.to_Pandas()


# In[ ]:


tabela_aggnm_solicitante.to_csv('C:/Spark/bin/tabela_aggnm_solicitante.csv',encoding='windows-1252',index=False)


# In[ ]:


tabela_aggnm_sup_diaria.to_Pandas()


# In[ ]:


tabela_aggnm_sup_diaria.to_csv('C:/Spark/bin/tabela_aggnm_solicitante.csv',encoding='windows-1252',index=False)

