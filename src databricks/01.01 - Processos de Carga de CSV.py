# Databricks notebook source
# DBTITLE 1,Importação de Bibliotecas
from pyspark.sql.functions import *
from pyspark.sql.functions import date_format
from pyspark.sql.functions import col
from pyspark.sql.functions import ceil


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS tcc;

# COMMAND ----------

# DBTITLE 1,Função para ajustar os tipos de colunas
def ajusta_colunas(df):
  df = df.drop('LICENCA_NAO_REMUNERADA', 'LICENCA_REMUNERADA', 'PROCESSO_RETORNO_INSS', 'TOTAL_AUSENCIAS', 'INSS', 'DISPOSICAO_EMPRESA')
  
  df= (
    df.
    withColumn("DATA", to_date(col("DATA"), "yyyy-MM-dd")).
    withColumn("NOMEAREA", trim("NOMEAREA")).
    withColumn("TURNO", trim("TURNO")).
    withColumn("FUNCAO", trim("FUNCAO")).
    withColumn("POSTOS", col("POSTOS").cast("Short")).
    withColumn("FALTAS", when(col("FALTAS")=='NULL', 0).otherwise(col("FALTAS"))).
    withColumn("FALTAS", col("FALTAS").cast("Short")).
    withColumn("FERIAS", when(col("FERIAS")=='NULL', 0).otherwise(col("FERIAS"))).
    withColumn("FERIAS", col("FERIAS").cast("Short")).  
    withColumn("SAIDAANTECIPADA", when(col("SAIDAANTECIPADA")=='NULL', 0).otherwise(col("SAIDAANTECIPADA"))).
    withColumn("SAIDAANTECIPADA", col("SAIDAANTECIPADA").cast("Short")).
    withColumn("RECICLAGEM", when(col("RECICLAGEM")=='NULL', 0).otherwise(col("RECICLAGEM"))).
    withColumn("RECICLAGEM", col("RECICLAGEM").cast("Short")).
    withColumn("SUSPENSAO", when(col("SUSPENSAO")=='NULL', 0).otherwise(col("SUSPENSAO"))).
    withColumn("SUSPENSAO", col("SUSPENSAO").cast("Short")).
    withColumn('TOTAL_AUSENCIAS_FUNCIONARIO', col('FALTAS') + col('SAIDAANTECIPADA') + col('SUSPENSAO')).
    withColumn('TOTAL_AUSENCIAS_GERAL', col('FALTAS') + col('FERIAS') + col('SAIDAANTECIPADA') + col('RECICLAGEM') + col('SUSPENSAO'))    
   )
  
  return df
  


# COMMAND ----------

# DBTITLE 1,Importar CSV da Base Completa
df = spark.read.csv('/FileStore/tables/tcc_csv/base_completa.csv', header=True, sep=';')

df = ajusta_colunas(df)

df.write.mode('overwrite').saveAsTable('tcc.basecompleta')


# COMMAND ----------

# DBTITLE 1,Importar CSV do Filtro de Exclusão
df = spark.read.csv('/FileStore/tables/tcc_csv/base_filtro_exclusao.csv', header=True, sep=';')

df = ajusta_colunas(df)

df.write.mode('overwrite').saveAsTable('tcc.basefiltroexclusao')


# COMMAND ----------

# DBTITLE 1,Importar CSV da Base de Treino
df = spark.read.csv('/FileStore/tables/tcc_csv/base_treino.csv', header=True, sep=';')

df = ajusta_colunas(df)

df.write.mode('overwrite').saveAsTable('tcc.basetreino')


# COMMAND ----------

# DBTITLE 1,Importar CSV da Base de Teste
df = spark.read.csv('/FileStore/tables/tcc_csv/base_teste.csv', header=True, sep=';')

df = ajusta_colunas(df)
 
df.write.mode('overwrite').saveAsTable('tcc.basetest')

# COMMAND ----------

# DBTITLE 1,Importar CSV da Base da Reserva
df = spark.read.csv('/FileStore/tables/tcc_csv/base_reserva.csv', header=True, sep=';')
df = (
  df.
  withColumn("NOMEAREA", trim("NOMEAREA")).
  withColumn("TURNO", trim("TURNO")).
  withColumn("FUNCAO", trim("FUNCAO")).  
  withColumn("DATA", to_date(col("DATA"), "yyyy-MM-dd")).  
  withColumn("ID_LOCAL", col("ID_LOCAL").cast("Short")).
  withColumn("QTDE_POSTOS", col("QTDE_POSTOS").cast("Short"))
)

df = df.drop('ID', 'ID_LOCAL')

df.write.mode('overwrite').saveAsTable('tcc.basereservatecnica')

# COMMAND ----------

# DBTITLE 1,Importar CSV com Dados de Área e Local
df = spark.read.csv('/FileStore/tables/tcc_csv/base_area_local.csv', header=True, sep=';')

df = (df.
  withColumn("NOMEAREA", trim("NOMEAREA")).
  withColumn("LOCALIDADE", trim("LOCALIDADE")).
  withColumn("UF", trim("UF"))
     )

df.write.mode('overwrite').saveAsTable('tcc.arealocal')

# COMMAND ----------

# DBTITLE 1,Gerar Resumo da Reserva
df_area_local = spark.table("tcc.arealocal")
df_area_saopaulo = (df_area_local.
                    where("UF=='SP'").
                    where("LOCALIDADE=='SÃO PAULO'").
                    select('NOMEAREA')
                    )
                     
tabela_base_reserva = spark.table("tcc.basereservatecnica")
df_reserva = (tabela_base_reserva.
              where("FUNCAO='VIGILANTE'").
              where("TURNO='DIURNO'").
              orderBy("DATA")
             )

df_plantao = (df_reserva.alias('reserva').
           join(df_area_saopaulo.alias('local'), 
           df_reserva["NOMEAREA"]==df_area_local["NOMEAREA"], how='inner').
           select("reserva.DATA",
                  "reserva.QTDE_POSTOS").
          groupBy("Data").
          sum("QTDE_POSTOS").
          withColumnRenamed("SUM(QTDE_POSTOS)", "QTDE_PLANTAO").
          orderBy("DATA"))

df_plantao.write.mode('overwrite').saveAsTable('tcc.reservaqtdeplantao')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tcc.reservaqtdeplantao

# COMMAND ----------

# DBTITLE 1,Gerar Resumo dos Postos
tabela_base_treino = spark.table("tcc.basetreino")
df_plantao =  spark.table("tcc.reservaqtdeplantao")

df_treino = (tabela_base_treino.
            groupBy("DATA").
            sum('POSTOS', 'TOTAL_AUSENCIAS_FUNCIONARIO', 'TOTAL_AUSENCIAS_GERAL', 'FALTAS', 'FERIAS', 'SAIDAANTECIPADA', 
                'RECICLAGEM', 'SUSPENSAO').
            withColumnRenamed('sum(POSTOS)', 'QTDE_POSTOS_TRABALHO').
            withColumnRenamed('sum(TOTAL_AUSENCIAS_FUNCIONARIO)', 'QTDE_AUSENCIAS_FUNCIONARIO').
            withColumnRenamed('sum(TOTAL_AUSENCIAS_GERAL)', 'QTDE_AUSENCIAS_GERAL').
            withColumnRenamed('sum(FALTAS)', 'QTDE_FALTAS').
            withColumnRenamed('sum(FERIAS)', 'QTDE_FERIAS').
            withColumnRenamed('sum(SAIDAANTECIPADA)', 'QTDE_SAIDAANTECIPADA').
            withColumnRenamed('sum(RECICLAGEM)', 'QTDE_RECICLAGEM').
            withColumnRenamed('sum(SUSPENSAO)', 'QTDE_SUSPENSAO').
            orderBy("DATA", ascending=False))

df_treino = (df_treino.alias('treino').
             join(df_plantao.alias('plantao'), df_treino["DATA"]==df_plantao["DATA"]).
             select("treino.DATA",
                    "treino.QTDE_POSTOS_TRABALHO",
                    "plantao.QTDE_PLANTAO",
                    "treino.QTDE_FALTAS",                    
                    "treino.QTDE_FERIAS",                    
                    "treino.QTDE_SAIDAANTECIPADA",                    
                    "treino.QTDE_RECICLAGEM",                    
                    "treino.QTDE_SUSPENSAO",
                    "treino.QTDE_AUSENCIAS_FUNCIONARIO",
                    "treino.QTDE_AUSENCIAS_GERAL")
              )

df_treino.write.mode('overwrite').saveAsTable('tcc.abttreino')

# COMMAND ----------

# DBTITLE 1,Média de Salários Vigentes das Áreas
df_area_saopaulo = (df_area_local.
                    where("UF=='SP'").
                    where("LOCALIDADE=='SÃO PAULO'").
                    select('NOMEAREA')
                    )

df = spark.read.csv('/FileStore/tables/tcc_csv/base_folha_salarial_area.csv', header=True, sep=';')
df_area_salario_medio = (df.
      withColumn("DTIMPLANTACAO", to_date(col("DTIMPLANTACAO"), "yyyy-MM-dd")).  
      withColumn("DTENCERRAMENTO", to_date(col("DTENCERRAMENTO"), "yyyy-MM-dd")).
      where(col("DTENCERRAMENTO").isNull()).      
      withColumn("MEDIA_SALARIO", col("MEDIA_SALARIO").cast("float")).
      drop("DTENCERRAMENTO", "DTIMPLANTACAO", "RELLOCAL").
      where(col("FUNCAO")=='VIGILANTE').
      where(col("TURNO")=='DIURNO').
      select("NOMEAREA", "MEDIA_SALARIO").
      groupBy("NOMEAREA").
      avg("MEDIA_SALARIO").
      withColumnRenamed("avg(MEDIA_SALARIO)", "MEDIA_SALARIO")
     )

df_area_salario_medio.write.mode('overwrite').saveAsTable('tcc.area_salario_medio')


# COMMAND ----------

# DBTITLE 1,Exibindo Quantidade de Instâncias por Base
# MAGIC %sql
# MAGIC select 'Base Completa', count(*)  instancias from tcc.basecompleta
# MAGIC union
# MAGIC select 'Base com Filtros de Exclusão', count(*) instancias from tcc.basefiltroexclusao
# MAGIC union
# MAGIC select 'Base Treino', count(*) instancias from tcc.basetreino
# MAGIC union
# MAGIC select 'Base Teste', count(*) instancias from tcc.basetest
# MAGIC union
# MAGIC select 'Base Reserva Técnica', count(*) instancias from tcc.basereservatecnica
# MAGIC union
# MAGIC select 'Área e Localidade', count(*) instancias from tcc.arealocal
# MAGIC union
# MAGIC select 'Quantidades de Diurnos, Vigilantes em SP da Reserva', count(*) instancias from tcc.reservaqtdeplantao
# MAGIC union
# MAGIC select 'Quantidade Salário Médio Área', count(*) instancias from tcc.area_salario_medio
# MAGIC union
# MAGIC select 'ABT Treino', count(*) instancias from tcc.abttreino

# COMMAND ----------

# MAGIC %fs rm dbfs:/FileStore/tables/tcc_csv/bt_treino.csv/abt_train.csv

# COMMAND ----------

# DBTITLE 1,Gerar CSV do Treino
df_abt_treino = spark.table("tcc.abttreino")

df_abt_treino = (df_abt_treino.
                orderBy("DATA"))

df_abt_treino.write.options(header=True, delimiter=";").csv('/FileStore/tables/tcc_csv/bt_treino.csv/abt_train.csv', header=True, sep=';')



# COMMAND ----------

display(df_abt_treino)

# COMMAND ----------

# MAGIC %fs rm dbfs:/FileStore/tables/tcc_csv/teste.csv

# COMMAND ----------

# DBTITLE 1,Gerar CSV Teste
df_abt_teste = spark.table("tcc.basetest")

df_abt_teste = (df_abt_teste.
                orderBy("DATA"))

df_abt_teste.write.options(header=True, delimiter=";").csv('/FileStore/tables/tcc_csv/bt_treino.csv/abt_test.csv', header=True, sep=';')



# COMMAND ----------

display(df_abt_teste)

# COMMAND ----------


