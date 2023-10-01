# Databricks notebook source
from pyspark.sql.functions import date_format
from pyspark.sql.functions import col
from pyspark.sql.functions import ceil

df_treino = spark.table('tcc.abt_treino')

# COMMAND ----------

df_treino.printSchema()

# COMMAND ----------

# DBTITLE 1,Evolução por mês das quantidades de postos
df_auxiliar = (df_treino.
               withColumn("ano", date_format('DATA', 'yyyy')).
               withColumn("mes", date_format('DATA', 'MM')).
               withColumn("sobressalente", col("QTDE_PLANTAO")-col('QTDE_AUSENCIAS'))
              )

df_auxiliar = (df_auxiliar.
               withColumn("mes", col('mes').cast("Short")).
               withColumn("ano", col('ano').cast("Short"))
              )

df_auxiliar = (df_auxiliar.
              groupBy('ano', 'mes').
              avg('QTDE_POSTOS_TRABALHO', 'QTDE_PLANTAO', 'QTDE_AUSENCIAS', 'sobressalente').
              withColumnRenamed("avg(QTDE_POSTOS_TRABALHO)", "Media_Postos_Trabalho").
              withColumnRenamed("avg(QTDE_PLANTAO)", "Media_Quantidade_Plantonistas").
              withColumnRenamed("avg(QTDE_AUSENCIAS)", "Media_Quantidade_Ausencias").
              withColumnRenamed("avg(sobressalente)", "Media_Sobressalente").
              withColumn("Media_Postos_Trabalho", ceil('Media_Postos_Trabalho')).
              withColumn("Media_Quantidade_Plantonistas", ceil('Media_Quantidade_Plantonistas')).
              withColumn("Media_Quantidade_Ausencias", ceil('Media_Quantidade_Ausencias')).
              withColumn("Plantonistas_por_Postos", col('Media_Quantidade_Plantonistas')/col('Media_Postos_Trabalho')).
              withColumn("Media_Sobressalente", ceil('Media_Sobressalente')).
              orderBy('ano', 'mes'))

display(df_auxiliar)

# COMMAND ----------

# DBTITLE 1,Distribuição de Eventos que geram Ausência no Trabalho
df_auxiliar = (df_treino.
               groupBy().
              sum('QTDE_AUSENCIAS'))

total_ausencias = df_auxiliar.collect()[0][0]

df_auxiliar = (df_treino.
               groupBy().
               sum('QTDE_FALTAS', 'QTDE_FERIAS', 'QTDE_SAIDAANTECIPADA', 
                   'QTDE_RECICLAGEM', 'QTDE_SUSPENSAO').
               withColumn('PERCENTUAL_FALTAS', col('sum(QTDE_FALTAS)')/total_ausencias).
               withColumn('PERCENTUAL_FERIAS', col('sum(QTDE_FERIAS)')/total_ausencias).
               withColumn('PERCENTUAL_QTDE_SAIDAANTECIPADA', col('sum(QTDE_SAIDAANTECIPADA)')/total_ausencias).
               withColumn('PERCENTUAL_QTDE_RECICLAGEM', col('sum(QTDE_RECICLAGEM)')/total_ausencias).
               withColumn('PERCENTUAL_QTDE_SUSPENSAO', col('sum(QTDE_SUSPENSAO)')/total_ausencias)
              
              )

print(total_ausencias)

display(df_auxiliar)

# QTDE_LICENCA_REMUNERADA
# QTDE_LICENCA_NAO_REMUNERADA
# QTDE_PROCESSO_RETORNO_INSS

# COMMAND ----------

# DBTITLE 1,Total de Faltas por Semana
df_auxiliar = (df_treino.
               groupBy().
              sum('QTDE_FALTAS'))

total_faltas = df_auxiliar.collect()[0][0]

df_auxiliar = (df_treino.
               withColumn("dia_semana", date_format('DATA', 'E'))
              )

df_auxiliar = (df_auxiliar.
              groupBy('dia_semana').
              sum('QTDE_FALTAS').
              withColumn('PERCENTUAL_FALTAS', col('sum(QTDE_FALTAS)')/total_faltas).
              orderBy('PERCENTUAL_FALTAS', ascending=False))

display(df_auxiliar)


# COMMAND ----------

# DBTITLE 1,Faltas por Mês
df_auxiliar = (df_treino.
               groupBy().
              sum('QTDE_FALTAS'))

total_faltas = df_auxiliar.collect()[0][0]

df_auxiliar = (df_treino.
               withColumn("dia_semana", date_format('DATA', 'MM'))
              )

df_auxiliar = (df_auxiliar.
              groupBy('dia_semana').
              sum('QTDE_FALTAS').
              withColumn('PERCENTUAL_FALTAS', col('sum(QTDE_FALTAS)')/total_faltas).
              orderBy('PERCENTUAL_FALTAS', ascending=False))

display(df_auxiliar)


# COMMAND ----------

# DBTITLE 1,Saída Antecipada por Semana
df_auxiliar = (df_treino.
               groupBy().
              sum('QTDE_SAIDAANTECIPADA'))

total_faltas = df_auxiliar.collect()[0][0]

df_auxiliar = (df_treino.
               withColumn("dia_semana", date_format('DATA', 'E'))
              )

df_auxiliar = (df_auxiliar.
              groupBy('dia_semana').
              sum('QTDE_SAIDAANTECIPADA').
              withColumn('PERCENTUAL_QTDE_SAIDAANTECIPADA', col('sum(QTDE_SAIDAANTECIPADA)')/total_faltas).
              orderBy('PERCENTUAL_QTDE_SAIDAANTECIPADA', ascending=False))

display(df_auxiliar)


# COMMAND ----------

# DBTITLE 1,Saída Antecipada por Mes
df_auxiliar = (df_treino.
               groupBy().
              sum('QTDE_SAIDAANTECIPADA'))

total_faltas = df_auxiliar.collect()[0][0]

df_auxiliar = (df_treino.
               withColumn("dia_semana", date_format('DATA', 'MM'))
              )

df_auxiliar = (df_auxiliar.
              groupBy('dia_semana').
              sum('QTDE_SAIDAANTECIPADA').
              withColumn('PERCENTUAL_QTDE_SAIDAANTECIPADA', col('sum(QTDE_SAIDAANTECIPADA)')/total_faltas).
              orderBy('PERCENTUAL_QTDE_SAIDAANTECIPADA', ascending=False))

display(df_auxiliar)


# COMMAND ----------

# DBTITLE 1,Suspensões por Semana
df_auxiliar = (df_treino.
               groupBy().
              sum('QTDE_SUSPENSAO'))

total_faltas = df_auxiliar.collect()[0][0]

df_auxiliar = (df_treino.
               withColumn("dia_semana", date_format('DATA', 'E'))
              )

df_auxiliar = (df_auxiliar.
              groupBy('dia_semana').
              sum('QTDE_SUSPENSAO').
              withColumn('PERCENTUAL_QTDE_SUSPENSAO', col('sum(QTDE_SUSPENSAO)')/total_faltas).
              orderBy('PERCENTUAL_QTDE_SUSPENSAO', ascending=False))

display(df_auxiliar)


# COMMAND ----------

# DBTITLE 1,Suspensões por Mês
df_auxiliar = (df_treino.
               groupBy().
              sum('QTDE_SUSPENSAO'))

total_faltas = df_auxiliar.collect()[0][0]

df_auxiliar = (df_treino.
               withColumn("dia_semana", date_format('DATA', 'MM'))
              )

df_auxiliar = (df_auxiliar.
              groupBy('dia_semana').
              sum('QTDE_SUSPENSAO').
              withColumn('PERCENTUAL_QTDE_SUSPENSAO', col('sum(QTDE_SUSPENSAO)')/total_faltas).
              orderBy('PERCENTUAL_QTDE_SUSPENSAO', ascending=False))

display(df_auxiliar)


# COMMAND ----------

# DBTITLE 1,Distribuição de Férias por Mês
df_auxiliar = (df_treino.
               groupBy().
              sum('QTDE_FERIAS'))

total_faltas = df_auxiliar.collect()[0][0]

df_auxiliar = (df_treino.
               withColumn("dia_semana", date_format('DATA', 'MM'))
              )

df_auxiliar = (df_auxiliar.
              groupBy('dia_semana').
              sum('QTDE_FERIAS').
              withColumn('PERCENTUAL_QTDE_FERIAS', col('sum(QTDE_FERIAS)')/total_faltas).
              orderBy('PERCENTUAL_QTDE_FERIAS', ascending=False))

display(df_auxiliar)


# COMMAND ----------

# DBTITLE 1,Distribuição de Cursos de Reciclagem por Mes
df_auxiliar = (df_treino.
               groupBy().
              sum('QTDE_RECICLAGEM'))

total_faltas = df_auxiliar.collect()[0][0]

df_auxiliar = (df_treino.
               withColumn("dia_semana", date_format('DATA', 'MM'))
              )

df_auxiliar = (df_auxiliar.
              groupBy('dia_semana').
              sum('QTDE_RECICLAGEM').
              withColumn('PERCENTUAL_QTDE_RECICLAGEM', col('sum(QTDE_RECICLAGEM)')/total_faltas).
              orderBy('PERCENTUAL_QTDE_RECICLAGEM', ascending=False))

display(df_auxiliar)

