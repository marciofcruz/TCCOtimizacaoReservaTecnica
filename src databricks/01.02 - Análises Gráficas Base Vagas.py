# Databricks notebook source
# MAGIC %sql
# MAGIC select 'Base Completa', count(*)  instancias from tcc.base_completa
# MAGIC union
# MAGIC select 'Base com Filtros de Exclusão', count(*) instancias from tcc.base_filtro_exclusao
# MAGIC union
# MAGIC select 'Base Treino', count(*) instancias from tcc.base_treino
# MAGIC union
# MAGIC select 'Base Teste', count(*) instancias from tcc.baseteste
# MAGIC union
# MAGIC select 'Base Reserva Técnica', count(*) instancias from tcc.base_reservatecnica
# MAGIC union
# MAGIC select 'Área e Localidade', count(*) instancias from tcc.area_local
# MAGIC union
# MAGIC select 'Quantidades de Diurnos, Vigilantes em SP da Reserva', count(*) instancias from tcc.reserva_qtde_plantao
# MAGIC union
# MAGIC select 'Quantidade Salário Médio Área', count(*) instancias from tcc.area_salario_medio
# MAGIC union
# MAGIC select 'ABT Teste', count(*) instancias from tcc.abt_treino

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tcc.area_local
# MAGIC where
# MAGIC LOCALIDADE='SÃO PAULO'

# COMMAND ----------

# DBTITLE 1,Distribuição de Postos por UF
tabela_area_local = spark.table("tcc.area_local")
tabela_base_completa = spark.table("tcc.base_completa")

# tabela_area_local.printSchema()
# tabela_base_completa.printSchema()

teste = tabela_base_completa.groupby().sum('POSTOS')
teste['sum(POSTOS)'].cast('int')
total_geral = teste.collect()[0][0]

df_postos = (
    tabela_area_local.alias('local').
    join(tabela_base_completa.alias('postos'), tabela_base_completa["NOMEAREA"] == tabela_area_local["NOMEAREA"], how="inner").
    select(["local.UF",
           "postos.POSTOS"]).
    groupBy("UF").
    sum("POSTOS").
    withColumnRenamed('sum(POSTOS)', 'QTDE_POSTOS')
   )

df_soma = df_postos.groupby().sum('QTDE_POSTOS')
df_soma['sum(QTDE_POSTOS)'].cast('int')
total_geral = df_soma.collect()[0][0]


df_postos = (
    df_postos.
    withColumn('PROPORCAO', df_postos['QTDE_POSTOS']/total_geral*100)
)

df_postos = df_postos.drop('QTDE_POSTOS')
df_postos = df_postos.orderBy("PROPORCAO", ascending=False)

df_postos = (df_postos.
           withColumn('PORCENTAGEM', df_postos['PROPORCAO']/100))

display(df_postos)

# teste = df_postos.groupby().sum('TESTE')
# teste = teste.collect()[0][0]
# print(teste)

# COMMAND ----------

# DBTITLE 1,As 05 cidades do Estado de São Paulo que possuem mais postos
tabela_area_local = spark.table("tcc.area_local")
tabela_base_completa = spark.table("tcc.base_completa")

teste = tabela_base_completa.groupby().sum('POSTOS')
teste['sum(POSTOS)'].cast('int')
total_geral = teste.collect()[0][0]

df_postos = (
    tabela_area_local.alias('local').
    join(tabela_base_completa.alias('postos'), tabela_base_completa["NOMEAREA"] == tabela_area_local["NOMEAREA"], how="inner").
    where(tabela_area_local['UF']=='SP').
    select(["local.LOCALIDADE",
           "postos.POSTOS"]).
    groupBy("LOCALIDADE").
    sum("POSTOS").
    withColumnRenamed('sum(POSTOS)', 'QTDE_POSTOS')  
)

df_soma = df_postos.groupby().sum('QTDE_POSTOS')
df_soma['sum(QTDE_POSTOS)'].cast('int')
total_geral = df_soma.collect()[0][0]


df_postos = (
    df_postos.
    withColumn('PROPORCAO', df_postos['QTDE_POSTOS']/total_geral*100)
)



df_postos = df_postos.drop('QTDE_POSTOS')
df_postos = df_postos.orderBy("PROPORCAO", ascending=False).limit(5)

df_postos = (df_postos.
           withColumn('PORCENTAGEM', df_postos['PROPORCAO']/100))


display(df_postos)

# teste = df_postos.groupby().sum('TESTE')
# teste = teste.collect()[0][0]
# print(teste)

# COMMAND ----------

# DBTITLE 1,05 Cargos que estão em mais postos em São Paulo
tabela_area_local = spark.table("tcc.area_local")
tabela_base_completa = spark.table("tcc.base_completa")

teste = tabela_base_completa.groupby().sum('POSTOS')
teste['sum(POSTOS)'].cast('int')
total_geral = teste.collect()[0][0]

df_postos = (
    tabela_area_local.alias('local').
    join(tabela_base_completa.alias('postos'), tabela_base_completa["NOMEAREA"] == tabela_area_local["NOMEAREA"], how="inner").
    where(tabela_area_local['UF']=='SP').
    where(tabela_area_local['LOCALIDADE']=='SÃO PAULO').
    select(["postos.FUNCAO",
           "postos.POSTOS"]).
    groupBy("FUNCAO").
    sum("POSTOS").
    withColumnRenamed('sum(POSTOS)', 'QTDE_POSTOS')  
)

df_soma = df_postos.groupby().sum('QTDE_POSTOS')
df_soma['sum(QTDE_POSTOS)'].cast('int')
total_geral = df_soma.collect()[0][0]


df_postos = (
    df_postos.
    withColumn('PROPORCAO', df_postos['QTDE_POSTOS']/total_geral*100)
)

df_postos = df_postos.drop('QTDE_POSTOS')
df_postos = df_postos.orderBy("PROPORCAO", ascending=False).limit(5)

df_postos = (df_postos.
           withColumn('PORCENTAGEM', df_postos['PROPORCAO']/100))

display(df_postos)

# teste = df_postos.groupby().sum('TESTE')
# teste = teste.collect()[0][0]
# print(teste)

# COMMAND ----------

# DBTITLE 1,Vigilantes Noturnos e Diurnos estão na mesma proporção na Base
tabela_area_local = spark.table("tcc.area_local")
tabela_base_completa = spark.table("tcc.base_completa")

teste = tabela_base_completa.groupby().sum('POSTOS')
teste['sum(POSTOS)'].cast('int')
total_geral = teste.collect()[0][0]

df_postos = (
    tabela_area_local.alias('local').
    join(tabela_base_completa.alias('postos'), tabela_base_completa["NOMEAREA"] == tabela_area_local["NOMEAREA"], how="inner").
    where(tabela_area_local['UF']=='SP').
    where(tabela_area_local['LOCALIDADE']=='SÃO PAULO').
    where(tabela_base_completa['FUNCAO']=='VIGILANTE').
    select(["postos.TURNO",
           "postos.POSTOS"]).
    groupBy("TURNO").
    sum("POSTOS").
    withColumnRenamed('sum(POSTOS)', 'QTDE_POSTOS')  
)

df_soma = df_postos.groupby().sum('QTDE_POSTOS')
df_soma['sum(QTDE_POSTOS)'].cast('int')
total_geral = df_soma.collect()[0][0]


df_postos = (
    df_postos.
    withColumn('PROPORCAO', df_postos['QTDE_POSTOS']/total_geral*100)
)

df_postos = df_postos.drop('QTDE_POSTOS')
df_postos = df_postos.orderBy("PROPORCAO", ascending=False).limit(2)


display(df_postos)


# COMMAND ----------

# DBTITLE 1,Carregar Média de Salário de Vigilantes de São Paulo e Diurnos
df_area_saopaulo = (spark.table("tcc.area_local").
                    where("UF=='SP'").
                    where("LOCALIDADE=='SÃO PAULO'").
                    select('NOMEAREA'))

df_area_salario_medio = spark.table('tcc.area_salario_medio')

df_teste  = (df_area_saopaulo.alias('area').
          join(df_area_salario_medio.alias('salario'),
               df_area_saopaulo['NOMEAREA']==df_area_salario_medio['NOMEAREA'],
               how='inner').
          select("MEDIA_SALARIO").
          groupBy().
          avg("MEDIA_SALARIO"))

media_salario_reserva = df_teste.collect()[0][0]

folha_mensal_vigilante_atual = round(media_salario_reserva*21, 2)
folha_mensal_vigilante_sobressalente = round(media_salario_reserva*13, 2)



# COMMAND ----------

# DBTITLE 1,Conclusão da Análise
# Pelo período analisado foi verificado que é preciso haver em média 8 vigilantes plantonistas mas a reserva hoje possui em média 21 colaboradores.
# Reduzindo 13 pessoas do quadro, traria uma economia mensal de R$ 23.656,30 na folha. Ou seja, 61% sobre os R$ 38.214,04.


# COMMAND ----------


