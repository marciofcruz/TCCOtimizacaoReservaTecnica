wd <- Sys.getenv("RWD")

if (!file.exists(wd)) {
  stop(paste("O diretorio definido na variavel de ambiente RWD nao foi encontrado",wd, sep="="))
} 

setwd(wd)

source("./funcoes_externas_tcc.r", echo=T, prompt.echo = "", spaced=F)


serie_treino <- base_treino$QTDE_SAIDAANTECIPADA
serie_teste <- base_teste$SAIDAANTECIPADA

serie_treino <- tail(serie_treino, 90)

media_serie <- ceiling(mean(serie_treino))

paste('Média Diária:', mean(serie_treino))

#Análise Exploratória da Série
summary(serie_treino)

# Gráfico da serie
par(mfrow=c(1,1))
ts.plot(serie_treino)

# Teste de Estacionariedade
adf.test(serie_treino)

#Identificação
#Gráfico de Autocorrelação e Autocorrelação Parcial da série
par(mfrow=c(1,2))
acf(serie_treino, main="ACF")
pacf(serie_treino,main="PACF")

auxiliar <- rep("0", 17)
toString(auxiliar)

# Referência do Automático pela Biblioteca
# rm(modelo_auto)
modelo_auto <- auto.arima(serie_treino)
modelo_auto
analisar_modelo('Auto', modelo_auto)
buscar_melhor_lag('Auto',  serie_teste, modelo_auto, 1, nrow(base_teste), T)


# Ensaio 1
# rm(modelo_manual)
modelo_manual <- arima(serie_treino, order = c(1,0,0), 
                       fixed = c(NA,
                                 NA), method = c("ML"))
analisar_modelo('AR(1)', modelo_manual)
buscar_melhor_lag('AR(1)',  serie_teste, modelo_manual, 1, nrow(base_teste), T)

# Ensaio 1
# rm(modelo_manual)
modelo_manual <- arima(serie_treino, order = c(17,0,0), 
                       fixed = c(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, NA,
                                 NA), method = c("ML"))
analisar_modelo('AR(0,17)', modelo_manual)
buscar_melhor_lag('AR(0,17)',  serie_teste, modelo_manual, 1, nrow(base_teste), T)


predicao <- predict(modelo_manual, n.ahead = nrow(base_teste))

base_teste["SAIDAANTECIPADA_PRED"] <- 0
for(i in 1:nrow(base_teste)) {                                          
  print(base_teste[i,]$SAIDAANTECIPADA)
  base_teste[i,]["SAIDAANTECIPADA_PRED"] <- ceiling(predicao$pred[i])
}

saveRDS(modelo_manual, file = 'models/model_ts_saidaantecipada.Rds')


base_teste["SAIDAANTECIPADA_MEDIA"] <- media_serie

saveRDS(modelo_manual, file = 'models/model_ts_saidaantecipada.Rds')

paste('Predição Modelo: MAPE: ',mape(base_teste$SAIDAANTECIPADA, base_teste$SAIDAANTECIPADA_PRED), "MAE:", mae(base_teste$SAIDAANTECIPADA, base_teste$SAIDAANTECIPADA_PRED))
paste('Compararação com Média: MAPE: ',mape(base_teste$SAIDAANTECIPADA, base_teste$SAIDAANTECIPADA_MEDIA), "MAE:", mae(base_teste$SAIDAANTECIPADA, base_teste$SAIDAANTECIPADA_MEDIA))
