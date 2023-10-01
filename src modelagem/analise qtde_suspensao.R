wd <- Sys.getenv("RWD")

if (!file.exists(wd)) {
  stop(paste("O diretorio definido na variavel de ambiente RWD nao foi encontrado",wd, sep="="))
} 

setwd(wd)

source("./funcoes_externas_tcc.r", echo=T, prompt.echo = "", spaced=F)


serie_treino <- base_treino$QTDE_SUSPENSAO
serie_teste <- base_teste$SUSPENSAO

serie_treino <- tail(serie_treino, 60)

media_serie <- ceiling(mean(serie_treino))
paste('Média Diária:', media_serie)

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

auxiliar <- rep("0", 4)
toString(auxiliar)

# Referência do Automático pela Biblioteca
# rm(modelo_auto)
modelo_auto <- auto.arima(serie_treino)
modelo_auto
analisar_modelo('Auto', modelo_auto)
buscar_melhor_lag('Auto',  serie_teste, modelo_auto, 1, nrow(base_teste), T)

# Ensaio 1
# rm(modelo_manual)
modelo_manual <- arima(serie_treino, order = c(8,0,0), 
                       fixed = c(0, 0, 0, 0, 0, 0, 0, NA,
                                 NA), method = c("ML"))
analisar_modelo('AR(8)', modelo_manual)
buscar_melhor_lag('AR(8)',  serie_teste, modelo_manual, 1, nrow(base_teste), T)

# Ensaio 1
# rm(modelo_manual)
modelo_manual <- arima(serie_treino, order = c(1,0,0), 
                       fixed = c(NA,
                                 NA), method = c("ML"))
analisar_modelo('AR(1)', modelo_manual)
buscar_melhor_lag('AR(1)',  serie_teste, modelo_manual, 1, nrow(base_teste), T)


predicao <- predict(modelo_manual, n.ahead = nrow(base_teste))

base_teste["SUSPENSAO_PRED"] <- 0
for(i in 1:nrow(base_teste)) {                                          
  base_teste[i,]["SUSPENSAO_PRED"] <- ceiling(predicao$pred[i])
}

saveRDS(modelo_manual, file = 'models/model_ts_suspensao.Rds')

base_teste["SUSPENSAO_MEDIA"] <- media_serie
paste('Predição Modelo: MAPE: ',mape(base_teste$SUSPENSAO, base_teste$SUSPENSAO_PRED), "MAE:", mae(base_teste$SUSPENSAO, base_teste$SUSPENSAO_PRED))
paste('Compararação com Média: MAPE: ',mape(base_teste$SUSPENSAO, base_teste$SUSPENSAO_MEDIA), "MAE:", mae(base_teste$SUSPENSAO, base_teste$SUSPENSAO_MEDIA))


