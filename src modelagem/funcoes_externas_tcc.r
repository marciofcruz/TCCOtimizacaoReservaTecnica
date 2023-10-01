# Marcio Fernandes Cruz
# Funções Auxiliares para ser utilizado pelos scripts principais de análise
getwd()

rm(list = ls())

limite_lags <- 7

options(scipen=999)
options(width = 1024)

library(utils)
library(tseries)
library(lmtest)
library(forecast)
library(Metrics)

# Funções para uso do script
analisar_modelo <- function(nome, modelo) {
  print(nome)
  print(coeftest(modelo))
  
  #Análise de Resíduos
  par(mfrow=c(1,2))
  acf(residuals(modelo), main=paste("ACF Resíduos - ", nome))
  pacf(residuals(modelo), main=paste("PACF Resíduos - ", nome))  
  
}

executar_predicao <- function(serie_teste, modelo, tamanho) {
  predicao <- predict(modelo, n.ahead = tamanho)
  
  metricas= 
    list(sse(serie_teste[1:tamanho], predicao$pred), 
         mae(serie_teste[1:tamanho], predicao$pred), 
         mape(serie_teste[1:tamanho], predicao$pred))
  
  return(metricas) 
}


buscar_melhor_lag <- function(nome_modelo, serie_teste, modelo, lag_minima, lag_maxima, verbose) {
  
  melhor_lag <- 0
  melhor_mape <- -1
  lag <- lag_minima
  
  while (lag <= lag_maxima) {
    # Previsão com modelo Manual
    metricas <- executar_predicao(serie_teste, modelo, lag)
    metrica_mape <-as.numeric(metricas[3])
    
    if (verbose) {
      print(paste("Lag:", lag, "MAPE:", metricas[3], "MAE: ", metricas[2], "SSE:", metricas[1]))
    }
    
    if (melhor_mape==-1) {
      melhor_lag <- 1
      melhor_mape <- metrica_mape
    }
    
    if (metrica_mape != Inf) {

      if (metrica_mape < melhor_mape)  {
        melhor_lag <- lag
        melhor_mape <- metrica_mape
      }
      
    }
    
    lag <- lag + 1
  }
  
  metricas <- executar_predicao(serie_teste, modelo, melhor_lag)
  metrica_sse <-as.numeric(metricas[1])
  metrica_mae <-as.numeric(metricas[2])
  metrica_mape <-as.numeric(metricas[3])
  
  print(paste("1) Modelo: ", nome_modelo))
  print(paste("1.1) Lag Analisadas entre ", lag_minima, "e", lag_maxima))
  print(paste("1.2) Melhor lag de acordo com o MAPE:", melhor_lag))
  print(paste("1,3) Neste lag, o erro absoluto médio (MAPE) é ", metrica_mape, "%"))
  print(paste("1.4) Neste Lag, o erro médio absoluto (MAE) é de ", round(metrica_mae)))
  print(paste("1.5) Neste Lag, o soma dos erro médio absoluto (SSE) é de ", round(metrica_sse)))
  
  if (verbose) {
    predicao <- predict(modelo, n.ahead = lag_maxima)
    
    par(mfrow=c(1,1))
    ts.plot(serie_teste[1:lag_maxima], predicao$pred, lty=c(1,3), col=c(5,2))
    
  }  
}


# Carregar Base
base_treino <- read.table("dados/train.csv", header = T, sep = ",", skip = 0)
base_teste <- read.table("dados/test.csv", header = T, sep = ",", skip = 0)

#Separar elementos minimos para teste
base_teste <- base_teste[1:limite_lags,]

base_teste$DIA <- 0

for(dia in 1:nrow(base_teste)) {
  print(dia)
  base_teste[dia,]$DIA <- dia
}

