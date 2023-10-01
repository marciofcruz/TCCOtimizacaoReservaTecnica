wd <- Sys.getenv("RWD")

if (!file.exists(wd)) {
  stop(paste("O diretorio definido na variavel de ambiente RWD nao foi encontrado",wd, sep="="))
} 

setwd(wd)

#Gerar Modelos
source("./analise qtde_faltas.R", echo=T, prompt.echo = "", spaced=F)
source("./analise qtde_saida_antecipada.R", echo=T, prompt.echo = "", spaced=F)
source("./analise qtde_suspensao.R", echo=T, prompt.echo = "", spaced=F)

source("./funcoes_externas_tcc.r", echo=T, prompt.echo = "", spaced=F)

library(ggplot2)
#install.packages("reshape2")
library(reshape2)

model <- readRDS(file = 'models/model_ts_faltas.Rds')
predicao <- predict(model, n.ahead = nrow(base_teste))
base_teste["FALTAS_PRED"] <- 0
for(i in 1:nrow(base_teste)) {                                          
  base_teste[i,]["FALTAS_PRED"] <- ceiling(predicao$pred[i])
}

rm(model)
rm(predicao)
model <- readRDS(file = 'models/model_ts_saidaantecipada.Rds')
predicao <- predict(model, n.ahead = nrow(base_teste))
base_teste["SAIDAANTECIPADA_PRED"] <- 0
for(i in 1:nrow(base_teste)) {                                          
  base_teste[i,]["SAIDAANTECIPADA_PRED"] <- ceiling(predicao$pred[i])
}

rm(model)
rm(predicao)
model <- readRDS(file = 'models/model_ts_suspensao.Rds')
predicao <- predict(model, n.ahead = nrow(base_teste))
base_teste["SUSPENSAO_PRED"] <- 0
for(i in 1:nrow(base_teste)) {                                          
  base_teste[i,]["SUSPENSAO_PRED"] <- ceiling(predicao$pred[i])
}

base_teste$TOTAL_AUSENCIAS_PRED <- base_teste$FALTAS_PRED+base_teste$SAIDAANTECIPADA_PRED+base_teste$SUSPENSAO_PRED

base_analise <- base_teste[c("DIA", "FALTAS",  "FALTAS_PRED", "SAIDAANTECIPADA", "SAIDAANTECIPADA_PRED", "SUSPENSAO", "SUSPENSAO_PRED", "TOTAL_AUSENCIAS_FUNCIONARIO",  "TOTAL_AUSENCIAS_PRED")]  

gfg_plot <- ggplot(base_analise, aes(DIA)) +  
  geom_line(aes(y = FALTAS), color = "blue") +
  geom_line(aes(y = FALTAS_PRED), color = "red")
gfg_plot
paste("MAE:", mae(base_analise$FALTAS, base_analise$FALTAS_PRED))
paste("MAPE:", mape(base_analise$FALTAS, base_analise$FALTAS_PRED))


gfg_plot <- ggplot(base_analise, aes(DIA)) +  
  geom_line(aes(y = SAIDAANTECIPADA), color = "blue") +
  geom_line(aes(y = SAIDAANTECIPADA_PRED), color = "red")
gfg_plot
paste("MAE:", mae(base_analise$SAIDAANTECIPADA, base_analise$SAIDAANTECIPADA_PRED))
paste("MAPE:", mape(base_analise$SAIDAANTECIPADA, base_analise$SAIDAANTECIPADA_PRED))


gfg_plot <- ggplot(base_analise, aes(DIA)) +  
  geom_line(aes(y = SUSPENSAO), color = "blue") +
  geom_line(aes(y = SUSPENSAO_PRED), color = "red")
gfg_plot
paste("MAE:", mae(base_analise$SUSPENSAO, base_analise$SUSPENSAO_PRED))
paste("MAPE:", mape(base_analise$SUSPENSAO, base_analise$SUSPENSAO_PRED))

gfg_plot <- ggplot(base_analise, aes(DIA)) +  
  geom_line(aes(y = TOTAL_AUSENCIAS_FUNCIONARIO), color = "blue") +
  geom_line(aes(y = TOTAL_AUSENCIAS_PRED), color = "red")
gfg_plot
paste("MAE:", mae(base_analise$TOTAL_AUSENCIAS_FUNCIONARIO, base_analise$TOTAL_AUSENCIAS_PRED))
paste("MAPE:", mape(base_analise$TOTAL_AUSENCIAS_FUNCIONARIO, base_analise$TOTAL_AUSENCIAS_PRED))

