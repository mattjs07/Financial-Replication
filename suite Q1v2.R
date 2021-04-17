library(gtools) #for the function quantcut() 
library(latex2exp)
library(data.table)
library(dplyr)
library(ggplot2)
library(gridExtra)
library(future)
library(furrr)
library(beepr)

plan(multisession, gc = TRUE, workers = 8)   # Setting up for parallezlization. Using multisession with 8 threads

setwd("C:/Users/matti/Desktop/M2_S2/Financial_Econ/Replication_paper")
data <- fread("Prospect_Allv2.csv")
data <- mutate(data, cap = SHROUT * PRC, ri_rf = RET - rf) #


DATES <- table(data$date)  %>%  as.matrix() %>% rownames() %>% as.integer %>%  as.list()

Quantiles_All =  function(liste, df){
  
  Quantiles_T =  function(x, dt = df){
    d <- filter(dt, date == x)
    d <- cbind(d, deciles = as.integer(quantcut(d$U, 10)) )
    return(d)
  }
  
  d <- future_map(liste, Quantiles_T, .progress = TRUE )
  beep()
  return(rbindlist(d))
}


data <- Quantiles_All(liste = DATES, df = data)
data <- arrange(data, PERMNO, date)

data <- mutate(group_by(data,PERMNO), RET_T1 = lead(RET,1),  rf_T1 = lead(rf,1)) %>%  ungroup() %>% relocate(RET_T1, .after = RET) %>%  relocate(rf_T1, .after = rf)


stat1 <- data %>% group_by(deciles) %>% summarise( E_ret1 = mean(RET_T1, na.rm = TRUE    ), E_ri_rf = mean(ri_rf),  E_rm = mean(rm_rf))
data <- as.data.table(data)
stat2 <- data[ cap >= 1, .(E_logcap = mean(log(cap))), by = deciles] # Filter out the obs for which cap <1 to avoid -Inf
stat <- merge(stat1, stat2)

param = lapply( FUN = function(x){ z <- lm(data = subset(data, deciles == x) , ri_rf ~ rm_rf)%>% summary(); return(data.frame(alpha =  z$coefficients[1,1], betas = z$coefficients[2,1]))}, X = 1:10)
param = param %>%  rbindlist()
dd <- cbind(stat, param)

p_b <- ggplot(data = cbind(dd, x = 1:10), aes(y = betas, x= x)) + geom_point() + geom_line(aes(color = "red"), show.legend = FALSE) + 
  ylim(0,1.5) + scale_x_discrete(name ="Deciles",limits= factor(1:10)) + labs(title = TeX("CAPM $\\beta_{i} $ per deciles ")) +
  theme( plot.title = element_text(hjust = 0.5))


p_a <- ggplot(data = cbind(dd, x = 1:10), aes(y = alpha, x= x)) + geom_point() + geom_line(aes(color = "red"), show.legend = FALSE) + 
  ylim(-0.05, 0.05) + scale_x_discrete(name ="Deciles",limits= factor(1:10)) + labs(title = TeX("CAPM $\\alpha_{i} $ per deciles ")) +
  theme( plot.title = element_text(hjust = 0.5))

grid.arrange(tableGrob(dd), p_b, p_a, nrow = 2, layout_matrix = rbind(c(1,1), c(2,3)))


### QUestion 2 #### 

# Long P1, Short P10 

strategy <- function(df){
  
  
  df <- mutate(df, ri_rf = RET - rf)
  
  DATES <- table(df$date)  %>%  as.matrix() %>% rownames() %>% as.integer %>%  as.list()
  
  df <- Quantiles_All(liste = DATES, df = df)
  df <- arrange(df, PERMNO, date)
  
  df <- mutate(group_by(df,PERMNO), RET_T1 = lead(RET,1),  rf_T1 = lead(rf,1)) %>%  ungroup() %>% relocate(RET_T1, .after = RET) %>%  relocate(rf_T1, .after = rf)
  
  D1_10 <- filter(df, deciles == 1 | deciles == 10)
  D1_10 <- mutate(df, Long_Short = ifelse(deciles == 1, RET_T1 - rf_T1, - rf_T1 - RET_T1 ))
  
  sharpe_ratio <- data.frame( sharpe_ratio = mean(D1_10$rm_rf)/ sqrt(var(D1_10$rm_rf)))
  excess_ret <- data.frame( excess_ret = mean(D1_10$Long_Short, na.rm = TRUE))
  alpha = lm(data = D1_10 , Long_Short ~ rm_rf) %>% summary()
  alpha <- data.frame( alpha = alpha$coefficients[1,1])
  
  return(cbind(sharpe_ratio, excess_ret, alpha))
}

data <- fread("Prospect_Allv2.csv")

strat <- strategy(data)
grid.arrange(tableGrob(strat))

##### QUestion 3 ####


######

source("C:/Users/matti/Desktop/M2_S2/Financial_Econ/Replication_paper/PU_loader_modified.R")
df <- fread("df_bydate.csv")
Stocks_11 <- PU_loader_end(ID, gamma = 1, delta = 1, df = df) #start time = 18h20


data  <-  merge.data.table(df, Stocks_11, all.x = FALSE, all.y = TRUE)
data <- arrange(data, PERMNO, date)

fwrite(data, "Prospect_All_11.csv")
#######

data11 <- fread("Prospect_All_11.csv")

strat_11 <- strategy(data11)
grid.arrange(tableGrob(strat_11))

#### Question 4 #####

#####
Stocks_alpha <- PU_loader_end(ID, alpha = 1, df = df, nworkers = 8) 
data  <-  merge.data.table(df, Stocks_alpha, all.x = FALSE, all.y = TRUE)
data <- arrange(data, PERMNO, date)

fwrite(data, "Prospect_All_alpha.csv")
#####

data_a1 <- fread("Prospect_All_alpha.csv")

strat_a1 <- strategy(data_a1)
grid.arrange(tableGrob(strat_a1))



STRATEGIES <- rbind(strat, strat_11, strat_a1)

rownames( STRATEGIES) <-  c("Q2", "Q3", "Q4")










