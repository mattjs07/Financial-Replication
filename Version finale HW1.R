library(gtools) #for the function quantcut()
library(latex2exp)
library(data.table)
library(dplyr)
library(ggplot2)
library(gridExtra)
library(future) # for parallelization
library(furrr)
library(beepr)
library(gtable)


setwd("C:/Users/matti/Desktop/M2_S2/Financial_Econ/Replication_paper")
plan(multisession, gc = TRUE, workers = 8)   # Setting up for parallezlization. Using multisession with 8 threads

###############################
#### Building the dataset #####

returns <-fread("eb889d9be7128497.csv")
excess <- fread("rm_rf.CSV")

returns$date <- substr(returns$date, 0, 6) #matching formats, keep only the first 6 digit
returns$date <- as.integer(returns$date) # So that types match
excess <- rename(excess, date = V1) # so that name match

df <- merge.data.table(returns, excess, by = c("date"), all.x = TRUE, all.y = FALSE)

df <- df %>%mutate(PRC = abs(PRC)) #taking absolute value 
df <- df %>%  arrange(PERMNO, date) #arrange in ascending order by PERMNO and date
df$RET <- as.numeric(df$RET)
table(is.na(df$RET)) # Some NAs introduced because there was "", "B", "C"

df <- df %>% filter(!is.na(df$RET)) # We filter the RET that are NAs as we consider that they are "not observed". Thereby to compute the Prospective utilities,
                                    # the individual will not take them into account

fwrite(df, "df_bydate.csv") #the one used to compute Prospective utilities


###########################

df <- fread("df_bydate.csv")

# filtering out, less than 60 month obs 
size <- count(df, PERMNO) #number of observations per PERMNO
out <- which(size$n < 60) # indices of PERMNOs with number of obs < 60
out<- size$PERMNO[out] # PERMNOs with number of obs < 60

df <- filter(df, !(PERMNO %in% out)) # filtering out <60 obs PERMNO

#### COMPUTING PROSPECTIVE UTILITIES ### 

# I give a PERMNO to this function, it then compute all the 60 observations subset on which to compute PU. Returns a dataframe with Prospective utilities.
# Allows to choose the values of the parameters and the number of threads used in parallelization.
# With parallelization, running this function takes around 1h40 on my laptop.
PU_loader_end <- function(IDlist,  gamma = 0.61, delta = 0.69, alpha = 0.5, df, nworkers = 8){
  
  plan(multisession, gc = TRUE, workers = nworkers)
  
  get_p <- function(x){   # I gather the basic infos (would be simplified but this way could be applied to sets > or < than 60 obs)
    if( !is.data.frame(x)) stop("x must be a data frame")
    n <- nrow(x)
    t <- table(x$RET < 0)
    m <- t["TRUE"] %>% as.integer
    if( is.na(m)) m = 0
    p <- 1/n
    Dict <- list(n,m,p)
    names(Dict) <- c("n","m","p")    # Returns number of rows (in our case 60) the number of obs with RET <0 and the objective probability
    return(Dict)
  }
  
  v <- function(x){  ifelse(x >=0, x^alpha, -2.5*(-x)^alpha)} #utility function
  
  w_plus <-  function(P) P^gamma / (P^gamma + (1-P)^gamma)^(1/gamma)
  w_moins <-  function(P) P^delta/ (P^delta+ (1-P)^delta)^(1/delta)
  
  Pi <- function(x){                                         # This functions compute Pi (the perceived probability)
    if( !is.data.frame(x)) stop("x must be a data frame")    # It returns a vector of probability
    d <- get_p(x)
    n <-  as.integer(d["n"])
    m <- as.integer(d["m"])
    p <- as.numeric(d["p"])
    d <- lapply( X = 1:n, FUN = function(i){
      if(i == n) w_plus(p)
      else if(i == 1) w_moins(p)
      else if(i > m)  w_plus( (n - i +1) * p) - w_plus( (n - i) * p) 
      else w_moins( i * p) - w_moins( (i - 1) * p)  }  )
    return(unlist(d))
  }
  
  VV <- function(x){                                        # Compute a vector with utilities
    if( !is.data.frame(x)) stop("x must be a data frame")
    d <- x %>% select(RET) %>% mutate(RET = v(RET))
    return(as.vector(t(d)))
  }
  
  PU <- function(x){                                        # Compute Prospective utility by linear algebra (for one PERMNO at one date)
    if( !is.data.frame(x)) stop("x must be a data frame")
    VV(x) %*% Pi(x) %>% as.numeric 
  }
  
  PU_t <- function(x){                                      # COmpute PU for a PERMNO at every date. 
    if( !(x %in% df$PERMNO )) stop("invalid identifier")    #It returns a dataframe with the additional variable of Prospective utility "U"
    z <- filter(df, PERMNO == x)
    m <- nrow(z) - 59
    g <- lapply(FUN = function(i){ d <- z[i:(59+i)] %>% arrange(RET)
    U <- PU(d)
    dd <- data.frame(z[60:nrow(z)], U = U)
    return(dd)}, X = 1:m) 
    return(rbindlist(g))
  }
  
  PU_loader <- function(liste){
    q <- future_map( liste, PU_t, .progress = TRUE)   # This loops PU_t over every PERMNO. Using parallelization
  }
  
  return(rbindlist(PU_loader(liste = IDlist)))        # This binds all the computed dataframes together, to make the final dataframe.
}

ID <- as.list(unique(data$PERMNO))  # All the PERMNOs

### PU for Q1 and 2 ###

Stocks <- PU_loader(ID, df)
fwrite(Stocks, "Prospect_Allv2.csv")

### PU for Q3 ###

Stocks_11 <- PU_loader_end(ID, gamma = 1, delta = 1, df=df)
fwrite(data, "Prospect_All_11.csv")

### PU for Q4 ###

Stocks_a1 <- PU_loader_end(ID, alpha = 1, df=df)
fwrite(Stocks_a1, "Prospect_All_alpha.csv")

######################################


#####################################
########### QUESTION 1 ##############
#####################################


data <- fread("Prospect_Allv2.csv")   ## The dataset with the prospective utilities for Question 1 
data <- mutate(data, cap = SHROUT * PRC, ri_rf = RET - rf) #Adding a column for excess returns called ri_rf


DATES <- as.list(unique(data$date)) #list with all DATES, needed for following functions

Quantiles_All =  function(liste, df){     #This function assign PERMNO to a decile for Every t 
  
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

data <- mutate(group_by(data,PERMNO), RET_T1 = lead(RET,1),  rf_T1 = lead(rf,1), ri_rf_T1 = lead(ri_rf,1)) %>%  ungroup() %>% relocate(RET_T1, .after = RET) %>%  relocate(rf_T1, .after = rf)
#New columns with returns at t+1 and risk free at t+1.

stat1 <- data %>% group_by(deciles) %>% summarise( E_ret1 = mean(RET_T1, na.rm = TRUE    ), E_ri_rf = mean(ri_rf),  E_rm = mean(rm_rf))
data <- as.data.table(data)
stat2 <- data[ cap >= 1, .(E_logcap = mean(log(cap))), by = deciles] # Filter out the obs for which cap <1 to avoid -Inf
stat <- merge(stat1, stat2)

#compute CAPM for each decile
param = lapply( FUN = function(x){ z <- lm(data = subset(data, deciles == x) , ri_rf ~ rm_rf)%>% summary(); return(data.frame(alpha =  z$coefficients[1,1], betas = z$coefficients[2,1]))}, X = 1:10)
param = param %>%  rbindlist()
dd <- cbind(stat, param)

#some visual supports
p_b <- ggplot(data = cbind(dd, x = 1:10), aes(y = betas, x= x)) + geom_point() + geom_line(aes(color = "red"), show.legend = FALSE) + 
  ylim(0,1.5) + scale_x_discrete(name ="Deciles",limits= factor(1:10)) + labs(title = TeX("CAPM $\\beta_{i} $ per deciles ")) +
  theme( plot.title = element_text(hjust = 0.5))

p_a <- ggplot(data = cbind(dd, x = 1:10), aes(y = alpha, x= x)) + geom_point() + geom_line(aes(color = "red"), show.legend = FALSE) + 
  ylim(-0.05, 0.05) + scale_x_discrete(name ="Deciles",limits= factor(1:10)) + labs(title = TeX("CAPM $\\alpha_{i} $ per deciles ")) +
  theme( plot.title = element_text(hjust = 0.5))

grid.arrange(tableGrob(dd), p_b, p_a, nrow = 2, layout_matrix = rbind(c(1,1), c(2,3)))

### Prospective utility evolution by deciles 

graph <- data[, .(mean_U = mean(U), mean_RET_t1 = mean(RET_T1, na.rm = TRUE)), by = .(deciles, date)]
graph$deciles <- as.factor(graph$deciles)

cc <- scales::seq_gradient_pal("blue", "yellow", "Lab")(seq(0,1,length.out = 10))
cc[1] <- "cyan1"
cc[10] <- "red"

Av_U <- ggplot(graph, aes(x = date, y = mean_U, color = deciles)) + geom_smooth(size = 1, se = F) + geom_line() + 
  scale_colour_manual(values=cc) + labs(x = "date", y = "Average Prospective Utility", title = "Evolution of the Average Prospective Utility by deciles") + 
  theme(plot.title = element_text(hjust = 0.5))  + guides(colour = guide_legend(reverse=T))

Av_RET_t1 <- ggplot(graph, aes(x = date, y = mean_RET_t1, color = deciles)) + geom_smooth(size = 0.75, se = F) + 
  scale_colour_manual(values=cc) + labs(x = "date", y = "Average Returns at t+1", title = "Average Returns at t+1 by deciles") + 
  theme(plot.title = element_text(hjust = 0.5))  + guides(colour = guide_legend(reverse=T))

grid.arrange(Av_U, Av_RET_t1, nrow = 2)


ret_by_date <- data %>% mutate(year = substr(date, 0, 4) )
  
  
ret_by_date <- ret_by_date[, .(av_ret = mean(RET), var_ret = var(RET), n = .N), by = year]


ret_plot <- ggplot(ret_by_date) + geom_col(aes(x = year, y = av_ret)) +
  theme(axis.text.x = element_text(angle=90)) + labs(y = "Average returns", title = "Average returns per year") +
  theme(plot.title = element_text(hjust = 0.5)) +   theme(axis.text.x = element_blank(), 
                                                          axis.ticks.x = element_blank(), 
                                                          axis.title.x = element_blank())

add_plot <-  ggplot(ret_by_date) + geom_col(aes(x = year,y = var_ret), fill = "lightblue") + 
  geom_col(aes(x = year,y = n/nrow(data)), fill = "red", alpha = 0.2) +
  theme(axis.text.x = element_text(angle=90)) + labs(y = "Variance and share of observations") +
  theme(legend.background = element_rect("grey"))  + 
  labs(caption = "In the bottom graph the variance of returns in each year is displayed in blue. The share of observations for a given year is displayed in red (as might explain size of variance)") + 
  theme(plot.caption = element_text(hjust = 0))


ggplot(ret_by_date) + geom_smooth( aes(x = n, y = var_ret))
lm( data  = ret_by_date, var_ret ~ n ) %>%  summary()


g2 <- ggplotGrob(ret_plot)
g3 <- ggplotGrob(add_plot)
g <- rbind(g2, g3, size = "first")
grid.arrange(g)

descriptive <- data[, .(mean_U = mean(U), mean_RET = mean(RET, na.rm = T), mean_RET_t1 = mean(RET_T1, na.rm = TRUE),
                        mean_ri_rf = mean(ri_rf, na.rm = T),  mean_ri_rf_t1 = mean(ri_rf_T1, na.rm = T), mean_PRC = mean(PRC, na.rm = T)), by = .(deciles)]

tableGrob(descriptive) %>% grid.arrange()

#####################################
########### QUESTION 2 ##############
#####################################

# Long P1, Short P10 
strategy <- function(df){   # A function that returns a dataframe with sharpe ratio, excess returns and CAPM alpha for a given dataset
                                                    #Allows to code only once and reuse the function in Q2 / Q3 / Q4 
  df <- mutate(df, ri_rf = RET - rf)
  
  DATES <- as.list(unique(data$date))
  
  df <- Quantiles_All(liste = DATES, df = df)
  df <- arrange(df, PERMNO, date)
  
  df <- mutate(group_by(df,PERMNO), RET_T1 = lead(RET,1),  rf_T1 = lead(rf,1), rm_rf_T1 = lead(rm_rf, 1)) %>%  ungroup() %>% relocate(RET_T1, .after = RET) %>%  relocate(rf_T1, .after = rf) %>% relocate(rm_rf_T1, .after = rm_rf) 
  
  D1_10 <- filter(df, deciles == 1 | deciles == 10)       #Keep observations in 1st or 10th decile
  D1_10 <- mutate(df, Long_Short = ifelse(deciles == 1, RET_T1 - rf_T1, rf_T1 - RET_T1 ))  #The returns from Long/Short strategy
  
  sharpe_ratio <- data.frame( sharpe_ratio = mean(D1_10$Long_Short, na.rm = TRUE)/ sqrt(var(D1_10$Long_Short, na.rm = TRUE)))
  excess_ret <- data.frame( excess_ret = mean(D1_10$Long_Short, na.rm = TRUE)) #Excess returns computed from Long-short strategy's returns
  alpha = lm(data = D1_10 , Long_Short ~ rm_rf_T1) %>% summary()
  alpha <- data.frame( alpha = alpha$coefficients[1,1])
  
  return(cbind(sharpe_ratio, excess_ret, alpha))
}

data <- fread("Prospect_Allv2.csv") #Takes the "raw" version of the dataframe since everything is done INSIDE the function (quantiles)

strat <- strategy(data)
grid.arrange(tableGrob(strat))

##### QUestion 3 ####

data11 <- fread("Prospect_All_11.csv")

strat_11 <- strategy(data11)
grid.arrange(tableGrob(strat_11))

#### Question 4 #####

data_a1 <- fread("Prospect_All_alpha.csv")

strat_a1 <- strategy(data_a1)
grid.arrange(tableGrob(strat_a1))

#Finally binding the results into one dataframe to compare !! 
STRATEGIES <- rbind(strat, strat_11, strat_a1)
rownames(STRATEGIES) <-  c("Q2", "Q3", "Q4")
grid.arrange(tableGrob(STRATEGIES))
