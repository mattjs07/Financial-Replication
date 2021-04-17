setwd("C:/Users/matti/Desktop/M2_S2/Financial_Econ/Replication paper")

library(dplyr)
library(data.table)
library(ggplot2)
library(gridExtra)
library(beepr)

df <- fread("df_bydate.csv")
df <- df %>% select(date, PERMNO, RET)

# filtering out, less than 60 month obs 
size <- count(df, PERMNO)
out <- which(size$n < 60)
out<- size$PERMNO[out]

df <- filter(df, !(PERMNO %in% out))

########################################
######## COMPUTING PU #################
########################################

get_p <- function(x){ 
  if( !is.data.frame(x)) stop("x must be a data frame")
  n <- nrow(x)
  t <- table(x$RET < 0)
  m <- t["TRUE"] %>% as.integer
  if( is.na(m)) m = 0
  p <- 1/n
  Dict <- list(n,m,p)
  names(Dict) <- c("n","m","p")
  return(Dict)
}

v <- function(x){  ifelse(x >=0, x^0.5, -2.5*(-x)^0.5)} #utility function


w_plus <-  function(P) P^0.61 / (P^0.61 + (1-P)^0.61)^(1/0.61)
w_moins <-  function(P) P^0.69 / (P^0.69 + (1-P)^0.69)^(1/0.69)


Pi <- function(x){
  if( !is.data.frame(x)) stop("x must be a data frame")
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

VV <- function(x){
  if( !is.data.frame(x)) stop("x must be a data frame")
  d <- x %>% select(RET) %>% mutate(RET = v(RET))
  return(as.vector(t(d)))
}


PU <- function(x){
  if( !is.data.frame(x)) stop("x must be a data frame")
  VV(x) %*% Pi(x) %>% as.numeric 
}

PU_t <- function(x){
  if( !(x %in% df$PERMNO )) stop("invalid identifier")
  end <- data.frame( PERMNO = integer(), date = integer(), U = numeric())
  z <- filter(df, PERMNO == x)
  m <- nrow(z) - 59
  for(i in 1:m){ 
    d <- z[i:(59+1)] %>% arrange(RET)
    U <- PU(d)
    dd <- data.frame(PERMNO = x, date = z[(59 + i), "date"], U = U)
    end <- rbind(end, dd)
  }
  return(end)
}

PU_t2 <- function(x){
  if( !(x %in% df$PERMNO )) stop("invalid identifier")
  z <- filter(df, PERMNO == x)
  m <- nrow(z) - 59
  g <- lapply(FUN = function(i){ d <- z[i:(59+1)] %>% arrange(RET)
  U <- PU(d)
  dd <- data.frame(PERMNO = x, date = z[(59 + i), "date"], U = U)
  return(dd)}, X = 1:m) 
  return(bind_rows(g, .id = "column_label"))
}

PU_t3 <- function(x){
  if( !(x %in% df$PERMNO )) stop("invalid identifier")
  z <- filter(df, PERMNO == x)
  m <- nrow(z) - 59
  g <- lapply(FUN = function(i){ d <- z[i:(59+1)] %>% arrange(RET)
  U <- PU(d)
  dd <- data.frame(PERMNO = x, date = z[(59 + i), "date"], U = U)
  return(dd)}, X = 1:m) 
  return(rbindlist(g))
}


PU_loader <- function(start,end){
  pb <- txtProgressBar(min = 0, max = end-start + 1, style = 3)
  q <- lapply( X = start:end, FUN = function(x){Sys.sleep(0.1)
    z = ID[[x]]
    PU_t3(z)
    setTxtProgressBar(pb, x)
  }); beep()
  
}

PU_loader2 <- function(liste){
  pb <- txtProgressBar(min = 0, max = length(liste), style = 3)
  q <- lapply( X = 1:length(liste), FUN = function(x){Sys.sleep(0.1)
    z = liste[[x]]
    setTxtProgressBar(pb, x) 
    return(PU_t3(z))
  })
}

library(future)
plan(multisession)

ID <- table(df$PERMNO) %>%  as.matrix() %>% rownames() %>% as.integer %>%  as.list()
Stocks <- PU_loader2(ID) #started 10h47
Stocks <- rbindlist(Stocks)





