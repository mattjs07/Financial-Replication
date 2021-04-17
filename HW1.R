setwd("C:/Users/matti/Desktop/M2_S2/Financial_Econ/Replication paper")

library(dplyr)
library(data.table)
library(ggplot2)
library(gridExtra)
library(beepr)

###############################
#### Building the dataset #####

returns <-fread("eb889d9be7128497.csv")
excess <- fread("rm_rf.CSV")

returns$date <- substr(returns$date, 0, 6) #matching formats, keep only the first 6 digit
returns$date <- as.integer(returns$date) # So that types match
excess <- dplyr::rename(excess, date = V1) # so that name match

df <- merge.data.table(returns, excess, by = c("date"), all.x = TRUE, all.y = FALSE)

df <- df %>%mutate(PRC = abs(PRC)) #taking absolute value 

nrow(df) == nrow(returns)

df$RET <- as.numeric(df$RET)
table(is.na(df$RET)) # Because there was "", "B", "C" 
df <- df %>% filter(!is.na(df$RET))
df <- df %>%  arrange(PERMNO, date)

data.table::fwrite(df, "df_bydate.csv")
############################

df <- fread("df_bydate.csv")

df <- df %>% select(date, PERMNO, RET)

# filtering out, less than 60 month obs 
size <- count(df, PERMNO)
out <- which(size$n < 60)
out<- size$PERMNO[out]

df <- filter(df, !(PERMNO %in% out))


n <- dplyr::count(df, PERMNO)
length(n$n)




#Object ID to extract the names of every PERMNO --> used latter to loop over
ID <- table(df$PERMNO) %>%  as.matrix() %>% rownames() %>% as.integer %>%  as.list()


get_p <- function(x){ 
  if( !(x %in% df$PERMNO )) stop("invalid identifier")
  idx <- which(df$PERMNO == x )
  z <- filter(df, PERMNO == x)
  n <- nrow(z)
  t <- table(z$RET < 0)
  m <- t["TRUE"] %>% as.integer
  if( is.na(m)) m = 0
  p <- 1/n
  Dict <- list(x,idx, n,m,p)
  names(Dict) <- c("x","idx in df", "n","m","p")
  return(Dict)
}

v <- function(x){  ifelse(x >=0, x^0.5, -2.5*(-x)^0.5)} #utility function
g <- ggplot(data = data.frame(x = 0), mapping = aes(x = x)); g + stat_function(fun = v) + xlim(-1,1)  #representation of v(x)


w_plus <-  function(P) P^0.61 / (P^0.61 + (1-P)^0.61)^(1/0.61)
w_moins <-  function(P) P^0.69 / (P^0.69 + (1-P)^0.69)^(1/0.69)

##### visualizing w as gamma changes #####
w <- function(a, P){
  data.frame(P =P , value = P^a / (P^a + (1-P)^a)^(1/a))
}

library(plyr)
params <- data.frame(a = c(0.1,0.61,0.69))
all <- mdply(params, w, P = seq(0, 1, 0.1))

detach("package:plyr", unload=TRUE)
ggplot(all, aes(P, value, colour=factor(a)))+ geom_line( size = 1) +  facet_wrap(~a,scales="free", ncol=1) + 
  guides(colour =guide_legend(title="gamma")) + labs( title = "weighting function w(P)") + theme(plot.title = element_text(hjust = 0.5)) 
#########################################


Pi <- function(x){
  if( !(x %in% df$PERMNO )) stop("invalid identifier")
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
  if( !(x %in% df$PERMNO )) stop("invalid identifier")
  d <- filter(df, PERMNO == x) %>% select(RET) %>% mutate(RET = v(RET))
  return(as.vector(t(d)))
}


PU <- function(x){
  if( !(x %in% df$PERMNO )) stop("invalid identifier")
  VV(x) %*% Pi(x) %>% as.numeric 
}


Stocks <- lapply(FUN = PU, X= ID)

Stocks2 <- as.numeric(Stocks) %>%  data.frame()
Stocks2 <- rename(Stocks2 , PU = .)
Stocks2$ID <- ID
Stocks2 <- arrange(Stocks2, PU)

q <- quantile(Stocks2$PU, c(10,90)/100)

Decils <- function(x)
  q <- quantile(x$PU, seq(10,90,10)/100)
  D <- filter
  filter(Stocks2, PU < q[1])




#used to debug Pi 
for(x in 1:length(ID)){
  z <- Pi(ID[x])
  print(list(x, class(z)))
  if(class(z))
}



