#build an index s.t = groups of 60 obs. Then can compute PU_t by group ! using data.table 

setwd("C:/Users/matti/Desktop/M2_S2/Financial_Econ/Replication paper")

library(dplyr)
library(data.table)
library(ggplot2)
library(gridExtra)
library(beepr)

# can compute before hand the vector of probability since always the same !
df <- fread("df_bydate.csv")
df <- df %>% select(date, PERMNO, RET)


# filtering out, less than 60 month obs 
size <- count(df, PERMNO)
out <- which(size$n < 60)
out<- size$PERMNO[out]

df <- filter(df, !(PERMNO %in% out))

#### p


View(df)

sub = function(x){
  n = size[PERMNO == x] %>% as.matrix()
  m = n[2] - 59 
 ij = which(df$PERMNO == x)
 mm = 1:m
 d <- function(i) df[i:(59+i),]
 lapply(FUN = d, X = mm)
}

All_sub <- lapply(ID, sub)




PU2 <- function(x){ ~# A FINIR 
  end
  d<- arrange(x, RET)
  U <- VV(d) %*% Pi(d) %>% as.numeric 
  return( data.frame( PERMNO = d[1,"PERMNO"], date = integer(), U = numeric())      )
}


PU_t <- function(x){
  if( !(x %in% df$PERMNO )) stop("invalid identifier")
  
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

library(pbapply)

ID <- table(df$PERMNO) %>%  as.matrix() %>% rownames() %>% as.integer %>%  as.list()
system.time(All_sub <- pblapply( ID, sub))




