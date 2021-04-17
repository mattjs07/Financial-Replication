
PU_loader_end <- function(IDlist,  gamma = 0.61, delta = 0.69, alpha = 0.5, df, nworkers = 7){
  
  plan(multisession, gc = TRUE, workers = nworkers)
  
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

v <- function(x){  ifelse(x >=0, x^alpha, -2.5*(-x)^alpha)} #utility function

w_plus <-  function(P) P^gamma / (P^gamma + (1-P)^gamma)^(1/gamma)
w_moins <-  function(P) P^delta/ (P^delta+ (1-P)^delta)^(1/delta)

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
  z <- filter(df, PERMNO == x)
  m <- nrow(z) - 59
  g <- lapply(FUN = function(i){ d <- z[i:(59+i)] %>% arrange(RET)
  U <- PU(d)
  dd <- data.frame(PERMNO = x, date = z[(59 + i), "date"], U = U)
  return(dd)}, X = 1:m) 
  return(rbindlist(g))
}

PU_loader <- function(liste){
  q <- future_map( liste, PU_t, .progress = TRUE)
}

return(rbindlist(PU_loader(liste = IDlist)))
}
