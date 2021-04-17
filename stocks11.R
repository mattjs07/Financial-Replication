Stocks_11 <- PU_loader_end(ID, gamma = 1, delta = 1)
#started at 15h47


df <- fread("df_bydate.csv")
data  <-  merge.data.table(df, Stocks11, all.x = FALSE, all.y = TRUE)
data <- arrange(data, PERMNO, date)

fwrite(data, "Prospect_All_11.csv")