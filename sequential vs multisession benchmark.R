library(microbenchmark)
library(future.apply)
library(future)
library(gridExtra)


plan(sequential)
bench <- microbenchmark(lapply(ID[1:3], PU_t), future_lapply(ID[1:3], PU_t), times = 100 )
plan(multisession)
bench2 <- microbenchmark(lapply(ID[1:3], PU_t), future_lapply(ID[1:3], PU_t), times = 100 )

p1 <- autoplot(bench)   + ggtitle("With plan(sequential)") + theme(plot.title = element_text(hjust = 0.5)) + ylim(3,9)
p2 <- autoplot(bench2) + ggtitle("With plan(multisession)") + theme(plot.title = element_text(hjust = 0.5))  + ylim(3,9)

grid.arrange(p1, p2, top = "Benchmark with 100 iterations ") 

bench;bench2
 