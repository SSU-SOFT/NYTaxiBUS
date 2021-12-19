library(rhdfs)
hdfs.init()
library(rmr2)

rm(list=ls())
# local 가져오기
#rmr.options(backend = "local")
#files<-c("./data/taxi/info.csv", "./data/taxi/sample_combined1.csv"); files
# Hadoop 가져오기
rmr.options(backend = "hadoop")
files <- hdfs.ls("/data/taxi/combined")$file; files
# info.csv에 포함된 변수 이름과 클래스 정보 읽기
mr <- mapreduce(input = files[1], 
                input.format = make.input.format(
                  format = "csv", sep=",", stringsAsFactors=F)
)

res <- from.dfs(mr) 
ress <- values(res)
colnames.tmp <- as.character(ress[,1]); colnames.tmp
class.tmp <- as.character(ress[,2]); class.tmp
# 변수 이름
colnames <- colnames.tmp[-1]; colnames
# 변수 클래스 
class <- class.tmp[-1]; class 
class[c(6,8,9,10)] <- "numeric"

cbind(colnames, class)

# 자료의 input format 지정
input.format <- make.input.format( 
  format = "csv", sep = ",", 
  stringsAsFactors = F, 
  col.names = colnames, colClasses = class)
files <- files[-1]
files

########################## 전처리 ################################
# 파일 하나만 입력 자료로 사용
#mr <- mapreduce( input = files[1], input.format = input.format)
mr <- mapreduce( input = files, input.format = input.format)
res.data <- from.dfs(mr);
taxi <- res.data$val
taxi <- taxi[!(taxi$fare_amount < 2.5 | taxi$total_amount < 2.5),] # 요금이 2.5미만인 경우 제거
taxi <- taxi[!(taxi$passenger_count == 0 | taxi$passenger_count > 6),] # 승객수 0명, 6명 초과 제거
# 뉴욕 구간이 아닌 것을 제거
taxi <- subset(taxi, pickup_longitude > -75 & pickup_longitude < -73 & pickup_latitude > 40 & pickup_latitude < 42)
taxi <- subset(taxi, dropoff_longitude > -75 & dropoff_longitude < -73 & dropoff_latitude > 40 & dropoff_latitude < 42)
taxi <- subset(taxi, 0 < trip_distance & trip_distance < 1000)
taxi <- subset(taxi, 0 < trip_time_in_secs & trip_time_in_secs < 4000000)
head(taxi, 10)

######################## 공항 이용객 시간대별 카운트 #######################################
count.map <- function(K, V){
  result <- lapply(V[, 13], function(x){
    datetime <- strsplit(x, " ")
    time <- strsplit(datetime[[1]][2], ":")
    return (time[[1]][1])
  });
  
  k = unlist(result)
  keyval(k, V['passenger_count'])
}

count.reduce <- function(K, V){
  keyval(K, sum(V))
}

count_passenger <- function (LT, RB, data) {
  dropoff <- subset(data, ((RB[1] <= dropoff_latitude) & (dropoff_latitude <= LT[1])))
  dropoff <- subset(dropoff, ((LT[2] <= dropoff_longitude) & (dropoff_longitude <= RB[2])))
  pickup <- subset(data, ((RB[1] <= pickup_latitude) & (pickup_latitude <= LT[1])))
  pickup <- subset(pickup, ((LT[2] <= pickup_longitude) & (pickup_longitude <= RB[2])))
  total_dat <- rbind(dropoff, pickup)
  
  res <- from.dfs(mapreduce(input=to.dfs(total_dat), map=count.map, reduce=count.reduce)); res
  res <- as.data.frame(res); res
  result_cnt <- data.frame(sort(res$key), res$val[order(res$key)])
  names(result_cnt) <- c('t', 'cnt');
  result_cnt['cnt'] <- result_cnt['cnt'] / (30 * 12);
  plot(result_cnt, type='b', pch = 19, col=2, frame = FALSE,xlab = "Timestamp",ylab = "Average Passenger")
  num <- mean(result_cnt$cnt); 
  return (num)
}

# - John F Kenedy 국제공항
JFK_LT = c(40.649352, -73.793321)
JFK_RB = c(40.639029, -73.775726)

# - 뉴욕 라과디아 공항
LG_LT = c(40.776372, -73.877144)
LG_RB = c(40.766438, -73.860201)

# - 뉴어크 리버티 국제공항
Newark_LT = c(40.696027, -74.184740)
Newark_RB = c(40.687360, -74.176749)

JFK_num <- count_passenger(JFK_LT, JFK_RB, taxi)
LG_num <- count_passenger(LG_LT, LG_RB, taxi)
Newark_num <- count_passenger(Newark_LT, Newark_RB, taxi)

######################## 상관관계 ###############################

merge_data <- function(LT, RB, data) {
  dropoff <- subset(data, ((RB[1] <= dropoff_latitude) & (dropoff_latitude <= LT[1])))
  dropoff <- subset(dropoff, ((LT[2] <= dropoff_longitude) & (dropoff_longitude <= RB[2])))
  pickup <- subset(data, ((RB[1] <= pickup_latitude) & (pickup_latitude <= LT[1])))
  pickup <- subset(pickup, ((LT[2] <= pickup_longitude) & (pickup_longitude <= RB[2])))
  
  return (rbind(dropoff, pickup))
}

check_corr <- function(data) {
  total_dat <- data[,-c(1,2)]
  panel.cor <- function(x, y, digits = 2, prefix = "", cex.cor, ...)
  {
    usr <- par("usr"); on.exit(par(usr))
    par(usr = c(0, 1, 0, 1))
    r <- abs(cor(x, y))
    txt <- format(c(r, 0.123456789), digits = digits)[1]
    txt <- paste0(prefix, txt)
    if(missing(cex.cor)) cex.cor <- 0.8/strwidth(txt)
    text(0.5, 0.5, txt, cex = cex.cor * r)
  }
  
  pairs(total_dat[, c(3,4,5,6,7,8,14,15)], upper.panel = panel.cor, lower.panel = NULL)
  # total_amount -> 8 / trip_time_in_secs = 14 / trip_distance = 15
}

# - John F Kenedy 국제공항
JFK_LT = c(40.649352, -73.793321)
JFK_RB = c(40.639029, -73.775726)

# - 뉴욕 라과디아 공항
LG_LT = c(40.776372, -73.877144)
LG_RB = c(40.766438, -73.860201)

# - 뉴어크 리버티 국제공항
Newark_LT = c(40.696027, -74.184740)
Newark_RB = c(40.687360, -74.176749)

total_dat <- rbind(merge_data(JFK_LT, JFK_RB, taxi), merge_data(LG_LT, LG_RB, taxi), merge_data(Newark_LT, Newark_RB, taxi))
check_corr(total_dat)

############################# 회귀분석 ###################################

map.fun <- function(k, v) {
  dat <- data.frame(fare_amount = v$fare_amount, trip_distance = v$trip_distance)
  Xk <- model.matrix(fare_amount ~ trip_distance, dat)
  yk <- as.matrix(dat[, 1])
  
  XtXk <- crossprod(Xk, Xk)
  Xtyk <- crossprod(Xk, yk)
  ytyk <- crossprod(yk, yk)
  
  keyval(1, list(XtXk, Xtyk, ytyk))
}

reduce.fun <- function(k, v){
  XtX <- Reduce("+", v[seq_along(v) %% 3 == 1])
  Xty <- Reduce("+", v[seq_along(v) %% 3 == 2])
  yty <- Reduce("+", v[seq_along(v) %% 3 == 0])
  
  res <- list(XtX = XtX, Xty=Xty, yty=yty)
  
  keyval(1, res)
}

fare_Regression <- function (LT, RB, data) {
  dropoff <- subset(data, ((RB[1] <= dropoff_latitude) & (dropoff_latitude <= LT[1])))
  dropoff <- subset(dropoff, ((LT[2] <= dropoff_longitude) & (dropoff_longitude <= RB[2])))
  pickup <- subset(data, ((RB[1] <= pickup_latitude) & (pickup_latitude <= LT[1])))
  pickup <- subset(pickup, ((LT[2] <= pickup_longitude) & (pickup_longitude <= RB[2])))
  total_dat <- rbind(dropoff, pickup)
  
  result <- values(from.dfs(mapreduce(input=to.dfs(total_dat), map=map.fun, reduce=reduce.fun, combine=TRUE)))
  nn <- result$XtX[1,1]; nn
  beta.hat <- solve(result$XtX, result$Xty); beta.hat
  plot(total_dat$trip_distance, total_dat$fare_amount, col=6, pch=16)
  abline(beta.hat[1], beta.hat[2]) 
  
  return (beta.hat)
}

# - John F Kenedy 국제공항
JFK_LT = c(40.649352, -73.793321)
JFK_RB = c(40.639029, -73.775726)

# - 뉴욕 라과디아 공항
LG_LT = c(40.776372, -73.877144)
LG_RB = c(40.766438, -73.860201)

# - 뉴어크 리버티 국제공항
Newark_LT = c(40.696027, -74.184740)
Newark_RB = c(40.687360, -74.176749)

JFK_reg <- fare_Regression(JFK_LT, JFK_RB, taxi)
LG_reg <- fare_Regression(LG_LT, LG_RB, taxi)
Newark_reg <- fare_Regression(Newark_LT, Newark_RB, taxi)

