library(rhdfs)
hdfs.init()
library(rmr2)


find_kvalue <- function(LT, RB){
  # local 가져오기
  rmr.options(backend = "local")
  files<-c("./data/taxi/info.csv", "./data/taxi/sample_combined1.csv"); files
  
  
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
  
  ########################## 전처리 ################################
  # 파일 하나만 입력 자료로 사용
  #mr <- mapreduce( input = files[1], input.format = input.format)
  mr <- mapreduce( input = files, input.format = input.format)
  res.data <- from.dfs(mr);
  
  taxi <- res.data$val
  taxi <- taxi[!(taxi$fare_amount < 2.5 | taxi$total_amount < 2.5),] # 요금이 2.5미만인 경우 제거
  taxi <- taxi[!(taxi$passenger_count == 0 | taxi$passenger_count == 208),] # 승객수 0명, 208명 제거
  taxi <- subset(taxi, pickup_longitude > -75 & pickup_longitude < -73 & pickup_latitude > 40 & pickup_latitude < 42)
  taxi <- subset(taxi, dropoff_longitude > -75 & dropoff_longitude < -73 & dropoff_latitude > 40 & dropoff_latitude < 42)
  taxi <- subset(taxi, 0 < trip_time_in_secs & trip_time_in_secs < 4000000)
  head(taxi,10)
  
  # 도착이 공항인 택시
  dropoff <- subset(taxi, ((RB[1] <= dropoff_latitude) & (dropoff_latitude <= LT[1])))
  dropoff <- subset(dropoff, ((LT[2] <= dropoff_longitude) & (dropoff_longitude <= RB[2])))
  
  # 출발이 공항인 택시
  pickup <- subset(taxi, ((RB[1] <= pickup_latitude) & (pickup_latitude <= LT[1])))
  pickup <- subset(pickup, ((LT[2] <= pickup_longitude) & (pickup_longitude <= RB[2])))
  
  # 출발과 도착이 공항인 택시 GPS
  dropoff_dat <- cbind(dropoff['pickup_latitude'], dropoff['pickup_longitude'])
  pickup_dat <- cbind(pickup['dropoff_latitude'], pickup['dropoff_longitude'])
  names(dropoff_dat) <- c("latitude", "longitude")
  names(pickup_dat) <- c("latitude", "longitude")
  total_dat <- rbind(dropoff_dat, pickup_dat)
  
  ################################폴리곤으로 제한###########################################
  require(sp)

  #뉴욕시 전체 
  lat <- c(40.91334054972457, 40.87856519709389, 40.71742641845681, 40.59240527356296, 40.57154564303528, 40.74812490240338)
  lon <- c(-73.90957780414897, -73.78460832953785, -73.72899004688126, -73.74066301978449, -74.0112013329536, -74.0139479148132)

  total_dat<-subset(total_dat,(point.in.polygon(latitude,longitude,lat,lon,mode.checked = FALSE))==1)
  
  avg_sil <- function(k, data) {
    data <- scale(data)
    km.res <- kmeans(data, centers = k)
    ss <- silhouette(km.res$cluster, dist(data))
    avgSil <- mean(ss[,3])
    return (avgSil)
  }
  sil = NULL
  for (i in 2:30){
    sil[i] <- avg_sil(i, total_dat)
  }
  plot(sil, type='b', pch = 19, col=2, frame = FALSE,xlab = "Number of clusters K",ylab = "Average Silhouettes")
  
}

# 존 F. 케네디 국제공항
#JFK_LT = c(40.649352, -73.793321)
#JFK_RB = c(40.639029, -73.775726)

# 라과디아 공항
#LGA_LT = c(40.776372, -73.877144)
#LGA_RB = c(40.766438, -73.860201)

# 뉴어크 국제공항
Newark_LT = c(40.696027, -74.184740)
Newark_RB = c(40.687360, -74.176749)


find_kvalue(Newark_LT, Newark_RB)
