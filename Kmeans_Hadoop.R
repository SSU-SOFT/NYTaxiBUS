library(rhdfs)
hdfs.init()
library(rmr2)

rm(list=ls())
# Hadoop 가져오기
rmr.options(backend = "hadoop")
# HDFS 상의 taxi 자료 파일 확인
hdfs.ls("/data/taxi/combined")
# 폴더에 포함된 파일 목록 files에 할당
files <- hdfs.ls("/data/taxi/combined")$file; files
files[1]
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
taxi <- taxi[!(taxi$fare_amount < 2.5 | taxi$total_amount < 2.5 || taxi$total_amount ==900),] # 요금이 2.5미만인 경우 제거
taxi <- taxi[!(taxi$passenger_count>6),] # 승객수 0명, 208명 제거
taxi <- subset(taxi, pickup_longitude > -75 & pickup_longitude < -73 & pickup_latitude > 40 & pickup_latitude < 42)
taxi <- subset(taxi, dropoff_longitude > -75 & dropoff_longitude < -73 & dropoff_latitude > 40 & dropoff_latitude < 42)
taxi <- subset(taxi, 0 < trip_time_in_secs & trip_time_in_secs < 4000000)
head(taxi,10)

########################### 공항별 좌표 계산  ###########################

#JFK_LT = c(40.649352, -73.793321)
#JFK_RB = c(40.639029, -73.775726)


#LG_LT = c(40.776372, -73.877144)
#LG_RB = c(40.766438, -73.860201)

#Newark_LT = c(40.696027, -74.184740)
#Newark_RB = c(40.687360, -74.176749)

LT =c(40.649352, -73.793321)
RB =c(40.639029, -73.775726)

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
# 맨해튼 시
lat <- c(40.82933575889813, 40.753759184930544, 40.702398460626355, 40.70998130193526, 40.797353721667065) 
lon <- c(-73.95244750207654,-74.00756554892766,-74.0179767355551,-73.97898582093077,-73.92876715602198)

#뉴욕시 전체 
lat <- c(40.91334054972457, 40.87856519709389, 40.71742641845681, 40.59240527356296, 40.57154564303528, 40.74812490240338)
lon <- c(-73.90957780414897, -73.78460832953785, -73.72899004688126, -73.74066301978449, -74.0112013329536, -74.0139479148132)

total_dat<-subset(total_dat,(point.in.polygon(latitude,longitude,lat,lon,mode.checked = FALSE))==1)
scaled <- scale(total_dat)


############################# K-means #############################################
dist.fun <- function(C, P){
  apply(C, 1, function(x) colSums((t(P) - x)^2))
}

kmeans.map<-function(.,P) {
  
  nearest<-if(is.null(C)){
    sample(1:cluster_number,nrow(P),replace=TRUE)
  }
  else {
    D<-dist.fun(C, P)
    nearest<-max.col(-D)
  }
  
  keyval(nearest,cbind(1, P))
}

#택시 좌표를 얻기 위한 Map 함수
kmeans.gmap<-function(.,P) {
  
  D<-dist.fun(C, P)
  nearest<-max.col(-D)
  
  loc <- scale(P,center=FALSE,scale=1/attr(scaled,'scaled:scale'))
  loc <- scale(loc,center=-attr(scaled,'scaled:center'),scale=FALSE)
  loc<-as.data.frame(loc)
  loc['cluster'] <- nearest
  
  keyval(1,loc)
}

kmeans.reduce<-function(k,P) keyval(k,t(as.matrix(apply(P,2,sum))))

C=NULL
num.iter = 10 #iter 값
cluster_number = 8 #K값

## ggmap 초기화
library("ggmap")
register_google(key='구글맵APIkey')
map<-get_map(location='Manhatten', zoom=10)

gmap <- ggmap(map)
gmap

## K-means 수행
for(i in 1:num.iter){
  mr<-from.dfs(mapreduce(to.dfs(scaled),map=kmeans.map,reduce=kmeans.reduce));
  C<-values(mr)[,-1]/values(mr)[,1];
  if(i==num.iter){
    locs<-from.dfs(mapreduce(to.dfs(scaled),map=kmeans.gmap)); #마지막에 클러스터에 해당하는 각 좌표값을 저장한다.
  }
  
  #지정한 클러스터 개수보다 결과로 나온 클러스터 개수가 적을 시  
  if(nrow(C)<cluster_number){
    C<-rbind(C,matrix(rnorm((cluster_number-nrow(C))*nrow(C)),ncol=nrow(C))%*%C)
  }
}

centers <- scale(C,center=FALSE,scale=1/attr(scaled,'scaled:scale'))
centers <- scale(centers,center=-attr(scaled,'scaled:center'),scale=FALSE)

##지도에 택시 좌표와 center 좌표를 시각화
gmap <-gmap+geom_point(data=locs$val, aes(x=longitude, y = latitude, color = rainbow(cluster_number)[cluster], alpha = 0.01))
gmap <- gmap+geom_point(data=as.data.frame(centers), aes(x=longitude,y=latitude))
gmap
