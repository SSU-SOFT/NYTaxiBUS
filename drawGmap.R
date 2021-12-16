library(rhdfs)
hdfs.init()
library(rmr2)

rm(list=ls())
# local 가져오기
rmr.options(backend = "local")
files<-c("./data/taxi/info.csv", "./data/taxi/sample_combined1.csv"); files
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
mr <- mapreduce( input = files[1], input.format = input.format)
#mr <- mapreduce( input = files, input.format = input.format)
res.data <- from.dfs(mr);
taxi <- res.data$val
taxi <- taxi[!(taxi$fare_amount < 2.5 | taxi$total_amount < 2.5),] # 요금이 2.5미만인 경우 제거
taxi <- taxi[!(taxi$passenger_count == 0 | taxi$passenger_count == 208),] # 승객수 0명, 208명 제거
taxi <- subset(taxi, pickup_longitude > -75 & pickup_longitude < -73 & pickup_latitude > 40 & pickup_latitude < 42)
taxi <- subset(taxi, dropoff_longitude > -75 & dropoff_longitude < -73 & dropoff_latitude > 40 & dropoff_latitude < 42)
taxi <- subset(taxi, 0 < trip_time_in_secs & trip_time_in_secs < 4000000)
head(taxi, 10)

########################### John F Kenedy 국제공항 ###########################
# : 40.649352, -73.793321 (Left Top)
# : 40.639029, -73.775726 (right Bottom)
JFK_LT = c(40.649352, -73.793321)
JFK_RB = c(40.639029, -73.775726)
# 도착이 JFK 공항인 택시
JFK_dropoff <- subset(taxi, ((JFK_RB[1] <= dropoff_latitude) & (dropoff_latitude <= JFK_LT[1])))
JFK_dropoff <- subset(JFK_dropoff, ((JFK_LT[2] <= dropoff_longitude) & (dropoff_longitude <= JFK_RB[2])))

# 출발이 JFK 공항인 택시
JFK_pickup <- subset(taxi, ((JFK_RB[1] <= pickup_latitude) & (pickup_latitude <= JFK_LT[1])))
JFK_pickup <- subset(JFK_pickup, ((JFK_LT[2] <= pickup_longitude) & (pickup_longitude <= JFK_RB[2])))

# 출발과 도착이 JFK 공항인 택시 GPS
JFK_dropoff_dat <- cbind(JFK_dropoff['pickup_latitude'], JFK_dropoff['pickup_longitude'])
JFK_pickup_dat <- cbind(JFK_pickup['dropoff_latitude'], JFK_pickup['dropoff_longitude'])
names(JFK_dropoff_dat) <- c("latitude", "longitude")
names(JFK_pickup_dat) <- c("latitude", "longitude")
JFK_total_dat <- rbind(JFK_dropoff_dat, JFK_pickup_dat)

################################폴리곤으로 제한###########################################

require(sp)
# 맨해튼 시
lat <- c(40.82933575889813, 40.753759184930544, 40.702398460626355, 40.70998130193526, 40.797353721667065) 
lon <- c(-73.95244750207654,-74.00756554892766,-74.0179767355551,-73.97898582093077,-73.92876715602198)

#뉴욕시 전체 
lat <- c(40.91334054972457, 40.87856519709389, 40.71742641845681, 40.59240527356296, 40.57154564303528, 40.74812490240338)
lon <- c(-73.90957780414897, -73.78460832953785, -73.72899004688126, -73.74066301978449, -74.0112013329536, -74.0139479148132)

JFK_total_dat<-subset(JFK_total_dat,(point.in.polygon(latitude,longitude,lat,lon,mode.checked = FALSE))==1)

JFK_scaled <- scale(JFK_total_dat)


############################# K 값 구하기 #########################################
avg_sil <- function(k, data) {
  data <- scale(data)
  km.res <- kmeans(data, centers = k)
  ss <- silhouette(km.res$cluster, dist(data))
  avgSil <- mean(ss[,3])
  return (avgSil)
}
JFK_sil = NULL
for (i in 2:30){
  JFK_sil[i] <- avg_sil(i, JFK_total_dat)
}
plot(JFK_sil, type='b', pch = 19, col=2, frame = FALSE,xlab = "Number of clusters K",ylab = "Average Silhouettes")

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


kmeans.gmap<-function(.,P) {
  
    D<-dist.fun(C, P)
    nearest<-max.col(-D)

    JFK_tmp <- scale(P,center=FALSE,scale=1/attr(JFK_scaled,'scaled:scale'))
    JFK_tmp <- scale(JFK_tmp,center=-attr(JFK_scaled,'scaled:center'),scale=FALSE)
    #class(JFK_tmp)
    JFK_tmp<-as.data.frame(JFK_tmp)
    JFK_tmp['cluster'] <- nearest

    keyval(1,JFK_tmp)
}

kmeans.reduce<-function(k,P) keyval(k,t(as.matrix(apply(P,2,sum))))

C=NULL
num.iter = 10+1
cluster_number = 12
## ggmap 초기화

register_google(key='AIzaSyCMaV4yY0ZirrR_dbKmSn74PRPu4O9Q26c')
map<-get_map(location='Manhatten', zoom=10)
gmap <- ggmap(map)

gmap

for(i in 1:num.iter){
  print(i)
  if(i==num.iter){
    tmp<-from.dfs(mapreduce(to.dfs(JFK_scaled),map=kmeans.gmap));
  }else{
    mr<-from.dfs(mapreduce(to.dfs(JFK_scaled),map=kmeans.map,reduce=kmeans.reduce));
    C<-values(mr)[,-1]/values(mr)[,1];
  }
 
  if(nrow(C)<cluster_number){
    C<-rbind(C,matrix(rnorm((cluster_number-nrow(C))*nrow(C)),ncol=nrow(C))%*%C)
  }
}

tmp

C

class(tmp$val)

JFK_centers <- scale(C,center=FALSE,scale=1/attr(JFK_scaled,'scaled:scale'))

JFK_centers <- scale(JFK_centers,center=-attr(JFK_scaled,'scaled:center'),scale=FALSE)

JFK_centers

gmap <-gmap+geom_point(data=tmp$val, aes(x=longitude, y = latitude, color = rainbow(cluster_number)[cluster], alpha = 0.01))

#gmap <- gmap+geom_point(data=as.data.frame(JFK_total_dat), aes(x=longitude, y = latitude, color = rainbow(cluster_number)[cluster], alpha = 0.01))
gmap <- gmap+geom_point(data=as.data.frame(JFK_centers), aes(x=longitude,y=latitude))
gmap

########################### 뉴욕 라과디아 공항 ###########################
# : 40.776372, -73.877144 (Left Top)
# : 40.766438, -73.860201 (Right Bottom)
LG_LT = c(40.776372, -73.877144)
LG_RB = c(40.766438, -73.860201)
LG_dropoff <- subset(taxi, ((LG_RB[1] <= dropoff_latitude) & (dropoff_latitude <= LG_LT[1])))
LG_dropoff <- subset(LG_dropoff, ((LG_LT[2] <= dropoff_longitude) & (dropoff_longitude <= LG_RB[2])))

########################### 뉴어크 리버티 국제공항 ###########################
# : 40.696027, -74.184740 (Left Top)
# : 40.687360, -74.176749 (Right Bottom)
Newark_LT = c(40.696027, -74.184740)
Newark_RB = c(40.687360, -74.176749)
Newark_dropoff <- subset(taxi, ((Newark_RB[1] <= dropoff_latitude) & (dropoff_latitude <= Newark_LT[1])))
Newark_dropoff <- subset(Newark_dropoff, ((Newark_LT[2] <= dropoff_longitude) & (dropoff_longitude <= Newark_RB[2])))
