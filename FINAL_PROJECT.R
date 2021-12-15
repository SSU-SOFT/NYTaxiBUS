library(rhdfs)
hdfs.init()
library(rmr2)

# geo coding 시각화
#install.packages('ggplot2')
#install.packages('ggmap')
#install.packages("devtools")


rm(list=ls())
rmr.options(backend = "local")
#rmr.options(backend = "hadoop")

files<-list()
files[1]<-"./data/taxi/info.csv";
files[2]<-"./data/taxi/sample_combined1.csv"

mr <- mapreduce(input = files[1], 
                input.format = make.input.format(
                  format = "csv", sep=",", stringsAsFactors=F)
)

res <- from.dfs(mr) 
ress <- values(res)
colnames.tmp <- as.character(ress[,1]); 
class.tmp <- as.character(ress[,2]); 
colnames <- colnames.tmp[-1]; 
class <- class.tmp[-1]; class 
class[c(6,8,9,10)] <- "numeric"

cbind(colnames, class)

input.format <- make.input.format( 
  format = "csv", sep = ",", 
  stringsAsFactors = F, 
  col.names = colnames, colClasses = class)
files <- files[-1]
files
colnames

data<-read.csv("./data/taxi/sample_combined1.csv")
names(data)<-colnames

data <- subset(data, pickup_latitude >= 40 & pickup_latitude <= 42)
data <- subset(data, pickup_longitude >= -75 & pickup_longitude <= -73)
data <- subset(data, dropoff_latitude >= 40 & dropoff_latitude <= 42)
data <- subset(data, dropoff_longitude >= -75 & dropoff_longitude <= -73)
head(data, 10)

# - John F Kenedy 국제공함
# : 40.649352, -73.793321 (Left Top)
# : 40.639029, -73.775726 (right Bottom)
JFK_LT = c(40.649352, -73.793321)
JFK_RB = c(40.639029, -73.775726)
JFK_dropoff <- subset(data, ((JFK_RB[1] <= dropoff_latitude) & (dropoff_latitude <= JFK_LT[1])))
JFK_dropoff <- subset(JFK_dropoff, ((JFK_LT[2] <= dropoff_longitude) & (dropoff_longitude <= JFK_RB[2])))

# - 뉴욕 라과디아 공항
# : 40.776372, -73.877144 (Left Top)
# : 40.766438, -73.860201 (Right Bottom)
LG_LT = c(40.776372, -73.877144)
LG_RB = c(40.766438, -73.860201)
LG_dropoff <- subset(data, ((LG_RB[1] <= dropoff_latitude) & (dropoff_latitude <= LG_LT[1])))
LG_dropoff <- subset(LG_dropoff, ((LG_LT[2] <= dropoff_longitude) & (dropoff_longitude <= LG_RB[2])))

# - 뉴어크 리버티 국제공항
# : 40.696027, -74.184740 (Left Top)
# : 40.687360, -74.176749 (Right Bottom)
Newark_LT = c(40.696027, -74.184740)
Newark_RB = c(40.687360, -74.176749)
Newark_dropoff <- subset(data, ((Newark_RB[1] <= dropoff_latitude) & (dropoff_latitude <= Newark_LT[1])))
Newark_dropoff <- subset(Newark_dropoff, ((Newark_LT[2] <= dropoff_longitude) & (dropoff_longitude <= Newark_RB[2])))

JFK_dropoff_dat <- cbind(JFK_dropoff['pickup_latitude'], JFK_dropoff['pickup_longitude'])
LG_dropoff_dat <- cbind(LG_dropoff['pickup_latitude'], LG_dropoff['pickup_longitude'])
Newark_dropoff_dat <- cbind(Newark_dropoff['pickup_latitude'], Newark_dropoff['pickup_longitude'])

cluster_number = 20
JFK_dropoff_fit <- kmeans(JFK_dropoff_dat, centers = cluster_number)
LG_dropoff_fit <- kmeans(LG_dropoff_dat, centers = cluster_number)
Newark_dropoff_fit <- kmeans(Newark_dropoff_dat, centers = cluster_number)

#데이터에 클러스터 column 추가
JFK_dropoff['cluster'] <- JFK_dropoff_fit$cluster
LG_dropoff['cluster'] <- LG_dropoff_fit$cluster
Newark_dropoff['cluster'] <- Newark_dropoff_fit$cluster

# 시각화
library(ggplot2)
library(ggmap)

#Google Map API Key 등록
register_google(key='AIzaSyCMaV4yY0ZirrR_dbKmSn74PRPu4O9Q26c')

#JFK Cluster좌표 시각화
map<-get_map(location='Manhatten', zoom=11)
gmap <- ggmap(map)
gmap <- gmap+geom_point(data=as.data.frame(JFK_dropoff), aes(x=pickup_longitude, y = pickup_latitude, color = rainbow(cluster_number)[cluster], alpha = 0.1))
gmap <- gmap+geom_point(data=as.data.frame(JFK_dropoff_fit$centers),aes(x=pickup_longitude,y=pickup_latitude))
gmap

#LG Cluster좌표 시각화
map<-get_map(location='Manhatten', zoom=11)
gmap <- ggmap(map)
gmap <- gmap+geom_point(data=as.data.frame(LG_dropoff), aes(x=pickup_longitude, y = pickup_latitude, color = rainbow(cluster_number)[cluster], alpha = 0.1))
gmap <- gmap+geom_point(data=as.data.frame(LG_dropoff_fit$centers),aes(x=pickup_longitude,y=pickup_latitude))
gmap

#Newark Cluster좌표 시각화
map<-get_map(location='Manhatten', zoom=11)
gmap <- ggmap(map)
gmap <- gmap+geom_point(data=as.data.frame(Newark_dropoff), aes(x=pickup_longitude, y = pickup_latitude, color = rainbow(cluster_number)[cluster], alpha = 0.1))
gmap <- gmap+geom_point(data=as.data.frame(Newark_dropoff_fit$centers), aes(x=pickup_longitude,y=pickup_latitude))
gmap



################################## 출발점이 공항인 경우 ##################################
# - John F Kenedy 국제공함
# : 40.649352, -73.793321 (Left Top)
# : 40.639029, -73.775726 (right Bottom)
JFK_LT = c(40.649352, -73.793321)
JFK_RB = c(40.639029, -73.775726)
JFK_pickup <- subset(data, ((JFK_RB[1] <= pickup_latitude) & (pickup_latitude <= JFK_LT[1])))
JFK_pickup <- subset(JFK_pickup, ((JFK_LT[2] <= pickup_longitude) & (pickup_longitude <= JFK_RB[2])))

# - 뉴욕 라과디아 공항
# : 40.776372, -73.877144 (Left Top)
# : 40.766438, -73.860201 (Right Bottom)
LG_LT = c(40.776372, -73.877144)
LG_RB = c(40.766438, -73.860201)
LG_pickup <- subset(data, ((LG_RB[1] <= pickup_latitude) & (pickup_latitude <= LG_LT[1])))
LG_pickup <- subset(LG_pickup, ((LG_LT[2] <= pickup_longitude) & (pickup_longitude <= LG_RB[2])))

# - 뉴어크 리버티 국제공항
# : 40.696027, -74.184740 (Left Top)
# : 40.687360, -74.176749 (Right Bottom)
Newark_LT = c(40.696027, -74.184740)
Newark_RB = c(40.687360, -74.176749)
Newark_pickup <- subset(data, ((Newark_RB[1] <= pickup_latitude) & (pickup_latitude <= Newark_LT[1])))
Newark_pickup <- subset(Newark_pickup, ((Newark_LT[2] <= pickup_longitude) & (pickup_longitude <= Newark_RB[2])))

JFK_pickup_dat <- cbind(JFK_pickup['dropoff_latitude'], JFK_pickup['dropoff_longitude'])
LG_pickup_dat <- cbind(LG_pickup['dropoff_latitude'], LG_pickup['dropoff_longitude'])
Newark_pickup_dat <- cbind(Newark_pickup['dropoff_latitude'], Newark_pickup['dropoff_longitude'])

cluster_number = 20
JFK_pickup_fit <- kmeans(JFK_pickup_dat, centers = cluster_number)
LG_pickup_fit <- kmeans(LG_pickup_dat, centers = cluster_number)
Newark_pickup_fit <- kmeans(Newark_pickup_dat, centers = cluster_number)

#데이터에 클러스터 column 추가
JFK_pickup['cluster'] <- JFK_pickup_fit$cluster
LG_pickup['cluster'] <- LG_pickup_fit$cluster
Newark_pickup['cluster'] <- Newark_pickup_fit$cluster

#Google Map API Key 등록
register_google(key='AIzaSyCMaV4yY0ZirrR_dbKmSn74PRPu4O9Q26c')

#JFK Cluster좌표 시각화
map<-get_map(location='Manhatten', zoom=11)
gmap <- ggmap(map)
gmap <- gmap+geom_point(data=as.data.frame(JFK_pickup), aes(x=dropoff_longitude, y = dropoff_latitude, color = rainbow(cluster_number)[cluster], alpha = 0.1))
gmap <- gmap+geom_point(data=as.data.frame(JFK_pickup_fit$centers),aes(x=dropoff_longitude,y=dropoff_latitude))
gmap

#LG Cluster좌표 시각화
map<-get_map(location='Manhatten', zoom=11)
gmap <- ggmap(map)
gmap <- gmap+geom_point(data=as.data.frame(LG_pickup), aes(x=dropoff_longitude, y = dropoff_latitude, color = rainbow(cluster_number)[cluster], alpha = 0.1))
gmap <- gmap+geom_point(data=as.data.frame(LG_pickup_fit$centers),aes(x=dropoff_longitude,y=dropoff_latitude))
gmap

#Newark Cluster좌표 시각화
map<-get_map(location='Manhatten', zoom=11)
gmap <- ggmap(map)
gmap <- gmap+geom_point(data=as.data.frame(Newark_pickup), aes(x=dropoff_longitude, y = dropoff_latitude, color = rainbow(cluster_number)[cluster], alpha = 0.1))
gmap <- gmap+geom_point(data=as.data.frame(Newark_pickup_fit$centers), aes(x=dropoff_longitude,y=dropoff_latitude))
gmap

################################## 출발점이 공항인 경우 ##################################


################################## 공항에서 출발 & 공항에서 도착 merge ############################
names(JFK_dropoff_dat) <- c("latitude", "longitude")
names(JFK_pickup_dat) <- c("latitude", "longitude")
names(LG_dropoff_dat) <- c("latitude", "longitude")
names(LG_pickup_dat) <- c("latitude", "longitude")
names(Newark_dropoff_dat) <- c("latitude", "longitude")
names(Newark_pickup_dat) <- c("latitude", "longitude")

JFK_total_dat <- rbind(JFK_dropoff_dat, JFK_pickup_dat)
JFK_scaled <- scale(JFK_total_dat,center=TRUE,scale=TRUE);

LG_total_dat <- rbind(LG_dropoff_dat, LG_pickup_dat)
Newark_total_dat <- rbind(Newark_dropoff_dat, Newark_pickup_dat)


cluster_number = 20
JFK_total_fit <- kmeans(JFK_scaled, centers = cluster_number)
JFK_total_fit$centers

JFK_centers <- scale(JFK_total_fit$centers,center=FALSE,scale=1/attr(JFK_scaled,'scaled:scale'))
JFK_centers <- scale(JFK_centers,center=-attr(JFK_scaled,'scaled:center'),scale=FALSE)

JFK_centers
JFK_total_dat['cluster'] <- JFK_total_fit$cluster

# K 값 구하기
avg_sil <- function(k, data) {
  data <- scale(data)
  km.res <- kmeans(data, centers = k)
  ss <- silhouette(km.res$cluster, dist(data))
  avgSil <- mean(ss[,3])
  return (avgSil)
}
JFK_sil = NULL
LG_sil = NULL
Newark_sil = NULL
for (i in 2:100){
  JFK_sil[i] <- avg_sil(i, JFK_total_dat)
  LG_sil[i] <- avg_sil(i, LG_total_dat)
  Newark_sil[i] <- avg_sil(i, Newark_total_dat)
}
plot(JFK_sil, type='b', pch = 19, col=2, frame = FALSE,xlab = "Number of clusters K",ylab = "Average Silhouettes")
plot(LG_sil, type='b', pch = 19, col=3, frame = FALSE,xlab = "Number of clusters K",ylab = "Average Silhouettes")
plot(Newark_sil, type='b', pch = 19, col=4, frame = FALSE,xlab = "Number of clusters K",ylab = "Average Silhouettes")


LG_total_fit <- kmeans(LG_total_dat, centers = 8)
LG_total_dat['cluster'] <- LG_total_fit$cluster


# JFK

map<-get_map(location='Manhatten', zoom=10)
gmap <- ggmap(map)
gmap <- gmap+geom_point(data=as.data.frame(JFK_total_dat), aes(x=longitude, y = latitude, color = rainbow(cluster_number)[cluster], alpha = 0.01))
gmap <- gmap+geom_point(data=as.data.frame(JFK_centers), aes(x=longitude,y=latitude))
gmap

# LG
map<-get_map(location='Manhatten', zoom=10)
gmap <- ggmap(map)
gmap <- gmap+geom_point(data=as.data.frame(LG_total_dat), aes(x=longitude, y = latitude, color = rainbow(cluster_number)[cluster], alpha = 0.01))
gmap <- gmap+geom_point(data=as.data.frame(LG_total_fit$centers), aes(x=longitude,y=latitude))
gmap

# Newark
Newark_total_fit <- kmeans(Newark_total_dat, centers = 5)
Newark_total_dat['cluster'] <- Newark_total_fit$cluster
map<-get_map(location='Manhatten', zoom=10)
gmap <- ggmap(map)
gmap <- gmap+geom_point(data=as.data.frame(Newark_total_dat), aes(x=longitude, y = latitude, color = rainbow(cluster_number)[cluster], alpha = 0.1))
gmap <- gmap+geom_point(data=as.data.frame(Newark_total_fit$centers), aes(x=longitude,y=latitude))
gmap

################################## 공항에서 출발 & 공항에서 도착 merge ############################
