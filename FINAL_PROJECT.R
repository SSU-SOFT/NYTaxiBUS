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
head(data, 10)

# - John F Kenedy 국제공함
# : 40.649352, -73.793321 (Left Top)
# : 40.639029, -73.775726 (right Bottom)
JFK_LT = c(40.649352, -73.793321)
JFK_RB = c(40.639029, -73.775726)
JFK <- subset(data, ((JFK_RB[1] <= dropoff_latitude) & (dropoff_latitude <= JFK_LT[1])))
JFK <- subset(JFK, ((JFK_LT[2] <= dropoff_longitude) & (dropoff_longitude <= JFK_RB[2])))

# - 뉴욕 라과디아 공항
# : 40.776372, -73.877144 (Left Top)
# : 40.766438, -73.860201 (Right Bottom)
#LG_LT = c(40.776372, -73.877144)
#LG_RB = c(40.766438, -73.860201)
#LG <- subset(data, ((LG_RB[1] <= dropoff_latitude) & (dropoff_latitude <= LG_LT[1])))
#LG <- subset(LG, ((LG_LT[2] <= dropoff_longitude) & (dropoff_longitude <= LG_RB[2])))

# - 뉴어크 리버티 국제공항
# : 40.696027, -74.184740 (Left Top)
# : 40.687360, -74.176749 (Right Bottom)
#Newark_LT = c(40.696027, -74.184740)
#Newark_RB = c(40.687360, -74.176749)
#Newark <- subset(data, ((Newark_RB[1] <= dropoff_latitude) & (dropoff_latitude <= Newark_LT[1])))
#Newark <- subset(Newark, ((Newark_LT[2] <= dropoff_longitude) & (dropoff_longitude <= Newark_RB[2])))

dat <- cbind(JFK['pickup_latitude'], JFK['pickup_longitude']); dat
fit <- kmeans(dat, centers = 10)

fit$cluster
fit$centers

# 시각화
library(ggplot2)
library(ggmap)

register_google(key='AIzaSyCMaV4yY0ZirrR_dbKmSn74PRPu4O9Q26c')
ggmap(get_map(location='Manhatten', zoom=11))
