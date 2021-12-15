library(rhdfs)
hdfs.init()
library(rmr2)
library(cluster)
rmr.options(backend = "local")

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

class(data)
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
dat2 <- cbind(JFK['dropoff_latitude'], JFK['dropoff_longitude']); 

dist.fun <- function(C, P){
  apply(C, 1, function(x) colSums((t(P) - x)^2))
}



################################폴리곤으로 제한###########################################
require(sp)
lat <- c(40.82933575889813, 40.753759184930544, 40.702398460626355, 40.70998130193526, 40.797353721667065) 
lon <- c(-73.95244750207654,-74.00756554892766,-74.0179767355551,-73.97898582093077,-73.92876715602198)

dat_inMan<-subset(dat,(point.in.polygon(pickup_latitude,pickup_longitude,lat,lon,mode.checked = FALSE))==1)

##################blog code##########################
nearest<-NULL
kmeans.map2<-function(k,v) {
  nearest<<-if(is.null(C)){
        #print("C in null")
        #print(num.clusters)
        sample(1:num.clusters,nrow(v),replace=TRUE)
      }
      else {
        D<-dist.fun(C,v)
        nearest<-max.col(-D)
      }
  #print("map run")
 
  keyval(nearest,v)
}
kmeans.reduce<-function(k,v){
  print(v)
  keyval(k,t(as.matrix(apply(v,2,mean))))
} 



############################professor code#####################################
num.clusters<-5
kmeans.map<-function(.,P) {
  nearest<-if(is.null(C)){
    sample(1:num.clusters,nrow(P),replace=TRUE)
  }
  else {
    D<-dist.fun(C, P)
    nearest<-max.col(-D)
  }
  
  #print(cbind(1, P))
 
  keyval(nearest,cbind(1, P))
}

kmeans.reduce<-function(k,P) keyval(k,t(as.matrix(apply(P,2,sum))))


save_map<-NULL

Kmeans_mr<-function(P,num.iters=5){
 
  for(i in 1:num.iters){
    print(i)
    #save_map[i]<<-from.dfs(mapreduce(to.dfs(dat),map=kmeans.map2));
    mr<-from.dfs(mapreduce(to.dfs(scale(dat_inMan)),map=kmeans.map,reduce=kmeans.reduce));
    C<<-values(mr)[,-1]/values(mr)[,1];
    #if(nrow(C)<num.clusters){
    #  C<<-rbind(C,matrix(rnorm((num.clusters-nrow(C))*nrow(C)),ncol=nrow(C))%*%C)
    #}
  }
  return(C)
}

#실행은 여기부터 
C<-NULL
num.iters<-10
centers<-Kmeans_mr(P=to.dfs(dat_inMan),num.iters=num.iters)
centers


save_map

#원래 데이터 정재
fit <- kmeans(dat, centers = 10)


fit$cluster
fit$centers
fit
fit_filter <- subset(as.data.frame(fit$centers), ((40 <= pickup_latitude) & (pickup_latitude <= 42)))
class(fit$centers)

# 시각화
library(ggplot2)
library(ggmap)

#center 정제

centers_filtered<- subset(as.data.frame(centers), ((40 <= pickup_latitude) & (pickup_latitude <= 42)))
centers_filtered

register_google(key='AIzaSyCMaV4yY0ZirrR_dbKmSn74PRPu4O9Q26c')
map<-get_map(location='NewYork', zoom=11)
ggmap(map)+geom_point(data=centers_filtered,aes(x=pickup_longitude,y=pickup_latitude,color='red',alpha=0.3))

fit_filter
centers_filtered

num.iters<-10
centers<-Kmeans_mr(P=to.dfs(dat_inMan),num.iters=num.iters)
centers

ggmap(map)+geom_point(data=centers_filtered,aes(x=pickup_longitude,y=pickup_latitude,color='red',alpha=0.3))



