rm(list=ls())
gc()
library(lubridate)
library(stringr)
StartYear5 <- lubridate::year(Sys.Date())-5  #2016
EndYear <- lubridate::year(Sys.Date())-1    #2020
source("h:/ericg/16666LAWA/LAWA2021/WaterQuality/Scripts/SWQ_state_functions.R")
source("h:/ericg/16666LAWA/LAWA2021/Scripts/LAWAFunctions.R")

dir.create(paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis/", format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)


riverSiteTable=loadLatestSiteTableRiver()

if(!exists('wqdata')){
  wqdata=loadLatestDataRiver()
  
  #Write out for ITEffect
  wqdata$DetectionLimit=1
  wqdata$DetectionLimit[wqdata$Symbol=="<"] <- 0
  wqdata$DetectionLimit[wqdata$Symbol==">"] <- 2
  storeSci=options('scipen')
  options(scipen=5)
  write.csv(wqdata%>%transmute(LAWAID=gsub('_NIWA','',LawaSiteID,ignore.case = T),
                               Region=Region,
                               Site=CouncilSiteID,
                               Date=format(lubridate::dmy(Date),'%Y-%m-%d'),
                               Value=Value,
                               Parameter=Measurement,
                               DetectionLimit=DetectionLimit),
            file=paste0("h:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                        "/ITERiversRawData",format(Sys.time(),"%d%b%Y"),".csv"),row.names=F,na="NULL")
  options(scipen=storeSci);rm(storeSci)

# delved=read_csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis/",
#                          pattern="ITERiversRawData",full.names = T,recursive = T),1),guess_max=350000)
}
  wqdYear=lubridate::year(dmy(wqdata$Date))
  wqdata <- wqdata[which((wqdYear>=(StartYear5) & wqdYear<=EndYear)),]
  rm(wqdYear)
  #561842

wqparam <- c("BDISC","DIN","DRP","ECOLI","NH4","NO3N","PH","TN","TON","TP","TURB","TURBFNU") 

suppressWarnings(rm(wqdata_A,wqdata_med,wqdata_n,lawadata,lawadata_q))
library(parallel)
library(doParallel)
workers <- makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
   library(doBy)
  library(plyr)
  library(dplyr)
})
startTime=Sys.time()

foreach(i = 1:length(wqparam),.combine = rbind,.errorhandling = "stop")%dopar%{
  wqdata_A = wqdata%>%filter(tolower(Measurement)==tolower(wqparam[i]))
  if(dim(wqdata_A)[1]==0){return(NULL)}
  # #CENSORING
  # #Previously for state, left-censored was repalced by half the limit value, and right-censored was imputed
  # #left-censored replacement value is the maximum of left-censored values, per site
  wqdata_A$origValue=wqdata_A$Value
   #CENSORING from 12/8/2021
  #left-censored replacement value is half the face value
  if(any(wqdata_A$CenType=="Left")){
    wqdata_A$Value[wqdata_A$CenType=="Left"] <- wqdata_A$origValue[wqdata_A$CenType=="Left"]/2
  }
  if(any(wqdata_A$CenType=="Right")){
    wqdata_A$Value[wqdata_A$CenType=="Right"] <- wqdata_A$origValue[wqdata_A$CenType=="Right"]*1.1
  }
  wqdata_A <- as.data.frame(wqdata_A)  
  wqdata_A$Date=lubridate::dmy(wqdata_A$Date)
  
  freqs <- wqdata_A%>%split(.$LawaSiteID)%>%purrr::map(~freqCheck(.))%>%unlist
  wqdata_A$Frequency=freqs[wqdata_A$LawaSiteID]  
  rm(freqs)
  
  state <- c("Site","Catchment","Region","NZ")
  level <- c("LandcoverAltitude","Landcover","Altitude","None")
  sa11 <- StateAnalysis(wqdata_A,state[1],level[1])
  sa41 <- StateAnalysis(wqdata_A,state[4],level[1])
  sa42 <- StateAnalysis(wqdata_A,state[4],level[2])
  sa43 <- StateAnalysis(wqdata_A,state[4],level[3])
  sa44 <- StateAnalysis(wqdata_A,state[4],level[4])  #e.g. this gives median value across the whole country
  
  sa <- rbind(sa11,sa41,sa42,sa43,sa44)
  rm(wqdata_A)
  rm(sa11,sa41,sa42,sa43,sa44)
  return(sa)
}->sa
stopCluster(workers)
rm(workers)
cat(Sys.time()-startTime)  #19 seconds 13Aug2021

# State Analysis output contains quantiles for each Measurement by site.
names(sa) <-   c("LawaSiteID", "Measurement", "Q0", "Q25", "Q50","Q75", "Q100",
                 "SWQAltitude", "SWQLanduse", "Region","Agency", "CouncilSiteID", "SiteID", "N", "Scope")

sa <- sa%>%dplyr::select(LawaSiteID,CouncilSiteID,SiteID,Region,Agency,SWQAltitude,SWQLanduse,Measurement,Scope,Q0,Q25,Q50,Q75,Q100,N)
# filter sa to remove any LawaSiteIDS that are NA
sa <- sa[!is.na(sa$LawaSiteID),]

write.csv(sa,file=paste0("h:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),
                         "/sa",StartYear5,"-",EndYear,".csv"),row.names = F)
# sa <- read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis/",pattern=paste0("^sa",StartYear5,"-",EndYear,".csv"),recursive=T,full.names=T,ignore.case=T),1),stringsAsFactors = F)

#As of 2018 we're only doing it at site

ss1 <-   StateScore(df = sa,scopeIn = 'site',altitude = "",Landcover = "",wqparam = wqparam,comparison=1)
ss21 <-  StateScore(df = sa,scopeIn = 'site',altitude = "upland",Landcover = "",wqparam,comparison=2)
ss22 <-  StateScore(df = sa,scopeIn = 'site',altitude = "lowland",Landcover = "",wqparam,comparison=2)
ss31 <-  StateScore(df = sa,scopeIn = 'site',altitude = "",Landcover = "rural",wqparam,comparison=3)
ss32 <-  StateScore(df = sa,scopeIn = 'site',altitude = "",Landcover = "forest",wqparam,comparison=3)
ss33 <-  StateScore(df = sa,scopeIn = 'site',altitude = "",Landcover = "urban",wqparam,comparison=3)
ss411 <- StateScore(df = sa,scopeIn = 'site',altitude = "upland",Landcover = "rural",wqparam,comparison=4)
ss412 <- StateScore(df = sa,scopeIn = 'site',altitude = "upland",Landcover = "forest",wqparam,comparison=4)
ss413 <- StateScore(df = sa,scopeIn = 'site',altitude = "upland",Landcover = "urban",wqparam = wqparam,comparison=4)
ss421 <- StateScore(df = sa,scopeIn = 'site',altitude = "lowland",Landcover = "rural", wqparam = wqparam,comparison=4)
ss422 <- StateScore(df = sa,scopeIn = 'site',altitude = "lowland",Landcover = "forest",wqparam = wqparam,comparison=4)
ss423 <- StateScore(df = sa,scopeIn = 'site',altitude = "lowland",Landcover = "urban", wqparam = wqparam,comparison=4)
#LAWAState 1 is good, 4 is bad, quartiles
ss <- rbind(ss1,ss21,ss22,ss31,ss32,ss33,ss411,ss412,ss413,ss421,ss422,ss423)
rm(ss1,ss21,ss22,ss31,ss32,ss33,ss411,ss412,ss413,ss421,ss422,ss423)
ss=ss[order(ss$LawaSiteID),]


#ss <- read.csv(tail(dir(path = "h:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis",pattern = paste0("^state.*csv"),recursive = T,full.names=T,ignore.case = T),1),stringsAsFactors = F)

ss <- ss%>%dplyr::transmute(LawaID=LawaSiteID,
                            CouncilSiteID=CouncilSiteID,
                            Agency=Agency,
                            Parameter=Measurement,
                            Median=Q50,
                            StateScore=LAWAState,
                            ComparisonGroup=StateGroup)

#Make altitude and landgroup columns to reflect the grouping being used for comparison, (not the characteristics of the site). 
#Allow the appearance of 'all' as a Landcover or altitude

sssg=gsub(pattern = 'site\\|',replacement = "",x = ss$ComparisonGroup)
sssg=strsplit(sssg,'\\|')

AltitudeGroup = rep('All',dim(ss)[1])
AltitudeGroup[which(sapply(sssg,FUN=function(x)any('lowland'%in%x)))]='Lowland'
AltitudeGroup[which(sapply(sssg,FUN=function(x)any('upland' %in%x)))]='Upland'

LanduseGroup = rep('All',dim(ss)[1])
LanduseGroup[which(sapply(sssg,FUN=function(x)any('forest'%in%x)))]='Forest'
LanduseGroup[which(sapply(sssg,FUN=function(x)any('rural' %in%x)))]='Rural'
LanduseGroup[which(sapply(sssg,FUN=function(x)any('urban' %in%x)))]='Urban'

ss$Altitude=AltitudeGroup
ss$Landuse=LanduseGroup
rm(sssg)
rm(AltitudeGroup,LanduseGroup)

write.csv(ss,
          file=paste0("h:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),
                      "/AuditRiverState",format(Sys.time(),"%d%b%Y"),".csv"),row.names = F)


write.csv(ss%>%dplyr::select(LawaID,Parameter,Altitude,Landuse,Median,StateScore),
          file=paste0("h:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),
                         "/ITERiverState",format(Sys.time(),"%d%b%Y"),".csv"),row.names = F)

# ss <- read.csv(tail(dir(path = "h:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis",pattern = 'AuditRiverState',full.names = T,recursive=T),1),stringsAsFactors = F)


