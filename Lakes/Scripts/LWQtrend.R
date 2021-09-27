rm(list=ls())
library(tidyverse)
source("h:/ericg/16666LAWA/LAWA2021/Scripts/LAWAFunctions.R")
#source("h:/ericg/16666LAWA/LAWA2021/WaterQuality/scripts/SWQ_state_functions.R")
source("h:/ericg/16666LAWA/LWPTrends_v2101/LWPTrends_v2101.R")
try(dir.create(paste0("h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/",format(Sys.Date(),"%Y-%m-%d")),showWarnings = F))

Mode=function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

EndYear <- year(Sys.Date())-1
startYear5 <- EndYear - 5+1
startYear10 <- EndYear - 10+1
startYear15 <- EndYear - 15+1

lakesSiteTable=loadLatestSiteTableLakes()

#Load the latest made 
if(!exists('lakeData')){
  lakeDataFileName=tail(dir(path = "H:/ericg/16666LAWA/LAWA2021/Lakes/Data",pattern = "LakesWithMetadata.csv",
                            recursive = T,full.names = T,ignore.case=T),1)
  lakeData=read_csv(lakeDataFileName,guess_max = 50000)
  rm(lakeDataFileName)
  lakeData$myDate <- as.Date(as.character(lakeData$Date),"%d-%b-%y")
  lakeData$myDate[which(lakeData$Agency=='ac')] <- as.Date(lubridate::dmy(lakeData$Date[which(lakeData$Agency=='ac')]))
  lakeData <- GetMoreDateInfo(lakeData)
  lakeData$monYear = format(lakeData$myDate,"%b-%Y")
  lakeData$qtrYear = paste0(lakeData$Qtr,lakeData$Year)
  
  lakeData$Season <- lakeData$Month
  SeasonString <- sort(unique(lakeData$Season))
  lakeData <- lakeData%>%dplyr::rename("CenType"="centype")
  lakeData$CenType[lakeData$CenType%in%c("Left","L")]='lt'
  lakeData$CenType[lakeData$CenType%in%c("Right","R")]='gt'
  lakeData$CenType[!lakeData$CenType%in%c("lt","gt")]='not'
  
  lakeData$NewValues=lakeData$Value
  if(mean(lakeData$Value[which(lakeData$Measurement%in%c('NH4N','TN','TP'))],na.rm=T)<250){
    lakeData$Value[which(lakeData$Measurement%in%c('NH4N','TN','TP'))]=lakeData$Value[which(lakeData$Measurement%in%c('NH4N','TN','TP'))]*1000
  }
}


# lakeData$uclid = paste(lakeData$CouncilSiteID,lakeData$LawaSiteID,sep='||')
# lakesSiteTable$uclid = paste(lakesSiteTable$CouncilSiteID,lakesSiteTable$LawaSiteID,sep='||')

# https://www.lawa.org.nz/learn/factsheets/calculating-water-quality-trends/


#15 year trend ####
library(parallel)
library(doParallel)

lakeDatafor15=lakeData%>%filter(Year>=startYear15 & Year <= EndYear & !Measurement%in%c("pH","CYANOTOX"))

uLLSIDs=unique(lakeDatafor15$LawaSiteID)
uMeasures=unique(lakeDatafor15$Measurement)
nMax=length(table(lakeDatafor15$LawaSiteID,lakeDatafor15$Measurement)[table(lakeDatafor15$LawaSiteID,lakeDatafor15$Measurement)>0])
cat('\n',length(uLLSIDs),'\n')
u=1

startTime=Sys.time()
workers <- makeCluster(5)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  #library(doBy)
  library(plyr)
  library(dplyr)
  
})
foreach(u = u:length(uLLSIDs),.combine=rbind,.errorhandling="stop")%dopar%{
  # trendTable15=NULL
  # for(u in 1:length(uLLSIDs)){
  subDat=lakeDatafor15%>%dplyr::filter(LawaSiteID==uLLSIDs[u])
  siteTrendTable15=data.frame(LawaSiteID=uLLSIDs[u], Measurement=uMeasures, nMeasures=NA, nFirstYear=NA,
                              nLastYear=NA,numMonths=NA, numQuarters=NA, numYears=NA,
                              Observations=NA,KWstat=NA,pvalue=NA, SeasNote=NA,
                              nObs=NA, S=NA, VarS=NA, D=NA,tau=NA, Z=NA, p=NA, C=NA,Cd=NA, MKAnalysisNote=NA,
                              prop.censored=NA, prop.unique=NA, no.censorlevels=NA,
                              Median=NA, Sen_VarS=NA,AnnualSenSlope=NA, Intercept=NA, Sen_Lci=NA,
                              Sen_Uci=NA, SSAnalysisNote=NA,Sen_Probability=NA, Sen_Probabilitymax=NA,
                              Sen_Probabilitymin=NA, Percent.annual.change=NA,
                              standard=NA,ConfCat=NA, period=15, frequency=NA)
  for(uparam in 1:length(uMeasures)){
    subSubDat=subDat%>%filter(subDat$Measurement==uMeasures[uparam])%>%as.data.frame
    siteTrendTable15$nMeasures[uparam]=dim(subSubDat)[1]
    if(dim(subSubDat)[1]>0){
      subSubDat$Value=signif(subSubDat$Value,6)#Censoring fails with tiny machine rounding errors
      subSubDat$Season=factor(subSubDat$Month)
      siteTrendTable15$nFirstYear[uparam]=length(which(subSubDat$Year==startYear15))
      siteTrendTable15$nLastYear[uparam]=length(which(subSubDat$Year==EndYear))
      siteTrendTable15$numMonths[uparam]=length(unique(subSubDat$monYear[!is.na(subSubDat$Value)]))#dim(subSubDat)[1]
      siteTrendTable15$numQuarters[uparam]=length(unique(subSubDat$qtrYear[!is.na(subSubDat$Value)]))
      siteTrendTable15$numYears[uparam]=length(unique(subSubDat$Year[!is.na(subSubDat$Value)]))
      
      #For 15 year monthly we want 90% of measures and 90% of years
      if(siteTrendTable15$numMonths[uparam] >= 0.9*12*15 & siteTrendTable15$numYears[uparam]>=13){
        siteTrendTable15$frequency[uparam]='monthly'
      }else{
      
        subSubDat$Season=subSubDat$Qtr
        if(siteTrendTable15$numQuarters[uparam] >= 0.9*4*15 & siteTrendTable15$numYears[uparam] >=13){
          siteTrendTable15$frequency[uparam]='quarterly'
        }else{
          siteTrendTable15$frequency[uparam]=paste0('unassessed, nM=',siteTrendTable15$numMonths[uparam],'(162);',
                                                   ' nQ=',siteTrendTable15$numQuarters[uparam],'(54);',
                                                   ' nY=',siteTrendTable15$numYears[uparam],'(13)')
        }
      }
      if(siteTrendTable15$frequency[uparam]%in%c("quarterly","monthly")){
        SeasonString <- sort(unique(subSubDat$Season))
        st <- SeasonalityTest(x = subSubDat,main=uMeasures[uparam],ValuesToUse = "Value",do.plot =F)
        siteTrendTable15[uparam,names(st)] <- st
        if(!is.na(st$pvalue)&&st$pvalue<0.05){
          sk <- SeasonalKendall(x = subSubDat,ValuesToUse = "Value",HiCensor = T,doPlot = F)
          sss <- SeasonalSenSlope(HiCensor=T,x = subSubDat,ValuesToUse = "Value",ValuesToUseforMedian = "Value",doPlot = F)
          siteTrendTable15[uparam,names(sk)] <- sk
          siteTrendTable15[uparam,names(sss)] <- sss
          rm(sk,sss)
        }else{
          mk <- MannKendall(x = subSubDat,ValuesToUse = "Value",Year="Season",HiCensor = T,doPlot=F)
          ss <- SenSlope(HiCensor=T,x = subSubDat,ValuesToUse = "Value",ValuesToUseforMedian = "Value",doPlot = F)
          siteTrendTable15[uparam,names(mk)] <- mk
          siteTrendTable15[uparam,names(ss)] <- ss
          rm(mk,ss)
        }
        rm(st)
      }
      rm(subSubDat)
    }
  }
  rm(subDat,uparam)
  #trendTable15 =  rbind(trendTable15,siteTrendTable15)
  return(siteTrendTable15)
}-> trendTable15
stopCluster(workers)
Sys.time()-startTime
#13/8/21  16s 1296

rm(workers,uLLSIDs,uMeasures,u,lakeDatafor15)
rownames(trendTable15) <- NULL
trendTable15$Sen_Probability[trendTable15$Measurement!="Secchi"]=1-(trendTable15$Sen_Probability[trendTable15$Measurement!="Secchi"])
trendTable15$Probabilitymin[trendTable15$Measurement!="Secchi"]=1-(trendTable15$Probabilitymin[trendTable15$Measurement!="Secchi"])
trendTable15$Probabilitymax[trendTable15$Measurement!="Secchi"]=1-(trendTable15$Probabilitymax[trendTable15$Measurement!="Secchi"])
trendTable15$Cd[trendTable15$Measurement!="Secchi"]=1-(trendTable15$Cd[trendTable15$Measurement!="Secchi"])
trendTable15$Agency=lakesSiteTable$Agency[match(tolower(trendTable15$LawaSiteID),tolower(lakesSiteTable$LawaSiteID))]
trendTable15$Region=lakesSiteTable$Region[match(tolower(trendTable15$LawaSiteID),tolower(lakesSiteTable$LawaSiteID))]
trendTable15$LFENZID =    lakesSiteTable$LFENZID[match(tolower(trendTable15$LawaSiteID),tolower(lakesSiteTable$LawaSiteID))]
trendTable15$LType =    lakesSiteTable$LType[match(tolower(trendTable15$LawaSiteID),tolower(lakesSiteTable$LawaSiteID))]
trendTable15$ConfCat <- cut(trendTable15$Cd, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
                            labels = c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
trendTable15$ConfCat=factor(trendTable15$ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
trendTable15$TrendScore=as.numeric(trendTable15$ConfCat)-3
trendTable15$TrendScore[is.na(trendTable15$TrendScore)]<-(NA)

save(trendTable15,file=paste0("h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/Trend15Year.rData"))
rm(trendTable15)



#10 year trend ####
lakeDatafor10=lakeData%>%filter(Year>=startYear10 & Year <= EndYear & !Measurement%in%c("pH","CYANOTOX"))

uLLSIDs=unique(lakeDatafor10$LawaSiteID)
uMeasures=unique(lakeDatafor10$Measurement)
nMax=length(table(lakeDatafor10$LawaSiteID,lakeDatafor10$Measurement)[table(lakeDatafor10$LawaSiteID,lakeDatafor10$Measurement)>0])

cat('\n',length(uLLSIDs),'\n')
u=1

startTime=Sys.time()
workers <- makeCluster(5)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  library(plyr)
  library(dplyr)
  
})
foreach(u = u:length(uLLSIDs),.combine=rbind,.errorhandling="stop")%dopar%{
  subDat=lakeDatafor10%>%filter(LawaSiteID==uLLSIDs[u])
  siteTrendTable10=data.frame(LawaSiteID=uLLSIDs[u],Measurement=uMeasures, nMeasures=NA, nFirstYear=NA,
                              nLastYear=NA,numMonths=NA, numQuarters=NA, numYears=NA,
                              Observations=NA,KWstat=NA,pvalue=NA, SeasNote=NA,
                              nObs=NA, S=NA, VarS=NA, D=NA,tau=NA, Z=NA, p=NA, C=NA,Cd=NA, MKAnalysisNote=NA,
                              prop.censored=NA, prop.unique=NA, no.censorlevels=NA,
                              Median=NA, Sen_VarS=NA,AnnualSenSlope=NA, Intercept=NA, Sen_Lci=NA,
                              Sen_Uci=NA, SSAnalysisNote=NA,Sen_Probability=NA, Sen_Probabilitymax=NA,
                              Sen_Probabilitymin=NA, Percent.annual.change=NA,
                              standard=NA,ConfCat=NA, period=15, frequency=NA)
  for(uparam in 1:length(uMeasures)){
    subSubDat=subDat%>%filter(subDat$Measurement==uMeasures[uparam])%>%as.data.frame
    siteTrendTable10$nMeasures[uparam]=dim(subSubDat)[1]
    if(dim(subSubDat)[1]>0){
      subSubDat$Value=signif(subSubDat$Value,6)#Censoring fails with tiny machine rounding errors
      subSubDat$Season = subSubDat$Month
      siteTrendTable10$nFirstYear[uparam]=length(which(subSubDat$Year==startYear10))
      siteTrendTable10$nLastYear[uparam]=length(which(subSubDat$Year==EndYear))
      siteTrendTable10$numMonths[uparam]=length(unique(subSubDat$monYear[!is.na(subSubDat$Value)]))#dim(subSubDat)[1]
    siteTrendTable10$numQuarters[uparam]=length(unique(subSubDat$qtrYear[!is.na(subSubDat$Value)]))
        siteTrendTable10$numYears[uparam]=length(unique(subSubDat$Year[!is.na(subSubDat$Value)]))
      
      #For 10 year monthly we want 90% of measures and 90% of years
      if(siteTrendTable10$numMonths[uparam] >= 0.9*12*10 & siteTrendTable10$numYears[uparam]>=9){
        siteTrendTable10$frequency[uparam]='monthly'
      }else{
        subSubDat$Season=subSubDat$Qtr
        if(siteTrendTable10$numQuarters[uparam] >= 0.9*4*10 & siteTrendTable10$numYears[uparam] >=9){
          siteTrendTable10$frequency[uparam]='quarterly'
        }else{
          siteTrendTable10$frequency[uparam]=paste0('unassessed, nM=',siteTrendTable10$numMonths[uparam],'(108);',
                                                    ' nQ=',siteTrendTable10$numQuarters[uparam],'(36);',
                                                    ' nY=',siteTrendTable10$numYears[uparam],'(9)')
        }
      }
      if(siteTrendTable10$frequency[uparam]%in%c("monthly","quarterly")){
        st <- SeasonalityTest(x = subSubDat,main=uMeasures[uparam],ValuesToUse = "Value",do.plot =F)
        siteTrendTable10[uparam,names(st)] <- st
        if(!is.na(st$pvalue)&&st$pvalue<0.05){
          sk <- SeasonalKendall(x = subSubDat,ValuesToUse = "Value",HiCensor = T,doPlot = F)
          sss <- SeasonalSenSlope(HiCensor=T,x = subSubDat,ValuesToUse = "Value",ValuesToUseforMedian = "Value",doPlot = F)
          siteTrendTable10[uparam,names(sk)] <- sk
          siteTrendTable10[uparam,names(sss)] <- sss
          rm(sk,sss)
        }else{
          mk <- MannKendall(x = subSubDat,ValuesToUse = "Value",HiCensor = T,doPlot=F)
          ss <- SenSlope(HiCensor=T,x = subSubDat,ValuesToUse = "Value",ValuesToUseforMedian = "Value",doPlot = F)
          siteTrendTable10[uparam,names(mk)] <- mk
          siteTrendTable10[uparam,names(ss)] <- ss
          rm(mk,ss)
        }
        rm(st)
      }
      rm(subSubDat)
    }
  }
  rm(subDat,uparam)
  return(siteTrendTable10)
}-> trendTable10
stopCluster(workers)
rm(workers,uLLSIDs,uMeasures,u,lakeDatafor10)
Sys.time()-startTime
#13/8/21 16s 1288

rownames(trendTable10) <- NULL
trendTable10$Sen_Probability[trendTable10$Measurement!="Secchi"]=1-(trendTable10$Sen_Probability[trendTable10$Measurement!="Secchi"])
trendTable10$Probabilitymin[trendTable10$Measurement!="Secchi"]=1-(trendTable10$Probabilitymin[trendTable10$Measurement!="Secchi"])
trendTable10$Probabilitymax[trendTable10$Measurement!="Secchi"]=1-(trendTable10$Probabilitymax[trendTable10$Measurement!="Secchi"])
trendTable10$Cd[trendTable10$Measurement!="Secchi"]=1-(trendTable10$Cd[trendTable10$Measurement!="Secchi"])
trendTable10$Agency=lakesSiteTable$Agency[match(tolower(trendTable10$LawaSiteID),tolower(lakesSiteTable$LawaSiteID))]
trendTable10$Region=lakesSiteTable$Region[match(tolower(trendTable10$LawaSiteID),tolower(lakesSiteTable$LawaSiteID))]
trendTable10$LFENZID =    lakesSiteTable$LFENZID[match(tolower(trendTable10$LawaSiteID),tolower(lakesSiteTable$LawaSiteID))]
trendTable10$LType =    lakesSiteTable$LType[match(tolower(trendTable10$LawaSiteID),tolower(lakesSiteTable$LawaSiteID))]
trendTable10$ConfCat <- cut(trendTable10$Cd, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
                            labels = c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
trendTable10$ConfCat=factor(trendTable10$ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
trendTable10$TrendScore=as.numeric(trendTable10$ConfCat)-3
trendTable10$TrendScore[is.na(trendTable10$TrendScore)]<-(NA)
save(trendTable10,file=paste0("h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/Trend10Year.rData"))
rm(trendTable10)





#5 year trend ####
lakeDatafor5=lakeData%>%filter(Year>=startYear5 & Year <= EndYear & !Measurement%in%c("pH","CYANOTOX"))
lakeDatafor5$Season=lakeDatafor5$Month

uLLSIDs=unique(lakeDatafor5$LawaSiteID)
uMeasures=unique(lakeDatafor5$Measurement)
nMax=length(table(lakeDatafor5$LawaSiteID,lakeDatafor5$Measurement)[table(lakeDatafor5$LawaSiteID,lakeDatafor5$Measurement)>0])

cat('\n',length(uLLSIDs),'\n')
u=1

startTime=Sys.time()
workers <- makeCluster(5)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  library(plyr)
  library(dplyr)
  
})
foreach(u = u:length(uLLSIDs),.combine=rbind,.errorhandling="stop")%dopar%{
  subDat=lakeDatafor5%>%filter(LawaSiteID==uLLSIDs[u])
  siteTrendTable5=data.frame(LawaSiteID=uLLSIDs[u],Measurement=uMeasures, nMeasures=NA, nFirstYear=NA,
                             nLastYear=NA,numMonths=NA, numQuarters=NA, numYears=NA,
                             Observations=NA,KWstat=NA,pvalue=NA, SeasNote=NA,
                             nObs=NA, S=NA, VarS=NA, D=NA,tau=NA, Z=NA, p=NA, C=NA,Cd=NA, MKAnalysisNote=NA,
                             prop.censored=NA, prop.unique=NA, no.censorlevels=NA,
                             Median=NA, Sen_VarS=NA,AnnualSenSlope=NA, Intercept=NA, Sen_Lci=NA,
                             Sen_Uci=NA, SSAnalysisNote=NA,Sen_Probability=NA, Sen_Probabilitymax=NA,
                             Sen_Probabilitymin=NA, Percent.annual.change=NA,
                             standard=NA,ConfCat=NA, period=15, frequency=NA)
  for(uparam in 1:length(uMeasures)){
    subSubDat=subDat%>%filter(subDat$Measurement==uMeasures[uparam])%>%as.data.frame
    siteTrendTable5$nMeasures[uparam]=dim(subSubDat)[1]
    if(dim(subSubDat)[1]>0){
      subSubDat$Value=signif(subSubDat$Value,6)#Censoring fails with tiny machine rounding errors
      subSubDat$Season = subSubDat$Month
      siteTrendTable5$nFirstYear[uparam]=length(which(subSubDat$Year==startYear5))
      siteTrendTable5$nLastYear[uparam]=length(which(subSubDat$Year==EndYear))
      siteTrendTable5$numMonths[uparam]=length(unique(subSubDat$monYear[!is.na(subSubDat$Value)]))#dim(subSubDat)[1]
      siteTrendTable5$numQuarters[uparam]=length(unique(subSubDat$qtrYear[!is.na(subSubDat$Value)]))
      siteTrendTable5$numYears[uparam]=length(unique(subSubDat$Year[!is.na(subSubDat$Value)]))
      #For 5 year monthly we want 90% of measures 
      if(siteTrendTable5$numMonths[uparam] >= 0.9*12*5 & siteTrendTable5$numYears[uparam]>=4){
        siteTrendTable5$frequency[uparam]='monthly'
      }else{
        siteTrendTable5$frequency[uparam]=paste0('unassessed, nM=',siteTrendTable5$numMonths[uparam],'(54);',
                                                 ' nY=',siteTrendTable5$numYears[uparam],'(4)')
      }
      if(siteTrendTable5$frequency[uparam]=="monthly"){
        st <- SeasonalityTest(x = subSubDat,main=uMeasures[uparam],ValuesToUse = "Value",do.plot =F)
        siteTrendTable5[uparam,names(st)] <- st
        if(!is.na(st$pvalue)&&st$pvalue<0.05){
          sk <- SeasonalKendall(x = subSubDat,ValuesToUse = "Value",HiCensor = T,doPlot = F)
          sss <- SeasonalSenSlope(HiCensor=T,x = subSubDat,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
          siteTrendTable5[uparam,names(sk)] <- sk
          siteTrendTable5[uparam,names(sss)] <- sss
          rm(sk,sss)
        }else{
          mk <- MannKendall(x = subSubDat,ValuesToUse = "Value",HiCensor = T,doPlot=F)
          ss <- SenSlope(HiCensor=T,x = subSubDat,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
          siteTrendTable5[uparam,names(mk)] <- mk
          siteTrendTable5[uparam,names(ss)] <- ss
          rm(mk,ss)
        }
        rm(st)
      }
      rm(subSubDat)
    }
  }
  rm(subDat,uparam)
  return(siteTrendTable5)
}->trendTable5
stopCluster(workers)
rm(workers,uLLSIDs,uMeasures,u,lakeDatafor5)

Sys.time()-startTime
#13/8/21 8.8s 1288



rownames(trendTable5) <- NULL
trendTable5$Sen_Probability[trendTable5$Measurement!="Secchi"]=1-(trendTable5$Sen_Probability[trendTable5$Measurement!="Secchi"])
trendTable5$Probabilitymin[trendTable5$Measurement!="Secchi"]=1-(trendTable5$Probabilitymin[trendTable5$Measurement!="Secchi"])
trendTable5$Probabilitymax[trendTable5$Measurement!="Secchi"]=1-(trendTable5$Probabilitymax[trendTable5$Measurement!="Secchi"])
trendTable5$Cd[trendTable5$Measurement!="Secchi"]=1-(trendTable5$Cd[trendTable5$Measurement!="Secchi"])
trendTable5$Agency=lakesSiteTable$Agency[match(tolower(trendTable5$LawaSiteID),tolower(lakesSiteTable$LawaSiteID))]
trendTable5$Region =    lakesSiteTable$Region[match(tolower(trendTable5$LawaSiteID),tolower(lakesSiteTable$LawaSiteID))]
trendTable5$LFENZID =    lakesSiteTable$LFENZID[match(tolower(trendTable5$LawaSiteID),tolower(lakesSiteTable$LawaSiteID))]
trendTable5$LType =    lakesSiteTable$LType[match(tolower(trendTable5$LawaSiteID),tolower(lakesSiteTable$LawaSiteID))]
trendTable5$ConfCat <- cut(trendTable5$Cd, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
                           labels = c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
trendTable5$ConfCat=factor(trendTable5$ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
trendTable5$TrendScore=as.numeric(trendTable5$ConfCat)-3
trendTable5$TrendScore[is.na(trendTable5$TrendScore)]<-(NA)
save(trendTable5,file=paste0("h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/Trend5Year.rData"))
rm(trendTable5)




#Reload, combine and output ####

load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/",
              pattern = "Trend15Year.rData",full.names = T,recursive = T),1),verbose =T)
load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/",
              pattern = "Trend10Year.rData",full.names = T,recursive = T),1),verbose = T)
load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/",
              pattern = "Trend5Year.rData",full.names = T,recursive = T),1),verbose = T)



if(year(Sys.Date())==2021){
  bugFix15 = which(trendTable15$Z==0)
  table(trendTable15$Agency[bugFix15])
  trendTable15$p[bugFix15] <- 1
  trendTable15$C[bugFix15] <- 0.5
  trendTable15$Cd[bugFix15] <- 0.5
  trendTable15$ConfCat[bugFix15] <- "Indeterminate"
  trendTable15$TrendScore[bugFix15] <- 0
  rm(bugFix15)
  bugFix10 = which(trendTable10$Z==0)
  table(trendTable10$Agency[bugFix10])
  trendTable10$p[bugFix10] <- 1
  trendTable10$C[bugFix10] <- 0.5
  trendTable10$Cd[bugFix10] <- 0.5
  trendTable10$ConfCat[bugFix10] <- "Indeterminate"
  trendTable10$TrendScore[bugFix10] <- 0
  rm(bugFix10)
  bugFix5 = which(trendTable5$Z==0)
  table(trendTable5$Agency[bugFix5])
  trendTable5$p[bugFix5] <- 1
  trendTable5$C[bugFix5] <- 0.5
  trendTable5$Cd[bugFix5] <- 0.5
  trendTable5$ConfCat[bugFix5] <- "Indeterminate"
  trendTable5$TrendScore[bugFix5] <- 0
  rm(bugFix5)
}




combTrend <- rbind(rbind(trendTable15,trendTable10),trendTable5)

combTrend$LawaSiteID = sapply(combTrend$LawaSiteID,FUN=function(s)strFrom(s=s,c='\\|\\|'))

combTrend$CouncilSiteID = lakesSiteTable$CouncilSiteID[match(tolower(gsub('_NIWA','',combTrend$LawaSiteID)),tolower(lakesSiteTable$LawaSiteID))]
combTrend <- combTrend%>%
  dplyr::select(LawaSiteID,CouncilSiteID,LFENZID,Agency,Region,LType,
                Measurement,nMeasures,frequency,period,TrendScore,ConfCat,MKAnalysisNote,SSAnalysisNote,everything())

write.csv(combTrend,
          paste0("h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/",
                 format(Sys.Date(),"%Y-%m-%d"),"/LakesWQ_Trend",
                 format(Sys.time(),"%d%b%Y"),".csv"),row.names = F)

rm(combTrend)

#For ITE
#LakeSiteTrend5
trendTable5$TrendScore[is.na(trendTable5$TrendScore)] <- (-99)
trendTable10$TrendScore[is.na(trendTable10$TrendScore)] <- (-99)
trendTable15$TrendScore[is.na(trendTable15$TrendScore)] <- (-99)
write.csv(trendTable5%>%dplyr::transmute(LAWAID=LawaSiteID,
                                         Parameter=Measurement,
                                         Trend5=TrendScore),
          file=paste0("h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                      "/ITELakeSiteTrend5",format(Sys.time(),"%d%b%Y"),".csv"),row.names=F)
write.csv(trendTable10%>%dplyr::transmute(LAWAID=LawaSiteID,
                                         Parameter=Measurement,
                                         Trend10=TrendScore),
          file=paste0("h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                      "/ITELakeSiteTrend10",format(Sys.time(),"%d%b%Y"),".csv"),row.names=F)
write.csv(trendTable15%>%dplyr::transmute(LAWAID=LawaSiteID,
                                         Parameter=Measurement,
                                         Trend15=TrendScore),
          file=paste0("h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                      "/ITELakeSiteTrend15",format(Sys.time(),"%d%b%Y"),".csv"),row.names=F)


# combTrend <- read.csv(tail(dir("h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/","LakesWQ_Trend",recursive = T,full.names = T,ignore.case = T),1),stringsAsFactors = F)

