rm(list=ls())
library(tidyverse)
library(parallel)
library(doParallel)
source("h:/ericg/16666LAWA/LWPTrends_v1901/LWPTrends_v1901.R")
source("h:/ericg/16666LAWA/LAWA2020/Scripts/LAWAFunctions.R")
source("h:/ericg/16666LAWA/LAWA2020/WaterQuality/scripts/SWQ_state_functions.R")
dir.create(paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)

startTime=Sys.time()
Mode=function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

EndYear <- lubridate::year(Sys.Date())-1
startYear5 <- EndYear - 5+1
startYear10 <- EndYear - 10+1
startYear15 <- EndYear - 15+1

riverSiteTable=loadLatestSiteTableRiver()

#Load the latest made 
if(!exists('wqdata')){
  wqdataFileName=tail(dir(path = "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data",pattern = "AllCouncils.csv",recursive = T,full.names = T),1)
  cat(wqdataFileName)
  wqdata=read_csv(wqdataFileName,guess_max = 100000)%>%as.data.frame
  rm(wqdataFileName)
  wqdata$myDate <- as.Date(as.character(wqdata$Date),"%d-%b-%Y")
  wqdata$myDate[wqdata$myDate<as.Date('2000-01-01')] <- as.Date(as.character(wqdata$Date[wqdata$myDate<as.Date('2000-01-01')]),"%d-%b-%y")
  wqdata <- GetMoreDateInfo(wqdata)
  wqdata$monYear = format(wqdata$myDate,"%b-%Y")
  wqdata$qtrYear = paste0(wqdata$Qtr,wqdata$Year)
  wqdata$CenType[wqdata$CenType%in%c("Left","L")]='lt'
  wqdata$CenType[wqdata$CenType%in%c("Right","R")]='gt'
  wqdata$CenType[!wqdata$CenType%in%c("lt","gt")]='not'
  
  wqdata$NewValues=wqdata$Value
}

#15 year trend ####
datafor15=wqdata%>%filter(Year>=startYear15 & Year <= EndYear & !Measurement%in%c("PH","TURBFNU"))

usites=unique(datafor15$LawaSiteID)
uMeasures=unique(datafor15$Measurement)
if("TURBFNU"%in%uMeasures){uMeasures=uMeasures[-which(uMeasures=="TURBFNU")]}
cat('\n',length(usites),'\n')
usite=1
workers <- makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  # library(doBy)
  library(plyr)
  library(dplyr)
  
})
startTime=Sys.time()
foreach(usite = usite:length(usites),.combine=rbind,.errorhandling="stop")%dopar%{
  siteTrendTable15=data.frame(LawaSiteID=usites[usite],Measurement=as.character(uMeasures),nMeasures = NA_integer_,
                              nFirstYear=NA_real_,nLastYear=NA_real_,numMonths=NA_real_,numQuarters=NA_real_,numYears=NA_real_,
                              Observations=NA_real_,KWstat = NA_real_,pvalue = NA_real_,
                              nObs = NA_integer_, S = NA_real_, VarS = NA_real_,D = NA_real_, tau = NA_real_,
                              Z = NA_real_, p = NA_real_,MKProbability=NA_real_,AnalysisNote=NA_real_,prop.censored=NA_real_,
                              prop.unique=NA_real_,no.censorlevels=NA_real_,Median = NA_real_, Sen_VarS = NA_real_,
                              AnnualSenSlope = NA_real_,Intercept = NA_real_, Lci = NA_real_, Uci = NA_real_,
                              AnalysisNoteSS=NA_real_, Sen_Probability = NA_real_,Probabilitymax = NA_real_,
                              Probabilitymin = NA_real_, Percent.annual.change = NA_real_,standard=NA_real_,
                              ConfCat=NA_real_,period=15,frequency=NA_real_)
  subDat=datafor15%>%dplyr::filter(LawaSiteID==usites[usite])
  for(uparam in 1:length(uMeasures)){
    subSubDat=subDat%>%filter(subDat$Measurement==uMeasures[uparam])
    siteTrendTable15$nMeasures[uparam]=dim(subSubDat)[1]
    if(dim(subSubDat)[1]>0){
      SSD_med <- subSubDat#%>%
      SSD_med$Value=signif(SSD_med$Value,6)#Censoring fails with tiny machine rounding errors
      SSD_med$Season = factor(SSD_med$Month)
      siteTrendTable15$nFirstYear[uparam]=length(which(SSD_med$Year==startYear15))
      siteTrendTable15$nLastYear[uparam]=length(which(SSD_med$Year==EndYear))
      siteTrendTable15$numMonths[uparam]=length(unique(SSD_med$monYear[!is.na(SSD_med$Value)]))#dim(SSD_med)[1]
      siteTrendTable15$numQuarters[uparam]=length(unique(SSD_med$qtrYear[!is.na(SSD_med$Value)]))
      siteTrendTable15$numYears[uparam]=length(unique(SSD_med$Year[!is.na(SSD_med$Value)]))
      
      #For 15 year monthly we want 90% of measures and 90% of years
      if(siteTrendTable15$numMonths[uparam] >= 0.9*12*15 & siteTrendTable15$numYears[uparam]>=13){ #162
        siteTrendTable15$frequency[uparam]='monthly'
      }else{
        SSD_med$Season=SSD_med$Qtr
        if(siteTrendTable15$numQuarters[uparam] >= 0.9*4*15 & siteTrendTable15$numYears[uparam] >=13){
          siteTrendTable15$frequency[uparam]='quarterly'
        }else{
          siteTrendTable15$frequency[uparam]='unassessed'
        }
      }
      if(siteTrendTable15$frequency[uparam]!='unassessed'){
        SeasonString <- sort(unique(SSD_med$Season))
        st <- SeasonalityTest(x = SSD_med,main=uMeasures[uparam],ValuesToUse = "Value",do.plot =F)
        siteTrendTable15[uparam,names(st)] <- st
        if(!is.na(st$pvalue)&&st$pvalue<0.05){
          sk <- SeasonalKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot = F)
          sss <- SeasonalSenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
          siteTrendTable15[uparam,names(sk)] <- sk
          siteTrendTable15[uparam,names(sss)] <- sss
          rm(sk,sss)
        }else{
          mk <- MannKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot=F)
          ss <- SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
          siteTrendTable15[uparam,names(mk)] <- mk
          siteTrendTable15[uparam,names(ss)] <- ss
          rm(mk,ss)
        }
        rm(st)
      }
      rm(subSubDat)
      rm(SSD_med)
    }
  }
  rm(subDat,uparam)
  return(siteTrendTable15)
}-> trendTable15
stopCluster(workers)
rm(workers,usites,uMeasures,usite,datafor15)
cat(Sys.time()-startTime) #Jun23 2.41

rownames(trendTable15) <- NULL
trendTable15$Sen_Probability[trendTable15$Measurement!="BDISC"]=1-(trendTable15$Sen_Probability[trendTable15$Measurement!="BDISC"])
trendTable15$Probabilitymin[trendTable15$Measurement!="BDISC"]=1-(trendTable15$Probabilitymin[trendTable15$Measurement!="BDISC"])
trendTable15$Probabilitymax[trendTable15$Measurement!="BDISC"]=1-(trendTable15$Probabilitymax[trendTable15$Measurement!="BDISC"])
trendTable15$MKProbability[trendTable15$Measurement!="BDISC"]=1-(trendTable15$MKProbability[trendTable15$Measurement!="BDISC"])
trendTable15$Agency=riverSiteTable$Agency[match(trendTable15$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable15$SWQAltitude =  riverSiteTable$SWQAltitude[match(trendTable15$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable15$SWQLanduse =   riverSiteTable$SWQLanduse[match(trendTable15$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable15$Region =    riverSiteTable$Region[match(trendTable15$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable15$ConfCat <- cut(trendTable15$MKProbability, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
                            labels = c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
trendTable15$ConfCat=factor(trendTable15$ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
trendTable15$TrendScore=as.numeric(trendTable15$ConfCat)-3
trendTable15$TrendScore[is.na(trendTable15$TrendScore)]<-(NA)
save(trendTable15,file=paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/Trend15Year.rData"))
rm(trendTable15)



#10 year trend ####
datafor10=wqdata%>%filter(Year>=startYear10 & Year <= EndYear & !Measurement%in%c("PH","TURBFNU"))%>%drop_na(LawaSiteID)

usites=unique(datafor10$LawaSiteID)
uMeasures=unique(datafor10$Measurement)
cat('\n',length(usites),'\n')
usite=1
library(parallel)
library(doParallel)
workers <- makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  library(plyr)
  library(dplyr)
  
})
startTime=Sys.time()
foreach(usite = usite:length(usites),.combine=rbind,.errorhandling="stop")%dopar%{
  siteTrendTable10=data.frame(LawaSiteID=usites[usite],Measurement=as.character(uMeasures),nMeasures = NA_integer_,
                              nFirstYear=NA_real_,nLastYear=NA_real_,numMonths=NA_real_,numQuarters=NA_real_,numYears=NA_real_,
                              Observations=NA_real_,KWstat = NA_real_,pvalue = NA_real_,
                              nObs = NA_integer_, S = NA_real_, VarS = NA_real_,D = NA_real_, tau = NA_real_,
                              Z = NA_real_, p = NA_real_,MKProbability=NA_real_,AnalysisNote=NA_real_,prop.censored=NA_real_,
                              prop.unique=NA_real_,no.censorlevels=NA_real_,Median = NA_real_, Sen_VarS = NA_real_,
                              AnnualSenSlope = NA_real_,Intercept = NA_real_, Lci = NA_real_, Uci = NA_real_,
                              AnalysisNoteSS=NA_real_, Sen_Probability = NA_real_,Probabilitymax = NA_real_,
                              Probabilitymin = NA_real_, Percent.annual.change = NA_real_,standard=NA_real_,
                              ConfCat=NA_real_,period=10,frequency=NA_real_)
  subDat=datafor10%>%dplyr::filter(LawaSiteID==usites[usite])
  for(uparam in 1:length(uMeasures)){
    subSubDat=subDat%>%filter(subDat$Measurement==uMeasures[uparam])
    siteTrendTable10$nMeasures[uparam]=dim(subSubDat)[1]
    if(dim(subSubDat)[1]>0){
      SSD_med <- subSubDat#%>%
      SSD_med$Value=signif(SSD_med$Value,6)#Censoring fails with tiny machine rounding errors
      SSD_med$Season = SSD_med$Month
      siteTrendTable10$nFirstYear[uparam]=length(which(SSD_med$Year==startYear10))
      siteTrendTable10$nLastYear[uparam]=length(which(SSD_med$Year==EndYear))
      siteTrendTable10$numMonths[uparam]=length(unique(SSD_med$monYear[!is.na(SSD_med$Value)]))#dim(SSD_med)[1]
      siteTrendTable10$numQuarters[uparam]=length(unique(SSD_med$qtrYear[!is.na(SSD_med$Value)]))#dim(SSD_med)[1]
      siteTrendTable10$numYears[uparam]=length(unique(SSD_med$Year[!is.na(SSD_med$Value)]))
      
      #For 10 year monthly we want 90% of measures (108) and 90% of years
      if(siteTrendTable10$numMonths[uparam] >= 0.9*12*10 & siteTrendTable10$numYears[uparam]>=9){
        siteTrendTable10$frequency[uparam]='monthly'
      }else{
        SSD_med$Season=SSD_med$Qtr
        #or 36 quarters
        if(siteTrendTable10$numQuarters[uparam] >= 0.9*4*10 & siteTrendTable10$numYears[uparam] >=9){
          siteTrendTable10$frequency[uparam]='quarterly'
        }else{
          siteTrendTable10$frequency[uparam]='unassessed'
        }
      }
      if(siteTrendTable10$frequency[uparam]!='unassessed'){
        st <- SeasonalityTest(x = SSD_med,main=uMeasures[uparam],ValuesToUse = "Value",do.plot =F)
        siteTrendTable10[uparam,names(st)] <- st
        if(!is.na(st$pvalue)&&st$pvalue<0.05){
          sk <- SeasonalKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot = F)
          sss <- SeasonalSenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
          sk$AnalysisNote=as.character(sk$AnalysisNote)
          sss$AnalysisNoteSS=as.character(sss$AnalysisNoteSS)
          siteTrendTable10[uparam,names(sk)] <- sk
          siteTrendTable10[uparam,names(sss)] <- sss
          rm(sk,sss)
        }else{
          mk <- MannKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot=F)
          ss <- SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
          mk$AnalysisNote=as.character(mk$AnalysisNote)
          ss$AnalysisNoteSS=as.character(ss$AnalysisNoteSS)
          siteTrendTable10[uparam,names(mk)] <- mk
          siteTrendTable10[uparam,names(ss)] <- ss
          rm(mk,ss)
        }
        rm(st)
      }
      rm(subSubDat)
      rm(SSD_med)
    }
  }
  rm(subDat,uparam)
  return(siteTrendTable10)
}-> trendTable10
stopCluster(workers)
rm(workers,usites,uMeasures,usite,datafor10)
cat(Sys.time()-startTime) #2.48

rownames(trendTable10) <- NULL
trendTable10$Sen_Probability[trendTable10$Measurement!="BDISC"]=1-(trendTable10$Sen_Probability[trendTable10$Measurement!="BDISC"])
trendTable10$Probabilitymin[trendTable10$Measurement!="BDISC"]=1-(trendTable10$Probabilitymin[trendTable10$Measurement!="BDISC"])
trendTable10$Probabilitymax[trendTable10$Measurement!="BDISC"]=1-(trendTable10$Probabilitymax[trendTable10$Measurement!="BDISC"])
trendTable10$MKProbability[trendTable10$Measurement!="BDISC"]=1-(trendTable10$MKProbability[trendTable10$Measurement!="BDISC"])
trendTable10$Agency=riverSiteTable$Agency[match(trendTable10$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable10$SWQAltitude =  riverSiteTable$SWQAltitude[match(trendTable10$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable10$SWQLanduse =   riverSiteTable$SWQLanduse[match(trendTable10$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable10$Region =    riverSiteTable$Region[match(trendTable10$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable10$ConfCat <- cut(trendTable10$MKProbability, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
                            labels = c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
trendTable10$ConfCat=factor(trendTable10$ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
trendTable10$TrendScore=as.numeric(trendTable10$ConfCat)-3
trendTable10$TrendScore[is.na(trendTable10$TrendScore)]<-(NA)
save(trendTable10,file=paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/Trend10Year.rData"))
rm(trendTable10)


#5 year trend ####
datafor5=wqdata%>%filter(Year>=startYear5 & Year <= EndYear & !Measurement%in%c("PH","TURBFNU"))

usites=unique(datafor5$LawaSiteID)
uMeasures=unique(datafor5$Measurement)
cat('\n',length(usites),'\n')
usite=1
workers <- makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  library(plyr)
  library(dplyr)
  
})
startTime=Sys.time()
foreach(usite = usite:length(usites),.combine=rbind,.errorhandling="stop")%dopar%{
  siteTrendTable5=data.frame(LawaSiteID=usites[usite],Measurement=as.character(uMeasures),nMeasures = NA_integer_,
                             nFirstYear=NA_real_,nLastYear=NA_real_,numMonths=NA_real_,numQuarters=NA_real_,numYears=NA_real_,
                             Observations=NA_real_,KWstat = NA_real_,pvalue = NA_real_,
                             nObs = NA_integer_, S = NA_real_, VarS = NA_real_,D = NA_real_, tau = NA_real_,
                             Z = NA_real_, p = NA_real_,MKProbability=NA_real_,AnalysisNote=NA_real_,prop.censored=NA_real_,
                             prop.unique=NA_real_,no.censorlevels=NA_real_,Median = NA_real_, Sen_VarS = NA_real_,
                             AnnualSenSlope = NA_real_,Intercept = NA_real_, Lci = NA_real_, Uci = NA_real_,
                             AnalysisNoteSS=NA_real_, Sen_Probability = NA_real_,Probabilitymax = NA_real_,
                             Probabilitymin = NA_real_, Percent.annual.change = NA_real_,standard=NA_real_,
                             ConfCat=NA_real_,period=5,frequency=NA_real_)
  subDat=datafor5%>%dplyr::filter(LawaSiteID==usites[usite])
  for(uparam in 1:length(uMeasures)){
    subSubDat=subDat%>%filter(subDat$Measurement==uMeasures[uparam])
    siteTrendTable5$nMeasures[uparam]=dim(subSubDat)[1]
    if(dim(subSubDat)[1]>0){
      SSD_med <- subSubDat#%>%
      SSD_med$Value=signif(SSD_med$Value,6)#Censoring fails with tiny machine rounding errors
      SSD_med$Season = SSD_med$Month
      siteTrendTable5$nFirstYear[uparam]=length(which(SSD_med$Year==startYear5))
      siteTrendTable5$nLastYear[uparam]=length(which(SSD_med$Year==EndYear))
      siteTrendTable5$numMonths[uparam]=length(unique(SSD_med$monYear[!is.na(SSD_med$Value)]))#dim(SSD_med)[1]
      siteTrendTable5$numQuarters[uparam]=length(unique(SSD_med$qtrYear[!is.na(SSD_med$Value)]))#dim(SSD_med)[1]
      siteTrendTable5$numYears[uparam]=length(unique(SSD_med$Year[!is.na(SSD_med$Value)]))
      
      #For 5 year monthly we want 90% of measures 
      if(siteTrendTable5$numMonths[uparam] >= 0.9*12*5){
        siteTrendTable5$frequency[uparam]='monthly'
      }else{
        siteTrendTable5$frequency[uparam]='unassessed'
      }
      if(siteTrendTable5$frequency[uparam]!='unassessed'){
        st <- SeasonalityTest(x = SSD_med,main=uMeasures[uparam],ValuesToUse = "Value",do.plot =F)
        siteTrendTable5[uparam,names(st)] <- st
        if(!is.na(st$pvalue)&&st$pvalue<0.05){
          sk <- SeasonalKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot = F)
          sss <- SeasonalSenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
          siteTrendTable5[uparam,names(sk)] <- sk
          siteTrendTable5[uparam,names(sss)] <- sss
          rm(sk,sss)
        }else{
          mk <- MannKendall(x = SSD_med,ValuesToUse = "Value",HiCensor = T,doPlot=F)
          ss <- SenSlope(HiCensor=T,x = SSD_med,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
          siteTrendTable5[uparam,names(mk)] <- mk
          siteTrendTable5[uparam,names(ss)] <- ss
          rm(mk,ss)
        }
        rm(st)
      }
      rm(subSubDat)
      rm(SSD_med)
    }
  }
  rm(subDat,uparam)
  return(siteTrendTable5)
}-> trendTable5
stopCluster(workers)
rm(workers,usites,uMeasures,usite,datafor5)
cat(Sys.time()-startTime) #23Jun 52.5

rownames(trendTable5) <- NULL
trendTable5$Sen_Probability[trendTable5$Measurement!="BDISC"]=1-(trendTable5$Sen_Probability[trendTable5$Measurement!="BDISC"])
trendTable5$Probabilitymin[trendTable5$Measurement!="BDISC"]=1-(trendTable5$Probabilitymin[trendTable5$Measurement!="BDISC"])
trendTable5$Probabilitymax[trendTable5$Measurement!="BDISC"]=1-(trendTable5$Probabilitymax[trendTable5$Measurement!="BDISC"])
trendTable5$MKProbability[trendTable5$Measurement!="BDISC"]=1-(trendTable5$MKProbability[trendTable5$Measurement!="BDISC"])
trendTable5$Agency=riverSiteTable$Agency[match(trendTable5$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable5$SWQAltitude =  riverSiteTable$SWQAltitude[match(trendTable5$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable5$SWQLanduse =   riverSiteTable$SWQLanduse[match(trendTable5$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable5$Region =    riverSiteTable$Region[match(trendTable5$LawaSiteID,riverSiteTable$LawaSiteID)]
trendTable5$ConfCat <- cut(trendTable5$MKProbability, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
                           labels = c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
trendTable5$ConfCat=factor(trendTable5$ConfCat,levels=c("Very likely degrading", "Likely degrading", "Indeterminate", 
                                                        "Likely improving", "Very likely improving"))
trendTable5$TrendScore=as.numeric(trendTable5$ConfCat)-3
trendTable5$TrendScore[is.na(trendTable5$TrendScore)]<-(NA)
save(trendTable5,file=paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/Trend5Year.rData"))
rm(trendTable5)


riverSiteTable=loadLatestSiteTableRiver()
load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",
              pattern = "Trend15Year.rData",full.names = T,recursive = T),1),verbose =T)
load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",
              pattern = "Trend10Year.rData",full.names = T,recursive = T),1),verbose = T)
load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",
              pattern = "Trend5Year.rData",full.names = T,recursive = T),1),verbose = T)


#number parameters per agency with trends, on average per site
table(trendTable15$Agency)/table(riverSiteTable$Agency)[match(names(table(trendTable15$Agency)),names(table(riverSiteTable$Agency)))]
table(trendTable10$Agency)/table(riverSiteTable$Agency)[match(names(table(trendTable10$Agency)),names(table(riverSiteTable$Agency)))]
table(trendTable5$Agency)/table(riverSiteTable$Agency)[match(names(table(trendTable5$Agency)),names(table(riverSiteTable$Agency)))]

trendTable5%>%group_by(Agency)%>%dplyr::select(Agency,frequency)%>%table
trendTable10%>%group_by(Agency)%>%dplyr::select(Agency,frequency)%>%table
trendTable15%>%group_by(Agency)%>%dplyr::select(Agency,frequency)%>%table



MCItrend=read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis",
                           pattern='MacroMCI_Trend',full.names = T,recursive = T,ignore.case = T),1),stringsAsFactors = F)
MCItrend%>%group_by(Agency)%>%dplyr::select(Agency,frequency)%>%table

# #Remove MDC DRP and ECOLI
# #See email Steffi Henkel 14/9/2018 to Kati Doehring, Eric Goodwin, Abi Loughnan and Peter Hamill
if(any(trendTable10$Agency=="mdc" & trendTable10$Measurement=="DRP")){
  trendTable10 <- trendTable10[-which(trendTable10$Agency=='mdc'& trendTable10$Measurement=="DRP"),]
  # 8264 to 8232
}
if(any(trendTable10$Agency=="mdc" & trendTable10$Measurement=="ECOLI")){
  trendTable10 <- trendTable10[-which(trendTable10$Agency=='mdc'& trendTable10$Measurement=="ECOLI"),]
  # to 8200
}
if(any(trendTable15$Agency=="mdc" & trendTable15$Measurement=="DRP")){
  trendTable15 <- trendTable15[-which(trendTable15$Agency=='mdc'& trendTable15$Measurement=="DRP"),]
  # 8264 to 8232
}
if(any(trendTable15$Agency=="mdc" & trendTable15$Measurement=="ECOLI")){
  trendTable15 <- trendTable15[-which(trendTable15$Agency=='mdc'& trendTable15$Measurement=="ECOLI"),]
  # to 8200
}


#Combine WQ trends
combTrend <- rbind(rbind(trendTable15,trendTable10),trendTable5)
combTrend$CouncilSiteID = riverSiteTable$CouncilSiteID[match(tolower(gsub('_NIWA','',combTrend$LawaSiteID)),tolower(riverSiteTable$LawaSiteID))]
#19840 23Jun
#24664 21Aug

#Save for ITE
combTrend$SWQAltitude=pseudo.titlecase(combTrend$SWQAltitude)
combTrend$SWQLanduse=pseudo.titlecase(combTrend$SWQLanduse)
combTrend$TrendScore[is.na(combTrend$TrendScore)] <- -99
write.csv(combTrend%>%transmute(LAWAID=LawaSiteID,
                                Parameter=Measurement,
                                Altitude=SWQAltitude,
                                Landuse=SWQLanduse,
                                TrendScore=TrendScore,
                                TrendFrequency=frequency,
                                TrendPeriod=period),
          file=paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                      "/ITERiverTrend",format(Sys.time(),"%d%b%Y"),".csv"),row.names=F)



#Add MCI trend and save ####
MCItrend=read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis",
                           pattern='MacroMCI_Trend',full.names = T,recursive = T,ignore.case = T),1),stringsAsFactors = F)
MCItrend$TrendScore[is.na(MCItrend$TrendScore)] <- -99
combTrend <- rbind(combTrend%>%
                     dplyr::select(LawaSiteID,CouncilSiteID,Agency,Region,Measurement,
                                   nMeasures,frequency,period,TrendScore,ConfCat,AnalysisNote,AnalysisNoteSS,everything()),
                   MCItrend%>%
                     dplyr::filter(period>=10)%>%
                     dplyr::select(LawaSiteID,CouncilSiteID,Agency,Region,Measurement,
                                   nMeasures,frequency,period,TrendScore,ConfCat,AnalysisNote,AnalysisNoteSS,everything()))
table(combTrend$frequency,combTrend$period)

write.csv(combTrend,
          paste0("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),
                 "/RiverWQ_Trend",format(Sys.time(),"%d%b%Y"),".csv"),row.names = F)

# combTrend=read.csv(tail(dir("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/","RiverWQ_Trend",	recursive = T,full.names = T,ignore.case=T),1),stringsAsFactors = F)

if(0){
#Audit trend attrition
  trendTable15$AnalysisNote[trendTable15$numMonths<163&trendTable15$numQuarters<54] <- '<54 quarters & <162 months'
  trendTable15$AnalysisNote[trendTable15$nMeasures==0] <- 'no data'
  trendTable10$AnalysisNote[trendTable10$numMonths<108&trendTable10$numQuarters<36] <- '<36 quarters & <108 months'
  trendTable10$AnalysisNote[trendTable10$nMeasures==0] <- 'no data'
  trendTable5$AnalysisNote[trendTable5$numMonths<54] <- '<54 months'
  trendTable5$AnalysisNote[trendTable5$nMeasures==0] <- 'no data'
  table(trendTable15$AnalysisNote[trendTable15$frequency=='unassessed'])
  table(trendTable15$frequency,is.na(trendTable15$TrendScore))
  table(trendTable10$frequency,is.na(trendTable10$TrendScore))
  table(trendTable5$frequency,is.na(trendTable5$TrendScore))
  knitr::kable(table(trendTable15$AnalysisNote[is.na(trendTable15$TrendScore)&trendTable15$frequency!='unassessed']),format='rst')
  knitr::kable(table(trendTable10$AnalysisNote[is.na(trendTable10$TrendScore)&trendTable10$frequency!='unassessed']),format='rst')
  knitr::kable(table(trendTable5$AnalysisNote[is.na(trendTable5$TrendScore)&trendTable5$frequency!='unassessed']),format='rst')
}

