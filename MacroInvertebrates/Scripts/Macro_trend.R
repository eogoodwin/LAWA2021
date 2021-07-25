rm(list=ls())
gc()
library(tidyverse)
source("h:/ericg/16666LAWA/LAWA2021/Scripts/LAWAFunctions.R")
source("h:/ericg/16666LAWA/LWPTrends_v2101/LWPTrends_v2101.R")
# source("h:/ericg/16666LAWA/LAWA2021/WaterQuality/scripts/SWQ_state_functions.R")
dir.create(paste0("h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d")),recursive = T,showWarnings = F)


#NOTE = should probably tidy up the seasonality aspect of trend.


Mode=function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

EndYear <- year(Sys.Date())-1
startYear10 <- EndYear - 10+1
startYear15 <- EndYear - 15+1

siteTable=loadLatestSiteTableMacro()
#Load the latest made
if(!exists('macroData')){
  macroData=read.csv(tail(dir(path = "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data",
                              pattern = "MacrosWithMetadata.csv",recursive = T,full.names = T),1),stringsAsFactors = F)

  macroData$myDate <- as.Date(lubridate::dmy(as.character(macroData$Date)),"%d-%b-%Y")
  # macroData$Year=lubridate::isoyear(lubridate::dmy(macroData$Date))
  macroData=macroData[which(macroData$sYear<=EndYear),]  #48665 for startYear15
  # macroData$sYear=macroData$Year   #change to hydro year in 2020
  macroData$month=lubridate::month(lubridate::dmy(macroData$Date))
  macroData <- GetMoreDateInfo(macroData)
  # macroData$monYear = format(macroData$myDate,"%b-%Y")
  # macroData$Season <- macroData$Month
  macroData$Season=macroData$month
  SeasonString <- sort(unique(macroData$Season))
  macroData$Censored=F
  macroData$CenType="FALSE"
  macroData$LawaSiteID=tolower(macroData$LawaSiteID)
  }




uMeasures=c('MCI','QMCI','ASPM')

#15 year trend ####
library(parallel)
library(doParallel)


datafor15=macroData%>%filter(sYear>=startYear15 & sYear <= EndYear, Measurement%in%uMeasures)
usites=unique(datafor15$LawaSiteID)
cat(length(usites),'\n')
usite=1
workers <- makeCluster(5)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  library(doBy)
  library(plyr)
  library(dplyr)
  
})
startTime=Sys.time()
foreach(usite = usite:length(usites),.combine=rbind,.errorhandling="stop")%dopar%{
  # trendTable15=NULL
  # for(usite in 1:length(usites)){
  siteTrendTable15=data.frame(LawaSiteID=usites[usite], Measurement=uMeasures, nMeasures=NA, nFirstYear=NA,
                              nLastYear=NA,numMonths=NA, numQuarters=NA, numYears=NA,
                              Observations=NA,KWstat=NA,pvalue=NA, SeasNote=NA,
                              nObs=NA, S=NA, VarS=NA, D=NA,tau=NA, Z=NA, p=NA, C=NA,Cd=NA, MKAnalysisNote=NA,
                              prop.censored=NA, prop.unique=NA, no.censorlevels=NA,
                              Median=NA, Sen_VarS=NA,AnnualSenSlope=NA, Intercept=NA, Sen_Lci=NA,
                              Sen_Uci=NA, SSAnalysisNote=NA,Sen_Probability=NA, Sen_Probabilitymax=NA,
                              Sen_Probabilitymin=NA, Percent.annual.change=NA,
                              standard=NA,ConfCat=NA, period=15, frequency=NA)
  subDat=datafor15%>%dplyr::filter(LawaSiteID==usites[usite])
  for(uParam in 1:length(uMeasures)){
    subSubDat=subDat%>%filter(subDat$Measurement==uMeasures[uParam])%>%as.data.frame
    siteTrendTable15$nMeasures[uParam]=dim(subSubDat)[1]
    if(dim(subSubDat)[1]>0){
      subSubDat$Value=signif(subSubDat$Value,6)#Censoring fails with tiny machine rounding errors
      subSubDat$Season = paste0('y',subSubDat$sYear)
      SeasonString = sort(unique(sapply(startYear15:EndYear,function(x)paste0('y',x))))
      siteTrendTable15$nFirstYear[uParam]=length(which(subSubDat$sYear==startYear15&!is.na(subSubDat$Value)))
      siteTrendTable15$nLastYear[uParam]=length(which(subSubDat$sYear==EndYear&!is.na(subSubDat$Value)))
      siteTrendTable15$numYears[uParam]=length(unique(subSubDat$sYear[!is.na(subSubDat$Value)]))
      
      #For 15 year we want 13 years out of 15
      if(siteTrendTable15$numYears[uParam] >= 13){
        siteTrendTable15$frequency[uParam]='yearly'
      }else{
          siteTrendTable15$frequency[uParam] ='unassessed'
      }
      if(siteTrendTable15$frequency[uParam] != 'unassessed'){
        st <- SeasonalityTest(x = subSubDat,main=uMeasures[uParam],ValuesToUse = "Value",do.plot =F)
        siteTrendTable15[uParam,names(st)] <- st
        if(!is.na(st$pvalue)&&st$pvalue<0.05){
          sk <- SeasonalKendall(x = subSubDat,ValuesToUse = "Value",Year='sYear',HiCensor = T,doPlot = F)
          sss <- SeasonalSenSlope(HiCensor=T,x = subSubDat,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
          siteTrendTable15[uParam,names(sk)] <- sk
          siteTrendTable15[uParam,names(sss)] <- sss
          rm(sk,sss)
        }else{
          mk <- MannKendall(x = subSubDat,ValuesToUse = "Value",Year='sYear',HiCensor = T,doPlot=F)
          ss <- SenSlope(HiCensor=T,x = subSubDat,ValuesToUse = "Value",Year='sYear',ValuesToUseforMedian="Value",doPlot = F)
          siteTrendTable15[uParam,names(mk)] <- mk
          siteTrendTable15[uParam,names(ss)] <- ss
          rm(mk,ss)
        }
        rm(st)
      }
    }
      rm(subSubDat)
  }
    rm(subDat)
    #trendTable15 =  rbind(trendTable15,siteTrendTable15)}
  return(siteTrendTable15)
}-> trendTable15
stopCluster(workers)
rm(workers,usites,usite,datafor15)
Sys.time()-startTime
#17 Jul 11s
rownames(trendTable15) <- NULL
trendTable15$Agency=siteTable$Agency[match(trendTable15$LawaSiteID,tolower(siteTable$LawaSiteID))]
trendTable15$SWQAltitude =  siteTable$SWQAltitude[match(trendTable15$LawaSiteID,tolower(siteTable$LawaSiteID))]
trendTable15$SWQLanduse =   siteTable$SWQLanduse[match(trendTable15$LawaSiteID,tolower(siteTable$LawaSiteID))]
trendTable15$Region =    siteTable$Region[match(trendTable15$LawaSiteID,tolower(siteTable$LawaSiteID))]

trendTable15$ConfCat <- cut(trendTable15$Cd, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
                            labels = c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
trendTable15$ConfCat=factor(trendTable15$ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
trendTable15$TrendScore=as.numeric(trendTable15$ConfCat)-3
trendTable15$TrendScore[is.na(trendTable15$TrendScore)]<-(NA)
save(trendTable15,file=paste0("h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/Trend15Year.rData"))
rm(trendTable15)



#10 year trend ####
datafor10=macroData%>%filter(sYear>=startYear10 & sYear <= EndYear)
usites=unique(datafor10$LawaSiteID)
cat(length(usites),'\n')
usite=1
library(parallel)
library(doParallel)
workers <- makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  #library(doBy)
  library(plyr)
  library(dplyr)
  
})
startTime=Sys.time()
foreach(usite = usite:length(usites),.combine=rbind,.errorhandling="stop")%dopar%{
  siteTrendTable10=data.frame(LawaSiteID=usites[usite], Measurement=uMeasures, nMeasures=NA, nFirstYear=NA,
                              nLastYear=NA,numMonths=NA, numQuarters=NA, numYears=NA,
                              Observations=NA,KWstat=NA,pvalue=NA, SeasNote=NA,
                              nObs=NA, S=NA, VarS=NA, D=NA,tau=NA, Z=NA, p=NA, C=NA,Cd=NA, MKAnalysisNote=NA,
                              prop.censored=NA, prop.unique=NA, no.censorlevels=NA,
                              Median=NA, Sen_VarS=NA,AnnualSenSlope=NA, Intercept=NA, Sen_Lci=NA,
                              Sen_Uci=NA, SSAnalysisNote=NA,Sen_Probability=NA, Sen_Probabilitymax=NA,
                              Sen_Probabilitymin=NA, Percent.annual.change=NA,
                              standard=NA,ConfCat=NA, period=15, frequency=NA)
  subDat=datafor10%>%dplyr::filter(LawaSiteID==usites[usite])
  for(uParam in 1:length(uMeasures)){
    subSubDat=subDat%>%filter(subDat$Measurement==uMeasures[uParam])%>%as.data.frame
    siteTrendTable10$nMeasures[uParam]=dim(subSubDat)[1]
    if(dim(subSubDat)[1]>0){
    subSubDat$Value=signif(subSubDat$Value,6)#Censoring fails with tiny machine rounding errors
    subSubDat$Season = paste0('y',subSubDat$sYear)
    SeasonString = sort(unique(sapply(startYear10:EndYear,function(x)paste0('y',x))))
    siteTrendTable10$nFirstYear[uParam]=length(which(subSubDat$sYear==startYear10&!is.na(subSubDat$Value)))
    siteTrendTable10$nLastYear[uParam]=length(which(subSubDat$sYear==EndYear&!is.na(subSubDat$Value)))
    siteTrendTable10$numYears[uParam]=length(unique(subSubDat$sYear[!is.na(subSubDat$Value)]))
    
    #For 10 year we want 8 years out of 10
    if(siteTrendTable10$numYears >= 8){
      siteTrendTable10$frequency='yearly'
    }else{
      siteTrendTable10$frequency='unassessed'
    }
    if(siteTrendTable10$frequency!='unassessed'){
      st <- SeasonalityTest(x = subSubDat,main=uMeasures[uParam],ValuesToUse = "Value",do.plot =F)
      siteTrendTable10[uParam,names(st)] <- st
      if(!is.na(st$pvalue)&&st$pvalue<0.05){
        sk <- SeasonalKendall(x = subSubDat,ValuesToUse = "Value",HiCensor = T,doPlot = F)
        sss <- SeasonalSenSlope(HiCensor=T,x = subSubDat,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
        siteTrendTable10[uParam,names(sk)] <- sk
        siteTrendTable10[uParam,names(sss)] <- sss
        rm(sk,sss)
      }else{
        mk <- MannKendall(x = subSubDat,ValuesToUse = "Value",Year='sYear',HiCensor = T,doPlot=F)
        ss <- SenSlope(HiCensor=T,x = subSubDat,ValuesToUse = "Value",Year='sYear',ValuesToUseforMedian="Value",doPlot = F)
        siteTrendTable10[uParam,names(mk)] <- mk
        siteTrendTable10[uParam,names(ss)] <- ss
        rm(mk,ss)
      }
      rm(st)
    }
    }
    rm(subSubDat)
  }
  rm(subDat)
  return(siteTrendTable10)
}-> trendTable10
stopCluster(workers)
rm(workers,usites,usite,datafor10)
Sys.time()-startTime
#23 Jun 3.7s
rownames(trendTable10) <- NULL
trendTable10$Agency=siteTable$Agency[match(trendTable10$LawaSiteID,tolower(siteTable$LawaSiteID))]
trendTable10$SWQAltitude =  siteTable$SWQAltitude[match(trendTable10$LawaSiteID,tolower(siteTable$LawaSiteID))]
trendTable10$SWQLanduse =   siteTable$SWQLanduse[match(trendTable10$LawaSiteID,tolower(siteTable$LawaSiteID))]
trendTable10$Region =    siteTable$Region[match(trendTable10$LawaSiteID,tolower(siteTable$LawaSiteID))]

trendTable10$ConfCat <- cut(trendTable10$Cd, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
                            labels = c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
trendTable10$ConfCat=factor(trendTable10$ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
trendTable10$TrendScore=as.numeric(trendTable10$ConfCat)-3
trendTable10$TrendScore[is.na(trendTable10$TrendScore)]<-(NA)
save(trendTable10,file=paste0("h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/Trend10Year.rData"))
rm(trendTable10)




load(tail(dir(path="h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/",pattern="Trend15Year.rData",recursive = T,full.names = T),1),verbose=T)
load(tail(dir(path="h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/",pattern="Trend10Year.rData",recursive = T,full.names = T),1),verbose=T)

trendTable15 <- trendTable15%>%drop_na(LawaSiteID)
trendTable10 <- trendTable10%>%drop_na(LawaSiteID)

combTrend <- rbind(trendTable15,trendTable10)

combTrend$CouncilSiteID = siteTable$CouncilSiteID[match(tolower(combTrend$LawaSiteID),tolower(siteTable$LawaSiteID))]

#Write outputs ####
write.csv(combTrend,
          paste0("h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),
                                    "/MacroMCI_Trend",format(Sys.time(),"%d%b%Y"),".csv"),row.names = F)
rm(combTrend)



#And for ITE ####
# write.csv(trendTablemax%>%
#             transmute(LAWAID=LawaSiteID,
#                       Parameter=Measurement,
#                       Trend=TrendScore),
#           paste0("h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),
#                  "/ITEMacroTrendmax",format(Sys.time(),"%d%b%Y"),".csv"),row.names = F)
write.csv(trendTable10%>%
            transmute(LAWAID=LawaSiteID,
                      Parameter=Measurement,
                      Trend=TrendScore),
          paste0("h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),
                 "/ITEMacroTrend10",format(Sys.time(),"%d%b%Y"),".csv"),row.names = F)
write.csv(trendTable15%>%
            transmute(LAWAID=LawaSiteID,
                      Parameter=Measurement,
                      Trend=TrendScore),
          paste0("h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),
                 "/ITEMacroTrend15",format(Sys.time(),"%d%b%Y"),".csv"),row.names = F)

# combTrend=read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis",pattern="MacroMCI_Trend",recursive = T,full.names = T),1),stringsAsFactors = F)

savePlott=F
usites=unique(trendTable10$LawaSiteID)
uMeasures=unique(trendTable10$Measurement)
for(uParam in seq_along(uMeasures)){
  subTrend=trendTable10[which(trendTable10$Measurement==uMeasures[uParam]),]
  worstDeg <- which.max(subTrend$Cd) 
  bestImp <- which.min(subTrend$Cd)
  cat(subTrend$Cd[worstDeg],'\t')
  cat(subTrend$Cd[bestImp],'\n')
  if(savePlott){
    tiff(paste0("h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/BestWorstTenYear",uMeasures[uParam],".tif"),
         width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
  }else{
    windows()
  }
    par(mfrow=c(2,1),mar=c(2,4,1,2))
    theseDeg <- which(macroData$LawaSiteID==subTrend$LawaSiteID[worstDeg] &
                        macroData$Measurement==uMeasures[uParam] & dmy(macroData$Date)>dmy("1-1-2009"))
    theseImp <- which(macroData$LawaSiteID==subTrend$LawaSiteID[bestImp] &
                        macroData$Measurement==uMeasures[uParam] & dmy(macroData$Date)>dmy("1-1-2009"))
    
      MannKendall(x = macroData[theseDeg,],ValuesToUse = "Value",doPlot=F)
      SenSlope(x = macroData[theseDeg,],ValuesToUse = "Value",ValuesToUseforMedian = "Value",doPlot = T)
      MannKendall(x = macroData[theseImp,],ValuesToUse = "Value",doPlot=F)
      SenSlope(x = macroData[theseImp,],ValuesToUse = "Value",ValuesToUseforMedian = "Value",doPlot = T)
    if(names(dev.cur())=='tiff'){dev.off()}
    rm(theseDeg,theseImp)
  
  rm(worstDeg,bestImp)
}


#Make the coloured plot
par(mfrow=c(1,1))
tb <- plot(factor(trendTable10$Measurement),trendTable10$ConfCat,
           col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF"),main="Ten year trends")
tbp <- apply(X = tb,MARGIN = 1,FUN = function(x)x/sum(x))
mbp <- apply(tbp,MARGIN = 2,FUN=cumsum)
mbp <- rbind(0,mbp)
mbp = (mbp[-1,]+mbp[-6,])/2
if(savePlott){
 tiff(paste0("h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),"/TenYearTrends.tif"),
      width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
}
par(mfrow=c(1,1),mar=c(5,10,4,2))
barplot(tbp,main="Ten year trends",las=2,
        col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF"),yaxt='n')
axis(side = 2,at = mbp[,1],labels = colnames(tb),las=2,lty = 0)
text(0.75,mbp,tb)

if(names(dev.cur())=='tiff'){dev.off()}


