rm(list=ls())
gc()
library(tidyverse)
library(parallel)
library(doParallel)
source("h:/ericg/16666LAWA/LAWA2021/Scripts/LAWAFunctions.R")
source("h:/ericg/16666LAWA/LWPTrends_v2101/LWPTrends_v2101.R")
# source("h:/ericg/16666LAWA/LAWA2021/macroinvertebrates/scripts/SWQ_state_functions.R")
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
  macroData=read_csv(tail(dir(path = "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data",
                              pattern = "MacrosWithMetadata.csv",recursive = T,full.names = T),1),
                     guess_max = 10000)

  macroData$myDate <- as.Date(lubridate::dmy(as.character(macroData$Date)),"%d-%b-%Y")
  macroData=macroData[which(macroData$sYear<=EndYear),]  #48665 for startYear15
  #change to hydro year in 2020
  macroData$Year = macroData$sYear
  macroData$month=lubridate::month(lubridate::dmy(macroData$Date))
  
  #From Caroline Fraser, 25/8/21
  macroData<-GetMoreDateInfo(macroData,firstMonth = 7)
  NumBiAnnString<-c("BA1","BA2")
  MyBiAnnString<-c("BA2","BA1")
  macroData$BiAnn<-factor(ceiling(as.numeric(format(macroData$myDate, "%m"))/6),
                            levels=1:2,labels=NumBiAnnString)
  macroData$BiAnn<-factor( macroData$BiAnn,levels=MyBiAnnString)
  ##
  
  macroData$biYear = paste0(macroData$BiAnn,macroData$Year)
  # macroData$Season <- macroData$Month
  macroData$Season=macroData$BiAnn
  SeasonString <- sort(unique(macroData$Season))
  macroData$Censored=F
  macroData$CenType="not"
  macroData$LawaSiteID=tolower(macroData$LawaSiteID)
  }




uMeasures=c("MCI","QMCI","ASPM") 

#15 year trend ####


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
                              nLastYear=NA,numMonths=NA, numBiAnns=NA, numYears=NA,
                              Observations=NA,KWstat=NA,pvalue=NA, SeasNote=NA,
                              nObs=NA, S=NA, VarS=NA, D=NA,tau=NA, Z=NA, p=NA, C=NA,Cd=NA, MKAnalysisNote=NA,
                              prop.censored=NA, prop.unique=NA, no.censorlevels=NA,
                              Median=NA, Sen_VarS=NA,AnnualSenSlope=NA, Intercept=NA, Sen_Lci=NA,
                              Sen_Uci=NA, SSAnalysisNote=NA,Sen_Probability=NA, Sen_Probabilitymax=NA,
                              Sen_Probabilitymin=NA, Percent.annual.change=NA,
                              standard=NA,ConfCat=NA, period=15, frequency=NA,seasonal=F)
  subDat=datafor15%>%dplyr::filter(LawaSiteID==usites[usite])
  for(uParam in 1:length(uMeasures)){
    subSubDat=subDat%>%filter(subDat$Measurement==uMeasures[uParam])%>%as.data.frame
    siteTrendTable15$nMeasures[uParam]=dim(subSubDat)[1]
    if(dim(subSubDat)[1]>0){
      subSubDat$Value=signif(subSubDat$Value,6)#Censoring fails with tiny machine rounding errors
      
      siteTrendTable15$nFirstYear[uParam]=length(which(subSubDat$sYear==startYear15&!is.na(subSubDat$Value)))
      siteTrendTable15$nLastYear[uParam]=length(which(subSubDat$sYear==EndYear&!is.na(subSubDat$Value)))
      siteTrendTable15$numBiAnns[uParam]=length(unique(subSubDat$biYear[!is.na(subSubDat$Value)]))
      siteTrendTable15$numYears[uParam]=length(unique(subSubDat$sYear[!is.na(subSubDat$Value)]))
      
      #For 15 year biannual we want 90% of measures and 90% of years
      if(siteTrendTable15$numBiAnns[uParam] >= 0.9*2*15 & siteTrendTable15$numYears[uParam]>=13){ #27
        siteTrendTable15$frequency[uParam]='biannual'
      }else{
        subSubDat$Season=NA
        if(siteTrendTable15$numYears[uParam] >=13){
          siteTrendTable15$frequency[uParam]='annual'
        }else{
          siteTrendTable15$frequency[uParam]='unassessed'
        }
      }

      if(siteTrendTable15$frequency[uParam] == 'biannual'){
        st <- SeasonalityTest(x = subSubDat,main=uMeasures[uParam],ValuesToUse = "Value",do.plot =F)
        siteTrendTable15[uParam,names(st)] <- st
        if(!is.na(st$pvalue)&&st$pvalue<0.05){
          siteTrendTable15$seasonal[uParam]=T
          sk <- SeasonalKendall(x = subSubDat,ValuesToUse = "Value",Year='sYear',HiCensor = T,doPlot = F)
          sss <- SeasonalSenSlope(HiCensor=T,x = subSubDat,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
          siteTrendTable15[uParam,names(sk)] <- sk
          siteTrendTable15[uParam,names(sss)] <- sss
          rm(sk,sss)
        }
        rm(st)
      }
      if(siteTrendTable15$frequency[uParam] == 'annual' | (is.na(siteTrendTable15$nObs[uParam])
                                                           & siteTrendTable15$frequency[uParam]=='biannual')){
        #THen we havent yet done an analysis.  We'll do the annual one
        
        mk <- MannKendall(x = subSubDat,ValuesToUse = "Value",Year='sYear',HiCensor = T,doPlot=F)
        ss <- SenSlope(HiCensor=T,x = subSubDat,ValuesToUse = "Value",Year='sYear',
                       ValuesToUseforMedian="Value",doPlot = F)
        siteTrendTable15[uParam,names(mk)] <- mk
        siteTrendTable15[uParam,names(ss)] <- ss
        rm(mk,ss)
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

#17 Jul      11s
#13 Aug 2021 11s
#26 Aug 21 33 s

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
                              nLastYear=NA,numMonths=NA, numBiAnns=NA, numYears=NA,
                              Observations=NA,KWstat=NA,pvalue=NA, SeasNote=NA,
                              nObs=NA, S=NA, VarS=NA, D=NA,tau=NA, Z=NA, p=NA, C=NA,Cd=NA, MKAnalysisNote=NA,
                              prop.censored=NA, prop.unique=NA, no.censorlevels=NA,
                              Median=NA, Sen_VarS=NA,AnnualSenSlope=NA, Intercept=NA, Sen_Lci=NA,
                              Sen_Uci=NA, SSAnalysisNote=NA,Sen_Probability=NA, Sen_Probabilitymax=NA,
                              Sen_Probabilitymin=NA, Percent.annual.change=NA,
                              standard=NA,ConfCat=NA, period=10, frequency=NA,seasonal=F)
  subDat=datafor10%>%dplyr::filter(LawaSiteID==usites[usite])
  for(uParam in 1:length(uMeasures)){
    subSubDat=subDat%>%filter(subDat$Measurement==uMeasures[uParam])%>%as.data.frame
    siteTrendTable10$nMeasures[uParam]=dim(subSubDat)[1]
    if(dim(subSubDat)[1]>0){
      subSubDat$Value=signif(subSubDat$Value,6)#Censoring fails with tiny machine rounding errors
      siteTrendTable10$nFirstYear[uParam]=length(which(subSubDat$sYear==startYear10&!is.na(subSubDat$Value)))
      siteTrendTable10$nLastYear[uParam]=length(which(subSubDat$sYear==EndYear&!is.na(subSubDat$Value)))
      siteTrendTable10$numBiAnns[uParam]=length(unique(subSubDat$biYear[!is.na(subSubDat$Value)]))
      siteTrendTable10$numYears[uParam]=length(unique(subSubDat$sYear[!is.na(subSubDat$Value)]))
      
      #For 10 year biannual we want 90% of measures and 90% of years
      if(siteTrendTable10$numBiAnns[uParam] >= 0.9*2*10 & siteTrendTable10$numYears[uParam]>=9){ #18
        siteTrendTable10$frequency[uParam]='biannual'
      }else{
        subSubDat$Season=NA
        if(siteTrendTable10$numYears[uParam] >= 9){
          siteTrendTable10$frequency[uParam]='annual'
        }else{
          siteTrendTable10$frequency[uParam]='unassessed'
        }
      }
      
      
      if(siteTrendTable10$frequency[uParam] == 'biannual'){
        st <- SeasonalityTest(x = subSubDat,main=uMeasures[uParam],ValuesToUse = "Value",do.plot =F)
        siteTrendTable10[uParam,names(st)] <- st
        if(!is.na(st$pvalue)&&st$pvalue<0.05){
          siteTrendTable10$seasonal[uParam]=T
          sk <- SeasonalKendall(x = subSubDat,ValuesToUse = "Value",Year='sYear',HiCensor = T,doPlot = F)
          sss <- SeasonalSenSlope(HiCensor=T,x = subSubDat,ValuesToUse = "Value",ValuesToUseforMedian="Value",doPlot = F)
          siteTrendTable10[uParam,names(sk)] <- sk
          siteTrendTable10[uParam,names(sss)] <- sss
          rm(sk,sss)
        }
        rm(st)
      }
      if(siteTrendTable10$frequency[uParam] == 'annual' | (is.na(siteTrendTable10$nObs[uParam]) & 
                                                           siteTrendTable10$frequency[uParam]=='biannual')){
        #THen we havent yet done an analysis.  We'll do the annual one
        
        mk <- MannKendall(x = subSubDat,ValuesToUse = "Value",Year='sYear',HiCensor = T,doPlot=F)
        ss <- SenSlope(HiCensor=T,x = subSubDat,ValuesToUse = "Value",Year='sYear',
                       ValuesToUseforMedian="Value",doPlot = F)
        siteTrendTable10[uParam,names(mk)] <- mk
        siteTrendTable10[uParam,names(ss)] <- ss
        rm(mk,ss)
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
#13 Aug 9.8s


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


if(lubridate::year(Sys.Date())==2021){
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
}

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
library(showtext)
if(!'source'%in%font_families()){
  font_add(family = 'source',regular = "h:/ericg/16666LAWA/LAWA2021/SourceFont/SourceSansPro-Regular.ttf")
  # font_add_google("Source Sans Pro",family='source')
}

savePlott=T
usites=unique(trendTable10$LawaSiteID)
uMeasures=c("MCI","QMCI")

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
showtext_begin()  
    par(mfrow=c(2,1),mar=c(2,4,1,2),family='source',cex.lab=2,cex.axis=2,cex.main=3)
    theseDeg <- which(macroData$LawaSiteID==subTrend$LawaSiteID[worstDeg] &
                        macroData$Measurement==uMeasures[uParam] & dmy(macroData$Date)>dmy("1-1-2009"))
    theseImp <- which(macroData$LawaSiteID==subTrend$LawaSiteID[bestImp] &
                        macroData$Measurement==uMeasures[uParam] & dmy(macroData$Date)>dmy("1-1-2009"))
    
      MannKendall(x = as.data.frame(macroData[theseDeg,]),ValuesToUse = "Value",doPlot=F)
      SenSlope(x = as.data.frame(macroData[theseDeg,]),ValuesToUse = "Value",ValuesToUseforMedian = "Value",doPlot = T,legend.pos='bottom',mymain=unique(subTrend$LawaSiteID[worstDeg]))
      MannKendall(x = as.data.frame(macroData[theseImp,]),ValuesToUse = "Value",doPlot=F)
      SenSlope(x = as.data.frame(macroData[theseImp,]),ValuesToUse = "Value",ValuesToUseforMedian = "Value",doPlot = T,legend.pos='bottom',mymain=unique(subTrend$LawaSiteID[bestImp]))
   showtext_end()
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
 tiff(paste0("h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/",
             format(Sys.Date(),"%Y-%m-%d"),"/TenYearTrends.tif"),
      width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
}
par(mfrow=c(1,1),mar=c(5,10,4,2))
barplot(tbp,main="Ten year trends",las=2,
        col=c("#dd1111FF","#ee4411FF","#bbbbbbFF","#11cc11FF","#008800FF"),yaxt='n')
axis(side = 2,at = mbp[,1],labels = colnames(tb),las=2,lty = 0)
text(0.67,mbp[,1],tb[1,])
text(1.9,mbp[,2],tb[2,])
text(3.11,mbp[,3],tb[3,])

if(names(dev.cur())=='tiff'){dev.off()}


