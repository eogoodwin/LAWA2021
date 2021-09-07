#Calculate state (median) ####
#Get a median value per site/Measurement combo
dir.create(paste0("h:/ericg/16666LAWA/LAWA2021/Groundwater/Analysis/",format(Sys.Date(),"%Y-%m-%d")))
startTime=Sys.time()
GWmedians <- GWdataRelevantVariables%>%
  group_by(LawaSiteID,Measurement)%>%
  dplyr::summarise(.groups='keep',
                   median=quantile(Value,probs = 0.5,type=5,na.rm=T),
                   MAD = quantile(abs(median-Value),probs=0.5,type=5,na.rm=T),
                   count = n(),
                   minPerYear = min(as.numeric(table(factor(as.character(lubridate::year(myDate)),
                                                            levels=as.character(startYear5:EndYear))))),  
                   nYear = length(unique(Year)),
                   nQuart=length(unique(Qtr)),
                   nMonth=length(unique(Month)),
                   # censoredCount = sum(!is.na(CenType)),
                   censoredCount = sum(Value<=(0.5*LcenLim) | Value>=(1.1*RcenLim)),
                   CenType = Mode(CenBin),
                   Frequency=unique(Frequency))%>%
  ungroup%>%
  mutate(censoredPropn = censoredCount/count)
Sys.time()-startTime #1.7s
#5328 of 13

GWmedians$CenType = as.character(GWmedians$CenType)
GWmedians$CenType[GWmedians$CenType==1] <- '<'
GWmedians$CenType[GWmedians$CenType==2] <- '>'
GWmedians$censMedian = GWmedians$median
GWmedians$censMedian[GWmedians$censoredPropn>=0.5 & GWmedians$CenType=='<'] <- 
  paste0('<',2*GWmedians$median[GWmedians$censoredPropn>=0.5 & GWmedians$CenType=='<'])
GWmedians$censMedian[GWmedians$censoredPropn>=0.5 & GWmedians$CenType=='>'] <- 
  paste0('>',GWmedians$median[GWmedians$censoredPropn>=0.5 & GWmedians$CenType=='>']/1.1)

#E coli detection ####
#1 is detect, 2 is non-detect
# GWmedians$EcoliDetect=NA
# GWmedians$EcoliDetect[which(GWmedians$Measurement=="E.coli")] <- "1"  #Detect
# GWmedians$EcoliDetect[which(GWmedians$Measurement=="E.coli"&
#                               (GWmedians$censoredPropn>0.5|
#                                  GWmedians$median==0|
#                                  is.na(GWmedians$median)))] <- "2"  #Non-detect
# table(GWmedians$EcoliDetect)   
#Changing the 'censoredCount' above.  
#When it's based on censoredCount = sum(!is.na(CenType)), I get 88 detects, 688 non detects
#When it's based on censoredCount = sum(Value<=(0.5*LcenLim) | Value>=(1.1*RcenLim)) I get 89 detects, 687 non detects


GWmedians$EcoliDetectAtAll=NA
GWmedians$EcoliDetectAtAll[which(GWmedians$Measurement=="E.coli")] <- "1"  #Detect
GWmedians$EcoliDetectAtAll[which(GWmedians$Measurement=="E.coli"&
                                   (GWmedians$censoredPropn==1|
                                      GWmedians$median==0|
                                      is.na(GWmedians$median)))] <- "2"  #Non-detect
table(GWmedians$EcoliDetectAtAll)
#Changing the 'censoredCount' above.
#When it's based on censoredCount = sum(!is.na(CenType)), I get 391 detects, 385 non detects
#When it's based on censoredCount = sum(Value<=(0.5*LcenLim) | Value>=(1.1*RcenLim)) I get 394 detects, 382 non detects



# table(GWmedians$EcoliDetect,GWmedians$EcoliDetectAtAll)


#3845 of 11 11-7
#3756 of 11 11-14
#3882 of 11 11-22
#4057 of 11 11-25
#4228 of 11 03-06-20
#4208 of 12 03-13-20
#4208 of 13 03-20-20  Added ecoliDetect   Emails in LAWA 2019 groudnwater folder
#4209 of 15 03-27-20
#4208 of 16 04-23-20 added ecoliDetectAtAll
#4319 of 16 04/08/20
#5053 of 16 10/8/20
#5473       14/8/20
#5527       24/8/20
#5532       28
#5532
#5542       14-9-20
#3113       3-8-21
#4855    6-8-21
#5354   13/8/21
#5321  27/8/21
#5328  3/9/21

GWmedians$meas = factor(GWmedians$Measurement,
                        levels=c("Ammoniacal nitrogen","Chloride","Dissolved reactive phosphorus",
                                 "E.coli","Electrical conductivity/salinity","Nitrate nitrogen"),
                        labels=c("NH4","Cl","DRP","ECOLI","NaCl","NO3"))


#Plotting
if(plotto){
  with(GWmedians[GWmedians$Frequency=='quarterly',],table(minPerYear,count))
  GWmedians[which(GWmedians$Frequency=='quarterly'&GWmedians$count==20),]
  par(mfrow=c(1,1))
  plot(GWmedians$median,GWmedians$MAD,log='xy',xlab='Median',ylab='MAD',
       col=as.numeric(factor(GWmedians$Measurement)),pch=16,cex=1)
  abline(0,1)
  GWmedians%>%filter(MAD>median)
  plot(GWmedians$median,GWmedians$MAD/GWmedians$median,log='x',xlab='Median',ylab='MAD/Median',
       col=as.numeric(factor(GWmedians$Measurement)),pch=16,cex=1)
  
  #Show data abundance with original filter cutoffs:
  par(mfrow=c(3,1))
  with(GWmedians%>%filter(Frequency=='monthly'),hist(count,main='monthly'));abline(v=30,lwd=2,col='red')
  with(GWmedians%>%filter(Frequency=='bimonthly'),hist(count,main='bimonthly',breaks = 20));abline(v=15,lwd=2,col='red')
  with(GWmedians%>%filter(Frequency=='quarterly'),hist(count,main='quarterly'));abline(v=10,lwd=2,col='red')
  
  par(mfrow=c(3,1))
  with(GWmedians%>%filter(Frequency=='monthly'),hist(minPerYear,main='monthly'));abline(v=6,lwd=2,col='red')
  with(GWmedians%>%filter(Frequency=='bimonthly'),hist(minPerYear,main='bimonthly',breaks = 20));abline(v=3,lwd=2,col='red')
  with(GWmedians%>%filter(Frequency=='quarterly'),hist(minPerYear,main='quarterly'));abline(v=2,lwd=2,col='red')
  
  par(mfrow=c(3,1))
  with(GWmedians%>%filter(Frequency=='monthly'),hist(nMonth,main='monthly'));abline(v=11.5,lwd=2,col='red')
  with(GWmedians%>%filter(Frequency=='bimonthly'),hist(nMonth,main='bimonthly',breaks=12));abline(v=5,lwd=2,col='red')
  with(GWmedians%>%filter(Frequency=='quarterly'),hist(nQuart,main='quarterly'));abline(v=3.5,lwd=2,col='red')
}
if(plotto){
  par(mfrow=c(3,3))
  plot(GWmedians$median,GWmedians$MAD/(GWmedians$median*GWmedians$count^0.5),
       log='xy',pch=c(1,16)[as.numeric(GWmedians$Exclude)+1],col=as.numeric(factor(GWmedians$Measurement)),cex=sqrt(GWmedians$count))
  GWmedians%>%split(GWmedians$Measurement)%>%
    purrr::map(~plot(.$median,.$MAD/(.$median*.$count^0.5),
                     log='xy',pch=c(1,16)[as.numeric(.$Exclude)+1],main=unique(.$Measurement),cex=sqrt(.$count)))
  
  par(mfrow=c(3,3))
  plot(GWmedians$median,GWmedians$MAD/(GWmedians$median),
       log='xy',lwd=c(2,1)[as.numeric(GWmedians$Exclude)+1],col=as.numeric(factor(GWmedians$Measurement)),cex=sqrt(GWmedians$count))
  abline(h=1)
  GWmedians%>%split(GWmedians$Measurement)%>%
    purrr::map(~{plot(.$median,.$MAD/(.$median),
                      log='xy',lwd=c(2,1)[as.numeric(.$Exclude)+1],main=unique(.$Measurement),cex=sqrt(.$count),ylim=c(0.001,5))
      abline(h=1)})
  
  par(mfrow=c(3,3))
  plot(GWmedians$median,GWmedians$MAD,
       log='xy',pch=c(1,16)[as.numeric(GWmedians$Exclude)+1],col=as.numeric(factor(GWmedians$Measurement)),cex=sqrt(GWmedians$count))
  abline(0,1)
  GWmedians%>%split(GWmedians$Measurement)%>%
    purrr::map(~{plot(.$median,.$MAD,
                      log='xy',pch=c(1,16)[as.numeric(.$Exclude)+1],main=unique(.$Measurement),cex=sqrt(.$count))
      abline(0,1)})
  
  table(GWmedians$count)
}
if(plotto){
  table(GWmedians$count)
  table(GWmedians$count,GWmedians$meas)
  table(GWmedians$count,GWmedians$Frequency)
  
  par(mfrow=c(1,1))
  plot(density(GWmedians$count,from=min(GWmedians$count),adjust=2),xlim=range(GWmedians$count))
  
  
  par(mfrow=c(3,3))
  uMeasures=unique(GWmedians$Measurement)
  for(measure in uMeasures){
    dtp = GWmedians%>%filter(Measurement==measure,median>=0)
    rtp = GWdataRelevantVariables%>%filter(Measurement==measure,Value>=0)
    if(!measure%in%('Water Level')){
      if(measure%in%c('Ammoniacal nitrogen','E.coli')){adjust=2}else{adjust=1}
      plot(density(log(dtp$median),na.rm=T,adjust=adjust),main=measure,xaxt='n',xlab='')
      lines(density(log(rtp$Value),na.rm=T,adjust=2*adjust),col='grey')
      rug(log(dtp$median))
      axis(side = 1,at = pretty(log(dtp$median)),labels=signif(exp(pretty(log(dtp$median))),2))
    }else{
      plot(density(dtp$median,na.rm=T),main=measure,xlab='')
      rug(dtp$median)
      lines(density(rtp$Value,na.rm=T),col='grey')
    }
  }
  rm(dtp,rtp,measure)
  #pick a site, pick a measurement
  nExample=0
  while(nExample<3){
    site=sample(x = unique(GWmedians$LawaSiteID),size = 1)
    meas=sample(x = unique(GWmedians$Measurement),size = 1)
    if(length(which(GWmedians$LawaSiteID==site&GWmedians$Measurement==meas))>0){
      toPlot = GWdataRelevantVariables%>%filter(LawaSiteID==site&Measurement==meas)%>%select(Value)%>%drop_na%>%unlist
      plot(density(toPlot,na.rm=T,from=0),xlab='',main=paste(site,meas))
      rug(toPlot)
      abline(v = GWmedians%>%filter(LawaSiteID==site&Measurement==meas)%>%select(median))
      nExample=nExample+1
    }
  }
  rm(site,meas)
}


GWmedians$Source = GWdata$Source[match(GWmedians$LawaSiteID,GWdata$LawaSiteID)]

GWmedians$StateVal = GWmedians$median
GWmedians$StateVal[GWmedians$Measurement=="E.coli"] <- GWmedians$EcoliDetectAtAll[GWmedians$Measurement=="E.coli"]

#Export Median values
write.csv(GWmedians,file = paste0('h:/ericg/16666LAWA/LAWA2021/Groundwater/Analysis/',format(Sys.Date(),"%Y-%m-%d"),
                                  '/ITEGWState',format(Sys.time(),"%d%b%Y"),'.csv'),row.names = F)
write.csv(GWmedians,file = paste0("c:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021/Groundwater Quality/EffectDelivery/",
                                  '/ITEGWState',format(Sys.time(),"%d%b%Y"),'.csv'),row.names = F)
rm(GWdataRelevantVariables)
if(exists('GWdataReducedTemporalResolution')){rm(GWdataReducedTemporalResolution)}

GWmedians <- read.csv(tail(dir(path='./Analysis',pattern='ITEGWState',full.names = T,recursive = T),1),stringsAsFactors = F)





