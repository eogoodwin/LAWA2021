#===================================================================================================
#  LAWA NATIONAL OBJECTIVES FRAMEWORK
#  Horizons Regional Council
#
#  4 September 2016
#
#  Creator: Kelvin Phan  2014
#
#  Updated by: Maree Patterson 2016
#              Sean Hodges
#             Eric Goodwin 2018 Cawthron Institute
#  Horizons Regional Council
#===================================================================================================


rm(list = ls())
gc()
library(tidyr)
library(parallel)
library(doParallel)
require(reshape2)
library(areaplot)
library(tidyverse)
setwd("h:/ericg/16666LAWA/LAWA2021/WaterQuality/")
source("h:/ericg/16666LAWA/LAWA2021/scripts/LAWAFunctions.R")
source("h:/ericg/16666LAWA/LAWA2021/WaterQuality/scripts/SWQ_NOF_Functions.R")
dir.create(paste0("h:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis/",format(Sys.Date(),'%Y-%m-%d')),showWarnings = F)
dir.create(paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/", format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)
riverSiteTable=loadLatestSiteTableRiver()

## Load NOF Bands
NOFbandDefinitions <- read_csv("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/NOFbandDefinitions3.csv")
NOFbandDefinitions <- NOFbandDefinitions[,1:11]
#   Band Median.Nitrate X95th.Percentile.Nitrate Median.Ammoniacal.N Max.Ammoniacal.N E..coli Ecoli95 EcoliRec540 EcoliRec260       MedianDRP          X95DRP
# 1    A         x<=1.0                   x<=1.5             x<=0.03          x<=0.05  x<=130  x<=540         x<5        x<20        x<=0.006        x<=0.021
# 2    B     x>1&x<=2.4             x>1.5&x<=3.5      x>0.03&x<=0.24    x>0.05&x<=0.4  x<=130 x<=1000  x>=5&x<=10 x>=20&x<=30 X>0.006&x<=0.01 x>0.021&x<=0.03
# 3    C   x>2.4&x<=6.9             x>3.5&x<=9.8       x>0.24&x<=1.3     x>0.4&x<=2.2  x<=130 x<=1200 x>=10&x<=20 x>=20&x<=34 x>0.01&x<=0.018 x>0.03&x<=0.054
# 4    D          x>6.9                    x>9.8              x>1.30            x>2.2   x>130  x>1200 x>=20&x<=30        x>34         x>0.018         x>0.054
# 5    E          x>Inf                    x>Inf               x>Inf            x>Inf   x>260  x>1200        x>30        x>50           x>Inf           x>Inf
#===================================================================================================

NPSFMSusSed <- read_csv("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/NPSFMSuspendedSediment.csv")%>%as.data.frame

## Load LAWA Data

#Reference Dates
EndYear <- lubridate::year(Sys.Date())-1
StartYear5 <- EndYear - 5 + 1
# firstYear = min(wqdYear,na.rm=T)
firstYear=2004
yr <- c(as.character(firstYear:EndYear),paste0(as.character(firstYear:(EndYear-4)),'to',as.character((firstYear+4):EndYear)))
rollyrs=which(grepl('to',yr))
nonrollyrs=which(!grepl('to',yr))
reps <- length(yr)

if(!exists('wqdata')){
  wqdata=loadLatestDataRiver()
  wqdYear=lubridate::year(dmy(wqdata$Date))
  wqdata <- wqdata[which(wqdYear>=firstYear & wqdYear<=EndYear),]
  rm(wqdYear)
}

wqparam <- c("BDISC","TURB","NH4","TURBFNU",
             "PH","TON","TN",
             "DRP","TP","ECOLI","DIN","NO3N") 

#Replace censored values with 0.5 or 1.1 x                 NO LONGER as of 10/9/2020 max censored or min censored, per site, 
#then calculate per site&date median values to take forward
workers <- makeCluster(6)
registerDoParallel(workers)
clusterCall(workers,function(){
library(tidyverse)
  source('H:/ericg/16666LAWA/LAWA2021/scripts/LAWAFunctions.R')
})
startTime=Sys.time()
foreach(i = 1:length(wqparam),.combine = rbind,.errorhandling = "stop")%dopar%{
  wqdata_A = wqdata%>%dplyr::filter(tolower(Measurement)==tolower(wqparam[i]))
  if(dim(wqdata_A)[1]==0){return(NULL)}
  wqdata_A$origValue=wqdata_A$Value
  #CENSORING
  #left-censored replacement value is half its face value
  if(any(wqdata_A$CenType=="Left")){
    wqdata_A$Value[wqdata_A$CenType=="Left"] <- wqdata_A$origValue[wqdata_A$CenType=="Left"]/2
  }
  if(any(wqdata_A$CenType=="Right")){
    wqdata_A$Value[wqdata_A$CenType=="Right"] <- wqdata_A$origValue[wqdata_A$CenType=="Right"]*1.1
  }
  
  wqdata_A$Date=lubridate::dmy(wqdata_A$Date)
  
  freqs <- wqdata_A%>%split(.$LawaSiteID)%>%purrr::map(~freqCheck(.))%>%unlist
  wqdata_A$Frequency=freqs[wqdata_A$LawaSiteID]  
  rm(freqs)
  #This medianing was until July 5 20nineteen in the SWQ_State script
  wqdata_med <- wqdata_A%>%
    dplyr::group_by(LawaSiteID,Date)%>%
    dplyr::summarise(.groups='keep',
                     SiteID=paste(unique(SiteID),collapse='&'),
                     CouncilSiteID=paste(unique(CouncilSiteID),collapse='&'),
                     Agency=paste(unique(Agency),collapse='&'),
                     Region=paste(unique(Region),collapse='&'),
                     Value=median(Value,na.rm=T),
                     Measurement=unique(Measurement,na.rm=T),  
                     n=n(),
                     Censored=any(Censored),
                     CenType=paste(unique(CenType[CenType!='FALSE']),collapse=''),
                     Landcover=paste(unique(Landcover),collapse=''),
                     SWQAltitude=paste(unique(SWQAltitude),collapse='')
    )%>%ungroup
  
  return(wqdata_med)
}->wqdataPerDateMedian
stopCluster(workers)
rm(workers)
cat(Sys.time()-startTime)  
#14.4 seconds June23
#36.2 s june30, now keeping all years
#44.5 aug21 all years
#44.8 aug21 from 2004 onward
#41.1  29June 2021
#55.8  16/7/2021
#53    30/7/2021
#51 12/8/21   1432691 to 1419310

# Saving the wqdataPerDateMedian table to be USED in NOF calculations. 
save(wqdataPerDateMedian,file=paste0("h:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),
                          "/wqdataPerDateMedian",StartYear5,"-",EndYear,"ec6.RData"))
# load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",pattern='wqdataPerDateMedian',recursive = T,full.names=T,ignore.case=T),1),verbose = T)

# Subset to just have the variables that are tested against NOF/NPSFM standards
sub_swq <- wqdataPerDateMedian%>%dplyr::select(c("LawaSiteID","CouncilSiteID","Date","Measurement","Value"))%>%
  dplyr::filter(tolower(Measurement)%in%tolower(c("NH4","TON","ECOLI","PH","DRP","NO3N","DIN","BDISC")))%>%
  dplyr::filter(lubridate::year(Date)<=EndYear)


table(lubridate::year(lubridate::dmy(wqdata$Date)),wqdata$Measurement)
#      BDISC   DIN   DRP ECOLI   NH4  NO3N    PH
# 2004  3802  2440  4656  2676  4735  1568  4049
# 2005  3904  2662  4915  3878  4990  1812  4290
# 2006  4919  2958  5958  5317  6114  2051  5467
# 2007  5138  3412  6670  6229  6843  2468  6484
# 2008  5576  3934  7430  6978  7598  2314  7116
# 2009  5786  4492  8041  7562  8148  3377  7872
# 2010  5642  4464  8150  7550  8211  3865  7797
# 2011  5775  4785  8551  7939  8562  4435  8274
# 2012  6206  5166  8939  8220  8985  4948  8741
# 2013  7153  6061  9944 10069 10013  6164  9754
# 2014  7533  6456 10494 10713 10520  6593 10255
# 2015  8038  6577 11030 11204 11046  6957 10813
# 2016  8381  6886 11277 11519 11337  7251 10943
# 2017  8603  7192 11652 11871 11668  7621 11043
# 2018  8672  7200 11638 12068 11660  7805 11168
# 2019  8556  7566 11516 11919 11499  7981 10950
# 2020  7496  6957 10291 10670 10323  7145  9314

rm(wqdataPerDateMedian,wqdata)


#+++++++++++++++++++++++++++++ Ammonia adjustment for pH++++++++++++++++++++++++++++++++++++
csv="H:/ericg/16666LAWA/LAWA2021/WaterQuality/metadata/NOFAmmoniaAdjustment.csv"
adjnh4=NH4adj(sub_swq,meas=c("NH4","PH"),csv = csv)
sub_swq<-rbind(sub_swq,adjnh4)
rm(adjnh4)
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

sub_swq$YearQuarter=paste0(quarters(sub_swq$Date),year(sub_swq$Date))
if(0){
  par(mfrow=c(3,2),mar=c(5,4,4,2))
  sub_swq%>%group_by(LawaSiteID,Measurement)%>%
    dplyr::summarise(nQ=length(unique(YearQuarter)),nA=n())%>%
    ungroup%>%
    split(.$Measurement)%>%purrr::map(~plot(rev(cumsum(rev(tabulate(.$nQ)))),main=unique(.$Measurement),ylim=c(0,1000),xlab='Num quarters',ylab='Number of sites'))
  par(mfrow=c(1,1))
  par(mfrow=c(3,2),mar=c(5,4,4,2))
  sub_swq%>%group_by(LawaSiteID,Measurement)%>%
    dplyr::summarise(nQ=length(unique(YearQuarter)),nA=n())%>%
    ungroup%>%
    split(.$Measurement)%>%purrr::map(~plot(rev(cumsum(rev(tabulate(.$nA)))),main=unique(.$Measurement),ylim=c(0,1000),xlim=c(0,250),xlab='Num measures',ylab='Number of sites'))
  par(mfrow=c(1,1))
  par(mfrow=c(3,2),mar=c(5,4,4,2))
  sub_swq%>%group_by(LawaSiteID,Measurement)%>%
    dplyr::summarise(nQ=length(unique(YearQuarter)),nA=n())%>%
    ungroup%>%
    split(.$Measurement)%>%purrr::map(~{plot(.$nA,.$nQ,main=unique(.$Measurement),
                                             ylim=c(0,60),xlim=c(0,250),xlab='Num measures',ylab='Number of quarters');abline(0,1)})
  par(mfrow=c(1,1))
  
  sub_swq%>%filter(Measurement=="PH")%>%group_by(LawaSiteID)%>%dplyr::summarise(nQ=length(unique(YearQuarter)),nA=n())%>%
    ungroup%>%filter(nQ==nA)
}

uLAWAids <- unique(sub_swq$LawaSiteID)
# uLAWAids = sample(uLAWAids,size=5,replace = F)
cat(length(uLAWAids),'\t')
if(exists("NOFSummaryTable")) { rm("NOFSummaryTable") }

workers <- makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
library(tidyverse)
  source('H:/ericg/16666LAWA/LAWA2021/scripts/LAWAFunctions.R',echo=F)
})
startTime=Sys.time()

foreach(i = 1:length(uLAWAids),.combine=rbind,.errorhandling="stop",.inorder=F)%dopar%{
  suppressWarnings(rm(tonsite,nh4site,ecosite,drpsite,suspsedsite,rightSite,value,Value)  )
  rightSite <- sub_swq%>%
    dplyr::filter(LawaSiteID==uLAWAids[i])%>%
    tidyr::drop_na(Value)%>%
    mutate(Year=format(Date,'%Y'))
  # create table of compliance with proposed National Objectives Framework ####
  Com_NOF <- data.frame (LawaSiteID               = rep(uLAWAids[i],length(yr)),
                         Year                     = yr,
                         NitrateMed               = as.numeric(rep(NA,reps)),
                         NitrateMed_Band          = rep(as.character(NA),reps),
                         Nitrate95                = as.numeric(rep(NA,reps)),
                         Nitrate95_Band           = rep(as.character(NA)),
                         Nitrate_Toxicity_Band    = rep(as.character(NA),reps),
                         NitrateAnalysisNote      = rep('',reps),
                         AmmoniacalMed            = as.numeric(rep(NA,reps)),
                         AmmoniacalMed_Band       = rep(as.character(NA),reps),
                         AmmoniacalMax            = as.numeric(rep(NA,reps)),
                         AmmoniacalMax_Band       = rep(as.character(NA),reps),
                         Ammonia_Toxicity_Band    = rep(as.character(NA),reps),
                         AmmoniaAnalysisNote      = rep('',reps),
                         EcoliMed                 = as.numeric(rep(NA,reps)),
                         EcoliMed_Band            = rep(as.character(NA),reps),
                         Ecoli95                  = as.numeric(rep(NA,reps)),
                         Ecoli95_Band             = rep(as.character(NA),reps),
                         EcoliRecHealth540        = as.numeric(rep(NA,reps)),
                         EcoliRecHealth540_Band   = rep(as.character(NA),reps),
                         EcoliRecHealth260        = as.numeric(rep(NA,reps)),
                         EcoliRecHealth260_Band   = rep(as.character(NA),reps),
                         EcoliAnalysisNote        = rep('',reps),
                         DRPMed                   = as.numeric(rep(NA,reps)),
                         DRPMed_Band              = rep(as.character(NA),reps),
                         DRP95                    = as.numeric(rep(NA,reps)),
                         DRP95_Band               = rep(as.character(NA),reps),
                         DRP_Summary_Band         = rep(as.character(NA),reps),
                         DRPAnalysisNote          = rep('',reps),
                         SusSedMed                = as.numeric(rep(NA,reps)),
                         SusSedBand               = rep(as.character(NA),reps),
                         SusSedAnalysisNote       = rep('',reps),
                         stringsAsFactors = FALSE)
  ###################### Nitrate  ########################################
  
    tonsite <- rightSite%>%
    dplyr::filter(Measurement%in%c("NO3N"))%>%
    pivot_wider(names_from = 'Measurement',values_from = Value)
  if(dim(tonsite)[1]>0){
    if(all(c("NO3N","TON")%in%names(tonsite))){
      tonsite <- tonsite%>%mutate(Value=ifelse(is.na(NO3N),TON,NO3N))
    }else{
      if('NO3N'%in%names(tonsite)){
        tonsite <- tonsite%>%dplyr::rename(Value=NO3N)
      }else{
        tonsite <- tonsite%>%dplyr::rename(Value=TON)
      }
    }  
  #Median Nitrate
  annualMedian <- tonsite%>%dplyr::group_by(Year)%>%dplyr::summarise(Value=quantile(Value,prob=0.5,type=5))
  if(dim(annualMedian)[1]!=0){
    Com_NOF$NitrateMed <- annualMedian$Value[match(Com_NOF$Year,annualMedian$Year)]
    #Rolling 5yr median
    rollingMeds=rolling5(siteChemSet = tonsite,quantProb = 0.5,nreq=54,quReq=18)
    rollFails= grepl(pattern = '^n',x = rollingMeds,ignore.case=T)
    rollSucc = grepl(pattern='^[[:digit:]]',rollingMeds)
    Com_NOF$NitrateMed[yr%in%names(rollingMeds)[rollSucc]] <- readr::parse_number(rollingMeds[rollSucc])
    Com_NOF$NitrateAnalysisNote[rollyrs[rollFails]] <- paste0(' Need 54 values for 5yr median, have ',rollingMeds[rollFails])
    #find the band which each value belong to
    Com_NOF$NitrateMed_Band <- sapply(Com_NOF$NitrateMed,NOF_FindBand,bandColumn = NOFbandDefinitions$`Median Nitrate`)
    Com_NOF$NitrateMed_Band[!is.na(Com_NOF$NitrateMed)] <- 
      sapply(Com_NOF$NitrateMed_Band[!is.na(Com_NOF$NitrateMed)],FUN=function(x){min(unlist(strsplit(x,split = '')))})
    rm(annualMedian,rollingMeds,rollFails,rollSucc)
    
    #95th percentile Nitrate
    annual95 <- tonsite%>%dplyr::group_by(Year)%>%dplyr::summarise(Value=quantile(Value,prob=0.95,type=5,na.rm=T))
    Com_NOF$Nitrate95 = annual95$Value[match(Com_NOF$Year,annual95$Year)]
    #Rolling 5yr 95%ile
    rolling95=rolling5(tonsite,0.95,nreq=54,quReq=18)
    rollFails= grepl(pattern = '^n',x = rolling95,ignore.case=T)
    rollSucc = grepl(pattern='^[[:digit:]]',rolling95)
    Com_NOF$Nitrate95[yr%in%names(rolling95)[rollSucc]] <- readr::parse_number(rolling95[rollSucc])
    Com_NOF$NitrateAnalysisNote[rollyrs[rollFails]] <- paste0(Com_NOF$NitrateAnalysisNote[rollyrs[rollFails]],
                                                                          ' Need 54 values and 20 quarters for 5yr 95%ile, have ',rolling95[rollFails])
    #find the band which each value belong to
    Com_NOF$Nitrate95_Band <- sapply(Com_NOF$Nitrate95,NOF_FindBand,bandColumn = NOFbandDefinitions$`95th Percentile Nitrate`)
    Com_NOF$Nitrate95_Band[!is.na(Com_NOF$Nitrate95)] <- 
      sapply(Com_NOF$Nitrate95_Band[!is.na(Com_NOF$Nitrate95)],FUN=function(x){min(unlist(strsplit(x,split = '')))})
    
    #Nitrate Toxicity
    #The worse of the two nitrate bands
    Com_NOF$Nitrate_Toxicity_Band = apply(Com_NOF%>%dplyr::select(NitrateMed_Band, Nitrate95_Band),1,max,na.rm=T)
    rm(annual95,rolling95,rollFails,rollSucc)
  }else{
    Com_NOF$NitrateAnalysisNote = paste0('n = ',sum(!is.na(tonsite$Value)),' Insufficient to calculate annual medians ')
  }}
  rm(tonsite)
  
  ###################### Ammonia  ############################
  nh4site=rightSite%>%dplyr::filter(Measurement=="NH4adj")
  if(all(nh4site$Value==-99)){
    Com_NOF$AmmoniaAnalysisNote=paste0('No pH data available for NH4 adjustment, so NH4 cannot be judged against NOF standards. ')
  }else{
    nh4site=nh4site[!(nh4site$Value==(-99)),]
    #Median Ammoniacal Nitrogen
    annualMedian <- nh4site%>%dplyr::group_by(Year)%>%dplyr::summarise(Value=quantile(Value,prob=0.5,type=5,na.rm=T))
    if(dim(annualMedian)[1]!=0){
      Com_NOF$AmmoniacalMed = annualMedian$Value[match(Com_NOF$Year,annualMedian$Year)]
      #Rolling 5yr median
      rollingMeds=rolling5(nh4site,0.5,nreq=54,quReq=18)
      rollFails= grepl(pattern = '^n',x = rollingMeds,ignore.case=T)
      rollSucc = grepl(pattern='^[[:digit:]]',rollingMeds)
      Com_NOF$AmmoniacalMed[yr%in%names(rollingMeds)[rollSucc]] <- readr::parse_number(rollingMeds[rollSucc])
      Com_NOF$AmmoniaAnalysisNote[rollyrs[rollFails]] <- paste0(' Need 54 values for 5yr median, have ',rollingMeds[rollFails])      
      Com_NOF$AmmoniacalMed_Band <- sapply(Com_NOF$AmmoniacalMed,NOF_FindBand,bandColumn=NOFbandDefinitions$`Median Ammoniacal N`) 
      Com_NOF$AmmoniacalMed_Band[!is.na(Com_NOF$AmmoniacalMed_Band)] <- 
        sapply(Com_NOF$AmmoniacalMed_Band[!is.na(Com_NOF$AmmoniacalMed_Band)],FUN=function(x){min(unlist(strsplit(x,split = '')))})
      rm(annualMedian,rollingMeds,rollFails,rollSucc)
      
      #max  Ammoniacal Nitrogen
      annualmax <- nh4site%>%dplyr::group_by(Year)%>%dplyr::summarise(Value=max(Value,na.rm=T))
      Com_NOF$AmmoniacalMax <- annualmax$Value[match(Com_NOF$Year,annualmax$Year)]
      #Rolling 5yr max
      rollingMax=rolling5(nh4site,1,nreq=54,quReq=18)
      rollFails= grepl(pattern = '^n',x = rollingMax,ignore.case=T)
      rollSucc = grepl(pattern='^[[:digit:]]',rollingMax)
      Com_NOF$AmmoniacalMax[yr%in%names(rollingMax)[rollSucc]] <- readr::parse_number(rollingMax[rollSucc])
      Com_NOF$AmmoniaAnalysisNote[rollyrs[rollFails]] <- paste0(Com_NOF$AmmoniaAnalysisNote[rollyrs[rollFails]],
                                                                            ' Need 54 values for 5yr max, have',rollingMax[rollFails])
      Com_NOF$AmmoniacalMax_Band <-sapply(Com_NOF$AmmoniacalMax,NOF_FindBand,bandColumn=NOFbandDefinitions$`Max Ammoniacal N`)
      Com_NOF$AmmoniacalMax_Band <- sapply(Com_NOF$AmmoniacalMax_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))})
      
      #Ammonia Toxicity
      Com_NOF$Ammonia_Toxicity_Band=apply(Com_NOF%>%dplyr::select(AmmoniacalMed_Band, AmmoniacalMax_Band),1,max,na.rm=T)
    }else{
      Com_NOF$AmmoniaAnalysisNote=paste0(Com_NOF$AmmoniaAnalysisNote,'n = ',sum(!is.na(nh4site$Value)),' Insufficient to calculate annual medians. ')
    }  
    rm(annualMax,rollingMax,rollFails,rollSucc)
  }
  rm(nh4site)
  
  ######################  E.Coli #########################################
  suppressWarnings(rm(cnEc_Band,cnEc95_Band,cnEcRecHealth540_Band,cnEcRecHealth260_Band,ecosite,ecosite))
  ecosite=rightSite%>%dplyr::filter(Measurement=="ECOLI")
  
  #E coli median
  annualMedian <- ecosite%>%dplyr::group_by(Year)%>%dplyr::summarise(Value=quantile(Value,prob=0.5,type=5,na.rm=T))
  if(dim(annualMedian)[1]!=0){
    Com_NOF$EcoliMed <- annualMedian$Value[match(Com_NOF$Year,annualMedian$Year)]
    #rolling 5yr or 6yr median
    rollingMeds=rolling5(ecosite,0.5,nreq=54,quReq=18)
    rollFails=grepl(pattern = '^n',x = rollingMeds,ignore.case=T)
    rollSucc = grepl(pattern='^[[:digit:]]',rollingMeds)
    Com_NOF$EcoliMed[yr%in%names(rollingMeds)[rollSucc]] = readr::parse_number(rollingMeds[rollSucc])
    Com_NOF$EcoliAnalysisNote[rollyrs[rollFails]] <- paste0(' Need 54 values for 5yr median, have',rollingMeds[rollFails])
    Com_NOF$EcoliMed_Band <- sapply(Com_NOF$EcoliMed,NOF_FindBand,bandColumn=NOFbandDefinitions$`E. coli`)
  }else{
    Com_NOF$EcoliAnalysisNote=paste0(Com_NOF$EcoliAnalysisNote,"n = ",sum(!is.na(ecosite$Value))," Insufficient data to calculate annual medians")
  }
  rm(annualMedian,rollingMeds,rollFails,rollSucc)
  
  #Ecoli 95th percentile 
  annual95 <- ecosite%>%dplyr::group_by(Year)%>%dplyr::summarise(Value=quantile(Value,prob=0.95,type=5,na.rm=T))
  if(length(annual95)!=0){
    Com_NOF$Ecoli95 <- annual95$Value[match(Com_NOF$Year,annual95$Year)]
    #rolling 5yr or 6yr 95%ile
    rolling95 = rolling5(ecosite,0.95,nreq=54,quReq=18)
    rollFails=grepl(pattern='^n',x=rolling95,ignore.case=T)
    rollSucc= grepl(pattern='^[[:digit:]]',x=rolling95)
    Com_NOF$Ecoli95[yr%in%names(rolling95)[rollSucc]] <- readr::parse_number(rolling95[rollSucc])
    Com_NOF$EcoliAnalysisNote[rollyrs[rollFails]] <- paste0(Com_NOF$EcoliAnalysisNote[rollyrs[rollFails]],
                                                            ' Need 54 values for 5yr 95%ile, have',rolling95[rollFails])
    Com_NOF$Ecoli95_Band <- sapply(Com_NOF$Ecoli95,NOF_FindBand,bandColumn=NOFbandDefinitions$Ecoli95)
  }else{
    Com_NOF$EcoliAnalysisNote=paste0(Com_NOF$EcoliAnalysisNote,"n = ",sum(!is.na(ecosite$Value))," Insufficient data to calculate annual max")
  }
  rm(annual95,rolling95,rollFails,rollSucc)
  
  #Exceedance percentages
  options(warn=-1)
  for(yy in 1:length(Com_NOF$Year)){
    if(!is.na(as.numeric(Com_NOF$Year[yy]))){
      #Single year
      ecv=ecosite$Value[which(ecosite$Year==Com_NOF$Year[yy])]
    if(length(ecv)>10){
      Com_NOF$EcoliRecHealth540[yy]=sum(ecv>540)/length(ecv)*100
      Com_NOF$EcoliRecHealth260[yy]=sum(ecv>260)/length(ecv)*100
    }
    }else{
      #Year range
      startYear = strTo(s = Com_NOF$Year[yy],c = 'to')
      stopYear = strFrom(s= Com_NOF$Year[yy],c = 'to')
      ecv=ecosite$Value[ecosite$Year>=startYear & ecosite$Year<=stopYear]
      if(length(ecv)>54){
        Com_NOF$EcoliRecHealth540[yy]=sum(ecv>540)/length(ecv)*100
        Com_NOF$EcoliRecHealth260[yy]=sum(ecv>260)/length(ecv)*100
      }
    }
  }
  options(warn=0)
  #Bands
   suppressWarnings(Com_NOF$EcoliRecHealth540_Band <- sapply(Com_NOF$EcoliRecHealth540,NOF_FindBand,bandColumn=NOFbandDefinitions$EcoliRec540))
   suppressWarnings(Com_NOF$EcoliRecHealth260_Band <- sapply(Com_NOF$EcoliRecHealth260,NOF_FindBand,bandColumn=NOFbandDefinitions$EcoliRec260))

  rm(ecosite)

  ###################### DRP ####
  drpsite <- rightSite%>%dplyr::filter(Measurement=='DRP')
  #Median DRP
  annualMedian <- drpsite%>%dplyr::group_by(Year)%>%dplyr::summarise(Value=quantile(Value,prob=0.5,type=5))
  if(dim(annualMedian)[1]!=0){
    Com_NOF$DRPMed <- annualMedian$Value[match(Com_NOF$Year,annualMedian$Year)]
    #Rolling 5yr median
    rollingMeds=rolling5(siteChemSet = drpsite,quantProb=0.5,nreq=54,quReq=18)
    rollFails = grepl(pattern = '^n',x = rollingMeds,ignore.case=T)
    rollSucc = grepl(pattern='^[[:digit:]]',rollingMeds)
    Com_NOF$DRPMed[yr%in%names(rollingMeds)[rollSucc]] <- readr::parse_number(rollingMeds[rollSucc])
    Com_NOF$DRPAnalysisNote[rollyrs[rollFails]] <- paste0(" Need 54 values for 5yr median, have",rollingMeds[rollFails])
    Com_NOF$DRPMed_Band <- sapply(X = Com_NOF$DRPMed,NOF_FindBand,bandColumn=NOFbandDefinitions$MedianDRP)
    Com_NOF$DRPMed_Band[!is.na(Com_NOF$DRPMed_Band)] <- 
      sapply(Com_NOF$DRPMed_Band[!is.na(Com_NOF$DRPMed_Band)],FUN=function(x){min(unlist(strsplit(x,split='')))})
    rm(annualMedian,rollingMeds,rollFails,rollSucc)
    
    #95th percentile DRP
    annual95 <- drpsite%>%dplyr::group_by(Year)%>%dplyr::summarise(Value=quantile(Value,prob=0.95,type=5,na.rm=T))
    Com_NOF$DRP95 = annual95$Value[match(Com_NOF$Year,annual95$Year)]
    #Rolling 5yr 95%ile
    rolling95=rolling5(drpsite,0.95,nreq=54,quReq=18)
    rollFails=grepl(pattern = '^n',x = rolling95,ignore.case=T)
    rollSucc = grepl(pattern='^[[:digit:]]',rolling95)
    Com_NOF$DRP95[yr%in%names(rolling95)[rollSucc]] <- readr::parse_number(rolling95[rollSucc])
    Com_NOF$DRPAnalysisNote[rollyrs[rollFails]] <- paste0(Com_NOF$DRPAnalysisNote[rollyrs[rollFails]],
                                                          ' Need 54 values for 5yr 95%ile, have ',rolling95[rollFails])
    #find the band which each value belong to
    Com_NOF$DRP95_Band <- sapply(Com_NOF$DRP95,NOF_FindBand,bandColumn = NOFbandDefinitions$`95DRP`)
    Com_NOF$DRP95_Band[!is.na(Com_NOF$DRP95_Band)] <- 
      sapply(Com_NOF$DRP95_Band[!is.na(Com_NOF$DRP95_Band)],FUN=function(x){min(unlist(strsplit(x,split = '')))})
    
    #DRP Summary
    #The worse of the two DRP bands
    Com_NOF$DRP_Summary_Band = apply(Com_NOF%>%dplyr::select(DRPMed_Band, DRP95_Band),1,max,na.rm=T)
    rm(annual95,rolling95,rollFails,rollSucc)
  }else{
    Com_NOF$DRPAnalysisNote = paste0('n = ',sum(!is.na(drpsite$Value)),' Insufficient to calculate annual medians ')
  }
  rm(drpsite)
  
  ################## BDISC CLARITY SUSPENDED SEDIMENT ####
  suspsedsite <- rightSite%>%dplyr::filter(Measurement=="BDISC")
  annualMedian <- suspsedsite%>%dplyr::group_by(Year)%>%dplyr::summarise(Value=quantile(Value,prob=0.5,type=5))
  if(dim(annualMedian)[1]!=0){
    sedimentClass = unique(riverSiteTable$SedimentClass[which(tolower(riverSiteTable$LawaSiteID) == tolower(uLAWAids[i]) )])
    Com_NOF$SusSedMed <- annualMedian$Value[match(Com_NOF$Year,annualMedian$Year)]
    #Rolling 5yr median
    rollingMeds = rolling5(siteChemSet = suspsedsite,quantProb=0.5,nreq=54,quReq=18) #NPSFM says 60, but we say 54
    rollFails   = grepl(pattern = '^n',x=rollingMeds,ignore.case=T)
    rollSucc    = grepl(pattern = '^[[:digit:]]',rollingMeds)
    Com_NOF$SusSedMed[yr%in%names(rollingMeds)[rollSucc]] <- readr::parse_number(rollingMeds[rollSucc])
    Com_NOF$SusSedAnalysisNote[rollyrs[rollFails]] <- paste0("Need 54 values for 5yr median, have ",rollingMeds[rollFails])
    Com_NOF$SusSedBand <- sapply(Com_NOF$SusSedMed,NOF_FindBand,bandColumn=NPSFMSusSed[,sedimentClass+1])
  }else{
    Com_NOF$SusSedAnalysisNote="No clarity data"
  }
  
  #clear up and return ####
    rm(rightSite)
  # if(dim(Com_NOF)[2]!=32){stop()}
  return(Com_NOF)
}->NOFSummaryTable
stopCluster(workers)
rm(workers)
cat(Sys.time()-startTime)  
#31s 7/9/2020
#19s  29 June2021
#30s 14.7.2021
#26s  16/7/2021
#29  23/7/2021  31320
#30  30/7/2021 31590
#26 12/8/21  31590
#31 13/8/21 31620

NOFSummaryTable$EcoliMed_Band <- sapply(NOFSummaryTable$EcoliMed,NOF_FindBand,bandColumn=NOFbandDefinitions$`E. coli`)

#These contain the best case out of these scorings, the worst of which contributes.
suppressWarnings(cnEc_Band <- sapply(NOFSummaryTable$EcoliMed_Band,FUN=function(x){
    min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEc95_Band <- sapply(NOFSummaryTable$Ecoli95_Band,FUN=function(x){
    min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEcRecHealth540_Band <- sapply(NOFSummaryTable$EcoliRecHealth540_Band,FUN=function(x){
  min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEcRecHealth260_Band <- sapply(NOFSummaryTable$EcoliRecHealth260_Band,FUN=function(x){
  min(unlist(strsplit(x,split = '')))}))  
NOFSummaryTable$EcoliSummaryband = as.character(apply(cbind(pmax(cnEc_Band,cnEc95_Band,cnEcRecHealth540_Band,cnEcRecHealth260_Band)),1,max))
rm("cnEc_Band","cnEc95_Band","cnEcRecHealth540_Band","cnEcRecHealth260_Band")

if(0){
  with(NOFSummaryTable,plot(as.factor(DRPMed_Band),DRPMed,log='y'))
  with(NOFSummaryTable,plot(as.factor(NitrateMed_Band),NitrateMed))
  with(NOFSummaryTable,plot(as.factor(Nitrate95_Band),Nitrate95))
  with(NOFSummaryTable,plot(as.factor(Nitrate_Toxicity_Band),Nitrate95))
  with(NOFSummaryTable[which(NOFSummaryTable$AmmoniacalMed>0),],plot(as.factor(AmmoniacalMed_Band),AmmoniacalMed,log='y'))
  with(NOFSummaryTable,plot(as.factor(EcoliSummaryband[EcoliMed>0]),EcoliMed[EcoliMed>0],log='y'))
  with(NOFSummaryTable,plot(as.factor(SusSedBand[SusSedMed>0]),SusSedMed[SusSedMed>0],log='y'))
  table(NOFSummaryTable$NitrateMed_Band,NOFSummaryTable$Nitrate_Toxicity_Band)
  table(NOFSummaryTable$Nitrate95_Band,NOFSummaryTable$Nitrate_Toxicity_Band)
  table(NOFSummaryTable$AmmoniacalMed_Band,NOFSummaryTable$Ammonia_Toxicity_Band)
  table(NOFSummaryTable$Ammoniacal95_Band,NOFSummaryTable$Nitrate_Toxicity_Band)
  table(NOFSummaryTable$Ecoli95_Band,NOFSummaryTable$EcoliSummaryband,useNA='a')
  table(NOFSummaryTable$EcoliRecHealth260_Band,NOFSummaryTable$EcoliMed_Band,useNA='a')
}

NOFSummaryTable <- merge(NOFSummaryTable, riverSiteTable,by = "LawaSiteID") 

NOFSummaryTable <- NOFSummaryTable%>%dplyr::select(LawaSiteID,CouncilSiteID,SiteID,Year:SusSedAnalysisNote,everything())

#############################Save the output table ############################
#For audit
write.csv(NOFSummaryTable,
          file = paste0("h:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),
                        "/NOFSummaryTable_All.csv"),row.names=F)
write.csv(NOFSummaryTable%>%dplyr::filter(grepl(pattern = 'to',x = Year)),
          file = paste0("h:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis/",format(Sys.Date(),"%Y-%m-%d"),
                        "/NOFSummaryTable_Rolling.csv"),row.names=F)

# NOFSummaryTable <- read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis/",pattern="NOFSummaryTable_All",recursive = T,full.names = T,ignore.case = T),1),stringsAsFactors = F)

# grep('note',names(NOFSummaryTable),ignore.case = T,value = T)
# nitrAN = unname(sapply( NOFSummaryTable$NitrateAnalysisNote, function(s)strFrom(s,c="%")))
# nitrAN = unname(sapply(nitrAN,function(s)strFrom(s,c='e')))
# nitrAN = nitrAN[nitrAN!=""]
# nitrAN = t(sapply(nitrAN,function(s)as.data.frame(t(parse_number(unlist(strsplit(s,',')))))))
# par(mfrow=c(2,1))
# plot(table(unlist(nitrAN)[1:dim(nitrAN)[1]]),ylab='count',xlab='Num records')
# plot(table(unlist(nitrAN)[(dim(nitrAN)[1]+1):(dim(nitrAN)[1]*2)]),ylab='count',xlab='Num quarters')
# par(mfrow=c(1,1))
# plot(unlist(nitrAN)[1:dim(nitrAN)[1]],unlist(nitrAN)[(dim(nitrAN)[1]+1):(dim(nitrAN)[1]*2)],
#      xlab='Num records',ylab='Num quarters')
# abline(v=53.9,h=19.9,lty=2)
# 
# ammAN = unname(sapply( NOFSummaryTable$AmmoniaAnalysisNote, function(s)strFrom(s,c="x")))
# ammAN = ammAN[ammAN!=""]
# ammAN = unname(sapply(ammAN,function(s)strFrom(s,c='e')))
# ammAN = t(sapply(ammAN,function(s)as.data.frame(t(parse_number(unlist(strsplit(s,',')))))))
# par(mfrow=c(2,1))
# plot(table(unlist(ammAN)[1:dim(ammAN)[1]]),ylab='count',xlab='Num records')
# plot(table(unlist(ammAN)[(dim(ammAN)[1]+1):(dim(ammAN)[1]*2)]),ylab='count',xlab='Num quarters')
# 
# ecoAN = unname(sapply( NOFSummaryTable$EcoliAnalysisNote, function(s)strFrom(s,c="m")))
# ecoAN = ecoAN[!ecoAN%in%c("","edians")]
# ecoAN = unname(sapply(ecoAN,function(s)strFrom(s,c='a')))
# ecoAN = unname(sapply(ecoAN,function(s)strFrom(s,c='e')))
# ecoAN = t(sapply(ecoAN,function(s)as.data.frame(t(parse_number(unlist(strsplit(s,',')))))))
# par(mfrow=c(2,1))
# plot(table(unlist(ecoAN)[1:dim(ecoAN)[1]]),ylab='count',xlab='Num records')
# plot(table(unlist(ecoAN)[(dim(ecoAN)[1]+1):(dim(ecoAN)[1]*2)]),ylab='count',xlab='Num quarters')
# 
# drpAN = unname(sapply( NOFSummaryTable$DRPAnalysisNote, function(s)strFrom(s,c="%")))
# drpAN = drpAN[!drpAN%in%c("","n = 0 Insufficient to calculate annual medians ")]
# drpAN = unname(sapply(drpAN,function(s)strFrom(s,c='e')))
# drpAN = unname(sapply(drpAN,function(s)strFrom(s,c='e')))
# drpAN = t(sapply(drpAN,function(s)as.data.frame(t(parse_number(unlist(strsplit(s,',')))))))
# par(mfrow=c(2,1))
# plot(table(unlist(drpAN)[1:dim(drpAN)[1]]),ylab='count',xlab='Num records')
# plot(table(unlist(drpAN)[(dim(drpAN)[1]+1):(dim(drpAN)[1]*2)]),ylab='count',xlab='Num quarters')


# ************
# Note this is not the full summary table - only rolling years
# NOFSummaryTable <- read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis/",pattern="NOFSummaryTable_Rolling",recursive = T,full.names = T,ignore.case = T),1),stringsAsFactors = F)
# ************

NOFSummaryTable$SWQAltitude=pseudo.titlecase(NOFSummaryTable$SWQAltitude)
NOFSummaryTable$SWQLanduse=pseudo.titlecase(NOFSummaryTable$SWQLanduse)

#For ITE
#Make outputs for ITE
# Reshape Output
RiverNOF <-
  NOFSummaryTable%>%
  dplyr::filter(Year=="2016to2020")%>%                #To include only the latest
  dplyr::rename(LAWAID=LawaSiteID,
                SiteName=CouncilSiteID,
                Year=Year)%>%
  dplyr::select(-SiteID,-accessDate,-Lat,-Long,-AltitudeCl,-SWQAltitude,-SWQLanduse,-rawSWQLanduse,-rawRecLandcover,
                -Altitude,-Landcover,-NZReach,-NZSegment,-SedimentClass,-Agency,-Region,-Catchment,-ends_with('Note'))%>%
  tidyr::drop_na(LAWAID)%>%
  dplyr::mutate_if(is.factor,as.character)%>%
  reshape2::melt(id.vars=c("LAWAID","SiteName","Year"))%>%
  dplyr::rename(Parameter=variable,Value=value)

#Get letter values in one column, and numeric values in the other
RiverNOF$Band=RiverNOF$Value
RiverNOF$Value=as.numeric(RiverNOF$Value)
RiverNOF$Band[!is.na(RiverNOF$Value)] <- NA

write.csv(RiverNOF,
          file=paste0("h:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                      "/ITERiverNOF",format(Sys.time(),"%d%b%Y"),".csv"),row.names=F)
rm(RiverNOF)


# RiverNOF = read_csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis/",
#                              pattern="ITERiverNOF",full.names = T,recursive = T),1))


