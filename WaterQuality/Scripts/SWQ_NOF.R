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
# NOFbandDefinitions <- NOFbandDefinitions[,1:13]
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
#51    12/8/21   1432691 to 1419310
#48    30/8/21   1475007 to 1462090
#53     8/9/21   1455119 to 1444836

# Saving the wqdataPerDateMedian table to be USED in NOF calculations. 
save(wqdataPerDateMedian,file=paste0("h:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),
                          "/wqdataPerDateMedian",StartYear5,"-",EndYear,"ec6.RData"))
# load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",pattern='wqdataPerDateMedian',recursive = T,full.names=T,ignore.case=T),1),verbose = T)

# Subset to just have the variables that are tested against NOF/NPSFM standards
sub_swq <- wqdataPerDateMedian%>%dplyr::select(c("Agency","LawaSiteID","CouncilSiteID","Date","Measurement","Value"))%>%
  dplyr::filter(tolower(Measurement)%in%tolower(c("NH4","TON","ECOLI","PH","DRP","NO3N","DIN","BDISC")))%>%
  dplyr::filter(lubridate::year(Date)<=EndYear)


table(lubridate::year(lubridate::dmy(wqdata$Date)),wqdata$Measurement)
#     BDISC   DIN   DRP ECOLI   NH4  NO3N    PH    TN   TON    TP  TURB TURBFNU
# 2004  3622  2476  4434  2709  4514  1970  3843  3470  4036  3906  4423      78
# 2005  3915  2698  4705  3709  4776  2221  4145  3596  4300  4004  4827     188
# 2006  5012  2988  5746  5138  5900  2480  5282  4527  5356  5166  5989     257
# 2007  5224  3438  6450  6036  6627  2889  6332  5161  6074  5874  6867     251
# 2008  5638  3959  7177  6748  7345  2703  6900  5433  6806  6543  7158     275
# 2009  5812  4522  7794  7341  7900  3872  7873  6539  7370  7042  7568     290
# 2010  5557  4488  7841  7269  7901  4259  7747  6766  7370  7004  7988     283
# 2011  5759  4812  8277  7681  8278  4929  8256  7301  7755  7426  8179     295
# 2012  6343  5370  8713  8015  8765  5521  8654  7884  8239  7889  8646     284
# 2013  7364  6453  9688  9853  9765  6804  9583  8911  9148  8913  9662     283
# 2014  7880  6871 10245 10510 10275  7123 10114  9412  9515  9420 10093     283
# 2015  8439  7074 10811 11020 10837  7960 10859 10087 10617 10082 10661     309
# 2016  8973  7563 11149 11418 11217  8351 10807 10803 11259 10551 10790     362
# 2017  9199  7856 11542 11744 11569  8756 10936 11524 11553 11260 10987     508
# 2018  9244  7400 11538 11885 11564  8900 11071 11522 11580 11260 10796     655
# 2019  9166  7810 11488 11870 11480  9077 10903 11423 11308 11171  9901    1627
# 2020  8137  7246 10522 10876 10563  8410  9529 10508 10245  9978  9099    1785

rm(wqdataPerDateMedian,wqdata)


#+++++++++++++++++++++++++++++ Ammonia adjustment for pH++++++++++++++++++++++++++++++++++++
csv="H:/ericg/16666LAWA/LAWA2021/WaterQuality/metadata/NOFAmmoniaAdjustment.csv"
adjnh4=NH4adj(sub_swq,meas=c("NH4","PH"),csv = csv)
sub_swq<-rbind(sub_swq,adjnh4)
rm(adjnh4,csv)
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# sub_swq$YearQuarter=paste0(quarters(sub_swq$Date),year(sub_swq$Date))
sub_swq <- sub_swq%>%
  mutate(Year=format(Date,'%Y'),
         YearQuarter=paste0(quarters(Date),Year))%>%
  drop_na(Value)%>%
  filter(Measurement!="NH4")

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
  suppressWarnings(rm(DINSite,NtoxSite,nh4site,ecosite,drpsite,suspsedsite,rightSite,value,Value)  )
  rightSite <- sub_swq%>%dplyr::filter(LawaSiteID==uLAWAids[i])
  
  # create table of compliance with proposed National Objectives Framework ####
  Com_NOF <- data.frame (LawaSiteID               = rep(uLAWAids[i],length(yr)),
                         Year                     = yr,
                         
                         DINMed                   = as.numeric(rep(NA,reps)),
                         DINMed_Band              = rep(as.character(NA),reps),
                         DIN95                    = as.numeric(rep(NA,reps)),
                         DIN95_Band               = rep(as.character(NA),reps),
                         DIN_Summary_Band          = rep(as.character(NA),reps),
                         DINAnalysisNote          = rep('',reps),
                         
                         NO3NMed               = as.numeric(rep(NA,reps)),
                         NO3NMed_Band          = rep(as.character(NA),reps),
                         NO3N95                = as.numeric(rep(NA,reps)),
                         NO3N95_Band           = rep(as.character(NA)),
                         NO3N_Toxicity_Band    = rep(as.character(NA),reps),
                         NO3NAnalysisNote      = rep('',reps),

                         TONMed                   = as.numeric(rep(NA,reps)),
                         TONMed_Band              = rep(as.character(NA),reps),
                         TON95                    = as.numeric(rep(NA,reps)),
                         TON95_Band               = rep(as.character(NA)),
                         TON_Toxicity_Band        = rep(as.character(NA),reps),
                         TONAnalysisNote          = rep('',reps),
                         
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
  
  ######################### DIN ##############
  DINSite <- rightSite%>%dplyr::filter(Measurement=="DIN")
  if(dim(DINSite)[1]>0){
    annualMedian <- DINSite%>%dplyr::group_by(Year)%>%dplyr::summarise(Value=quantile(Value,prob=0.5,type=5))
    if(dim(annualMedian)[1]!=0){
      Com_NOF$DINMed <- annualMedian$Value[match(Com_NOF$Year,annualMedian$Year)]
      #Rolling 5yr median
      rollingMeds=rolling5(siteChemSet = DINSite,quantProb = 0.5,nreq=54,quReq=18)
      rollFails= grepl(pattern = '^n',x = rollingMeds,ignore.case=T)
      rollSucc = grepl(pattern='^[[:digit:]]',rollingMeds)
      Com_NOF$DINMed[yr%in%names(rollingMeds)[rollSucc]] <- readr::parse_number(rollingMeds[rollSucc])
      Com_NOF$DINAnalysisNote[rollyrs[rollFails]] <- paste0(' Need 54 values for 5yr median, have ',rollingMeds[rollFails])
      #find the band which each value belong to
      Com_NOF$DINMed_Band <- sapply(Com_NOF$DINMed,NOF_FindBand,bandColumn = NOFbandDefinitions$MedianDIN)
      Com_NOF$DINMed_Band[!is.na(Com_NOF$DINMed)] <-
        sapply(Com_NOF$DINMed_Band[!is.na(Com_NOF$DINMed)],FUN=function(x){min(unlist(strsplit(x,split = '')))})
      rm(annualMedian,rollingMeds,rollFails,rollSucc)
      
      
      #95th percentile DIN
      annual95 <- DINSite%>%dplyr::group_by(Year)%>%dplyr::summarise(Value=quantile(Value,prob=0.95,type=5,na.rm=T))
      Com_NOF$DIN95 = annual95$Value[match(Com_NOF$Year,annual95$Year)]
      #Rolling 5yr 95%ile
      rolling95=rolling5(DINSite,0.95,nreq=54,quReq=18)
      rollFails= grepl(pattern = '^n',x = rolling95,ignore.case=T)
      rollSucc = grepl(pattern='^[[:digit:]]',rolling95)
      Com_NOF$DIN95[yr%in%names(rolling95)[rollSucc]] <- readr::parse_number(rolling95[rollSucc])
      Com_NOF$DINAnalysisNote[rollyrs[rollFails]] <- paste0(Com_NOF$DINAnalysisNote[rollyrs[rollFails]],
                                                                ' Need 54 values and 20 quarters for 5yr 95%ile, have ',rolling95[rollFails])
      #find the band which each value belong to
      Com_NOF$DIN95_Band <- sapply(Com_NOF$DIN95,NOF_FindBand,bandColumn = NOFbandDefinitions$`95DIN`)
      Com_NOF$DIN95_Band[!is.na(Com_NOF$DIN95)] <- 
        sapply(Com_NOF$DIN95_Band[!is.na(Com_NOF$DIN95)],FUN=function(x){min(unlist(strsplit(x,split = '')))})
      rm(annual95,rolling95,rollFails,rollSucc)
      
      #DIN Toxicity
      #The worse of the two DIN bands
      Com_NOF$DIN_Summary_Band = apply(Com_NOF%>%dplyr::select(DINMed_Band, DIN95_Band),1,max,na.rm=T)
    }else{
      Com_NOF$DINAnalysisNote = paste0('n = ',sum(!is.na(DINSite$Value)),' Insufficient to calculate annual medians ')
    }}
  rm(DINSite)
  
  ###################### NO3N  ########################################
  
    NtoxSite <- rightSite%>%
    dplyr::filter(Measurement%in%c("NO3N"))
  if(dim(NtoxSite)[1]>0){
  #Median NO3N
  annualMedian <- NtoxSite%>%dplyr::group_by(Year)%>%dplyr::summarise(Value=quantile(Value,prob=0.5,type=5))
  if(dim(annualMedian)[1]!=0){
    Com_NOF$NO3NMed <- annualMedian$Value[match(Com_NOF$Year,annualMedian$Year)]
    #Rolling 5yr median
    rollingMeds=rolling5(siteChemSet = NtoxSite,quantProb = 0.5,nreq=54,quReq=18)
    rollFails= grepl(pattern = '^n',x = rollingMeds,ignore.case=T)
    rollSucc = grepl(pattern='^[[:digit:]]',rollingMeds)
    Com_NOF$NO3NMed[yr%in%names(rollingMeds)[rollSucc]] <- readr::parse_number(rollingMeds[rollSucc])
    Com_NOF$NO3NAnalysisNote[rollyrs[rollFails]] <- paste0(' Need 54 values for 5yr median, have ',rollingMeds[rollFails])
    #find the band which each value belong to
    Com_NOF$NO3NMed_Band <- sapply(Com_NOF$NO3NMed,NOF_FindBand,bandColumn = NOFbandDefinitions$`Median Nitrate`)
    Com_NOF$NO3NMed_Band[!is.na(Com_NOF$NO3NMed)] <- 
      sapply(Com_NOF$NO3NMed_Band[!is.na(Com_NOF$NO3NMed)],FUN=function(x){min(unlist(strsplit(x,split = '')))})
    rm(annualMedian,rollingMeds,rollFails,rollSucc)
    
    #95th percentile NO3N
    annual95 <- NtoxSite%>%dplyr::group_by(Year)%>%dplyr::summarise(Value=quantile(Value,prob=0.95,type=5,na.rm=T))
    Com_NOF$NO3N95 = annual95$Value[match(Com_NOF$Year,annual95$Year)]
    #Rolling 5yr 95%ile
    rolling95=rolling5(NtoxSite,0.95,nreq=54,quReq=18)
    rollFails= grepl(pattern = '^n',x = rolling95,ignore.case=T)
    rollSucc = grepl(pattern='^[[:digit:]]',rolling95)
    Com_NOF$NO3N95[yr%in%names(rolling95)[rollSucc]] <- readr::parse_number(rolling95[rollSucc])
    Com_NOF$NO3NAnalysisNote[rollyrs[rollFails]] <- paste0(Com_NOF$NO3NAnalysisNote[rollyrs[rollFails]],
                                                                          ' Need 54 values and 20 quarters for 5yr 95%ile, have ',rolling95[rollFails])
    #find the band which each value belong to
    Com_NOF$NO3N95_Band <- sapply(Com_NOF$NO3N95,NOF_FindBand,bandColumn = NOFbandDefinitions$`95th Percentile Nitrate`)
    Com_NOF$NO3N95_Band[!is.na(Com_NOF$NO3N95)] <- 
      sapply(Com_NOF$NO3N95_Band[!is.na(Com_NOF$NO3N95)],FUN=function(x){min(unlist(strsplit(x,split = '')))})
    rm(annual95,rolling95,rollFails,rollSucc)
    
    #NO3N Toxicity
    #The worse of the two NO3N bands
    Com_NOF$NO3N_Toxicity_Band = apply(Com_NOF%>%dplyr::select(NO3NMed_Band, NO3N95_Band),1,max,na.rm=T)
  }else{
    Com_NOF$NO3NAnalysisNote = paste0('n = ',sum(!is.na(NtoxSite$Value)),' Insufficient to calculate annual medians ')
  }}
  rm(NtoxSite)
  
  #TON - based
  NtoxSite <- rightSite%>%dplyr::filter(Measurement%in%c("TON"))
  if(dim(NtoxSite)[1]>0){
    #Median TON
    annualMedian <- NtoxSite%>%dplyr::group_by(Year)%>%dplyr::summarise(Value=quantile(Value,prob=0.5,type=5))
    if(dim(annualMedian)[1]!=0){
      Com_NOF$TONMed <- annualMedian$Value[match(Com_NOF$Year,annualMedian$Year)]
      #Rolling 5yr median
      rollingMeds=rolling5(siteChemSet = NtoxSite,quantProb = 0.5,nreq=54,quReq=18)
      rollFails= grepl(pattern = '^n',x = rollingMeds,ignore.case=T)
      rollSucc = grepl(pattern='^[[:digit:]]',rollingMeds)
      Com_NOF$TONMed[yr%in%names(rollingMeds)[rollSucc]] <- readr::parse_number(rollingMeds[rollSucc])
      Com_NOF$TONAnalysisNote[rollyrs[rollFails]] <- paste0(' Need 54 values for 5yr median, have ',rollingMeds[rollFails])
      #find the band which each value belong to
      Com_NOF$TONMed_Band <- sapply(Com_NOF$TONMed,NOF_FindBand,bandColumn = NOFbandDefinitions$`Median Nitrate`)
      Com_NOF$TONMed_Band[!is.na(Com_NOF$TONMed)] <- 
        sapply(Com_NOF$TONMed_Band[!is.na(Com_NOF$TONMed)],FUN=function(x){min(unlist(strsplit(x,split = '')))})
      rm(annualMedian,rollingMeds,rollFails,rollSucc)
      
      #95th percentile TON
      annual95 <- NtoxSite%>%dplyr::group_by(Year)%>%dplyr::summarise(Value=quantile(Value,prob=0.95,type=5,na.rm=T))
      Com_NOF$TON95 = annual95$Value[match(Com_NOF$Year,annual95$Year)]
      #Rolling 5yr 95%ile
      rolling95=rolling5(NtoxSite,0.95,nreq=54,quReq=18)
      rollFails= grepl(pattern = '^n',x = rolling95,ignore.case=T)
      rollSucc = grepl(pattern='^[[:digit:]]',rolling95)
      Com_NOF$TON95[yr%in%names(rolling95)[rollSucc]] <- readr::parse_number(rolling95[rollSucc])
      Com_NOF$TONAnalysisNote[rollyrs[rollFails]] <- paste0(Com_NOF$TONAnalysisNote[rollyrs[rollFails]],
                                                                ' Need 54 values and 20 quarters for 5yr 95%ile, have ',rolling95[rollFails])
      #find the band which each value belong to
      Com_NOF$TON95_Band <- sapply(Com_NOF$TON95,NOF_FindBand,bandColumn = NOFbandDefinitions$`95th Percentile Nitrate`)
      Com_NOF$TON95_Band[!is.na(Com_NOF$TON95)] <- 
        sapply(Com_NOF$TON95_Band[!is.na(Com_NOF$TON95)],FUN=function(x){min(unlist(strsplit(x,split = '')))})
      rm(annual95,rolling95,rollFails,rollSucc)
      
      #TON Toxicity
      #The worse of the two TON bands
      Com_NOF$TON_Toxicity_Band = apply(Com_NOF%>%dplyr::select(TONMed_Band, TON95_Band),1,max,na.rm=T)
    }else{
      Com_NOF$TONAnalysisNote = paste0('n = ',sum(!is.na(NtoxSite$Value)),' Insufficient to calculate annual medians ')
    }}
  rm(NtoxSite)
  
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
    rm(annualmax,rollingMax,rollFails,rollSucc)
  }
  rm(nh4site)
  
  ######################  E.Coli #########################################
  suppressWarnings(rm(cnEc_Band,cnEc95_Band,cnEcRecHealth540_Band,cnEcRecHealth260_Band,ecosite,ecosite))
  ecosite=rightSite%>%dplyr::filter(Measurement=="ECOLI")
  
  #E coli median
  annualMedian <- ecosite%>%dplyr::group_by(Year)%>%dplyr::summarise(Value=quantile(Value,prob=0.5,type=5,na.rm=T))
  if(dim(annualMedian)[1]!=0){
    Com_NOF$EcoliMed <- annualMedian$Value[match(Com_NOF$Year,annualMedian$Year)]
    Com_NOF$EcoliAnalysisNote[is.na(Com_NOF$EcoliMed)&!is.na(as.numeric(Com_NOF$Year))] <- "No data this year"
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
    if(length(ecv)>10){ #New 2021 abundance requirement
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
  
  ################## BDISC CLARITY SUSPENDED SEDIMENT####
  suspsedsite <- rightSite%>%dplyr::filter(Measurement=="BDISC")
  annualMedian <- suspsedsite%>%dplyr::group_by(Year)%>%dplyr::summarise(Value=quantile(Value,prob=0.5,type=5))
  if(dim(annualMedian)[1]!=0){
    sedimentClass = unique(riverSiteTable$SedimentClass[which(tolower(riverSiteTable$LawaSiteID) == tolower(uLAWAids[i]) )[1]])
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
#37 8/9/21  30870



#Juggling NO3N vs total oxidised nitrogen for the nitrate toxicyt band score result evaluation
NOFSummaryTable$NO3N_Toxicity_Band[NOFSummaryTable$NO3N_Toxicity_Band==''] <- NA
NOFSummaryTable$TON_Toxicity_Band[NOFSummaryTable$TON_Toxicity_Band==''] <- NA


NOFSummaryTable$Nitrate_Toxicity_Band = ifelse(!is.na(NOFSummaryTable$NO3N_Toxicity_Band),
                                               NOFSummaryTable$NO3N_Toxicity_Band,
                                               NOFSummaryTable$TON_Toxicity_Band)

# Audit where this comes from which
table(NOFSummaryTable$Agency,is.na(NOFSummaryTable$NO3N_Toxicity_Band)&!is.na(NOFSummaryTable$TON_Toxicity_Band))
table(NOFSummaryTable$Agency,is.na(NOFSummaryTable$TON_Toxicity_Band)&!is.na(NOFSummaryTable$NO3N_Toxicity_Band))

with(NOFSummaryTable%>%filter(Year=='2016to2020'),table(Agency,!is.na(NO3N_Toxicity_Band)))
with(NOFSummaryTable%>%filter(Year=='2016to2020'),table(Agency,!is.na(TON_Toxicity_Band)&is.na(NO3N_Toxicity_Band)))
with(NOFSummaryTable%>%filter(Year=='2016to2020'),table(Agency,is.na(NO3N_Toxicity_Band)&is.na(TON_Toxicity_Band)))

#Finalise E Coli banding
NOFSummaryTable$EcoliMed_Band <- as.character(factor(NOFSummaryTable$EcoliMed_Band,
                                                     levels=c("","ABC","D","DE"),
                                                     labels=c("","A","D","E")))

NOFSummaryTable$EcoliRecHealth260_Band <- as.character(factor(NOFSummaryTable$EcoliRecHealth260_Band,
                                                              levels=c("","A","BC","C","D","DE"),
                                                              labels=c("","A","B","C","D","E")))


# NOFSummaryTable$EcoliMed_Band <- sapply(NOFSummaryTable$EcoliMed,
#                                         NOF_FindBand,
#                                         bandColumn=NOFbandDefinitions$`E. coli`)

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
  with(NOFSummaryTable,plot(as.factor(NO3NMed_Band),NO3NMed))
  with(NOFSummaryTable,plot(as.factor(NO3N95_Band),NO3N95))
  with(NOFSummaryTable,plot(as.factor(NO3N_Toxicity_Band),NO3N95))
  with(NOFSummaryTable[which(NOFSummaryTable$AmmoniacalMed>0),],
       plot(as.factor(AmmoniacalMed_Band),AmmoniacalMed,log='y'))
  with(NOFSummaryTable,plot(as.factor(EcoliSummaryband[EcoliMed>0]),EcoliMed[EcoliMed>0],log='y'))
  with(NOFSummaryTable,plot(as.factor(SusSedBand[SusSedMed>0]),SusSedMed[SusSedMed>0],log='y'))
  table(NOFSummaryTable$NO3NMed_Band,NOFSummaryTable$NO3N_Toxicity_Band)
  table(NOFSummaryTable$NO3N95_Band,NOFSummaryTable$NO3N_Toxicity_Band)
  table(NOFSummaryTable$AmmoniacalMed_Band,NOFSummaryTable$Ammonia_Toxicity_Band)
  table(NOFSummaryTable$AmmoniacalMax_Band,NOFSummaryTable$NO3N_Toxicity_Band)
  table(NOFSummaryTable$Ecoli95_Band,NOFSummaryTable$EcoliSummaryband,useNA='a')
  table(NOFSummaryTable$EcoliRecHealth260_Band,NOFSummaryTable$EcoliMed_Band,useNA='a')
}


#If LawaSiteIDs are duplicated in the siteTable, that will cause duplication of NOFresults here
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
                -Altitude,-Landcover,-NZReach,-NZSegment,-SedimentClass,-Agency,-Region,-Catchment,-ends_with('Note'),
                -starts_with("TON"),-starts_with("NO3"),-EcoliMed_Band,-Ecoli95_Band,-EcoliRecHealth260_Band,-EcoliRecHealth540_Band)%>%
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


# RiverNOF = read_csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis/",pattern="ITERiverNOF",full.names = T,recursive = T),1),guess_max=5000)


EcoliAudit = NOFSummaryTable%>%
   filter(is.na(as.numeric(Year)))%>%
  select(-starts_with(c("DRP","ammon","sussed","Nitr","TON"),ignore.case=T))%>%
  filter(is.na(EcoliSummaryband),EcoliAnalysisNote!="")
EcoliAudit$nmonth = as.numeric(sapply(EcoliAudit$EcoliAnalysisNote,
                                      FUN = function(s)  strTo(strFrom(s,c='haven='),c=', q=')))
EcoliAudit$nquart = as.numeric(sapply(EcoliAudit$EcoliAnalysisNote,
                                      FUN = function(s)  strTo(strFrom(s,c=', q='),c=' Need')))
EcoliAudit <- EcoliAudit%>%select(-starts_with('ecoli',ignore.case=T))

head(EcoliAudit)

by(data = (EcoliAudit$nmonth),
   INDICES = EcoliAudit$Agency,FUN = summary)
by(data = (EcoliAudit$nquart),
   INDICES = EcoliAudit$Agency,FUN = summary)


plot(EcoliAudit$nmonth,
     jitter(factor = 1.5,EcoliAudit$nquart),xlab='nMonth',ylab='nQuart')
 abline(v=54)
 abline(h=18)
 
 EcoliAudit%>%filter(nquart>=18&nmonth<54)%>%select(Agency,Year)%>%table
 EcoliAudit%>%filter(nquart>=18&nmonth<54)%>%
   select(Agency,LawaSiteID)%>%distinct%>%group_by(Agency)%>%summarise(nSite=length(unique(LawaSiteID)))
 
 
 
 EcoliAudit%>%filter(nquart>=18&nmonth<54,Agency=='ac')
 
 
 
 
 
 
 
 #Audit NIWA duplication
 
 RiverNOF%>%filter(LAWAID%in%c('nrwqn-00001', 'nrwqn-00012', 'nrwqn-00017', 'nrwqn-00021', 'nrwqn-00022', 'nrwqn-00028', 'nrwqn-00030', 'nrwqn-00047', 'nrwqn-00048'))%>%
   filter(grepl(pattern = 'band',x=Parameter,ignore.case=T))%>%drop_na(Band)%>%pivot_wider(names_from = 'Parameter',values_from = 'Band')%>%arrange(LAWAID)%>%as.data.frame
 
 