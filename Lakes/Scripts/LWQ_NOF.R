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
library(tidyr)
library(parallel)
library(doParallel)
require(reshape2)
library(tidyverse)
setwd("h:/ericg/16666LAWA/LAWA2021/Lakes/")
source("h:/ericg/16666LAWA/LAWA2021/scripts/LAWAFunctions.R")
source("h:/ericg/16666Lawa/LAWA2021/WaterQuality/scripts/SWQ_NOF_Functions.R")

dir.create(paste0("h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d')),showWarnings = F)

#hydrological year runs e.g. 1 July 2017 to 30 June 2018, and is named like 2017/18

lakeSiteTable <- loadLatestSiteTableLakes(maxHistory = 90)



lakeSiteTable$LType=tolower(lakeSiteTable$LType)
table(lakeSiteTable$LType)

## Load NOF Bands
NOFbandDefinitions <- read.csv("H:/ericg/16666LAWA/LAWA2021/Lakes/Metadata/NOFlakeBandDefinitions.csv", header = TRUE, stringsAsFactors=FALSE)
#   Band TotalNitrogenseasonally.stratified.and.brackish TotalNitrogenpolymictic TotalPhosphorus Median.Ammoniacal.N Max.Ammoniacal.N E..coli Ecoli95 EcoliRec540 EcoliRec260  Clarity ChlAMedian    ChlAMax
# 1    A                                          x<=160                  x<=300           x<=10             x<=0.03          x<=0.05  x<=130  x<=540         x<5        x<20      x>=7       x<=2      x<=10
# 2    B                                    x>160&x<=350            x>300&x<=500      x>10&x<=20      x>0.03&x<=0.24    x>0.05&x<=0.4  x<=130 x<=1000  x>=5&x<=10 x>=20&x<=30 x>=3&x<7   x>2&x<=5 x>10&x<=25
# 3    C                                    x>350&x<=750            x>500&x<=800      x>20&x<=50       x>0.24&x<=1.3     x>0.4&x<=2.2  x<=130 x<=1200 x>=10&x<=20 x>=20&x<=34 x>=1&x<3  x>5&x<=12 x>25&x<=60
# 4    D                                           x>750                   x>800            x>50              x>1.30            x>2.2   x>130  x>1200 x>=20&x<=30        x>34     x<1       x>12       x>60
# 5    E                                           x>Inf                   x>Inf           x>Inf               x>Inf            x>Inf   x>260  x>1200        x>30        x>50   x<(-1)      x>Inf      x>Inf

#Clarity inequalities shuffled 24/5/2021 by guidance suggestion from Bill Dyck via email from Jane Groom


#===================================================================================================
## Load LAWA Data
# loads lawaLakesdata dataframe  from LAWA_State.r  - has altered values from censoring, and calculated medians
lakesMonthlyMedians=read_csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis",
                                      pattern="lakeMonthlyMedian.*csv",  #"ForITE"
                                      recursive = T,full.names = T,ignore.case = T),1))
stopifnot(min(lakesMonthlyMedians$Year,na.rm=T)>1990)
#Reference Dates
EndYear <- lubridate::year(Sys.Date())-1
StartYear5 <- EndYear - 5 + 1
StartYear15 <- EndYear - 15 + 1
firstYear = min(lakesMonthlyMedians$Year,na.rm=T)
yr <- c(as.character(StartYear15:EndYear),
        paste0(as.character(StartYear15:(EndYear-4)),'to',as.character((StartYear15+4):EndYear)))
rollyrs=which(grepl('to',yr))
reps <- length(yr)

lakesMonthlyMedians$Date=lubridate::dmy(paste0('1-',lakesMonthlyMedians$month,'-',lakesMonthlyMedians$Year))
lakesMonthlyMedians$Measurement=toupper(lakesMonthlyMedians$Measurement)



# Subset to just have the variables that are tested against NOF standards ####
sub_lwq <- lakesMonthlyMedians%>%dplyr::filter(Year>=StartYear15)%>%
  dplyr::select(c("LawaSiteID","CouncilSiteID","Measurement","Value","Date","Year"))%>%
  dplyr::filter(tolower(Measurement)%in%tolower(c("NH4N","TN","TP","ECOLI","PH","SECCHI","CHLA","CYANOTOT","CYANOTOX")))
#63082 from 66483
sub_lwq$Measurement[sub_lwq$Measurement=="NH4N"] <- "NH4"
sub_lwq$YearQuarter=paste0(quarters(sub_lwq$Date),year(sub_lwq$Date))
lakeSiteTable$uclid=paste(tolower(trimws(lakeSiteTable$LawaSiteID)),
                          tolower(trimws(lakeSiteTable$CouncilSiteID)),sep='||')
sub_lwq$uclid      =paste(tolower(trimws(sub_lwq$LawaSiteID)),
                          tolower(trimws(sub_lwq$CouncilSiteID)),sep='||')

uclids = unique(sub_lwq$uclid)
cat(length(uclids),'\t')

if(exists("NOFSummaryTable")) { rm("NOFSummaryTable") }

workers=makeCluster(4)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(tidyverse)
})
startTime=Sys.time()
foreach(i = 1:length(uclids),.combine=rbind,.errorhandling='stop')%dopar%{
    suppressWarnings(rm(tnsite,tpsite,nh4site,ecosite,rightSite,value,Value)  )
  rightSite=sub_lwq[(sub_lwq$uclid==uclids[i]),]
  rightSite=rightSite[!is.na(rightSite$Value),]
  # create table of compliance with proposed National Objectives Framework ####
  Com_NOF <- data.frame (LawaSiteID               = rep(unique(rightSite$LawaSiteID),reps),
                         CouncilSiteID            = rep(unique(rightSite$CouncilSiteID),reps),
                         Year                     = yr,
                         NitrogenMed            = rep(as.numeric(NA),reps),
                         NitrogenMed_Band       = rep(as.character(NA),reps),
                         NitrateAnalysisNote      = rep('',reps),
                         PhosphorusMed          = rep(as.numeric(NA),reps),
                         PhosphorusMed_Band     = rep(as.character(NA),reps),
                         PhosphorusAnalysisNote   = rep('',reps),
                         AmmoniacalMed            = rep(as.numeric(NA),reps),
                         AmmoniacalMed_Band       = rep(as.character(NA),reps),
                         AmmoniacalMax            = rep(as.numeric(NA),reps),
                         AmmoniacalMax_Band       = rep(as.character(NA),reps),
                         Ammonia_Toxicity_Band    = rep(as.character(NA),reps),
                         AmmoniaAnalysisNote      = rep('',reps),
                         ClarityMedian            = rep(as.numeric(NA),reps),
                         ClarityMedian_Band       = rep(as.character(NA),reps),
                         ClarityAnalysisNote      = rep('',reps),
                         ChlAMed                  = rep(as.numeric(NA),reps),
                         ChlAMed_Band             = rep(as.character(NA),reps),
                         ChlAMax                  = rep(as.numeric(NA),reps),
                         ChlAMax_Band             = rep(as.character(NA),reps),
                         ChlASummaryBand          = rep(as.character(NA),reps),
                         ChlAAnalysisNote         = rep('',reps),
                         EcoliMedian              = rep(as.numeric(NA),reps),
                         EcoliBand                = rep(NA,reps),
                         Ecoli95                  = rep(as.numeric(NA),reps),
                         Ecoli95_Band             = rep(NA,reps),
                         EcoliRecHealth540        = rep(as.numeric(NA),reps),
                         EcoliRecHealth540_Band   = rep(NA,reps),
                         EcoliRecHealth260        = rep(as.numeric(NA),reps),
                         EcoliRecHealth260_Band   = rep(NA,reps),
                         EcoliSummaryBand         = rep(as.character(NA),reps),
                         EcoliAnalysisNote        = rep('',reps),
                         CyanoTOX80th              = rep(as.numeric(NA),reps),
                         CyanoTOT80th              = rep(as.numeric(NA),reps),
                         CyanoBand                = rep(NA,reps),
                         stringsAsFactors = FALSE)

  
  ######################  Nitrogen  #################################### ####
  tnsite=rightSite[rightSite$Measurement=="TN",]
  annualMedian <- tnsite%>%dplyr::group_by(Year)%>%dplyr::summarise(value=quantile(Value,prob=0.5,type=5))
  
  if(dim(annualMedian)[1]!=0){
    Com_NOF$NitrogenMed <- annualMedian$value[match(Com_NOF$Year,annualMedian$Year)]
    #Rolling 5yr median
    rollingMeds=rolling5(siteChemSet=tnsite,quantProb=0.5)
    rollFails=grepl(pattern = '^n',x = rollingMeds,ignore.case=T)
    rollSucc = grepl(pattern='^[[:digit:]]',rollingMeds)
    Com_NOF$NitrogenMed[yr%in%names(rollingMeds)[rollSucc]] <- readr::parse_number(rollingMeds[rollSucc])
    Com_NOF$NitrateAnalysisNote[rollyrs[rollFails]] <- paste0("Need 30 values for median, have ",
                                                              strFrom(s=rollingMeds[rollFails],c='y'))
    #find the Band which each value belong to
    if(lakeSiteTable$LType[which(lakeSiteTable$uclid==uclids[i])]%in%c("stratified","brackish","icoll","monomictic")){
      Com_NOF$NitrogenMed_Band <- sapply(Com_NOF$NitrogenMed,NOF_FindBand,bandColumn = NOFbandDefinitions$TotalNitrogenseasonally.stratified.and.brackish)
      Com_NOF$NitrogenMed_Band <- sapply(Com_NOF$NitrogenMed_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))})
    }else{ #assuming polymictic
      Com_NOF$NitrogenMed_Band <- sapply(Com_NOF$NitrogenMed,NOF_FindBand,bandColumn = NOFbandDefinitions$TotalNitrogenpolymictic)
      Com_NOF$NitrogenMed_Band <- sapply(Com_NOF$NitrogenMed_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))})
    }
    rm(rollingMeds,rollFails,rollSucc)
  }
  rm(tnsite,annualMedian)
  
  ######################  Phosphorus  #################################### ####
  tpsite=rightSite[rightSite$Measurement=="TP",]
  annualMedian <- tpsite%>%dplyr::group_by(Year)%>%dplyr::summarise(value=quantile(Value,prob=0.5,type=5,na.rm=T))
  
  if(dim(annualMedian)[1]!=0){
    Com_NOF$PhosphorusMed <- annualMedian$value[match(Com_NOF$Year,annualMedian$Year)]
    #Rolling 5yr median
    rollingMeds=rolling5(siteChemSet=tpsite,quantProb=0.5)
    rollFails=grepl(pattern = '^n',x = rollingMeds,ignore.case=T)
    rollSucc = grepl(pattern='^[[:digit:]]',rollingMeds)
    Com_NOF$PhosphorusMed[yr%in%names(rollingMeds)[rollSucc]] <- readr::parse_number(rollingMeds[rollSucc])
    Com_NOF$PhosphorusAnalysisNote[rollyrs[rollFails]] <- paste0("Need 30 values for median, have",
                                                                 strFrom(s=rollingMeds[rollFails],c='y'))
    #find the band which each value belong to
    Com_NOF$PhosphorusMed_Band <- sapply(Com_NOF$PhosphorusMed,NOF_FindBand,bandColumn = NOFbandDefinitions$TotalPhosphorus)
    Com_NOF$PhosphorusMed_Band <- sapply(Com_NOF$PhosphorusMed_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))})
    rm(rollingMeds,rollFails,rollSucc)
  }
  rm(tpsite,annualMedian)
  
  ###################### Ammonia  ######################## ####
  nh4site=rightSite[rightSite$Measurement=="NH4",]
  if(dim(nh4site)[1]==0){
    Com_NOF$AmmoniaAnalysisNote='No NH4 data '
  }else{
    if(all(nh4site$Value==-99)){
      Com_NOF$AmmoniaAnalysisNote='No pH data available for NH4 adjustment, so NH4 cannot be judged against NOF standards.'
    }else{
      #Median Ammoniacal Nitrogen
      annualMedian <- nh4site%>%dplyr::group_by(Year)%>%dplyr::summarise(value=quantile(Value,prob=0.5,type=5))
      if(dim(annualMedian)[1]!=0){
        Com_NOF$AmmoniacalMed = annualMedian$value[match(Com_NOF$Year,annualMedian$Year)]
        #Rolling 5yr median
        rollingMeds=rolling5(siteChemSet=nh4site,quantProb=0.5)
        rollFails=grepl(pattern = '^n',x = rollingMeds,ignore.case=T)
        rollSucc = grepl(pattern='^[[:digit:]]',rollingMeds)
        Com_NOF$AmmoniacalMed[yr%in%names(rollingMeds)[rollSucc]] <- readr::parse_number(rollingMeds[rollSucc])
        Com_NOF$AmmoniaAnalysisNote[rollyrs[rollFails]] <- paste0("Need 30 values for median, have",
                                                                     strFrom(s=rollingMeds[rollFails],c='y'))
        Com_NOF$AmmoniacalMed_Band <- sapply(Com_NOF$AmmoniacalMed,NOF_FindBand,bandColumn=NOFbandDefinitions$Median.Ammoniacal.N)
        Com_NOF$AmmoniacalMed_Band <- sapply(Com_NOF$AmmoniacalMed_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))})
        rm(rollingMeds,rollFails)
        
        #-------maximum annual Ammoniacal Nitrogen--------------------------------------
        annualMax <- nh4site%>%dplyr::group_by(Year)%>%dplyr::summarise(value=max(Value,na.rm=T))
        Com_NOF$AmmoniacalMax <- annualMax$value[match(Com_NOF$Year,annualMax$Year)]
        
        #rolling 5yr max
        rollingMax=rolling5(nh4site,quantProb=1.0)
        rollFails=is.na(as.numeric(rollingMax))
        rollFails=grepl(pattern = '^n',x = rollingMax,ignore.case=T)
        rollSucc = grepl(pattern='^[[:digit:]]',rollingMax)
        Com_NOF$AmmoniacalMax[yr%in%names(rollingMax)[rollSucc]]=readr::parse_number(rollingMax[rollSucc])
        Com_NOF$AmmoniaAnalysisNote[rollyrs[rollFails]] <- paste0('Need 30 values for median, have ',
                                                                     strFrom(s=rollingMax[rollFails],c='y'))
        Com_NOF$AmmoniacalMax_Band <-sapply(Com_NOF$AmmoniacalMax,NOF_FindBand,bandColumn=NOFbandDefinitions$Max.Ammoniacal.N) 
        Com_NOF$AmmoniacalMax_Band <- sapply(Com_NOF$AmmoniacalMax_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))})
        rm(rollingMax,rollFails,rollSucc,annualMax)
        #------------------Finding the band for Ammonia Toxicity-------------------------------
        Com_NOF$Ammonia_Toxicity_Band=apply(select(Com_NOF,AmmoniacalMed_Band, AmmoniacalMax_Band),1,max,na.rm=T)
      }else{
        Com_NOF$AmmoniaAnalysisNote=paste0(Com_NOF$AmmoniaAnalysisNote,'n = ',sum(!is.na(nh4site$Value)),
                                           ' Insufficient to calculate annual medians. ')
      }
      rm(annualMedian)
    }
  }
  rm(nh4site)
  
  ######################  E.Coli ##################################### ####
  suppressWarnings(rm(cnEc_Band,cnEc95_Band,cnEcRecHealth540_Band,cnEcRecHealth260_Band,ecosite,rawEcoli))
  ecosite=rightSite[rightSite$Measurement=="ECOLI",]
  # ecosite$year=lubridate::year(ecosite$Date)
  # rawEcoli=ecosite%>%dplyr::select(year,Value)%>%filter(!is.na(Value)&year>=StartYear5)
  annualMedian <- ecosite%>%dplyr::group_by(Year)%>%dplyr::summarise(value=quantile(Value,prob=0.5,type=5,na.rm=T))  
  
  if(dim(annualMedian)[1]!=0){
    Com_NOF$EcoliMedian <- annualMedian$value[match(Com_NOF$Year,annualMedian$Year)]
    # Com_NOF$EcoliPeriod=ifelse(is.na(Com_NOF$EcoliMedian),NA,1)
    #rolling 5yr median
    rollingMeds=rolling5(ecosite,0.5,nreq=54)
    rollFails=grepl(pattern = '^n',x = rollingMeds,ignore.case=T)
    rollSucc = grepl(pattern='^[[:digit:]]',rollingMeds)
    Com_NOF$EcoliMedian[yr%in%names(rollingMeds)[rollSucc]] = readr::parse_number(rollingMeds[rollSucc]) #not "as.numeric", because good results include a years suffix
    # Com_NOF$EcoliPeriod[yr%in%names(rollingMeds)] = ifelse(rollFails,NA,ifelse(grepl(pattern = '_6',rollingMeds),6,5))
    #bands
    Com_NOF$EcoliBand <- sapply(Com_NOF$EcoliMedian,NOF_FindBand,bandColumn=NOFbandDefinitions$E..coli)
  }else{
    Com_NOF$EcoliAnalysisNote=paste0(Com_NOF$EcoliAnalysisNote,"n = ",sum(!is.na(ecosite$Value)),
                                     " Insufficient data to calculate annual medians")
  }
  rm(annualMedian,rollingMeds,rollFails,rollSucc)
  
  #Ecoli 95th percentile 
  annual95 <- ecosite%>%dplyr::group_by(Year)%>%dplyr::summarise(value=quantile(Value,prob=0.95,type=5,na.rm=T))
  if(length(annual95)!=0){
    Com_NOF$Ecoli95 <- annual95$value[match(Com_NOF$Year,annual95$Year)]
    #rolling 5yr or 6yr 95%ile
    rolling95 = rolling5(ecosite,0.95,nreq=54)
    rollFails=grepl(pattern = '^n',x = rolling95,ignore.case=T)
    rollSucc = grepl(pattern='^[[:digit:]]',rolling95)
    Com_NOF$Ecoli95[yr%in%names(rolling95)[rollSucc]] <- readr::parse_number(rolling95[rollSucc])#not "as.numeric", because good results include a years suffix
    
    #bands 
    Com_NOF$Ecoli95_Band <- sapply(Com_NOF$Ecoli95,NOF_FindBand,bandColumn=NOFbandDefinitions$Ecoli95)
  }else{
    Com_NOF$EcoliAnalysisNote=paste0(Com_NOF$EcoliAnalysisNote,"n = ",sum(!is.na(ecosite$Value)),
                                     " Insufficient data to calculate annual max")
  }
  rm(annual95,rolling95,rollFails,rollSucc)
  
  # Exceedance percentages
  options(warn=-1)
  for(yy in 1:length(Com_NOF$Year)){
    if(!is.na(as.numeric(Com_NOF$Year[yy]))){
      ecv=ecosite$Value[which(ecosite$Year==Com_NOF$Year[yy])]
    }else{
      startYear = strTo(s = Com_NOF$Year[yy],c = 'to')
      stopYear = strFrom(s= Com_NOF$Year[yy],c = 'to')
      ecv=ecosite$Value[ecosite$Year>=startYear & ecosite$Year<=stopYear]
    }
    if(length(ecv)>0){
      Com_NOF$EcoliRecHealth540[yy]=sum(ecv>540)/length(ecv)*100
      Com_NOF$EcoliRecHealth260[yy]=sum(ecv>260)/length(ecv)*100
    }
  }
  options(warn=0)
  #Bands
  suppressWarnings(Com_NOF$EcoliRecHealth540_Band <- sapply(Com_NOF$EcoliRecHealth540,NOF_FindBand,bandColumn=NOFbandDefinitions$EcoliRec540))
  suppressWarnings(Com_NOF$EcoliRecHealth260_Band <- sapply(Com_NOF$EcoliRecHealth260,NOF_FindBand,bandColumn=NOFbandDefinitions$EcoliRec260))
  
  these=which(is.na(Com_NOF$EcoliMedian))
  if(length(these)>0){
    Com_NOF$EcoliAnalysisNote[these]=paste0(Com_NOF$EcoliAnalysisNote,
                                            ' Need ',rep(c(12,54),c(sum(!grepl('to',yr)),sum(grepl('to',yr)))),
                                            ' values for ',rep(c('annual','5yr'),c(sum(!grepl('to',yr)),sum(grepl('to',yr)))),
                                            ' have ',count5(ecosite,T))[these]
  }
  
  rm(ecosite)
  #################### Clarity ############ ####
  clarsite=rightSite[rightSite$Measurement=="SECCHI",]
  annualMedian <- clarsite%>%dplyr::group_by(Year)%>%dplyr::summarise(value=quantile(Value,prob=0.5,type=5,na.rm=T))

  if(length(annualMedian)!=0){
    Com_NOF$ClarityMedian = annualMedian$value[match(Com_NOF$Year,annualMedian$Year)]
    #Rolling 5yr median
    rollingMeds = rolling5(siteChemSet=clarsite,quantProb=0.5,nreq=30)
    rollFails=grepl(pattern = '^n',x = rollingMeds,ignore.case=T)
    rollSucc = grepl(pattern='^[[:digit:]]',rollingMeds)
    Com_NOF$ClarityMedian[yr%in%names(rollingMeds)[rollSucc]] <- readr::parse_number(rollingMeds[rollSucc])
    Com_NOF$ClarityAnalysisNote[rollyrs[rollFails]] <- paste0("Need 30 values for median, have",strFrom(s=rollingMeds[rollFails],c='y'))

    #find the band which each value belong to
    Com_NOF$ClarityMedian_Band <- sapply(Com_NOF$ClarityMedian,NOF_FindBand,bandColumn=NOFbandDefinitions$Clarity)
    Com_NOF$ClarityMedian_Band <- sapply(Com_NOF$ClarityMedian_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))})
    rm(rollingMeds,rollFails,rollSucc)
  }  
  rm(clarsite,annualMedian)
  
  
  #################### ChlA ############ ####
  chlSite=rightSite[rightSite$Measurement=="CHLA",]
  annualMedian <- chlSite%>%dplyr::group_by(Year)%>%dplyr::summarise(value=quantile(Value,prob=0.5,type=5,na.rm=T))

  if(length(annualMedian)!=0){
    Com_NOF$ChlAMed = annualMedian$value[match(Com_NOF$Year,annualMedian$Year)]
    #Rolling 5 yr median
    rollingMeds = rolling5(siteChemSet=chlSite,quantProb=0.5,nreq=30)
    rollFails=grepl(pattern = '^n',x = rollingMeds,ignore.case=T)
    rollSucc = grepl(pattern='^[[:digit:]]',rollingMeds)
    Com_NOF$ChlAMed[yr%in%names(rollingMeds)[rollSucc]] <- readr::parse_number(rollingMeds[rollSucc])
    Com_NOF$ChlAAnalysisNote[rollyrs[rollFails]] <- paste0("Need 30 values for median, have ",strFrom(s=rollingMeds[rollFails],c='y'))

    #find the band which each value belong to
    Com_NOF$ChlAMed_Band <- sapply(Com_NOF$ChlAMed,NOF_FindBand,bandColumn=NOFbandDefinitions$ChlAMedian)
    Com_NOF$ChlAMed_Band <- sapply(Com_NOF$ChlAMed_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))})
    rm(rollingMeds,rollFails,rollSucc)
  }
  rm(annualMedian)
  #ChlA max
  annualMax <- chlSite%>%dplyr::group_by(Year)%>%dplyr::summarise(value=max(Value,na.rm=T))
  
  if(length(annualMax)!=0){
    Com_NOF$ChlAMax = annualMax$value[match(Com_NOF$Year,annualMax$Year)]
    #Rolling max
    rollingMax=rolling5(chlSite,quantProb=0.95,nreq=30)
    rollFails=is.na(as.numeric(rollingMax))
    Com_NOF$ChlAMax[yr%in%names(rollingMax)]=as.numeric(rollingMax)
    Com_NOF$ChlAAnalysisNote[rollyrs[rollFails]] <- paste0("Need 30 values for max, have ",strFrom(s=rollingMax[rollFails],c='y'))
    #find the band which each value belong to
    Com_NOF$ChlAMax_Band <- sapply(Com_NOF$ChlAMax,NOF_FindBand,bandColumn=NOFbandDefinitions$ChlAMax)
    Com_NOF$ChlAMax_Band <- sapply(Com_NOF$ChlAMax_Band,FUN=function(x){min(unlist(strsplit(x,split = '')))})
    rm(rollingMax,rollFails)
  }  
    #------------------Finding the band for ChlA Summary-------------------------------
    Com_NOF$ChlASummaryBand=apply(select(Com_NOF,ChlAMed_Band, ChlAMax_Band),1,max,na.rm=T)
  rm(chlSite,annualMax)
  
  ############################ CYANO BACTERIA ########################
  #Table 10 NPSFM p 49
  cyanoSite = rightSite%>%filter(Measurement%in%c("CYANOTOX","CYANOTOT"))
  if(dim(cyanoSite)[1]>0){
    cyanoSite$Value[which(cyanoSite$Value==0)] <- NA          #DOUBLE CHECK
    cyanoSite <- cyanoSite%>%group_by(Date,Measurement)%>%
      summarise(.groups='keep',
                Value=quantile(Value,prob=0.5,type=5,na.rm=T),
                Year=first(Year),
                YearQuarter=first(YearQuarter))%>%
      ungroup%>%
      pivot_wider(names_from=Measurement,values_from=Value)
    annual80th = cyanoSite%>%
      dplyr::group_by(Year)%>%
      dplyr::summarise(across(where(is.numeric),quantile,prob=0.8,type=5,na.rm=T))%>%ungroup
    #   TOX80 = quantile(CYANOTOX,prob=0.8,type=5,na.rm=T),
    #                  TOT80 = quantile(CYANOTOT,prob=0.8,type=5,na.rm=T))%>%
    # ungroup
    if('CYANOTOX'%in%names(annual80th)){
      Com_NOF$CyanoTOX80th = annual80th$CYANOTOX[match(Com_NOF$Year,annual80th$Year)]
    }
    if('CYANOTOX'%in%names(annual80th)){
      Com_NOF$CyanoTOT80th = annual80th$CYANOTOT[match(Com_NOF$Year,annual80th$Year)]
    }
    #And rolling 3 (NPSFM spec)
    rolling80s = rolling3(cyanoSite = cyanoSite,quantProb = 0.8,nreq = 12)
    suppressWarnings({rollfails = which(apply(rolling80s%>%select(-Year),1,FUN=function(c)all(is.na(as.numeric(c)))))})
    Com_NOF$CyanoTOX80th = as.numeric(rolling80s$TOX[match(Com_NOF$Year,rolling80s$Year)])
    Com_NOF$CyanoTOT80th = as.numeric(rolling80s$TOT[match(Com_NOF$Year,rolling80s$Year)])
    # if(sum(!is.na(cyanoSite$CYANOTOT))>12|sum(!is.na(cyanoSite$CYANOTOX))>12)browser()
    Com_NOF$CyanoBand[which(Com_NOF$CyanoTOX80th>1.8|Com_NOF$CyanoTOT80th>10)] <- "D"
    Com_NOF$CyanoBand[which((Com_NOF$CyanoTOX80th>1&Com_NOF$CyanoTOX80th<=1.8)|
                              (Com_NOF$CyanoTOT80th>1&Com_NOF$CyanoTOX80th<=10))] <- "C"
    Com_NOF$CyanoBand[which((Com_NOF$CyanoTOX80th<=1|is.na(Com_NOF$CyanoTOX80th)) & Com_NOF$CyanoTOT80th<=1)] <- "B"
    Com_NOF$CyanoBand[which((Com_NOF$CyanoTOX80th<=1|is.na(Com_NOF$CyanoTOX80th)) & Com_NOF$CyanoTOT80th<=0.5)] <- "A"
  }
  return(Com_NOF)
}->NOFSummaryTable
stopCluster(workers)
rm(workers)


Sys.time()-startTime
rm(startTime)
#July6  3.8 secs
#Sept 16 6.2 secs

NOFSummaryTable$EcoliBand <- sapply(NOFSummaryTable$EcoliMed,NOF_FindBand,bandColumn=NOFbandDefinitions$E..coli)

#These contain the best case out of these scorings, the worst of which contributes.
suppressWarnings(cnEc_Band <- sapply(NOFSummaryTable$EcoliBand,FUN=function(x){
    min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEc95_Band <- sapply(NOFSummaryTable$Ecoli95_Band,FUN=function(x){
    min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEcRecHealth540_Band <- sapply(NOFSummaryTable$EcoliRecHealth540_Band,FUN=function(x){
  min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEcRecHealth260_Band <- sapply(NOFSummaryTable$EcoliRecHealth260_Band,FUN=function(x){
    min(unlist(strsplit(x,split = '')))}))  
NOFSummaryTable$EcoliSummaryBand = as.character(apply(cbind(pmax(cnEc_Band,cnEc95_Band,cnEcRecHealth540_Band,cnEcRecHealth260_Band)),1,max))
rm("cnEc_Band","cnEc95_Band","cnEcRecHealth540_Band","cnEcRecHealth260_Band")



if(0){
  with(NOFSummaryTable,plot(as.factor(NitrogenMed_Band),NitrogenMed,log='y'))
  with(NOFSummaryTable,plot(as.factor(PhosphorusMed_Band),PhosphorusMed,log='y'))
  with(NOFSummaryTable,plot(as.factor(AmmoniacalMed_Band),AmmoniacalMed+0.01,log='y'))
  with(NOFSummaryTable,plot(as.factor(AmmoniacalMax_Band),AmmoniacalMax,log='y'))
  with(NOFSummaryTable,plot(as.factor(ChlAMed_Band),ChlAMed,log='y'))
  with(NOFSummaryTable,plot(as.factor(ChlAMax_Band),ChlAMax,log='y'))
  with(NOFSummaryTable,plot(as.factor(ClarityMedian_Band),ClarityMedian,log='y'))
  
  table(NOFSummaryTable$AmmoniacalMed_Band,NOFSummaryTable$Ammonia_Toxicity_Band)
  table(NOFSummaryTable$AmmoniacalMax_Band,NOFSummaryTable$Ammonia_Toxicity_Band)
  table(NOFSummaryTable$AmmoniacalMax_Band,NOFSummaryTable$AmmoniacalMax_Band)
  table(NOFSummaryTable$Ecoli95_Band,NOFSummaryTable$EcoliSummaryBand)
}


NOFSummaryTable$CouncilSiteID=lakeSiteTable$CouncilSiteID[match(NOFSummaryTable$LawaSiteID,lakeSiteTable$LawaSiteID)]
NOFSummaryTable$Agency=lakeSiteTable$Agency[match(NOFSummaryTable$LawaSiteID,lakeSiteTable$LawaSiteID)]
NOFSummaryTable$Region=lakeSiteTable$Region[match(NOFSummaryTable$LawaSiteID,lakeSiteTable$LawaSiteID)]
NOFSummaryTable$SiteID=lakeSiteTable$SiteID[match(NOFSummaryTable$LawaSiteID,lakeSiteTable$LawaSiteID)]
NOFSummaryTable <- NOFSummaryTable%>%select(LawaSiteID:SiteID,Year:EcoliSummaryBand)




#############################Save output tables ############################
#Audit wants NOFLakesOverall.
write.csv(NOFSummaryTable,
          file = paste0("h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                        "/NOFLakesAll",format(Sys.time(),"%d%b%Y"),".csv"),row.names=F)
write.csv(NOFSummaryTable%>%dplyr::filter(grepl('to',Year)),
          file = paste0("h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                        "/NOFLakesOverall",format(Sys.time(),"%d%b%Y"),".csv"),row.names=F)



#Make outputs for ITE
# Reshape Output
NOFSummaryTableLong <- melt(data=NOFSummaryTable,
                            id.vars=c("LawaSiteID","CouncilSiteID","SiteID","Agency","Year","Region"))
LakeSiteNOF <- NOFSummaryTableLong%>%
  dplyr::select(LawaSiteID,variable,Year,value)%>%
  dplyr::filter(grepl('Band',variable,ignore.case=T))%>%
  dplyr::filter(grepl('to',Year))

LakeSiteNOF$parameter=LakeSiteNOF$variable
LakeSiteNOF$parameter[which(LakeSiteNOF$variable%in%c("Ecoli95","Ecoli95_Band",#"EcoliPeriod",
                                                     "EcoliMedian","EcoliBand",
                                                     "EcoliRecHealth260","EcoliRecHealth260_Band",
                                                     "EcoliRecHealth540","EcoliRecHealth540_Band",
                                                     "AmmoniacalMax_Band","AmmoniacalMed_Band",
                                                     "Nitrate_Toxicity_Band"))] <- NA

LakeSiteNOF$parameter <- gsub(pattern = "_*Band",replacement = "",x = LakeSiteNOF$parameter,ignore.case = T)
LakeSiteNOF$parameter <- gsub(pattern = "Tot_",replacement = "",x = LakeSiteNOF$parameter,ignore.case = T)
LakeSiteNOF$parameter <- gsub(pattern = "Max_",replacement = "",x = LakeSiteNOF$parameter,ignore.case = T)
LakeSiteNOF$parameter <- gsub(pattern = "95",replacement = "",x = LakeSiteNOF$parameter,ignore.case = T)
LakeSiteNOF$parameter <- gsub(pattern = "Med_",replacement = "",x = LakeSiteNOF$parameter,ignore.case = T)
LakeSiteNOF$parameter <- gsub(pattern = "Summary",replacement = "",x = LakeSiteNOF$parameter,ignore.case = T)
LakeSiteNOF$parameter <- gsub(pattern = "_Toxicity",replacement = "",x = LakeSiteNOF$parameter,ignore.case = T)
LakeSiteNOF$parameter <- gsub(pattern = "RecHealth...",replacement = "",x = LakeSiteNOF$parameter,ignore.case = T)
LakeSiteNOF$parameter <- gsub(pattern = "Ammoniacal",replacement = "Ammonia",x = LakeSiteNOF$parameter,ignore.case = T)

LakeSiteNOF <- LakeSiteNOF%>%drop_na(LawaSiteID)%>%filter(LawaSiteID!="")

write.csv(LakeSiteNOF%>%transmute(LAWAID=LawaSiteID,
                                  BandingRule=variable,
                                  #Parameter=parameter,
                                  Year=Year,
                                  NOFMedian=value)%>%filter(Year=="2016to2020"),
          file=paste0("h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                      "/ITELakeSiteNOF",format(Sys.time(),"%d%b%Y"),".csv"),row.names=F)
# itelakesitenof=read.csv(tail(dir("h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/","ITELakeSiteNOF[[:digit:]]",recursive=T,full.names = T),1))
rm(LakeSiteNOF)

#LakeSiteNOFGraph
write.csv(lakesMonthlyMedians%>%
            transmute(LAWAID=LawaSiteID,
                      Parameter=Measurement,
                      NOFDate=format(Date,'%Y-%m-%d'),
                      NOFValue=Value)%>%
            filter(Parameter!="CYANOTOX"),
          file=paste0("h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                      "/ITELakeSiteNOFGraph",format(Sys.time(),"%d%b%Y"),".csv"),row.names=F)
# itelakesitenofgraph=read.csv(tail(dir("h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/","ITELakeSiteNOFGraph",recursive=T,full.names = T),1))

#Audit impact of n requireemnts

if(0){
  dir(pattern='NOFLakesOverAll',recursive=T,full.names = T,ignore.case = T)
  prevNOF = read.csv("./Analysis/2021-09-09/NOFLakesOverall09Sep2021.csv",stringsAsFactors = F)
  nowNOF = read.csv("./Analysis/2021-09-16/NOFLakesOverall16Sep2021.csv",stringsAsFactors = F)  
  
names(nowNOF)[grepl('_band',names(nowNOF),ignore.case=T)]
  
  table(is.na(prevNOF$NitrogenMed_Band))
  table(is.na(nowNOF$NitrogenMed_Band))

  table(is.na(prevNOF$PhosphorusMed_Band))
  table(is.na(nowNOF$PhosphorusMed_Band))
  
  table(is.na(prevNOF$AmmoniacalMed_Band))
  table(is.na(nowNOF$AmmoniacalMed_Band))
  
  table(is.na(prevNOF$AmmoniacalMax_Band))
  table(is.na(nowNOF$AmmoniacalMax_Band))
  
  table(is.na(prevNOF$Ammonia_Toxicity_Band))
  table(is.na(nowNOF$Ammonia_Toxicity_Band))
  
  table(is.na(prevNOF$ClarityMedian_Band))
  table(is.na(nowNOF$ClarityMedian_Band))

  table(is.na(prevNOF$ChlAMed_Band))
  table(is.na(nowNOF$ChlAMed_Band))

  table(is.na(prevNOF$ChlAMax_Band))
  table(is.na(nowNOF$ChlAMax_Band))

  table((prevNOF$Ecoli95_Band==''))
  table((nowNOF$Ecoli95_Band==''))

  table((prevNOF$EcoliRecHealth540_Band==''))
  table((nowNOF$EcoliRecHealth540_Band==''))

  table((prevNOF$EcoliRecHealth260_Band==''))
  table((nowNOF$EcoliRecHealth260_Band==''))
  

}