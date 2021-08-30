rm(list=ls())
gc()
library(parallel)
library(doParallel)
library(tidyverse)
source('H:/ericg/16666LAWA/LAWA2021/scripts/LAWAFunctions.R')
# source('K:/R_functions/nzmg2WGS.r')
# source('K:/R_functions/nztm2WGS.r')

dir.create(paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/", format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)
dir.create(paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis/", format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)

siteTable=loadLatestSiteTableRiver()
agencies= c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc")
lawaset=c("NH4", "TURB","TURBFNU", "BDISC",  "DRP",  "ECOLI",  "TN",  "TP",  "TON",  "PH","DIN","NO3N")

if(0){
  
startTime=Sys.time()                                                                    #seconds / site
system.time({source("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Scripts/loadAC_list.R")})      #40 / 35
system.time({source("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Scripts/loadBOP_list.R")})     #169 / 49
system.time({source("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Scripts/loadECAN_list.R")})    #123 / 190
system.time({source("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Scripts/loadES_list.R")})      #117 / 60
system.time({source("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Scripts/loadGDC_list.R")})     #26 / 39
system.time({source("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Scripts/loadGWRC_list.R")})    #41 / 46
system.time({source("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Scripts/loadHBRC_list.R")})    # 
system.time({source("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Scripts/loadHRC_list.R")})     #180 / 136
system.time({source("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Scripts/loadMDC_list.R")})     #34 / 32
system.time({source("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Scripts/loadNCC_list.R")}) #25  X
system.time({source("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Scripts/loadNIWA_list.R")})    #58 / 77
system.time({source("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Scripts/loadNRC_list.R")})     #40 / 41
system.time({source("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Scripts/loadORC_list.R")})     #219   / 50
system.time({source("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Scripts/loadTDC_list.R")}) #
system.time({source("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Scripts/loadTRC_list.R")})   #221 / 13
system.time({source("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Scripts/loadWCRC_list.R")}) #
system.time({source("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Scripts/loadWRC_list.R")}) #
cat("Done load\n")
Sys.time()-startTime #23 minutes

doneload=T

}

for(agency in agencies){
  checkCSVageRiver(agency = agency)
}


##############################################################################
#                          COMBINE CSVs to COMBO
##############################################################################
#Build the combo ####
library(parallel)
library(doParallel)

  if(exists('wqdata'))rm(wqdata)
  rm(list=ls(pattern='mfl'))
  backfill=T
workers=makeCluster(6)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  library(readr)
  library(dplyr)
})
startTime=Sys.time()
foreach(agencyi =1:length(agencies),.combine = rbind,.errorhandling = 'stop')%dopar%{
  mfl=loadLatestCSVRiver(agencies[agencyi],maxHistory = 30)
    #BACKFILL except canterbury, hbrc, hrc
    if(backfill & !agencies[agencyi]%in%c('ecan','hbrc','hrc')){
      targetSites=tolower(siteTable$CouncilSiteID[siteTable$Agency==agencies[agencyi]])
      targetCombos=apply(expand.grid(targetSites,tolower(lawaset)),MARGIN = 1,FUN=paste,collapse='')
      currentSiteMeasCombos=unique(paste0(tolower(mfl$CouncilSiteID),tolower(mfl$Measurement)))
      missingCombos=targetCombos[!targetCombos%in%currentSiteMeasCombos]
      
      #Pull these out for LUke at HRC
      if(all(c("makakahi at end kaiparoro roadturb","mangatainoka at hukanuiturb")%in%missingCombos)){
        missingCombos=missingCombos[-which(missingCombos%in%c("makakahi at end kaiparoro roadturb","mangatainoka at hukanuiturb"))]
      }
      if(length(missingCombos)>0){
        agencyFiles = rev(dir(path = "H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",
                              pattern = paste0('^',agencies[agencyi],'.csv'),
                              full.names = T,recursive = T,ignore.case = T))[-1]
        for(af in seq_along(agencyFiles)){
          agencyCSV = read_csv(agencyFiles[af],guess_max=)
          if("Measurement"%in%names(agencyCSV)){
            agencyCSVsiteMeasCombo=paste0(tolower(agencyCSV$CouncilSiteID),tolower(agencyCSV$Measurement))
            if(any(missingCombos%in%unique(agencyCSVsiteMeasCombo))){
               browser()
              these=agencyCSVsiteMeasCombo%in%missingCombos
              agencyCSV=agencyCSV[these,]
              agencyCSV$Altitude=mfl$Altitude[match(agencyCSV$CouncilSiteID,mfl$CouncilSiteID)]
              agencyCSV$AltitudeCl=mfl$AltitudeCl[match(agencyCSV$CouncilSiteID,mfl$CouncilSiteID)]
             
               if('SWQLandcover'%in%names(agencyCSV)){
                agencyCSV <- agencyCSV%>%select(-SWQLandcover)
                agencyCSV$SWQLanduse=mfl$SWQLanduse[match(agencyCSV$CouncilSiteID,mfl$CouncilSiteID)]
                agencyCSV$Landcover=mfl$Landcover[match(agencyCSV$CouncilSiteID,mfl$CouncilSiteID)]
              }
              if(!'LawaSiteID'%in%names(agencyCSV)){
                agencyCSV$LawaSiteID=tolower(siteTable$LawaSiteID[match(tolower(agencyCSV$CouncilSiteID),
                                                                        tolower(siteTable$CouncilSiteID))])
              }else{
                agencyCSV$LawaSiteID=tolower(agencyCSV$LawaSiteID)
              }
              
              cat(agencyFiles[af],dim(agencyCSV)[1],'\n')
              
              if(!"rawSWQLanduse"%in%names(agencyCSV)){
                agencyCSV$rawSWQLanduse = siteTable$rawSWQLanduse[match(agencyCSV$LawaSiteID,siteTable$LawaSiteID)]
              }
              if(!"rawRecLandcover"%in%names(agencyCSV)){
                agencyCSV$rawRecLandcover = siteTable$rawRecLandcover[match(agencyCSV$LawaSiteID,siteTable$LawaSiteID)]
              }
              if(!"QC"%in%names(agencyCSV)){
                agencyCSV$QC=NA
              }
              mfl=rbind(mfl,agencyCSV[,names(mfl)])
              rm(agencyCSV,these)
              currentSiteMeasCombos=unique(paste0(tolower(mfl$CouncilSiteID),tolower(mfl$Measurement)))
              missingCombos=targetCombos[!targetCombos%in%currentSiteMeasCombos]
              if(length(missingCombos)==0){
                break
              }            
            }
          }
          suppressWarnings(rm(agencyCSV))
        }
        
        rm(missingCombos,targetCombos,currentSiteMeasCombos,targetSites)
      }
    }
  return(mfl)
}->wqdata   
stopCluster(workers)
rm(workers)
wqdata$Measurement=toupper(wqdata$Measurement)
Sys.time()-startTime  
#23/6/2020    797484
#23/6/2020pm  856506
#25/6/2020    999530
#3/7/2020    1005143
#9/7/2020    1040614  
#16/7/2020   1084848
#24/7/2020   1102252
#31/7/2020   1087519
#07/08/2020  1080963
#14/8/2020   1116097
#14/9/2020   1115591
#29/6/2020   1066742
#1/7/2021    1180278
#8/7/2021    1174823
#16/7/2021   1322226
#23/7/2021   1341448
#30/7/2021   1378214 #30s
#05/8/2021   1413164 26
#06/8/21     1434671
#09/8/21     1447976
#12/8/21     1448781
#13/8/21     1464492 31 s with backfill
#17/8/21     1464752
#20/8/21     1479324 33.6
#24/8/21     1470599
#27/8/21     1470856 from boprc and hrc, 102 and 155 respectively
#27/8/21     1474832 three enw gwrc sites
donecombine=T


wqdata$CouncilSiteID=trimws(tolower(wqdata$CouncilSiteID))
wqdata$CenType[wqdata$CenType%in%c("L","Left")] <- "Left"
wqdata$CenType[wqdata$CenType%in%c("R","Right")] <- "Right"
wqdata$Units=tolower(wqdata$Units)

siteTable$CouncilSiteID=trimws(tolower(siteTable$CouncilSiteID))
siteTable$SWQLanduse=pseudo.titlecase(siteTable$SWQLanduse)
siteTable$SWQAltitude=pseudo.titlecase(siteTable$SWQAltitude)
siteTable$SiteID=trimws(tolower(siteTable$SiteID))
siteTable$LawaSiteID=trimws(siteTable$LawaSiteID)
siteTable$Altitude=tolower(siteTable$Altitude)
siteTable$Landcover=tolower(siteTable$Landcover)
siteTable$Region=tolower(siteTable$Region)
siteTable$Agency=tolower(siteTable$Agency)
table(unique(wqdata$CouncilSiteID)%in%siteTable$CouncilSiteID)

wqdata <- merge(wqdata,
                siteTable%>%select(CouncilSiteID,SiteID,LawaSiteID,Agency,Region,
                                   SWQAltitude,SWQLanduse,Altitude,Landcover))

table(wqdata$Agency,is.na(wqdata$QC),useNA='a')  #QC data in AC, ECAN, GDC, GWRC,MDC, ORC 
table(wqdata$Region,useNA='a')
table(wqdata$Agency,useNA='a')
#    ac  boprc   ecan     es    gdc   gwrc   hbrc    hrc    mdc    ncc   niwa    nrc    orc    tdc    trc  wcrc    wrc
# 70659  54768 222636 107083  39888  89950 108161 211987  36018  22172 114686  62186  77052  19394  26640 29501 182051 
# 70659  54768 222636 107083  39888  85974 108161 211987  36018  22172 114686  62186  77052  19394  26640 29501 182051 
# 70659  54768 222636 107083  39888  85974 108161 211987  36018  22172 114686  62186  77052  19394  26640 29501 182051
# 70659  54666 222636 107083  39888  85974 108161 211832  36018  22090 114686  62186  77052  19394  26640 29501 182051
# 70659  54666 222636 107083  39888  85974 108161 220639  36018  22090 114686  62186  77052  19394  26640 29501 182051
# 70659  54666 224053 107083  39888  85974 108157 211826  36018  22090 114686  62186  69880  19394  26640 29501 182051
# 70659  54666 224053 107083  39888  85974 108157 211566  36018  22090 114686  62186  69880  19394  26640 29501 182051
# 70659  43017 224053 107083  37779  85974 112751 207414  36018  21624 113359  62186  69653  18705  24845 29501 182051
# 70659  60683 224074 107085  37789  86012 108214 210434  36020  22093 108318  75279  49574  17499  22689 29503 182051 
# 64588  54043 224074 107085  37789  86012 108214 210434  36020  22093 108318  75279  49574  17499  22689 28909 160544 
# 64588  54043 224074 107085  37789  61674 108214 210434  36020  22093 108318  75279  49574  17499  22689 47585 160544 
# 64588  54043 224074 102735  37789  61674 108246 258964  36020  22175 108318  81617  49574  17499   6483 23359 160544
# 64588  54043 223907  84384  37791  61674 117390 269600  36020  19490 108318  51925  49574  17499  20639 23289 160544
# 54617  47156 162662  73340  27466  54929  85873 160718   4289  16893 103590  41344  47780  14992  20917 22749 143024 
# 54061  49821 162662  81815  32391  54929  77699 160718  29796  16893 103590  41344  50056  15008  20917 22749 143024
# 54060  49821 162662  81815  32391  54929  77681 160718  29796  16893 103590  41344  50056  15008  20917 22749 143024 
# 54060  49417 162662  81815  32391  54929  77681 160718  29796  16893 103510  41344  50056  15005  20917 22749 143024 
# 55868  54043 165467  77948  36359  57951  84042 160718  31241  18914  15771  50493  46922  17247  20957 22841 151466 


table(wqdata$SWQLanduse,wqdata$SWQAltitude)
table(wqdata$Agency,wqdata$Measurement)


unique(wqdata$Units)
wqdata$Units = gsub(pattern = " units|_units| Units",replacement = "",x = wqdata$Units)
wqdata$Units = gsub(pattern = "_",replacement = "",x = wqdata$Units)
wqdata$Units = gsub(pattern = "^g/m.?.",replacement = "g/m3",x = wqdata$Units)
wqdata$Units = gsub(pattern = ".*/ *100m(l|L)",replacement = "/100ml",x = wqdata$Units)
wqdata$Units = gsub(pattern = ".*/ *100ml",replacement = "/100ml",x = wqdata$Units)
wqdata$Units = gsub(pattern = "-n|-p",replacement = "",x = wqdata$Units)
wqdata$Units = gsub(pattern = "/m3.",replacement = "/m3",x = wqdata$Units)
wqdata$Units = gsub(pattern = "g/m3|\\(mg/l|ppb",replacement = "mg/l",x = wqdata$Units)
wqdata$Units = gsub(pattern = "g/m3|\\(mg/l",replacement = "mg/l",x = wqdata$Units)
wqdata$Units = gsub(pattern = "meter",replacement = "m",x = wqdata$Units)
wqdata$Units = gsub(pattern = "---",replacement = "ph",x = wqdata$Units)
wqdata$Units = gsub(pattern = "formazin nephelometric unit",replacement = "fnu",x = wqdata$Units)

unique(wqdata$Units)
table(wqdata$Measurement,wqdata$Units)
table(wqdata$Agency,wqdata$Units)

wqdata$Measurement[which(wqdata$Units=='fnu')] <- "TURBFNU"
wqdata%>%filter(Measurement=="TURBFNU")%>%select(Agency,Units)%>%table

write.csv(table(wqdata$Units,wqdata$Agency),paste0('h:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis/',format(Sys.Date(),"%Y-%m-%d"),'/unitsagency.csv'),row.names = F)
write.csv(table(wqdata$Units,wqdata$Measurement),paste0('h:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis/',format(Sys.Date(),"%Y-%m-%d"),'/unitsmeasurement.csv'),row.names = F)



#855695 July8 2019
#932548 July17 2019
#947210 July22 2019
#989969 July 29 2019
#994671 Aug5 2019
#1008571 Aug12 2019
#1043640 Aug 14 2019
#1054395 Aug 19 2019
#1054302 Aug 21 2019
#1054314 Aug 26 2019
#1055056 Sep 9 2019
#797385 Jun 23 2020
#856506 Jun23 pm
#999356 Jun25 2020
#1004961 Jul32020
#1017914 Jul92020
#1026489 Jul9 after units and sorting AC and HBRC a bit
#1061942 16Jul2020
#1079348 24Jul2020
#1079547 31july2020
#1073182 7August2020
#1117289 14 August2020
#1117279 21 August 2020
#1116792 14Sept 2020
#1068009 29June 2021
#1178528 2July2021
#1183566  8/7/21
#1321880  16/7/2021
#1340103  23/7/21
#1364751  30/7/21
#1387542  5/8/21
#1418516  6/8/21
#1428486  9/8/21
#1448781  12/8/21
#1464492  13/8/21
#1464752  17/8/21
#1479324  20/8/21
#1470674 24/8/21
#1470856  26/8/21


wqdata$Symbol=""
wqdata$Symbol[wqdata$CenType=="Left"]='<'
wqdata$Symbol[wqdata$CenType=="Right"]='>'
wqdata$RawValue=paste0(wqdata$Symbol,wqdata$Value)


table(wqdata$Agency,wqdata$Measurement)/table(wqdata$Agency,wqdata$Measurement)

#       BDISC DIN DRP ECOLI NH4 NO3N  PH  TN TON  TP TURB TURBFNU
# ac        1   1   1     1   1    1   1   1   1   1    1        
# boprc     1       1     1   1        1   1   1   1    1        
# ecan      1   1   1     1   1    1   1   1   1   1    1        
# es        1   1   1     1   1    1   1   1   1   1    1        
# gdc       1   1   1     1   1    1   1   1   1   1    1       1
# gwrc      1   1   1     1   1    1   1   1   1   1    1        
# hbrc      1   1   1     1   1    1   1   1   1   1    1       1
# hrc       1   1   1     1   1    1   1   1   1   1    1       1
# mdc               1     1   1    1   1   1   1   1    1        
# ncc       1       1     1   1    1   1   1       1    1        
# niwa      1       1     1   1        1   1       1    1        
# nrc       1   1   1     1   1    1   1   1   1   1    1        
# orc       1   1   1     1   1    1   1   1   1   1    1        
# tdc       1       1     1   1    1   1   1   1   1    1        
# trc       1   1   1     1   1    1   1   1   1   1    1       1
# wcrc      1   1   1     1   1    1   1   1   1   1            1
# wrc       1       1     1   1        1   1   1   1    1        



##################

wqdata$LawaSiteID = tolower(wqdata$LawaSiteID)
dir.create(paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)
write.csv(wqdata,paste0("D:/LAWA/2021/AllCouncils.csv"),row.names = F)
file.copy(from = "D:/LAWA/2021/AllCouncils.csv",
          to=paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/AllCouncils.csv"),overwrite = T)
##################

