
rm(list=ls())
library(tidyverse)
library(areaplot)
library(lubridate)
# library(doBy)
library(showtext)
library(rgdal)
if(!'source'%in%font_families()){
  font_add(family = 'source',regular = "h:/ericg/16666LAWA/LAWA2021/SourceFont/SourceSansPro-Regular.ttf")
  # font_add_google("Source Sans Pro",family='source')
}
labelAreas <- function(areaTable,textcex=0.75,invert=F){
  colPos=apply(areaTable,1,sum)
  colPos=colPos/sum(colPos)
  colPos=cumsum(colPos)
  colPos=apply(cbind(c(0,colPos[1:(length(colPos)-1)]),colPos),1,mean)
  colPos=colPos*par('usr')[2]
  for(ll in 1:dim(areaTable)[1]){
    rowPos=areaTable[ll,]
    if(invert){
      rowPos = rev(rowPos/sum(rowPos))
    }else{
      rowPos = rowPos/sum(rowPos)
    }
    rowPos = cumsum(rowPos)
    rowPos=apply(cbind(c(0,rowPos[1:(length(rowPos)-1)]),rowPos),1,mean)
    if(invert){
      text(colPos[ll],rowPos[rev(areaTable[ll,])>0],rev(areaTable[ll,areaTable[ll,]>0]),cex=textcex)
    }else{
      text(colPos[ll],rowPos[areaTable[ll,]>0],areaTable[ll,areaTable[ll,]>0],cex=textcex)
    }
  }
}
setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality/")
source("h:/ericg/16666LAWA/LAWA2021/Scripts/LAWAFunctions.R")



nzmap <- readOGR('S:/New_S/Basemaps_Vector/NZ_Coastline/WGS_84/coast_wgs84.shp')
as.hexmode(c(242,242,242))
LAWAPalette=c("#95d3db","#74c0c1","#20a7ad","#007197",
              "#e85129","#ff8400","#ffa827","#85bb5b","#bdbcbc")
OLPalette=c('#cccccc','#f2f2f2')
plot(1:9,1:9,col=LAWAPalette,cex=5,pch=16)
NOFPal4 = LAWAPalette[c(5,7,8,3)]
NOFPal5 = LAWAPalette[c(5,6,7,8,3)]
AtoE = c("A","B","C","D","E","NA")
AtoD = c("A","B","C","D","NA")
#prep data ####
dir.create(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/"),showWarnings = F,recursive=T)
dir.create(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/"),showWarnings = F,recursive=T)

riverSiteTable=loadLatestSiteTableRiver()
macroSiteTable=loadLatestSiteTableMacro()

length(unique(c(tolower(riverSiteTable$LawaSiteID),tolower(macroSiteTable$LawaSiteID)))) #1515
sort(unique(c(tolower(riverSiteTable$LawaSiteID),tolower(macroSiteTable$LawaSiteID)))) #1515

#A few of those auckland sites had the worng coordinates as of Sept 2021
if(abs(macroSiteTable$Long[which(macroSiteTable$LawaSiteID=='lawa-102734')]-176.5804)<0.0001){
  macroSiteTable$Long[which(macroSiteTable$LawaSiteID=='lawa-102734')] <- 174.7273
  macroSiteTable$Lat[which(macroSiteTable$LawaSiteID=='lawa-102734')] <- (-36.8972)
}

if(abs(macroSiteTable$Long[which(macroSiteTable$LawaSiteID=='lawa-102744')]-174.5198)<0.0001){
  macroSiteTable$Long[which(macroSiteTable$LawaSiteID=='lawa-102744')] <- 174.8418
  macroSiteTable$Lat[which(macroSiteTable$LawaSiteID=='lawa-102744')] <- (-37.1811)
}


these = which(is.na(macroSiteTable$Region))

for(ms in seq_along(these)){
  dists = sqrt((macroSiteTable$Long[these[ms]]-riverSiteTable$Long)^2+(macroSiteTable$Lat[these[ms]]-riverSiteTable$Lat)^2)
  cat(min(dists)*1000,'\t')
  points(riverSiteTable$Long[which.min(dists)],riverSiteTable$Lat[which.min(dists)],pch=16,col='blue',cex=0.5)
  macroSiteTable$Region[these[ms]] <- riverSiteTable$Region[which.min(dists)]
  macroSiteTable$LawaSiteID[these[ms]] <- riverSiteTable$LawaSiteID[which.min(dists)]
}





NOFSummaryTable=read.csv(tail(dir(path = "h:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis/",
                                  pattern="NOFSummaryTable_Rolling",
                                  recursive=T,full.names = T),1),stringsAsFactors = F)
NOFSummaryTable <- NOFSummaryTable%>%filter(!Year%in%c('2004to2008','2005to2009','2006to2010'))  #Just, you see this way we're left with a single decade

#Drop out NIWA versions of sites that are monitored by NIWA and by council
nstSiteCount <- NOFSummaryTable%>%group_by(LawaSiteID)%>%summarise(n=n())%>%ungroup
NOFSummaryTable$n = nstSiteCount$n[match(NOFSummaryTable$LawaSiteID,nstSiteCount$LawaSiteID)]
toCut = which(NOFSummaryTable$n==20 & NOFSummaryTable$Agency=='niwa')
NOFSummaryTable = NOFSummaryTable[-toCut,]

NOFSummaryTable$SWQAltitude = pseudo.titlecase(tolower(NOFSummaryTable$SWQAltitude))
NOFSummaryTable$rawRecLandcover = riverSiteTable$rawRecLandcover[match(tolower(NOFSummaryTable$LawaSiteID),tolower(riverSiteTable$LawaSiteID))]
NOFSummaryTable$rawRecLandcover = factor(NOFSummaryTable$rawRecLandcover,
                                         levels=c("if","w","t","s","b","ef", "p","m", "u"))
NOFSummaryTable$gRecLC=NOFSummaryTable$rawRecLandcover
NOFSummaryTable$gRecLC <- factor(NOFSummaryTable$gRecLC,
                                 levels=c("if","w","t","s","b",
                                          "ef",
                                          "p","m",
                                          "u"),
                                 labels=c(rep("Native vegetation",5),
                                          "Plantation forest","Pasture","Pasture","Urban"))

suppressWarnings(cnEc_Band <- sapply(NOFSummaryTable$EcoliMed_Band,
                                     FUN=function(x){
  min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEc95_Band <- sapply(NOFSummaryTable$Ecoli95_Band,
                                       FUN=function(x){
  min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEcRecHealth540_Band <- sapply(NOFSummaryTable$EcoliRecHealth540_Band,
                                                 FUN=function(x){
  min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEcRecHealth260_Band <- sapply(NOFSummaryTable$EcoliRecHealth260_Band,
                                                 FUN=function(x){
  min(unlist(strsplit(x,split = '')))}))  
NOFSummaryTable$EcoliSummarybandwo95 = as.character(apply(cbind(pmax(cnEc_Band,cnEcRecHealth540_Band,cnEcRecHealth260_Band)),1,max))
rm("cnEc_Band","cnEc95_Band","cnEcRecHealth540_Band","cnEcRecHealth260_Band")


#The level order is from bottom to top on a spineplot.  
#Barplots need them the other way round, so need to reorder them, where used.
NOFSummaryTable$DIN_Summary_Band=factor(NOFSummaryTable$DIN_Summary_Band,
                                        levels=(AtoD),
                                        labels=(AtoD))
NOFSummaryTable$DIN_Summary_Band[is.na(NOFSummaryTable$DIN_Summary_Band)] <- "NA"


NOFSummaryTable$Nitrate_Toxicity_Band=factor(NOFSummaryTable$Nitrate_Toxicity_Band,
                                             levels=(AtoD),
                                             labels=(AtoD))
NOFSummaryTable$Nitrate_Toxicity_Band[is.na(NOFSummaryTable$Nitrate_Toxicity_Band)] <- "NA"

# NOFSummaryTable$TON_Toxicity_Band=factor(NOFSummaryTable$TON_Toxicity_Band,
#                                              levels=(AtoD),
#                                              labels=(AtoD))
# NOFSummaryTable$TON_Toxicity_Band[is.na(NOFSummaryTable$TON_Toxicity_Band)] <- "NA"


NOFSummaryTable$DRP_Summary_Band=factor(NOFSummaryTable$DRP_Summary_Band,
                                        levels=(AtoD),
                                        labels=(AtoD))
NOFSummaryTable$DRP_Summary_Band[is.na(NOFSummaryTable$DRP_Summary_Band)] <- "NA"

NOFSummaryTable$EcoliSummaryband=factor(NOFSummaryTable$EcoliSummaryband,
                                        levels=(AtoE),
                                        labels=(AtoE))
NOFSummaryTable$EcoliSummaryband[is.na(NOFSummaryTable$EcoliSummaryband)] <- "NA"
NOFSummaryTable$EcoliSummarybandwo95=factor(NOFSummaryTable$EcoliSummarybandwo95,
                                        levels=(AtoE),
                                        labels=(AtoE))
NOFSummaryTable$EcoliSummarybandwo95[is.na(NOFSummaryTable$EcoliSummarybandwo95)] <- "NA"

NOFSummaryTable$Ammonia_Toxicity_Band=factor(NOFSummaryTable$Ammonia_Toxicity_Band,
                                             levels=(AtoD),
                                             labels=(AtoD))
NOFSummaryTable$Ammonia_Toxicity_Band[is.na(NOFSummaryTable$Ammonia_Toxicity_Band)] <- "NA"

NOFSummaryTable$SusSedBand=factor(NOFSummaryTable$SusSedBand,
                                  levels=(AtoD),
                                  labels=(AtoD))
NOFSummaryTable$SusSedBand[is.na(NOFSummaryTable$SusSedBand)] <- "NA"




NOFSummaryTable$Long=riverSiteTable$Long[match(tolower(NOFSummaryTable$LawaSiteID),tolower(riverSiteTable$LawaSiteID))]
NOFSummaryTable$Lat=riverSiteTable$Lat[match(tolower(NOFSummaryTable$LawaSiteID),tolower(riverSiteTable$LawaSiteID))]
NOFlatest = droplevels(NOFSummaryTable%>%filter(Year=="2016to2020"))



#load and prep macro data ####
lawaMacroState5yr = read.csv(tail(dir(path='h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/',pattern='MACRO_STATE_ForITE|MacroState',recursive = T,full.names = T),1),stringsAsFactors = F)
lawaMacroState5yr = lawaMacroState5yr%>%filter(Parameter%in%c("MCI","ASPM","QMCI"))%>%dplyr::rename(LawaSiteID=LAWAID,Measurement=Parameter,Q50=Median)

MCINOF = read.csv(tail(dir(path = "h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/",
                           pattern="MacroRollingMCI",
                           recursive = T,full.names = T),1),stringsAsFactors = F)
QMCINOF = read.csv(tail(dir(path = "h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/",
                            pattern="MacroRollingQMCI",
                            recursive = T,full.names = T),1),stringsAsFactors = F)
ASPMNOF = read.csv(tail(dir(path = "h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/",
                            pattern="MacroRollingASPM",
                            recursive = T,full.names = T),1),stringsAsFactors = F)
macroNOF = left_join(MCINOF%>%select(-date)%>%rename(MCI=Value),
                     QMCINOF%>%select(-date)%>%rename(QMCI=Value),
                     by=c('LawaSiteID','sYear'),all=T)
macroNOF = left_join(macroNOF,
                     ASPMNOF%>%select(-date)%>%rename(ASPM=Value),
                     by=c('LawaSiteID','sYear'),all=T)

rm(MCINOF,QMCINOF,ASPMNOF)
MCINOF=macroNOF

MCINOF$Region=macroSiteTable$Region[match(gsub('_niwa','',tolower(MCINOF$LawaSiteID)),tolower(macroSiteTable$LawaSiteID))]

MCINOF$Region[MCINOF$LawaSiteID=='ebop-00049'] <- 'bay of plenty'
MCINOF$Region[MCINOF$LawaSiteID=='ecan-10004'] <- 'canterbury'
MCINOF$Region[MCINOF$LawaSiteID=='nrc-10008'] <- 'northland'
MCINOF$Region[MCINOF$LawaSiteID=='nrwqn-00006'] <- 'canterbury'
MCINOF$Region[MCINOF$LawaSiteID=='nrwqn-00013'] <- 'hawkes bay'
MCINOF$Region[MCINOF$LawaSiteID=='nrwqn-00029'] <- 'otago'
MCINOF$Region[MCINOF$LawaSiteID=='nrwqn-00007'] <- 'canterbury'
MCINOF$Region[MCINOF$LawaSiteID=='nrwqn-00009'] <- 'canterbury'
MCINOF$Region[MCINOF$LawaSiteID=='nrwqn-00015'] <- 'hawkes bay'
MCINOF$Region[MCINOF$LawaSiteID=='nrwqn-00042'] <- 'waikato'

MCINOF$MCIband = cut(MCINOF$rollMCI,
                     breaks = c(0,90,110,130,200),
                     labels = c("D","C","B","A"),
                     right = F,ordered_result = T)
table(MCINOF$MCIband)
MCINOF$MCIband=factor(as.character(MCINOF$MCIband),
                      levels=(AtoD),
                      labels=(AtoD))
MCINOF$MCIband[is.na(MCINOF$MCIband)] <- "NA"
table(MCINOF$MCIband)

MCINOF$Landcover = macroSiteTable$Landcover[match(gsub('_NIWA','',MCINOF$LawaSiteID,ignore.case = T),macroSiteTable$LawaSiteID)]
MCINOF$SWQLanduse = pseudo.titlecase(macroSiteTable$SWQLanduse[match(gsub('_NIWA','',MCINOF$LawaSiteID,ignore.case = T),macroSiteTable$LawaSiteID)])
MCINOF$AltitudeCl = macroSiteTable$AltitudeCl[match(gsub('_NIWA','',MCINOF$LawaSiteID,ignore.case = T),macroSiteTable$LawaSiteID)]
MCINOF$SWQAltitude = pseudo.titlecase(macroSiteTable$SWQAltitude[match(gsub('_NIWA','',MCINOF$LawaSiteID,ignore.case = T),macroSiteTable$LawaSiteID)])
MCINOF$rawRecLandcover = macroSiteTable$rawRecLandcover[match(gsub('_niwa','',tolower(MCINOF$LawaSiteID)),
                                                              tolower(macroSiteTable$LawaSiteID))]
MCINOF$rawRecLandcover[is.na(MCINOF$rawRecLandcover)] = riverSiteTable$rawRecLandcover[match(tolower(MCINOF$LawaSiteID[is.na(MCINOF$rawRecLandcover)]),
                                                                                             tolower(riverSiteTable$LawaSiteID))]
MCINOF$rawRecLandcover = factor(MCINOF$rawRecLandcover,levels=c("if","w","t","s","b","ef", "p","m","u"))
MCINOF$gRecLC = MCINOF$rawRecLandcover

MCINOF$gRecLC <- factor(MCINOF$gRecLC,levels=c("if","w","t","s", "b","ef", "p","m", "u"),
                        labels=c("Native vegetation","Native vegetation","Native vegetation",
                                 "Native vegetation","Native vegetation",
                                 "Plantation forest","Pasture","Pasture","Urban"))
# MCINOF$gRecLC[is.na(MCINOF$gRecLC)] <- 'Pasture'
MCINOF$Long =macroSiteTable$Long[match(tolower(MCINOF$LawaSiteID),tolower(gsub('_niwa','',macroSiteTable$LawaSiteID)))]
MCINOF$Lat =macroSiteTable$Lat[match(tolower(MCINOF$LawaSiteID),tolower(gsub('_niwa','',macroSiteTable$LawaSiteID)))]

MCINOF$Long[which(is.na(MCINOF$Long))] = riverSiteTable$Long[match(tolower(MCINOF$LawaSiteID[which(is.na(MCINOF$Long))]),
                                                                   tolower(gsub('_niwa','',riverSiteTable$LawaSiteID)))]
MCINOF$Lat[which(is.na(MCINOF$Lat))] = riverSiteTable$Lat[match(tolower(MCINOF$LawaSiteID[which(is.na(MCINOF$Lat))]),
                                                                tolower(gsub('_niwa','',riverSiteTable$LawaSiteID)))]


if(0){
  rec=read_csv("d:/RiverData/RECnz.txt")
  rec$gRecLC=factor(rec$LANDCOVER,levels=c('IF','W','T','S','B','EF','P','U'),
                    labels=c("Native vegetation","Native vegetation","Native vegetation","Native vegetation","Native vegetation",
                             "Plantation forest","Pasture","Urban"))
  "1 Native vegetation       196494/405538 = 48%"
  "2 Plantation forest        20812/405538 = 5%"
  "3 Pasture                 185124/405538 = 45%"
  "4 Urban                     3108/405538 = 1%"
  196494+20812+185124+3108 = 405538
}




#spacer ####
if(0){
  length(unique(gsub('_niwa','',tolower(riverSiteTable$LawaSiteID)))) #1036
  length(unique(gsub('_niwa','',tolower(macroSiteTable$LawaSiteID)))) #1114
  
  length(unique(gsub('_niwa','',tolower(NOFlatest$LawaSiteID)))) #1036
  length(unique(gsub('_niwa','',tolower(macroData$LawaSiteID)))) #1100
  
  unique(gsub('_niwa','',tolower(macroSiteTable$LawaSiteID)))[!unique(gsub('_niwa','',tolower(macroSiteTable$LawaSiteID)))%in%unique(gsub('_niwa','',tolower(macroData$LawaSiteID)))]
  unique(gsub('_niwa','',tolower(macroData$LawaSiteID)))[!unique(gsub('_niwa','',tolower(macroData$LawaSiteID)))%in%unique(gsub('_niwa','',tolower(macroSiteTable$LawaSiteID)))]
  
  length(unique(c(gsub('_niwa','',tolower(riverSiteTable$LawaSiteID)),
                  gsub('_niwa','',tolower(macroSiteTable$LawaSiteID))))) #1521
  
  length(unique(c(gsub('_niwa','',tolower(NOFlatest$LawaSiteID)),
                  gsub('_niwa','',tolower(macroData$LawaSiteID)))))
  #1514
  
  
  sum(!unique(tolower(NOFlatest$LawaSiteID))%in%unique(tolower(riverSiteTable$LawaSiteID)))
  
  sum(!unique(tolower(riverSiteTable$LawaSiteID))%in%unique(tolower(NOFlatest$LawaSiteID)))  #2 sitetable sites not in data
  unique(tolower(riverSiteTable$LawaSiteID))[!unique(tolower(riverSiteTable$LawaSiteID))%in%unique(tolower(NOFlatest$LawaSiteID))] #nrwqn-00050 and nrwqn-00048
  
  
  sum(!unique(gsub('_niwa','',tolower(macroSiteTable$LawaSiteID)))%in%unique(gsub('_niwa','',tolower(MCINOF$LawaSiteID))))  #20 sitetable sites not in 
  unique(gsub('_niwa','',tolower(macroSiteTable$LawaSiteID)))[!unique(gsub('_niwa','',tolower(macroSiteTable$LawaSiteID)))%in%unique(gsub('_niwa','',tolower(MCINOF$LawaSiteID)))] 
  #"ebop-00121", "ebop-00189", "lawa-101930", "nrc-00021", "orc-00131", 
  #"tdc-00017", "tdc-00008", "tdc-00016", "tdc-00038", "trc-00082", 
  #"nrwqn-00002", "nrwqn-00001", "orc-00155", "nrwqn-00025", "nrwqn-00041", 
  #"nrwqn-00044", "nrwqn-00043", "ebop-00075", "nrwqn-00039", "wcrc-00031"
  
}
