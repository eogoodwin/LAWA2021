

#Split 30/8/2021 into 5 subscripts.


#NOFPlots #### 
rm(list=ls())
library(tidyverse)
library(areaplot)
library(lubridate)
library(doBy)
library(showtext)
library(rgdal)
# font_add_google("Source Sans Pro",family='source')
labelAreas <- function(areaTable){
  colPos=apply(areaTable,1,sum)
  colPos=colPos/sum(colPos)
  colPos=cumsum(colPos)
  colPos=apply(cbind(c(0,colPos[1:(length(colPos)-1)]),colPos),1,mean)
  colPos=colPos*par('usr')[2]
  for(ll in 1:dim(areaTable)[1]){
    rowPos=areaTable[ll,]
    rowPos = rowPos/sum(rowPos)
    rowPos = cumsum(rowPos)
    rowPos=apply(cbind(c(0,rowPos[1:(length(rowPos)-1)]),rowPos),1,mean)
    text(colPos[ll],rowPos,areaTable[ll,],cex=0.75)
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



#prep data ####
dir.create(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)
riverSiteTable=loadLatestSiteTableRiver()
macroSiteTable=loadLatestSiteTableMacro()

length(unique(c(tolower(riverSiteTable$LawaSiteID),tolower(macroSiteTable$LawaSiteID)))) #1550

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
NOFSummaryTable$SWQAltitude = pseudo.titlecase(tolower(NOFSummaryTable$SWQAltitude))
NOFSummaryTable$rawRecLandcover = riverSiteTable$rawRecLandcover[match(tolower(NOFSummaryTable$LawaSiteID),tolower(riverSiteTable$LawaSiteID))]
NOFSummaryTable$rawRecLandcover = factor(NOFSummaryTable$rawRecLandcover,
                                         levels=c("if","w","t","s","b","ef", "p", "u"))
NOFSummaryTable$gRecLC=NOFSummaryTable$rawRecLandcover
NOFSummaryTable$gRecLC <- factor(NOFSummaryTable$gRecLC,
                                 levels=c("if","w","t","s","b",
                                          "ef",
                                          "p",
                                          "u"),
                                 labels=c(rep("Native vegetation",5),
                                          "Plantation forest","Pasture","Urban"))


NOFSummaryTable$Nitrate_Toxicity_Band=factor(NOFSummaryTable$Nitrate_Toxicity_Band,
                                             levels=c("A","B","C","D","NA"),
                                             labels=c("A","B","C","D","NA"))
NOFSummaryTable$Nitrate_Toxicity_Band[is.na(NOFSummaryTable$Nitrate_Toxicity_Band)] <- "NA"

NOFSummaryTable$DRP_Summary_Band=factor(NOFSummaryTable$DRP_Summary_Band,
                                        levels=c("A","B","C","D","NA"),
                                        labels=c("A","B","C","D","NA"))
NOFSummaryTable$DRP_Summary_Band[is.na(NOFSummaryTable$DRP_Summary_Band)] <- "NA"

NOFSummaryTable$EcoliSummaryband=factor(NOFSummaryTable$EcoliSummaryband,
                                        levels=c("A","B","C","D","E","NA"),
                                        labels=c("A","B","C","D","E","NA"))
NOFSummaryTable$EcoliSummaryband[is.na(NOFSummaryTable$EcoliSummaryband)] <- "NA"

NOFSummaryTable$Ammonia_Toxicity_Band=factor(NOFSummaryTable$Ammonia_Toxicity_Band,
                                             levels=c("A","B","C","D","NA"),
                                             labels=c("A","B","C","D","NA"))
NOFSummaryTable$Ammonia_Toxicity_Band[is.na(NOFSummaryTable$Ammonia_Toxicity_Band)] <- "NA"

NOFSummaryTable$SusSedBand=factor(NOFSummaryTable$SusSedBand,
                                        levels=c("A","B","C","D","NA"),
                                        labels=c("A","B","C","D","NA"))
NOFSummaryTable$SusSedBand[is.na(NOFSummaryTable$SusSedBand)] <- "NA"




NOFSummaryTable$Long=riverSiteTable$Long[match(tolower(NOFSummaryTable$LawaSiteID),tolower(riverSiteTable$LawaSiteID))]
NOFSummaryTable$Lat=riverSiteTable$Lat[match(tolower(NOFSummaryTable$LawaSiteID),tolower(riverSiteTable$LawaSiteID))]
NOFlatest = droplevels(NOFSummaryTable%>%filter(Year=="2015to2019"))



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
                      levels=c("A","B","C","D","NA"),
                      labels=c("A","B","C","D","NA"))
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
MCINOF$rawRecLandcover = factor(MCINOF$rawRecLandcover,levels=c("if","w","t","s","b","ef", "p","u"))
MCINOF$gRecLC = MCINOF$rawRecLandcover

MCINOF$gRecLC <- factor(MCINOF$gRecLC,levels=c("if","w","t","s", "b","ef", "p", "u"),
                        labels=c("Native vegetation","Native vegetation","Native vegetation","Native vegetation","Native vegetation",
                                 "Plantation forest","Pasture","Urban"))
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

#                 Export files to recreate these plots ####
#NO3 ####
NOxLUexport <- NOFlatest%>%select(Region,LandCover=gRecLC,Nitrate_Toxicity_Band)%>%
  filter(!Nitrate_Toxicity_Band%in%c("","NA")&!is.na(Nitrate_Toxicity_Band))
NOxLUexport <- NOxLUexport%>%group_by(Region,LandCover)%>%dplyr::summarise(.groups='keep',
                                                                           A=sum(Nitrate_Toxicity_Band=="A"),
                                                                           B=sum(Nitrate_Toxicity_Band=="B"),
                                                                           C=sum(Nitrate_Toxicity_Band=="C"),
                                                                           D=sum(Nitrate_Toxicity_Band=="D"))%>%ungroup
NOxLUexport <- NOxLUexport%>%pivot_longer(cols = A:D,names_to = 'Band',values_to = 'Count')%>%
  mutate(Indicator="NO3N")%>%select(Region,LandCover,Indicator,Band,Count)
#DRP ####
DRPLUexport <- NOFlatest%>%select(Region,LandCover=gRecLC,DRP_Summary_Band)%>%
  filter(!DRP_Summary_Band%in%c("","NA")&!is.na(DRP_Summary_Band))
DRPLUexport <- DRPLUexport%>%group_by(Region,LandCover)%>%dplyr::summarise(.groups='keep',
                                                                           A=sum(DRP_Summary_Band=="A"),
                                                                           B=sum(DRP_Summary_Band=="B"),
                                                                           C=sum(DRP_Summary_Band=="C"),
                                                                           D=sum(DRP_Summary_Band=="D"))%>%ungroup
DRPLUexport <- DRPLUexport%>%pivot_longer(cols = A:D,names_to = 'Band',values_to = 'Count')%>%
  mutate(Indicator="DRP")%>%select(Region,LandCover,Indicator,Band,Count)
#ECOLI ####
ECOLILUexport <- NOFlatest%>%select(Region,LandCover=gRecLC,EcoliSummaryband)%>%
  filter(!EcoliSummaryband%in%c("","NA")&!is.na(EcoliSummaryband))
ECOLILUexport <- ECOLILUexport%>%group_by(Region,LandCover)%>%dplyr::summarise(.groups='keep',
                                                                               A=sum(EcoliSummaryband=="A"),
                                                                               B=sum(EcoliSummaryband=="B"),
                                                                               C=sum(EcoliSummaryband=="C"),
                                                                               D=sum(EcoliSummaryband=="D"),
                                                                               E=sum(EcoliSummaryband=="E"))%>%ungroup
ECOLILUexport <- ECOLILUexport%>%pivot_longer(cols = A:E,names_to = 'Band',values_to = 'Count')%>%
  mutate(Indicator="ECOLI")%>%select(Region,LandCover,Indicator,Band,Count)
#NH4 ####
NH4LUexport <- NOFlatest%>%select(Region,LandCover=gRecLC,Ammonia_Toxicity_Band)%>%
  filter(!Ammonia_Toxicity_Band%in%c("","NA")&!is.na(Ammonia_Toxicity_Band))
NH4LUexport <- NH4LUexport%>%group_by(Region,LandCover)%>%dplyr::summarise(.groups='keep',
                                                                           A=sum(Ammonia_Toxicity_Band=="A"),
                                                                           B=sum(Ammonia_Toxicity_Band=="B"),
                                                                           C=sum(Ammonia_Toxicity_Band=="C"),
                                                                           D=sum(Ammonia_Toxicity_Band=="D"))%>%ungroup
NH4LUexport <- NH4LUexport%>%pivot_longer(cols = A:D,names_to = 'Band',values_to = 'Count')%>%
  mutate(Indicator="NH4")%>%select(Region,LandCover,Indicator,Band,Count)
#Sed ####
SedLUexport <- NOFlatest%>%select(Region,LandCover=gRecLC,SusSedBand)%>%
  filter(!SusSedBand%in%c("","NA")&!is.na(SusSedBand))
SedLUexport <- SedLUexport%>%group_by(Region,LandCover)%>%dplyr::summarise(.groups='keep',
                                                                           A=sum(SusSedBand=="A"),
                                                                           B=sum(SusSedBand=="B"),
                                                                           C=sum(SusSedBand=="C"),
                                                                           D=sum(SusSedBand=="D"))%>%ungroup
SedLUexport <- SedLUexport%>%pivot_longer(cols = A:D,names_to = 'Band',values_to = 'Count')%>%
  mutate(Indicator="SusSed")%>%select(Region,LandCover,Indicator,Band,Count)

#MCI ####
MCILUexport <- MCINOF%>%filter(!sYear==2020)%>%drop_na(gRecLC)%>%select(Region,LandCover=gRecLC,MCIband)%>%
  filter(!MCIband%in%c("","NA")&!is.na(MCIband))
MCILUexport <- MCILUexport%>%group_by(Region,LandCover)%>%dplyr::summarise(.groups='keep',
                                                                           A=sum(MCIband=="A"),
                                                                           B=sum(MCIband=="B"),
                                                                           C=sum(MCIband=="C"),
                                                                           D=sum(MCIband=="D"))%>%ungroup
MCILUexport <- MCILUexport%>%pivot_longer(cols=A:D,names_to = 'Band',values_to = 'Count')%>%
  mutate(Indicator="MCI")%>%select(Region,LandCover,Indicator,Band,Count)
#Combine and export ####

NOFLUexport <- do.call(rbind,list(NOxLUexport,DRPLUexport,ECOLILUexport,NH4LUexport,SedLUexport,MCILUexport))

 rm(NOxLUexport,DRPLUexport,ECOLILUexport,NH4LUexport,SedLUexport,MCILUexport)
write.csv(NOFLUexport,paste0('H:/ericg/16666LAWA/LAWA2021/NationalPicture/2021-08-27/',
                             'DownloadDataFiles/NOFResultsLatestByLandUseAndRegion.csv'),row.names=F)
#spaceer ####





#                      Now the time history one ####
#NO3N ####
NOxNOFs = NOFSummaryTable%>%drop_na(Nitrate_Toxicity_Band)%>%filter(!Nitrate_Toxicity_Band%in%c("","NA"))%>%
  select(-starts_with(c('Ecoli','Ammonia'),ignore.case = T))%>%
  dplyr::rename(Band=Nitrate_Toxicity_Band,LandCover=gRecLC)%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)),Year=strFrom(Year,'to'))%>%filter(nY==10)%>%ungroup%>%
  group_by(Region,LandCover,Year)%>%dplyr::summarise(.groups='keep',A=sum(Band=="A"),
                                                     B=sum(Band=="B"),
                                                     C=sum(Band=="C"),
                                                     D=sum(Band=="D"))%>%ungroup%>%
  pivot_longer(cols=A:D,names_to='Band',values_to='Count')%>%
  mutate(Indicator="NO3N")%>%
  dplyr::select(Region,LandCover,Indicator,Year,Band,Count)
#DRP ####
DRPNOFs = NOFSummaryTable%>%drop_na(DRP_Summary_Band)%>%filter(!DRP_Summary_Band%in%c("","NA"))%>%
  select(-starts_with(c('Ecoli','Ammonia','Nitra'),ignore.case = T))%>%
  dplyr::rename(Band=DRP_Summary_Band,LandCover=gRecLC)%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)),Year=strFrom(Year,'to'))%>%filter(nY==10)%>%ungroup%>%
  group_by(Region,LandCover,Year)%>%dplyr::summarise(.groups='keep',A=sum(Band=="A"),
                                                     B=sum(Band=="B"),
                                                     C=sum(Band=="C"),
                                                     D=sum(Band=="D"))%>%ungroup%>%
  pivot_longer(cols=A:D,names_to='Band',values_to='Count')%>%
  mutate(Indicator="DRP")%>%
  dplyr::select(Region,LandCover,Indicator,Year,Band,Count)
#ECOLI ####
ECOLINOFs = NOFSummaryTable%>%drop_na(EcoliSummaryband)%>%filter(!EcoliSummaryband%in%c("","NA"))%>%
  select(-starts_with(c('DRP','Ammonia','Nitra'),ignore.case = T))%>%
  dplyr::rename(Band=EcoliSummaryband,LandCover=gRecLC)%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)),Year=strFrom(Year,'to'))%>%filter(nY==10)%>%ungroup%>%
  group_by(Region,LandCover,Year)%>%dplyr::summarise(.groups='keep',
                                                     A=sum(Band=="A"),
                                                     B=sum(Band=="B"),
                                                     C=sum(Band=="C"),
                                                     D=sum(Band=="D"),
                                                     E=sum(Band=="E"))%>%ungroup%>%
  pivot_longer(cols=A:E,names_to='Band',values_to='Count')%>%
  mutate(Indicator="ECOLI")%>%
  dplyr::select(Region,LandCover,Indicator,Year,Band,Count)
#Sediment ####
SedNOFs = NOFSummaryTable%>%drop_na(SusSedBand)%>%filter(!SusSedBand%in%c("","NA"))%>%
  select(-starts_with(c('Ecoli','DRP','Nitra'),ignore.case = T))%>%
  dplyr::rename(Band=SusSedBand,LandCover=gRecLC)%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)),Year=strFrom(Year,'to'))%>%filter(nY==10)%>%ungroup%>%
  group_by(Region,LandCover,Year)%>%dplyr::summarise(.groups='keep',A=sum(Band=="A"),
                                                     B=sum(Band=="B"),
                                                     C=sum(Band=="C"),
                                                     D=sum(Band=="D"))%>%ungroup%>%
  pivot_longer(cols=A:D,names_to='Band',values_to='Count')%>%
  mutate(Indicator="SusSed")%>%
  dplyr::select(Region,LandCover,Indicator,Year,Band,Count)
#NH4 ####
NH4NOFs = NOFSummaryTable%>%drop_na(Ammonia_Toxicity_Band)%>%filter(!Ammonia_Toxicity_Band%in%c("","NA"))%>%
  select(-starts_with(c('Ecoli','DRP','Nitra'),ignore.case = T))%>%
  dplyr::rename(Band=Ammonia_Toxicity_Band,LandCover=gRecLC)%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)),Year=strFrom(Year,'to'))%>%filter(nY==10)%>%ungroup%>%
  group_by(Region,LandCover,Year)%>%dplyr::summarise(.groups='keep',A=sum(Band=="A"),
                                                     B=sum(Band=="B"),
                                                     C=sum(Band=="C"),
                                                     D=sum(Band=="D"))%>%ungroup%>%
  pivot_longer(cols=A:D,names_to='Band',values_to='Count')%>%
  mutate(Indicator="Ammonia")%>%
  dplyr::select(Region,LandCover,Indicator,Year,Band,Count)
#MCI ####
MCINOFsexp = MCINOF%>%filter(sYear>=2011)%>%group_by(LawaSiteID)%>%filter(!MCIband%in%c("","NA"))%>%drop_na(MCIband)%>%
  dplyr::mutate(nY=length(unique(sYear)))%>%filter(nY==10)%>%ungroup%>%
  dplyr::rename(Band=MCIband,LandCover=gRecLC)%>%droplevels%>%
  group_by(Region,LandCover,sYear)%>%dplyr::summarise(.groups='keep',A=sum(Band=="A"),
                                                      B=sum(Band=="B"),
                                                      C=sum(Band=="C"),
                                                      D=sum(Band=="D"))%>%ungroup%>%
  pivot_longer(cols=A:D,names_to='Band',values_to='Count')%>%
  mutate(Indicator="MCI")%>%
  dplyr::select(Region,LandCover,Indicator,Year=sYear,Band,Count)
#Combine and export ####
NOFHistoryExport <- do.call(rbind,list(NOxNOFs,DRPNOFs,ECOLINOFs,SedNOFs,NH4NOFs,MCINOFsexp))
write.csv(NOFHistoryExport,
          paste0('H:/ericg/16666LAWA/LAWA2021/NationalPicture/2021-08-27/',
                 'DownloadDataFiles/NOFResultsHistoryCompleteSites.csv'),row.names=F)

 #spacer ####
 



#                         NOF results by land use ####
#NOFNO3LU ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFNO3LU_map2019.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),xpd=T,mar=c(5,4,4,2),col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3])
layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
showtext_begin()
test <- NOFlatest%>%dplyr::filter(!Nitrate_Toxicity_Band%in%c("","NA")&!is.na(Nitrate_Toxicity_Band))%>%
  tidyr::drop_na(gRecLC)%>%dplyr::group_by(gRecLC,Nitrate_Toxicity_Band)%>%
  dplyr::summarise(.groups='keep',nc=n())%>%ungroup%>%arrange(gRecLC)%>%
  pivot_wider(values_from = 'nc',names_from = 'gRecLC')%>%arrange(Nitrate_Toxicity_Band)%>%as.matrix
vegTypeCounts = colSums(apply(test[,-1],2,as.numeric),na.rm = T)
testb <- apply(test,2,FUN=function(x)as.numeric(x)/sum(as.numeric(x),na.rm=T))
testb[is.na(testb)] <- 0
testb[,1]=test[,1]
bp <- barplot(as.matrix(testb)[,-1],col=LAWAPalette[c(3,8,7,5)],main="Nitrate toxicity",axes=F,
              ylab="",border =NA,cex.lab=4,cex.main=5,cex.names=4)
axis(side = 2,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],cex.axis=4)
mtext(side=1,line = 2,at = bp,text = paste0(vegTypeCounts,' sites'),cex = 2,col = LAWAPalette[3])
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(NOFlatest%>%filter(!Nitrate_Toxicity_Band%in%c("","NA")&!is.na(Nitrate_Toxicity_Band))%>%tidyr::drop_na(gRecLC),{
  points(Long,Lat,pch=16,cex=0.75,col='black')
  write.csv(cbind(Long,Lat,Nitrate_Toxicity_Band),paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFNO3mapPts.csv"),row.names=F)
}
)
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
testb=rbind(testb,test,c(sum(apply(test[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))),apply(test[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))))
write.csv(testb,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFNO3LU.csv"),row.names=F)
rm(list=ls(pattern='^test'))

#NOFDRPLU ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFDRPLU_map.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),xpd=T,mar=c(5,4,4,2),col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3])
layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
showtext_begin()
test <- NOFlatest%>%dplyr::filter(!DRP_Summary_Band%in%c("","NA")&!is.na(DRP_Summary_Band))%>%
  tidyr::drop_na(gRecLC)%>%dplyr::group_by(gRecLC,DRP_Summary_Band)%>%
  dplyr::summarise(.groups='keep',nc=n())%>%ungroup%>%arrange(gRecLC)%>%
  pivot_wider(values_from = 'nc',names_from = 'gRecLC')%>%arrange(DRP_Summary_Band)%>%as.matrix
vegTypeCounts = colSums(apply(test[,-1],2,as.numeric),na.rm = T)
testb <- apply(test,2,FUN=function(x)as.numeric(x)/sum(as.numeric(x),na.rm=T))
testb[is.na(testb)] <- 0
testb[,1]=test[,1]
bp <- barplot(as.matrix(testb)[,-1],col=LAWAPalette[c(3,8,7,5)],main="DRP Summary",axes=F,
              ylab="",border =NA,cex.lab=4,cex.main=5,cex.names=4)
axis(side = 2,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],cex.axis=4)
mtext(side=1,line = 2,at = bp,text = paste0(vegTypeCounts,' sites'),cex = 2,col = LAWAPalette[3])
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(NOFlatest%>%filter(!DRP_Summary_Band%in%c("","NA")&!is.na(DRP_Summary_Band))%>%tidyr::drop_na(gRecLC),{
  points(Long,Lat,pch=16,cex=0.75,col='black')
  write.csv(cbind(Long,Lat,DRP_Summary_Band),paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFDRPmapPts.csv"),row.names=F)
}
)
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
testb=rbind(testb,test,c(sum(apply(test[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))),apply(test[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))))
write.csv(testb,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFDRPLU.csv"),row.names=F)
rm(list=ls(pattern='^test'))


#NOFECOLILU ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFEcoliLU_map.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),xpd=T,mar=c(5,4,4,2),col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3])
layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
showtext_auto()

test <- NOFlatest%>%dplyr::filter(!EcoliSummaryband%in%c("","NA")&!is.na(EcoliSummaryband))%>%
  tidyr::drop_na(gRecLC)%>%dplyr::group_by(gRecLC,EcoliSummaryband)%>%
  dplyr::summarise(.groups='keep',nc=n())%>%ungroup%>%arrange(gRecLC)%>%pivot_wider(values_from = 'nc',names_from = 'gRecLC')%>%arrange(EcoliSummaryband)%>%as.matrix
vegTypeCounts = colSums(apply(test[,-1],2,as.numeric),na.rm = T)
testb <- apply(test,2,FUN=function(x)as.numeric(x)/sum(as.numeric(x),na.rm=T))
testb[is.na(testb)] <- 0
testb[,1]=test[,1]
bp <- barplot(as.matrix(testb)[,-1],col=LAWAPalette[c(3,8,7,6,5)],main=expression(italic(E.~coli)),axes=F,
              ylab="",border = NA,cex.lab=4,cex.main=5,cex.names=4)
axis(side = 2,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],cex.axis=4)
mtext(side=1,line = 2,at = bp,text = paste0(vegTypeCounts,' sites'),cex = 2,col = LAWAPalette[3])
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(NOFlatest%>%filter(!EcoliSummaryband%in%c("","NA")&!is.na(EcoliSummaryband))%>%tidyr::drop_na(gRecLC),{
  points(Long,Lat,pch=16,cex=0.75,col='black')
  write.csv(cbind(Long,Lat,EcoliSummaryband),paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFEcolimapPts.csv"),row.names=F)
})
if(names(dev.cur())=='tiff'){dev.off()}
testb=rbind(testb,test,c(sum(apply(test[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))),apply(test[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))))
write.csv(testb,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFEcoliLU.csv"),row.names=F)
rm(list=ls(pattern='^test'))

#NOFSedLU ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFSedLU_map.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),xpd=T,mar=c(5,4,4,2),col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3])
layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
showtext_auto()

test <- NOFlatest%>%dplyr::filter(!SusSedBand%in%c("","NA")&!is.na(SusSedBand))%>%
  tidyr::drop_na(gRecLC)%>%dplyr::group_by(gRecLC,SusSedBand)%>%
  dplyr::summarise(.groups='keep',nc=n())%>%ungroup%>%arrange(gRecLC)%>%pivot_wider(values_from = 'nc',names_from = 'gRecLC')%>%arrange(SusSedBand)%>%as.matrix
vegTypeCounts = colSums(apply(test[,-1],2,as.numeric),na.rm = T)
testb <- apply(test,2,FUN=function(x)as.numeric(x)/sum(as.numeric(x),na.rm=T))
testb[is.na(testb)] <- 0
testb[,1]=test[,1]
bp <- barplot(as.matrix(testb)[,-1],col=LAWAPalette[c(3,8,7,6,5)],main="Suspended sediment",axes=F,
              ylab="",border = NA,cex.lab=4,cex.main=5,cex.names=4)
axis(side = 2,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],cex.axis=4)
mtext(side=1,line = 2,at = bp,text = paste0(vegTypeCounts,' sites'),cex = 2,col = LAWAPalette[3])
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(NOFlatest%>%filter(!SusSedBand%in%c("","NA")&!is.na(SusSedBand))%>%tidyr::drop_na(gRecLC),{
  points(Long,Lat,pch=16,cex=0.75,col='black')
  write.csv(cbind(Long,Lat,SusSedBand),paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFSedmapPts.csv"),row.names=F)
})
if(names(dev.cur())=='tiff'){dev.off()}
testb=rbind(testb,test,c(sum(apply(test[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))),apply(test[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))))
write.csv(testb,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFSedLU.csv"),row.names=F)
rm(list=ls(pattern='^test'))



#NOFNH4LU ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFNH4LU_map.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),xpd=T,mar=c(5,4,4,2),col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3])
layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
showtext_begin()

test <- NOFlatest%>%dplyr::filter(!Ammonia_Toxicity_Band%in%c("","NA")&!is.na(Ammonia_Toxicity_Band))%>%
  tidyr::drop_na(gRecLC)%>%dplyr::group_by(gRecLC,Ammonia_Toxicity_Band)%>%
  dplyr::summarise(.groups='keep',nc=n())%>%ungroup%>%arrange(gRecLC)%>%
  pivot_wider(values_from = 'nc',names_from = 'gRecLC')%>%arrange(Ammonia_Toxicity_Band)%>%as.matrix
vegTypeCounts = colSums(apply(test[,-1],2,as.numeric),na.rm = T)
testb <- apply(test,2,FUN=function(x)as.numeric(x)/sum(as.numeric(x),na.rm=T))
testb[is.na(testb)] <- 0
testb[,1]=test[,1]
bp <- barplot(as.matrix(testb)[,-1],col=LAWAPalette[c(3,8,7,5)],main=expression(NH[4]),axes=F,
              ylab="",border = NA,cex.lab=4,cex.main=5,cex.names=4)
axis(side = 2,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],cex.axis=4)
mtext(side=1,line = 2,at = bp,text = paste0(vegTypeCounts,' sites'),cex = 2,col = LAWAPalette[3])
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(NOFlatest%>%filter(!Ammonia_Toxicity_Band%in%c("","NA")&!is.na(Ammonia_Toxicity_Band))%>%tidyr::drop_na(gRecLC),{
  points(Long,Lat,pch=16,cex=0.75,col='black')
  write.csv(cbind(Long,Lat,Ammonia_Toxicity_Band),paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFNH4mapPts.csv"),row.names=F)
})
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
testb=rbind(testb,test,c(sum(apply(test[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))),apply(test[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))))
write.csv(testb,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFNH4LU.csv"),row.names=F)
rm(list=ls(pattern='^test'))



#NOFMCILU ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFMCILU_map.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),xpd=T,mar=c(5,4,4,2),col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3])
layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
showtext_auto()

test <- MCINOF%>%dplyr::filter(sYear=="2020")%>%
  tidyr::drop_na(gRecLC)%>%dplyr::group_by(gRecLC,MCIband)%>%
  dplyr::summarise(.groups='keep',nc=n())%>%ungroup%>%arrange(gRecLC)%>%
  pivot_wider(values_from = 'nc',names_from = 'gRecLC')%>%arrange(MCIband)%>%as.matrix
vegTypeCounts = colSums(apply(test[,-1],2,as.numeric),na.rm = T)
testb <- apply(test,2,FUN=function(x)as.numeric(x)/sum(as.numeric(x),na.rm=T))
testb[is.na(testb)] <- 0
testb[,1]=test[,1]
bp <- barplot(as.matrix(testb)[,-1],col=LAWAPalette[c(3,8,7,5)],main='MCI',axes=F,
              ylab="",border = NA,cex.lab=4,cex.main=5,cex.names=4)
axis(side = 2,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],cex.axis=4)
mtext(side=1,line = 2,at = bp,text = paste0(vegTypeCounts,' sites'),cex = 2,col = LAWAPalette[3])
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(MCINOF%>%filter(sYear=="2020")%>%tidyr::drop_na(gRecLC),{
  points(Long,Lat,pch=16,cex=0.75,col='black')
  write.csv(cbind(Long,Lat,MCIband),paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFMCImapPts.csv"),row.names=F)
})
if(names(dev.cur())=='tiff'){dev.off()}
testb=rbind(testb,test,c(sum(apply(test[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))),apply(test[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))))
write.csv(testb,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFMCILU.csv"),row.names=F)
rm(list=ls(pattern='^test'))

#spacer ####





#Changes in NOF with complete sites ####
xlabpos=(1:10)*0.12-0.07
# NOFTrendNO3Tox ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendNO3Sum.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
NO3NOFs = NOFSummaryTable%>%drop_na(Nitrate_Toxicity_Band)%>%filter(!Nitrate_Toxicity_Band%in%c("","NA")&!is.na(Nitrate_Toxicity_Band))%>%
  select(-starts_with(c('Ecoli','Ammonia'),ignore.case = T))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSites=length(unique(NO3NOFs$LawaSiteID))

layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(strFrom(NO3NOFs$Year,'to')),factor(NO3NOFs$Nitrate_Toxicity_Band),ylab='',xlab='',
        col=LAWAPalette[c(5,7,8,3)],tol.ylab=0,main="Nitrate toxicity",
        axes=F,border = NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(NO3NOFs%>%filter(Year=='2016to2020'),
     points(Long,Lat,pch=16,cex=0.25,col='black')
)
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSites,"sites"))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFTrendNO3.csv"),row.names=T)
rm(NO3NOFs,nSites,iT)





# NOFTrendDRPSum ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendDRPSum.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
DRPNOFs = NOFSummaryTable%>%drop_na(DRP_Summary_Band)%>%filter(!DRP_Summary_Band%in%c("","NA")&!is.na(DRP_Summary_Band))%>%
  select(-starts_with(c('Ecoli','Ammonia'),ignore.case = T))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSites=length(unique(DRPNOFs$LawaSiteID))

layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(strFrom(DRPNOFs$Year,'to')),factor(DRPNOFs$DRP_Summary_Band),ylab='',xlab='',
        col=LAWAPalette[c(5,7,8,3)],tol.ylab=0,main="DRP Summary",
        axes=F,border = NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(DRPNOFs%>%filter(Year=='2016to2020'),
     points(Long,Lat,pch=16,cex=0.25,col='black')
)
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSites,"sites"))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFTrendDRP.csv"),row.names=T)
rm(DRPNOFs,nSites,iT)





# NOFTrendEcoliSummary ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendEcoliSummary.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
ecoliNOFs = NOFSummaryTable%>%filter(!EcoliSummaryband%in%c('','NA'))%>%drop_na(EcoliSummaryband)%>%
  select(-starts_with(c('ammon','nitrate'),ignore.case=T))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSites=length(unique(ecoliNOFs$LawaSiteID))

layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(strFrom(ecoliNOFs$Year,'to')),
        factor(levels=c("A","B","C","D","E"),ecoliNOFs$EcoliSummaryband),ylab='',xlab='',
        col=LAWAPalette[c(5,6,7,8,3)],tol.ylab=0,main=expression(italic(E.~coli)~Summary),
        axes=F,border = NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
points(ecoliNOFs$Long,ecoliNOFs$Lat,pch=16,cex=0.25,col='black')
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSites,"sites",""))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFTrendEColi.csv"),row.names=T)
rm(ecoliNOFs,nSites,iT)



# NOFTrendSediment  ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendSed.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
sedNOFs = NOFSummaryTable%>%filter(!SusSedBand%in%c('',"NA"))%>%drop_na(SusSedBand)%>%
  select(-starts_with(c('ecoli','nitrate'),ignore.case=T))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSites=length(unique(sedNOFs$LawaSiteID))

layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(strFrom(sedNOFs$Year,'to')),factor(sedNOFs$SusSedBand),ylab='',xlab='',
        col=LAWAPalette[c(5,7,8,3)],tol.ylab=0,main="Suspended sediment",
        axes=F,border = NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
points(sedNOFs$Long,sedNOFs$Lat,pch=16,cex=0.25,col='black')
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSites,"sites"))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFTrendSed.csv"),row.names=T)
rm(sedNOFs,nSites,iT)

# NOFTrendAmmoniacalTox  ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendAmmoniacalTox.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
ammonNOFs = NOFSummaryTable%>%filter(!Ammonia_Toxicity_Band%in%c('','NA'))%>%drop_na(Ammonia_Toxicity_Band)%>%
  select(-starts_with(c('ecoli','nitrate'),ignore.case=T))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSites=length(unique(ammonNOFs$LawaSiteID))

layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(strFrom(ammonNOFs$Year,'to')),factor(ammonNOFs$Ammonia_Toxicity_Band),ylab='',xlab='',
        col=LAWAPalette[c(5,7,8,3)],tol.ylab=0,main="Ammonia toxicity",
        axes=F,border = NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
points(ammonNOFs$Long,ammonNOFs$Lat,pch=16,cex=0.25,col='black')
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSites,"sites"))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFTrendNH4.csv"),row.names=T)
rm(ammonNOFs,nSites,iT)

# NOFTrendMCI  ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendMCI.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
MCINOFs = MCINOF%>%filter(sYear>=2011)%>%group_by(LawaSiteID)%>%filter(MCIband!='NA')%>%drop_na(MCIband)%>%
  dplyr::mutate(nY=length(unique(sYear)))%>%filter(nY==10)%>%ungroup%>%select(-nY)%>%droplevels
nSites=length(unique(MCINOFs$LawaSiteID))

layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(MCINOFs$sYear),factor(MCINOFs$MCIband),ylab='',xlab='',
        col=LAWAPalette[c(5,7,8,3)],tol.ylab=0,main="MCI",
        axes=F,border = NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
points(MCINOFs$Long,MCINOFs$Lat,pch=16,cex=0.25,col='black')
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSites,"sites"))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFTrendMCI.csv"),row.names=T)
rm(MCINOFs,nSites,iT)




#spacer ####


#Copy fioles to sharepoint ####
file.copy(from = dir(path=paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d")),
                     pattern = "tif$",full.names = T),
          to="c:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/SWQNationalPicture/Plots/",
          overwrite = T,copy.date = T)


#Spacer ####


#Regional trend plots ####
xlabpos=(1:10)*0.12-0.07

for(region in unique(NOFSummaryTable$Region)){
  #prep fig file ####
  tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrend",pseudo.titlecase(region),".tif"),
       width = 15,height=8,units='in',res=600,compression='lzw',type='cairo')
  showtext_begin()
  layout(matrix(c(1,1,3,04,04,06,13,13,15,
                  1,1,2,04,04,05,13,13,14,
                  1,1,2,04,04,05,13,13,14,
                  7,7,9,10,10,12,16,16,18,
                  7,7,8,10,10,11,16,16,17,
                  7,7,8,10,10,11,16,16,17),nrow=6,byrow=T))
  
  showtext_begin()
  
  # NO3 ####
  NO3NOFs = NOFSummaryTable%>%drop_na(Nitrate_Toxicity_Band)%>%
    filter(!Nitrate_Toxicity_Band%in%c('','NA')&Region==region)%>%
    group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%
    filter(nY==10)%>%ungroup%>%select(-nY)
  nSites=length(unique(NO3NOFs$LawaSiteID))
  if(nSites>0){
    par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(3,2,2,0.5),mgp=c(1.75,0.5,0),
        cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
    iT=plot(factor(strFrom(NO3NOFs$Year,'to')),
            factor(NO3NOFs$Nitrate_Toxicity_Band,levels=c("A","B","C","D")),ylab='',xlab='',
            col=LAWAPalette[c(5,7,8,3)],tol.ylab=0,main="Nitrate toxicity",
            axes=F,border = NA)
    axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
    axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
    mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
    par(mar=c(1,0,0,0))
    plot(nzmap,col=LAWAPalette[1],border = NA,xlim=range(NO3NOFs$Long,na.rm=T),ylim=range(NO3NOFs$Lat,na.rm=T))
    with(NO3NOFs%>%filter(Year=='2016to2020'),
         points(Long,Lat,pch=16,cex=0.5,col=LAWAPalette[c(3,8,7,5)][as.numeric(Nitrate_Toxicity_Band)])
    )
    plot.new()
    plot.window(xlim=c(0,10),ylim=c(0,10))
    par(xpd=T)
    plotrix::addtable2plot(0,-2,table = iT,display.colnames = T,display.rownames = T,vlines=T,xpad=1,ypad=0.5,cex=1.25)
    par(xpd=F)
    showtext_end()
  }else{    
    plot.new()
    plot.new()
    plot.new()
  }
  rm(NO3NOFs,nSites,iT)
  
  # DRP ####
  DRPNOFs = NOFSummaryTable%>%drop_na(DRP_Summary_Band)%>%
    filter(!DRP_Summary_Band%in%c('','NA')&Region==region)%>%
    group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%
    filter(nY==10)%>%ungroup%>%select(-nY)
  nSites=length(unique(DRPNOFs$LawaSiteID))
  if(nSites>0){
    par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(3,2,2,0.5),mgp=c(1.75,0.5,0),
        cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
    iT=plot(factor(strFrom(DRPNOFs$Year,'to')),
            factor(DRPNOFs$DRP_Summary_Band,levels=c("A","B","C","D")),ylab='',xlab='',
            col=LAWAPalette[c(5,7,8,3)],tol.ylab=0,main="DRP Summary",
            axes=F,border = NA)
    axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
    axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
    mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
    par(mar=c(1,0,0,0))
    plot(nzmap,col=LAWAPalette[1],border = NA,xlim=range(DRPNOFs$Long,na.rm=T),ylim=range(DRPNOFs$Lat,na.rm=T))
    with(DRPNOFs%>%filter(Year=='2016to2020'),
         points(Long,Lat,pch=16,cex=0.5,col=LAWAPalette[c(3,8,7,5)][as.numeric(DRP_Summary_Band)])
    )
    plot.new()
    plot.window(xlim=c(0,10),ylim=c(0,10))
    par(xpd=T)
    plotrix::addtable2plot(0,-2,table = iT,display.colnames = T,display.rownames = T,vlines=T,xpad=1,ypad=0.5,cex=1.25)
    par(xpd=F)
    showtext_end()
  }else{    
    plot.new()
    plot.new()
    plot.new()
  }
  rm(DRPNOFs,nSites,iT)
  
  # Ecoli #### 
  showtext_begin()
  ecoliNOFs = NOFSummaryTable%>%drop_na(EcoliSummaryband)%>%
    filter(!EcoliSummaryband%in%c('','NA')&Region==region)%>%
    group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%
    filter(nY==10)%>%ungroup%>%select(-nY)
  nSites=length(unique(ecoliNOFs$LawaSiteID))
  if(nSites>0){
    par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(3,2,2,0.5),mgp=c(1.75,0.5,0),
        cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
    iT=plot(factor(strFrom(ecoliNOFs$Year,'to')),
            factor(ecoliNOFs$EcoliSummaryband,levels=c("A","B","C","D","E")),ylab='',xlab='',
            col=LAWAPalette[c(5,6,7,8,3)],tol.ylab=0,main=expression(italic(E.~coli)~Summary),
            axes=F,border = NA)
    axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
    axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
    mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
    par(mar=c(1,0,0,0))
    plot(nzmap,col=LAWAPalette[1],border = NA,xlim=range(ecoliNOFs$Long,na.rm=T),ylim=range(ecoliNOFs$Lat,na.rm=T))
    with(ecoliNOFs%>%filter(Year=='2016to2020'),
         points(Long,Lat,pch=16,cex=0.5,col=LAWAPalette[c(3,8,7,6,5)][as.numeric(EcoliSummaryband)])
    )
    plot.new()
    plot.window(xlim=c(0,10),ylim=c(0,10))
    par(xpd=T)
    plotrix::addtable2plot(0,-2,table = iT,display.colnames = T,display.rownames = T,vlines=T,xpad=1,ypad=0.5,cex=1.25)
    par(xpd=F)
    showtext_end()
  }else{
    plot.new()
    plot.new()
    plot.new()
  }
  rm(ecoliNOFs,nSites,iT)

  # SED ####
  SEDNOFs = NOFSummaryTable%>%drop_na(SusSedBand)%>%
    filter(!SusSedBand%in%c('','NA')&Region==region)%>%
    group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%
    filter(nY==10)%>%ungroup%>%select(-nY)
  nSites=length(unique(SEDNOFs$LawaSiteID))
  if(nSites>0){
    par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(3,2,2,0.5),mgp=c(1.75,0.5,0),
        cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
    iT=plot(factor(strFrom(SEDNOFs$Year,'to')),
            factor(SEDNOFs$SusSedBand,levels=c("A","B","C","D")),ylab='',xlab='',
            col=LAWAPalette[c(5,7,8,3)],tol.ylab=0,main="Suspended Sediment",
            axes=F,border = NA)
    axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
    axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
    mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
    par(mar=c(1,0,0,0))
    plot(nzmap,col=LAWAPalette[1],border = NA,xlim=range(SEDNOFs$Long,na.rm=T),ylim=range(SEDNOFs$Lat,na.rm=T))
    with(SEDNOFs%>%filter(Year=='2016to2020'),
         points(Long,Lat,pch=16,cex=0.5,col=LAWAPalette[c(3,8,7,5)][as.numeric(SusSedBand)])
    )
    plot.new()
    plot.window(xlim=c(0,10),ylim=c(0,10))
    par(xpd=T)
    plotrix::addtable2plot(0,-2,table = iT,display.colnames = T,display.rownames = T,vlines=T,xpad=1,ypad=0.5,cex=1.25)
    par(xpd=F)
    showtext_end()
  }else{    
    plot.new()
    plot.new()
    plot.new()
  }
  rm(SEDNOFs,nSites,iT)
  
  # AmmoniacalTox  ####
  ammonNOFs = NOFSummaryTable%>%drop_na(Ammonia_Toxicity_Band)%>%
    filter(!Ammonia_Toxicity_Band%in%c('','NA')&Region==region)%>%
    group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%
    filter(nY==10)%>%ungroup%>%select(-nY)
  nSites=length(unique(ammonNOFs$LawaSiteID))
  if(nSites>0){
    par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(3,2,2,0.5),mgp=c(1.75,0.5,0),
        cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
    iT=plot(factor(strFrom(ammonNOFs$Year,'to')),
            factor(ammonNOFs$Ammonia_Toxicity_Band,levels=c("A","B","C","D")),ylab='',xlab='',
            col=LAWAPalette[c(5,7,8,3)],tol.ylab=0,main="Ammonia toxicity",
            axes=F,border = NA)
    axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
    axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
    mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
    par(mar=c(1,0,0,0))
    plot(nzmap,col=LAWAPalette[1],border = NA,xlim=range(ammonNOFs$Long,na.rm=T),ylim=range(ammonNOFs$Lat,na.rm=T))
    with(ammonNOFs%>%filter(Year=='2016to2020'),
         points(Long,Lat,pch=16,cex=0.5,col=LAWAPalette[c(3,8,7,5)][as.numeric(Ammonia_Toxicity_Band)])
    )
    plot.new()
    plot.window(xlim=c(0,10),ylim=c(0,10))
    par(xpd=T)
    plotrix::addtable2plot(0,-2,table = iT,display.colnames = T,display.rownames = T,vlines=T,xpad=1,ypad=0.5,cex=1.25)
    par(xpd=F)
    showtext_end()
  }else{    
    plot.new()
    plot.new()
    plot.new()
  }
  rm(ammonNOFs,nSites,iT)
  
  # MCI  ####
  showtext_begin()
  MCINOFs = MCINOF%>%drop_na(MCIband)%>%
    filter(!MCIband%in%c('','NA')&Region==region)%>%
    filter(sYear>=2011)%>%
    group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(sYear)))%>%
    filter(nY==10)%>%ungroup%>%select(-nY)
  nSites=length(unique(MCINOFs$LawaSiteID))
  if(nSites>0& !all(is.na(MCINOFs$Long))){
    par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(3,2,2,0.5),mgp=c(1.75,0.5,0),
        cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
    iT=plot(factor(MCINOFs$sYear),
            factor(MCINOFs$MCIband,levels=c("A","B","C","D")),ylab='',xlab='',
            col=LAWAPalette[c(5,7,8,3)],tol.ylab=0,main="MCI",
            axes=F,border = NA)
    axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
    axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
    mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
    par(mar=c(1,0,0,0))
    plot(nzmap,col=LAWAPalette[1],border = NA,xlim=range(MCINOFs$Long,na.rm=T),ylim=range(MCINOFs$Lat,na.rm=T))
    with(MCINOFs%>%filter(sYear==2020),
         points(Long,Lat,pch=16,cex=0.5,col=LAWAPalette[c(3,8,7,5)][as.numeric(MCIband)])
    )
    plot.new()
    plot.window(xlim=c(0,10),ylim=c(0,10))
    par(xpd=T)
    plotrix::addtable2plot(0,-2,table = iT,display.colnames = T,
                           display.rownames = T,vlines=T,xpad=1,ypad=0.5,cex=1.25)
    par(xpd=F)
    showtext_end()
  }else{
    plot.new()
    plot.new()
    plot.new()
  }
  rm(MCINOFs,nSites,iT)
  #Close plot ####
  if(names(dev.cur())=='tiff'){dev.off()}
}

#Copy fioles to sharepoint ####
file.copy(from = dir(path=tail(dir(path="h:/ericg/16666LAWA/LAWA2021/NationalPicture/",
                                   pattern='^2021',full.names = T),1),
                     pattern = "tif$",full.names = T),
          to="c:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/SWQNationalPicture/Plots/",
          overwrite = T,copy.date = T)



# #Copy fioles to sharepoint ####
file.copy(from = dir(path=tail(dir(path="h:/ericg/16666LAWA/LAWA2021/NationalPicture/",
                                   pattern='^2021',full.names = T),1),
                     pattern = "csv$",full.names = T,recursive=T),
          to="c:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/SWQNationalPicture/DownloadDataFiles/",
          overwrite = T,copy.date = T)


#Spacer ####





# NOFTrendNitrateTox ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendNitrateTox.tif"),
     width = 14,height=8,units='in',res=300,compression='lzw',type='cairo')
nitrateNOFs = NOFSummaryTable%>%drop_na(Nitrate_Toxicity_Band)%>%filter(!Nitrate_Toxicity_Band%in%c('','NA'))%>%
  select(-starts_with(c('Ecoli','Ammonia'),ignore.case = T))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nitrateNOFs$Nitrate_Toxicity_Band=factor(as.character(nitrateNOFs$Nitrate_Toxicity_Band),levels=(c("A","B","C","D")),labels=(c("A","B","C","D")))
nSites=length(unique(nitrateNOFs$LawaSiteID))

par(mfrow=c(1,2),cex=2,cex.main=1.5,col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3])
plot(factor(strFrom(nitrateNOFs$Year,'to')),factor(nitrateNOFs$Nitrate_Toxicity_Band),ylab='',xlab='',
     col=LAWAPalette[c(5,6,7,2)],tol.ylab=0,main="Nitrate toxicity\n5yr NOF grade",
     axes=F,border=LAWAPalette[4])->iT
axis(2,at=seq(0,1,by=0.2),labels = seq(0,1,by=0.2)*100,las=2,col=LAWAPalette[3])
axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3])
# labelAreas(iT)
mtext(side = 1,paste0("Showing only the ",nSites," complete sites."),line = 2,cex=2,col=LAWAPalette[3])
plot(nzmap)
with(nitrateNOFs%>%filter(Year=='2016to2020'),
     points(Long,Lat,pch=16,cex=0.25,col=LAWAPalette[1])
)
par(xpd=T)
plotrix::addtable2plot(156,-37,table = iT,display.colnames = T,display.rownames = T,vlines=T,xpad=1,ypad=0.5)
if(names(dev.cur())=='tiff'){dev.off()}
rm(nitrateNOFs,nSites,iT)

# Retired already as of at the start of the 2021 national pciture stage ####
if(0){
# tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/NOFDRPLU.tif"),
#      width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
# par(mfrow=c(2,1),xpd=T)
# with(NOFlatest%>%filter(!DRP_Summary_Band%in%c('','NA')),{
#   plot(factor(rawRecLandcover),factor(DRP_Summary_Band),off=0.5,
#        col=LAWAPalette[c(5,6,7,2)],
#        tol.ylab=0,xlab='LandCover from REC',ylab="NOF band",main="DRP Summary")->InvTab
#   labelAreas(InvTab)
#   plot(factor(gRecLC),factor(DRP_Summary_Band),off=0.5,
#        col=LAWAPalette[c(5,6,7,2)],
#        tol.ylab=0,xlab='LandCover from REC',ylab="NOF band",main="DRP Summary")->InvTab
#   labelAreas(InvTab)})
# if(names(dev.cur())=='tiff'){dev.off()}

# tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/NOFEcoliLU.tif"),
#      width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
# par(mfrow=c(2,1),xpd=T)
# with(NOFlatest%>%filter(!EcoliSummaryband%in%c('','NA')),{
# plot(factor(rawRecLandcover),factor(EcoliSummaryband),off=0.5,
#      col=LAWAPalette[c(5,6,7,2)],
#      tol.ylab=0,xlab='LandCover from REC',ylab="NOF band",main="E coli")->InvTab
# labelAreas(InvTab)
# plot(factor(gRecLC),factor(EcoliSummaryband),off=0.5,
#      col=LAWAPalette[c(5,6,7,2)],
#      tol.ylab=0,xlab='LandCover from REC',ylab="NOF band",main="E coli")->InvTab
# labelAreas(InvTab)})
# if(names(dev.cur())=='tiff'){dev.off()}
#
# tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/NOFNH4LU.tif"),
#      width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
# par(mfrow=c(2,1),xpd=T)
# with(NOFlatest%>%dplyr::filter(!Ammonia_Toxicity_Band%in%c('','NA')),{
#      plot(factor(rawRecLandcover),factor(Ammonia_Toxicity_Band),off=0.5,
#           col=c("#dd1111FF", "#cc7766FF", "#55bb66FF", "#008800FF","#AAAAAAFF"),
#           tol.ylab=0,xlab='LandCover from REC',ylab="NOF band",main="NH4 tox")->>InvTab
# labelAreas(InvTab)
# plot(factor(gRecLC),factor(Ammonia_Toxicity_Band),off=0.5,
#      col=c("#dd1111FF", "#cc7766FF", "#55bb66FF", "#008800FF","#AAAAAAFF"),
#      tol.ylab=0,xlab='LandCover from REC',ylab="NOF band",main="NH4 tox")->>InvTab
# labelAreas(InvTab)})
# if(names(dev.cur())=='tiff'){dev.off()}

# tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/NOFMCILU.tif"),
#      width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
# par(mfrow=c(2,1),xpd=T)
# with(MCINOF%>%filter(sYear=="2020"),{
# plot(factor(rawRecLandcover),factor(band),off=0.5,
#      col=LAWAPalette[c(5,6,7,2)],
#      tol.ylab=0,xlab='LandCover from REC',ylab="NOF band",main="MCI")->InvTab
# labelAreas(InvTab)
# plot(factor(gRecLC),factor(band),off=0.5,
#      col=LAWAPalette[c(5,6,7,2)],
#      tol.ylab=0,xlab='LandCover from REC',ylab="NOF band",main="MCI")->InvTab
# labelAreas(InvTab)
# })
# if(names(dev.cur())=='tiff'){dev.off()}

}





#Best + Worst trend plots ####
if(0){
  wqdataFileName=tail(dir(path = "H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data",pattern = "AllCouncils.csv",recursive = T,full.names = T),1)
  cat(wqdataFileName)
  wqdata=read_csv(wqdataFileName,guess_max = 100000)%>%as.data.frame
  rm(wqdataFileName)
  macroData=read_csv(tail(dir(path="H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data",pattern="MacrosWithMetadata",
  recursive = T,full.names = T),1))
  macroData$LawaSiteID = tolower(macroData$LawaSiteID)
  
  wqdata$myDate <- as.Date(lubridate::dmy(as.character(wqdata$Date)),"%d-%b-%Y")
  wqdata$myDate[wqdata$myDate<as.Date('2000-01-01')] <- as.Date(lubridate::dmy(as.character(wqdata$Date[wqdata$myDate<as.Date('2000-01-01')])),
                                                                "%d-%b-%y")
  wqdata <- GetMoreDateInfo(wqdata)
  wqdata$monYear = format(wqdata$myDate,"%b-%Y")
  wqdata$Season=wqdata$Month
  wqdata$Year = lubridate::year(lubridate::dmy(wqdata$Date))
  
  SeasonString=sort(unique(wqdata$Season))
  savePlott=T
  usites=unique(combTrend$LawaSiteID)
  uMeasures=unique(combTrend$Measurement)
  for(uparam in seq_along(uMeasures)){
    if(uMeasures[uparam]=='MCI'){
      subwq = macroData[macroData$Measurement=="MCI",]%>%as.data.frame
      subTrend=combTrend[which((combTrend$Measurement==uMeasures[uparam])),]
      subwq$Season=subwq$sYear
      subwq$Censored=FALSE
      subwq$CenType='not'???
        subwq$myDate = as.Date(lubridate::dmy(as.character(subwq$Date)),'%d-%b-%Y')
      subwq <- GetMoreDateInfo(subwq)
    }else{
      subwq=wqdata[wqdata$Measurement==uMeasures[uparam],]
      subTrend=trendTable10[which((trendTable10$Measurement==uMeasures[uparam]&trendTable10$frequency=='monthly')),]
    }
    worstDeg <- which.max(subTrend$MKProbability) 
    bestImp <- which.min(subTrend$MKProbability)
    leastKnown <- which.min(abs(subTrend$MKProbability-0.5))
    if(savePlott){
      tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/BestWorst",uMeasures[uparam],".tif"),
           width = 12,height=8,units='in',res=300,compression='lzw',type='cairo')
    }else{
      windows()
    }
    par(mfrow=c(3,1),mar=c(2,4,1,2))
    if(uMeasures[uparam]=='MCI'){
      logax=''
      theseDeg <- which(subwq$LawaSiteID==subTrend$LawaSiteID[worstDeg] )
      theseInd <- which(subwq$LawaSiteID==subTrend$LawaSiteID[leastKnown])
      theseImp <- which(subwq$LawaSiteID==subTrend$LawaSiteID[bestImp] )
    }else{
      logax='y'
      theseDeg <- which(subwq$LawaSiteID==subTrend$LawaSiteID[worstDeg] & subwq$Year>=startYear10)
      theseInd <- which(subwq$LawaSiteID==subTrend$LawaSiteID[leastKnown] & subwq$Year>=startYear10)
      theseImp <- which(subwq$LawaSiteID==subTrend$LawaSiteID[bestImp] & subwq$Year>=startYear10)
    }
    if(length(theseDeg)>0){
      st <- SeasonalityTest(x = subwq[theseDeg,],
                            main=uMeasures[uparam],ValuesToUse = 'Value',do.plot =F)
      if(!is.na(st$pvalue)&&st$pvalue<0.05){
        SeasonalKendall(x = subwq[theseDeg,],ValuesToUse = 'Value',doPlot = F)
        SeasonalSenSlope(HiCensor=T,x = subwq[theseDeg,],ValuesToUse = 'Value',ValuesToUseforMedian = 'Value',doPlot = T,
                         mymain = subTrend$LawaSiteID[worstDeg],logax=logax)
      }else{
        MannKendall(HiCensor=T,x = subwq[theseDeg,],ValuesToUse = 'Value',doPlot=F)
        SenSlope(HiCensor=T,x = subwq[theseDeg,],ValuesToUse = 'Value',ValuesToUseforMedian = 'Value',
                 doPlot = T,mymain = subTrend$LawaSiteID[worstDeg],logax=logax)
      }
    }
    if(length(theseInd)>0){
      st <- SeasonalityTest(x = subwq[theseInd,],main=uMeasures[uparam],ValuesToUse = 'Value',do.plot =F)
      if(!is.na(st$pvalue)&&st$pvalue<0.05){
        SeasonalKendall(HiCensor=T,x = subwq[theseInd,],ValuesToUse = 'Value',doPlot = F)
        SeasonalSenSlope(HiCensor=T,x = subwq[theseInd,],ValuesToUse = 'Value',ValuesToUseforMedian = 'Value',
                         doPlot = T,mymain = subTrend$LawaSiteID[leastKnown],logax=logax)
      }else{
        MannKendall(HiCensor=T,x = subwq[theseInd,],ValuesToUse = 'Value',doPlot=F)
        SenSlope(HiCensor=T,x = subwq[theseInd,],ValuesToUse = 'Value',ValuesToUseforMedian = 'Value',
                 doPlot = T,mymain = subTrend$LawaSiteID[leastKnown],logax=logax)
      }
    }
    if(length(theseImp)>0){
      st <- SeasonalityTest(x = subwq[theseImp,],main=uMeasures[uparam],ValuesToUse = 'Value',do.plot =F)
      if(!is.na(st$pvalue)&&st$pvalue<0.05){
        SeasonalKendall(HiCensor=T,x = subwq[theseImp,],ValuesToUse = 'Value',doPlot = F)
        if(is.na(SeasonalSenSlope(HiCensor=T,x = subwq[theseImp,],ValuesToUse = 'Value',ValuesToUseforMedian = 'Value',
                                  doPlot = T,mymain = subTrend$LawaSiteID[bestImp],logax=logax)$Sen_Probability)){
          SenSlope(HiCensor=T,x = subwq[theseImp,],ValuesToUse = 'Value',ValuesToUseforMedian = 'Value',
                   doPlot = T,mymain = subTrend$LawaSiteID[bestImp],logax=logax)
        }
      }else{
        MannKendall(HiCensor=T,x = subwq[theseImp,],ValuesToUse = 'Value',doPlot=F)
        SenSlope(HiCensor=T,x = subwq[theseImp,],ValuesToUse = 'Value',ValuesToUseforMedian = 'Value',
                 doPlot = T,mymain = subTrend$LawaSiteID[bestImp],logax=logax)
      }
    }
    if(names(dev.cur())=='tiff'){dev.off()}
    rm(theseDeg,theseImp,theseInd)
    rm(worstDeg,bestImp,leastKnown)
  }
  rm(uparam,uMeasures)
}



