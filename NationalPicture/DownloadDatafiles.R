rm(list=ls())
library(tidyverse)
source('H:/ericg/16666LAWA/LAWA2021/scripts/LAWAFunctions.R')
EndYear <- lubridate::isoyear(Sys.Date())-1
startYear15 <- EndYear - 15+1
StartYear10 <- EndYear - 10+1
StartYear5 <- EndYear -   5+1

writeOut=TRUE


# #####################################################################################
# LAKES: ####
lakeSiteTable=loadLatestSiteTableLakes()
#   Lake water quality monitoring dataset (2005-2019) ####
lakeData=tail(dir(path = "H:/ericg/16666LAWA/LAWA2021/Lakes/Data",pattern = "LakesWithMetadata.csv",
                          recursive = T,full.names = T,ignore.case=T),1)
lakeData=read.csv(lakeData,stringsAsFactors = F)

lakeData$Value[which(lakeData$Measurement%in%c('TN','TP'))]=
  lakeData$Value[which(lakeData$Measurement%in%c('TN','TP'))]*1000  #mg/L to mg/m3   #Until 4/10/18 also NH4N
lakeData$month=lubridate::month(lubridate::dmy(lakeData$Date))
lakeData$Year=lubridate::year(lubridate::dmy(lakeData$Date))
lakeData$monYear=paste0(lakeData$month,lakeData$Year)
lakeData=lakeData[which(lakeData$Year>=startYear15 & lakeData$Year<=EndYear),] #72353 to 69652

lakeWQmon <- lakeData%>%transmute(DateImported=accessDate,
                                  Region,Agency,LawaSiteID,SiteID,CouncilSiteID,LFENZID,
                                  Latitude=Lat,Longitude=Long,GeomorphicLType,LType,
                                  Indicator=Measurement,SampleDate=Date,Symbol=centype,CensoredValue=signif(Value,4))
if(writeOut)write.csv(lakeWQmon,
          paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                 "DownloadDatafiles/LakeWQMonDat.csv"),row.names = F)
rm(lakeWQmon,lakeData)

# Lake TLI (2005-2019) ####
lakeTLI=tail(dir(path="H:/ericg/16666LAWA/LAWA2021/Lakes/",
                 pattern="ITELakeTLI",recursive=T,full.names = T,ignore.case = T),1)
lakeTLI=read.csv(lakeTLI,stringsAsFactors = F)
lakeTLI=lakeTLI[which(lakeTLI$TLIYear>=startYear15 & lakeTLI$TLIYear<=EndYear),] #2064 to 1951
lakeTLI <- merge(all.x=F,x=lakeSiteTable%>%select(Region,Agency,LFENZID)%>%distinct,
                 all.y=T,y=lakeTLI%>%select(LFENZID=FENZID,Year=TLIYear,TLI))%>%select(Region,Agency,LFENZID,Year,TLI)
if(writeOut)write.csv(lakeTLI,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                         "DownloadDatafiles/LakeTLI.csv"),row.names=F)
rm(lakeTLI)

# Lake State ####
lakesState=tail(dir(path="H:/ericg/16666LAWA/LAWA2021/Lakes/",
                       pattern="ITELakeSiteState",recursive=T,full.names = T,ignore.case = T),1)
lakesState=read.csv(lakesState,stringsAsFactors = F)%>%
  dplyr::rename(LawaSiteID=LAWAID,Indicator=Parameter)%>%filter(Indicator!="TLI")
lakesState = merge(all.x=F,x = lakeSiteTable%>%select(Agency,Region,SiteID,CouncilSiteID,LawaSiteID,LFENZID),
                   all.y=T,y = lakesState%>%select(LawaSiteID,Indicator,Year,Median))

lakeNOF=tail(dir(path="H:/ericg/16666LAWA/LAWA2021/Lakes/",
                     pattern="ITELakeSiteNOF[^G]",recursive=T,full.names = T,ignore.case = T),1)
lakeNOF = read.csv(lakeNOF,stringsAsFactors = F)%>%
  dplyr::rename(LawaSiteID=LAWAID,NOFband=NOFMedian)%>%
  mutate(Year=strFrom(Year,'to'))

lakeNOF <- lakeNOF%>%filter(BandingRule%in%c("Tot_Nitr_Band","Tot_Phos_Band","Ammonia_Toxicity_Band","Med_Clarity_Band","ChlASummaryBand","E_coliSummaryband"))
lakeNOF$Indicator = gsub(pattern = 'Tot_|Med_|Max_|_Band|_Toxicity|Summary|band|RecHealth|260|540|_band|95|cal|Band',
                         replacement = '',lakeNOF$BandingRule)
lakeNOF$Indicator[lakeNOF$Indicator=="Ammonia"] <- "NH4N"
lakeNOF$Indicator[lakeNOF$Indicator=="ChlA"] <- "CHLA"
lakeNOF$Indicator[lakeNOF$Indicator=="Clarity"] <- "Secchi"
lakeNOF$Indicator[lakeNOF$Indicator=="E_coli"] <- "ECOLI"
lakeNOF$Indicator[lakeNOF$Indicator=="Nitr"] <- "TN"
lakeNOF$Indicator[lakeNOF$Indicator=="Phos"] <- "TP"
lakeNOF = merge(all.x=F,x = lakeSiteTable%>%select(Agency,Region,SiteID,CouncilSiteID,LawaSiteID,LFENZID),
                all.y=T,y = lakeNOF%>%select(LawaSiteID,Year,NOFband,Indicator)%>%drop_na(NOFband))

lakesStateNOF = merge(all.x=T,x=lakesState,
                   all.y=T,y=lakeNOF)%>%
                     select(Region,Agency,LawaSiteID,SiteID,CouncilSiteID,LFENZID,
                            Indicator,Year,Median,NOFband)%>%distinct
if(writeOut)write.csv(lakesStateNOF,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                           "DownloadDatafiles/LakeState.csv"),row.names=F)
rm(lakesState,lakeNOF,lakesStateNOF)

# Lake Trends ####
lst5=tail(dir(path="H:/ericg/16666LAWA/LAWA2021/Lakes/",
                        pattern="ITELakeSiteTrend5",recursive=T,full.names = T,ignore.case = T),1)
lst10=tail(dir(path="H:/ericg/16666LAWA/LAWA2021/Lakes/",
                         pattern="ITELakeSiteTrend10",recursive=T,full.names = T,ignore.case = T),1)
lst15=tail(dir(path="H:/ericg/16666LAWA/LAWA2021/Lakes/",
                         pattern="ITELakeSiteTrend15",recursive=T,full.names = T,ignore.case = T),1)
lst5 = read.csv(lst5,stringsAsFactors = F)
lst10 = read.csv(lst10,stringsAsFactors = F)
lst15 = read.csv(lst15,stringsAsFactors = F)
lst = merge(lst5,lst10,by = c("LAWAID","Parameter"))
lst = merge(all.x=T,x=lst,
            all.y=T,y=lst15,by=c("LAWAID","Parameter"))
rm(lst5,lst10,lst15)
lst <- merge(all.x=F,x=lakeSiteTable%>%select(Region,Agency,LawaSiteID,CouncilSiteID,SiteID,LFENZID),
             all.y=T,y=lst%>%transmute(LawaSiteID=LAWAID,Indicator=Parameter,Trend5,Trend10,Trend15))
if(writeOut)write.csv(lst,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                     "DownloadDatafiles/LakeTrend.csv"),row.names=F)
rm(lst)
rm(lakeSiteTable)  



#####################################################################
#RIVIERAS ####
riverSiteTable=loadLatestSiteTableRiver()
riverSiteTable$Agency=factor(riverSiteTable$Agency,levels=c("ac", "boprc", "ecan", "es", "gdc", "gwrc", "hbrc", "hrc", 
                                      "mdc", "ncc", "niwa", "nrc", "orc", "tdc", "trc", "wcrc", "wrc"),
                  labels=c("Auckland Council","Bay of Plenty Regional Council","Environment Canterbury","Environment Southalnd","Gisborne District Council","Greater Wellington Regional Council","Hawkes Bay Regional Council","Horizons Regional Council",
                           "Marlborough District Council","Nelson City Council","NIWA","Northland Regional Council","Otago Regional Council","Tasman District Council","Taranaki Regional Council","West Coast Regional Council","Waikato Regional Council"))

riverSiteTable$recLandcover=riverSiteTable$rawRecLandcover
riverSiteTable$recLandcover <- factor(riverSiteTable$recLandcover,levels=c("if","w","t","s","b",
                                                   "ef",
                                                   "p",
                                                   "u"),
                          labels=c(rep("Native vegetation",5),
                                   "Plantation forest","Pasture","Urban"))

rd <- loadLatestDataRiver()

rd$recLandcover=rd$rawRecLandcover
rd$recLandcover <- factor(rd$recLandcover,levels=c("if","w","t","s","b",
                                                                 "ef",
                                                                 "p",
                                                                 "u"),
                                 labels=c(rep("Native vegetation",5),
                                          "Plantation forest","Pasture","Urban"))


# River water quality monitoring dataset (2005-2019) ####
# Date Data Imported,	Region Name,	Agency,	LAWA Site ID,	SiteID,	CouncilSiteID,	Latitude,	Longitude,	SWQLandUse,
# 	SWQAltitude,	Catchment,	Indicator,	Indicator Unit of Measure, Sample Date,	Raw Value,	Symbol,	Censored Value,	License
rrd <- rd%>%filter(lubridate::year(lubridate::dmy(Date))>=2005&lubridate::year(lubridate::dmy(Date))<=2019)%>%
  transmute(DateDataImported=accessDate,Region,Agency,LawaSiteID,SiteID,CouncilSiteID,
            Lat,Long,WFSLanduse=SWQLanduse,WFSAltitude=SWQAltitude,recLandcover,
            Catchment,Indicator=Measurement,IndicatorUnitOfMeasure=Units,Date,RawValue,Symbol,Value)

rrd$Agency=factor(rrd$Agency,levels=c("ac", "boprc", "ecan", "es", "gdc", "gwrc", "hbrc", "hrc", 
                                      "mdc", "ncc", "niwa", "nrc", "orc", "tdc", "trc", "wcrc", "wrc"),
                  labels=c("Auckland Council","Bay of Plenty Regional Council","Environment Canterbury","Environment Southalnd","Gisborne District Council","Greater Wellington Regional Council","Hawkes Bay Regional Council","Horizons Regional Council",
                           "Marlborough District Council","Nelson City Council","NIWA","Northland Regional Council","Otago Regional Council","Tasman District Council","Taranaki Regional Council","West Coast Regional Council","Waikato Regional Council"))
if(writeOut)write.csv(rrd,"C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/DownloadDataFiles/RiverWQMonitoringData_LC.csv",row.names=F)

rrd%>%split(.$Region)%>%purrr::map(~{if(writeOut)write.csv(.,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                                                      "DownloadDatafiles/RiverWQMonitoringData",unique(.$Region),"_LC.csv"),row.names=F)})
if(writeOut)rrd%>%filter(Region%in%c("auckland", "bay of plenty", "gisborne","wellington", "hawkes bay", "manawatu-whanganui",  "waikato", "taranaki","northland"))%>%
  write.csv(.,"C:/users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/DownloadDataFiles/RiverWQMonitoringDataNI_LC.csv")
if(writeOut)rrd%>%filter(Region%in%c("canterbury", "southland",  "marlborough","nelson", "otago", "west coast", "tasman"))%>%
  write.csv(.,"C:/users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/DownloadDataFiles/RiverWQMonitoringDataSI_LC.csv")

rm(rrd)

# River water quality State ####
# Region 	Agency	LAWA Site ID	SiteID	CouncilSiteID	Latitude	Longitude	SWQLandUse	SWQAltitude	Catchment	Indicator	5-year median
# State quartile (all sites)	State quartile (same land use)	State quartile (same altitude)	State quartile (same land use and altitude)
# State NOF band	License
riverWQState=tail(dir(path="H:/ericg/16666LAWA/LAWA2021/WaterQuality/",
                    pattern="ITERiverState",recursive=T,full.names = T,ignore.case = T),1)
riverWQState = read_csv(riverWQState)%>%
  # dplyr::filter(Altitude=="All"&Landuse=="All")%>%
  dplyr::rename(Indicator=Parameter,LawaSiteID=LawaID)

riverWQState$Altitude = factor(riverWQState$Altitude,levels=c("All","Lowland","Upland"),labels=c("","SameAlt","SameAlt"))
riverWQState$Landuse =  factor(riverWQState$Landuse, levels=c("All","Forest","Rural","Urban"),labels=c('',rep("SameLU",3)))
riverWQStateW = riverWQState%>%dplyr::rename(quartile=StateScore)%>%
  pivot_wider(id_cols = c("LawaSiteID","Indicator"),names_from = c("Altitude","Landuse"),values_from = c("Median","quartile"))%>%
  select(-Median_SameAlt_,-Median__SameLU,-Median_SameAlt_SameLU)%>%dplyr::rename(Median_AllSites=Median__,quartile_AllSites=quartile__)

rm(riverWQState)

riverWQStateW <- merge(all.x=F,x=riverSiteTable%>%filter(Agency!='niwa')%>%dplyr::select(Region,Agency,LawaSiteID,SiteID,CouncilSiteID,Lat,Long,SWQLanduse,SWQAltitude,recLandcover,Catchment),
                       all.y=T,y=riverWQStateW%>%select(LawaSiteID,Indicator,Median_AllSites,quartile_AllSites,quartile__SameLU,quartile_SameAlt_,quartile_SameAlt_SameLU))

niwasiteTable=riverSiteTable%>%filter(tolower(Agency)=='niwa')
for(colFill in c("Region","Agency","SiteID","CouncilSiteID","Lat","Long","SWQLanduse","SWQAltitude",'recLandcover',"Catchment")){
  naInCol = is.na(riverWQStateW[,colFill])
  riverWQStateW[naInCol,colFill] <- niwasiteTable[match(gsub('_niwa','',tolower(riverWQStateW$LawaSiteID[naInCol])),
                                                       tolower(niwasiteTable$LawaSiteID)),colFill]
  rm(naInCol)
}
rm(colFill)
riverWQStateW <- riverWQStateW%>%dplyr::rename(Latitude=Lat,Longitude=Long,WFSLanduse=SWQLanduse,WFSAltitude=SWQAltitude)

riverNOF=tail(dir(path="H:/ericg/16666LAWA/LAWA2021/WaterQuality/",
                  pattern="ITERiverNOF",recursive=T,full.names = T,ignore.case = T),1)
riverNOF=read_csv(riverNOF,col_types = cols(
  LAWAID = col_character(),
  SiteName = col_character(),
  Year = col_character(),
  Parameter = col_character(),
  Value = col_double(),
  Band = col_character()
))%>%dplyr::rename(LawaSiteID=LAWAID,Indicator=Parameter,NOFBand=Band)%>%
  dplyr::filter(!Indicator%in%c("EcoliMed_Band","Ecoli95_Band","EcoliRecHealth260_Band","EcoliRecHealth540_Band",
                                "AmmoniacalMax_Band","AmmoniacalMed_Band",
                                "NitrateMed_Band","Nitrate95_Band","Nitrate_Toxicity_Band",
                                "DRPMed_Band","DRP95_Band"))%>%
  drop_na(NOFBand)%>%select(-Value)
riverNOF <- riverNOF%>%filter(NOFBand!="NaN")
riverNOF$Indicator = gsub(pattern = 'Tot_|Med_|Max_|_Band|_Toxicity|Summary|band|RecHealth|260|540|_band|95|cal|Band',
                         replacement = '',riverNOF$Indicator)
riverNOF$Indicator[riverNOF$Indicator=="Ammonia"] <- "NH4"
riverNOF$Indicator[riverNOF$Indicator=="Ecoli"] <- "ECOLI"
riverNOF$Indicator[riverNOF$Indicator=="DRP_"] <- "DRP"

riverWQStateW <- merge(x=riverWQStateW,all.x=T,
                      y=riverNOF%>%dplyr::select(-Year,-SiteName),all.y=F)
  # Region 	Agency	LAWA Site ID	SiteID	CouncilSiteID	Latitude	Longitude	WFSLandUse	WFSAltitude recLandcover	Catchment	Indicator	5-year median
  # State quartile (all sites)	State quartile (same land use)	State quartile (same altitude)	State quartile (same land use and altitude)
  # State NOF band	License
if(writeOut)write.csv(riverWQStateW,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                              "DownloadDatafiles/RiverWQStateResults_LC.csv"),row.names=F)
if(writeOut)write.csv(riverWQStateW%>%filter(Region%in%c("auckland", "bay of plenty", "gisborne","wellington", "hawkes bay", "manawatu-whanganui",  "waikato", "taranaki","northland")),
                      paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                             "DownloadDatafiles/RiverWQStateResultsNI_LC.csv"),row.names=F)
if(writeOut)write.csv(riverWQStateW%>%filter(Region%in%c("canterbury", "southland",  "marlborough","nelson", "otago", "west coast", "tasman")),
                      paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                             "DownloadDatafiles/RiverWQStateResultsSI_LC.csv"),row.names=F)
riverWQStateW%>%split(.$Region)%>%purrr::map(~{if(writeOut)write.csv(.,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                                                                    "DownloadDatafiles/RiverWQStateResults",unique(.$Region),"_LC.csv"),row.names=F)})

rm(riverNOF,riverWQStateW)



# River water quality Trends ####
# Region Name	Agency	LAWA Site ID	SiteID	CouncilSiteID	Latitude	Longitude	Catchment
# 	Indicator	TrendPeriod	TrendDataFrequency	TrendScore	Trend description	License
riverTrend=tail(dir(path="H:/ericg/16666LAWA/LAWA2021/WaterQuality/",
                      pattern="ITERiverTrend",recursive=T,full.names = T,ignore.case = T),1)
riverTrend <- read_csv(riverTrend)%>%as.data.frame
riverTrend$TrendDescription=factor(riverTrend$TrendScore,levels=c(-99,2,1,0,-1,-2),
                                   labels=c("Not determined","Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
riverTrend$LAWAID <- gsub('_niwa','',tolower(riverTrend$LAWAID))

riverSiteTable = rbind(riverSiteTable%>%dplyr::filter(Agency!='niwa'),riverSiteTable%>%dplyr::filter(Agency=='niwa'))

riverTrend=riverTrend%>%
  transmute(LawaSiteID=LAWAID,Indicator=Parameter,TrendPeriod,TrendDataFrequency=TrendFrequency,TrendScore,TrendDescription)
riverTrend$Region = riverSiteTable$Region[match(riverTrend$LawaSiteID,riverSiteTable$LawaSiteID)]
riverTrend$Agency = riverSiteTable$Agency[match(riverTrend$LawaSiteID,riverSiteTable$LawaSiteID)]
riverTrend$SiteID = riverSiteTable$SiteID[match(riverTrend$LawaSiteID,riverSiteTable$LawaSiteID)]
riverTrend$CouncilSiteID = riverSiteTable$CouncilSiteID[match(riverTrend$LawaSiteID,riverSiteTable$LawaSiteID)]
riverTrend$Latitude = riverSiteTable$Lat[match(riverTrend$LawaSiteID,riverSiteTable$LawaSiteID)]
riverTrend$Longitude = riverSiteTable$Long[match(riverTrend$LawaSiteID,riverSiteTable$LawaSiteID)]
riverTrend$WFSLanduse = riverSiteTable$SWQLanduse[match(riverTrend$LawaSiteID,riverSiteTable$LawaSiteID)]
riverTrend$WFSAltitude =riverSiteTable$SWQAltitude[match(riverTrend$LawaSiteID,riverSiteTable$LawaSiteID)] 
riverTrend$recLandcover=riverSiteTable$recLandcover[match(riverTrend$LawaSiteID,riverSiteTable$LawaSiteID)]
riverTrend$Catchment = riverSiteTable$Catchment[match(riverTrend$LawaSiteID,riverSiteTable$LawaSiteID)]

riverTrend <- riverTrend%>%select(Region,Agency,LawaSiteID,SiteID,CouncilSiteID,Latitude,Longitude,
                                  WFSLanduse,WFSAltitude,recLandcover,Catchment,Indicator,TrendPeriod,
                                  TrendDataFrequency,TrendScore,TrendDescription)

if(writeOut)write.csv(riverTrend,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                             "DownloadDatafiles/RiverWQTrendResults_LC.csv"),row.names=F)
if(writeOut)write.csv(riverTrend%>%filter(Region%in%c("auckland", "bay of plenty", "gisborne","wellington", "hawkes bay", "manawatu-whanganui",  "waikato", "taranaki","northland")),
                      paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                             "DownloadDatafiles/RiverWQTrendResultsNI_LC.csv"),row.names=F)
if(writeOut)write.csv(riverTrend%>%filter(Region%in%c("canterbury", "southland",  "marlborough","nelson", "otago", "west coast", "tasman")),
                      paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                             "DownloadDatafiles/RiverWQTrendResultsSI_LC.csv"),row.names=F)
riverTrend%>%split(.$Region)%>%purrr::map(~{if(writeOut)write.csv(.,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                                                                              "DownloadDatafiles/RiverWQTrendResults",unique(.$Region),"_LC.csv"),row.names=F)})
rm(riverTrend)



  
#Rolling NOF results ####
NOFSummaryTable=read.csv(tail(dir(path = "h:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis/",pattern="NOFSummaryTable_Rolling",
                                  recursive=T,full.names = T),1),stringsAsFactors = F)
NOFSummaryTable <- NOFSummaryTable%>%filter(Year!='2005to2009')  #Just, you see this way we're left with a single decade
NOFSummaryTable$SWQAltitude = pseudo.titlecase(tolower(NOFSummaryTable$SWQAltitude))
NOFSummaryTable$rawRecLandcover = riverSiteTable$rawRecLandcover[match(tolower(NOFSummaryTable$LawaSiteID),tolower(riverSiteTable$LawaSiteID))]
NOFSummaryTable$rawRecLandcover = factor(NOFSummaryTable$rawRecLandcover,levels=c("if","w","t","s","b","ef", "p", "u"))
NOFSummaryTable$recLandcover=NOFSummaryTable$rawRecLandcover
NOFSummaryTable$recLandcover <- factor(NOFSummaryTable$recLandcover,levels=c("if","w","t","s","b",
                                                                 "ef",
                                                                 "p",
                                                                 "u"),
                                 labels=c("Native vegetation","Native vegetation","Native vegetation","Native vegetation","Native vegetation",
                                          "Plantation forest","Pasture","Urban"))

NOFSummaryTable$Nitrate_Toxicity_Band=factor(NOFSummaryTable$Nitrate_Toxicity_Band,levels=c("D", "C", "B", "A", "NA"),labels=c("D", "C", "B", "A", "NA"))
NOFSummaryTable$Nitrate_Toxicity_Band[is.na(NOFSummaryTable$Nitrate_Toxicity_Band)] <- "NA"
NOFSummaryTable$EcoliSummaryband=factor(NOFSummaryTable$EcoliSummaryband,levels=c("E","D", "C", "B", "A", "NA"),labels=c("E","D", "C", "B", "A", "NA"))
NOFSummaryTable$EcoliSummaryband[is.na(NOFSummaryTable$EcoliSummaryband)] <- "NA"
NOFSummaryTable$Ammonia_Toxicity_Band=factor(NOFSummaryTable$Ammonia_Toxicity_Band,levels=c("D", "C", "B", "A", "NA"),labels=c("D", "C", "B", "A", "NA"))
NOFSummaryTable$Ammonia_Toxicity_Band[is.na(NOFSummaryTable$Ammonia_Toxicity_Band)] <- "NA"
NOFSummaryTable$DRP_Summary_Band=factor(NOFSummaryTable$DRP_Summary_Band,levels=c("D", "C", "B", "A", "NA"),labels=c("D", "C", "B", "A", "NA"))
NOFSummaryTable$DRP_Summary_Band[is.na(NOFSummaryTable$DRP_Summary_Band)] <- "NA"

NOFSummaryTable$Long=riverSiteTable$Long[match(tolower(NOFSummaryTable$LawaSiteID),tolower(riverSiteTable$LawaSiteID))]
NOFSummaryTable$Lat=riverSiteTable$Lat[match(tolower(NOFSummaryTable$LawaSiteID),tolower(riverSiteTable$LawaSiteID))]

NOFTableOut = NOFSummaryTable%>%select(LawaSiteID:Year,NZReach:Long,WFSLanduse=Landcover,WFSAltitude=AltitudeCl,recLandcover,Nitrate_Toxicity_Band,Ammonia_Toxicity_Band,EcoliSummaryband,DRP_Summary_Band)
which(apply(NOFTableOut[,15:18],1,FUN=function(x)all(x=="NA")))->toCut
NOFTableOut = NOFTableOut[-toCut,]
rm(toCut,NOFSummaryTable)
if(writeOut)write.csv(NOFTableOut,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                                         "DownloadDatafiles/RiverNOFHistory_LC.csv"),row.names=F)
################################################################# ####
#Macros ####
macroSiteTable <- loadLatestSiteTableMacro()
macroSiteTable$Agency=factor(macroSiteTable$Agency,levels=c("ac", "boprc", "ecan", "es", "gdc", "gwrc", "hbrc", "hrc", 
                                                            "mdc", "ncc", "niwa", "nrc", "orc", "tdc", "trc", "wcrc", "wrc"),
                             labels=c("Auckland Council","Bay of Plenty Regional Council","Environment Canterbury","Environment Southalnd","Gisborne District Council","Greater Wellington Regional Council","Hawkes Bay Regional Council","Horizons Regional Council",
                                      "Marlborough District Council","Nelson City Council","NIWA","Northland Regional Council","Otago Regional Council","Tasman District Council","Taranaki Regional Council","West Coast Regional Council","Waikato Regional Council"))

macroSiteTable$recLandcover=macroSiteTable$rawRecLandcover
macroSiteTable$recLandcover <- factor(macroSiteTable$recLandcover,levels=c("if","w","t","s","b",
                                                                           "ef",
                                                                           "p",
                                                                           "u"),
                                      labels=c(rep("Native vegetation",5),
                                               "Plantation forest","Pasture","Urban"))

  
# River Ecology monitoring data (2005-2019) ####					
# Date Data Imported	Region Name	Agency	LAWA Site ID	SiteID	CouncilSiteID	Latitude	Longitude	Catchment
# 	Indicator	Indicator Unit of Measure

macroData=tail(dir(path="H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/",
                           pattern="ITEMacroHistoric",recursive=T,full.names = T,ignore.case = T),1)
macroData <- read_csv(macroData)%>%
  filter(year(CollectionDate)>=2005 & year(CollectionDate)<=2019)%>%as.data.frame

macroData <- merge(x=macroSiteTable%>%
                     dplyr::select(Region,LawaSiteID,SiteID,CouncilSiteID,Latitude=Lat,Longitude=Long,Catchment,recLandcover),all.x=F,
                   y=macroData%>%
                     transmute(LawaSiteID=LAWAID,Agency=Agency,Date=CollectionDate,Indicator=Metric,Value=Value),all.y=T)

macroData$Region[macroData$Agency=='ac'] <- 'auckland'

niwasites = unique(macroData$LawaSiteID[macroData$Agency=='niwa'&is.na(macroData$Region)])
for(ns in seq_along(niwasites)){
  theseData = which(macroData$LawaSiteID==niwasites[ns])
  if(niwasites[ns]%in%riverSiteTable$LawaSiteID){
    thisRST = which(riverSiteTable$LawaSiteID==niwasites[ns])
    if(length(thisRST)>1){
      thisRST = which(riverSiteTable$LawaSiteID==niwasites[ns] & tolower(riverSiteTable$Agency)=='niwa')
      thatRST = which(riverSiteTable$LawaSiteID==niwasites[ns] & tolower(riverSiteTable$Agency)!='niwa')
    }
    if(length(thisRST)==0){    thisRST = which(riverSiteTable$LawaSiteID==niwasites[ns])[1];stop()}
    macroData$Region[theseData]=riverSiteTable$Region[thisRST]
    macroData$SiteID[theseData]=riverSiteTable$SiteID[thisRST]
    macroData$CouncilSiteID[theseData]=riverSiteTable$CouncilSiteID[thisRST]
    macroData$Latitude[theseData]=riverSiteTable$Lat[thisRST]
    macroData$Longitude[theseData]=riverSiteTable$Long[thisRST]
    macroData$Catchment[theseData]=riverSiteTable$Catchment[thisRST]
    if(is.na(macroData$Catchment)){
      macroData$Catchment[theseData]=riverSiteTable$Catchment[thatRST]
    }
  }else{
    if(niwasites[ns]%in%tolower(macroSiteTable$LawaSiteID)){
      thisMST = which(tolower(macroSiteTable$LawaSiteID)==niwasites[ns])
      if(length(thisMST)>1){browser()}
      macroData$Region[theseData]=macroSiteTable$Region[thisMST]
      macroData$SiteID[theseData]=macroSiteTable$SiteID[thisMST]
      macroData$CouncilSiteID[theseData]=macroSiteTable$CouncilSiteID[thisMST]
      macroData$Latitude[theseData]=macroSiteTable$Lat[thisMST]
      macroData$Longitude[theseData]=macroSiteTable$Long[thisMST]
      macroData$Catchment[theseData]=macroSiteTable$Catchment[thisMST]
      
    }
  }
}
rm(niwasites)
noRegionSites = unique(macroData$LawaSiteID[is.na(macroData$Region)&!is.na(macroData$Longitude)])
for(nrs in seq_along(noRegionSites)){
  theseData = which(macroData$LawaSiteID == noRegionSites[nrs])
  siteLong = unique(macroData$Longitude[theseData])
  siteLat = unique(macroData$Latitude[theseData])
  macrodists = sqrt((macroSiteTable$Lat[!is.na(macroSiteTable$Region)]-siteLat)^2+(macroSiteTable$Long[!is.na(macroSiteTable$Region)]-siteLong)^2)
  riverdists = sqrt((riverSiteTable$Lat[!is.na(riverSiteTable$Region)]-siteLat)^2+(riverSiteTable$Long[!is.na(riverSiteTable$Region)]-siteLong)^2)
  if(min(macrodists,na.rm=T)<min(riverdists,na.rm=T)){
    thisSite = macroSiteTable[which.min(macrodists),]
  }else{
    thisSite = riverSiteTable[which.min(riverdists),]
  }

  if(all(is.na(macroData$Region[theseData]))){
    macroData$Region[theseData] = thisSite$Region
  }else{
    macroData$Region[theseData] = unique(macroData$Region[theseData],na.rm=T)
  }
  
  if(all(is.na(macroData$Catchment[theseData]))){
    macroData$Catchment[theseData] = thisSite$Catchment
  }else{
    macroData$Catchment[theseData] = unique(macroData$Catchment[theseData],na.rm=T)
  }
  if(all(is.na(macroData$SiteID[theseData]))){
    macroData$SiteID[theseData] = thisSite$SiteID
  }else{
    macroData$SiteID[theseData] = unique(macroData$SiteID[theseData],na.rm=T)
  }
  if(all(is.na(macroData$CouncilSiteID[theseData]))){
    macroData$CouncilSiteID[theseData] = thisSite$CouncilSiteID
  }else{
    macroData$CouncilSiteID[theseData] = unique(macroData$CouncilSiteID[theseData],na.rm=T)
  }
  if(all(is.na(macroData$Latitude[theseData]))){
    macroData$Latitude[theseData] = thisSite$Latitude
  }else{
    macroData$Latitude[theseData] = unique(macroData$Latitude[theseData],na.rm=T)
  }
  if(all(is.na(macroData$Longitude[theseData]))){
    macroData$Longitude[theseData] = thisSite$Longitude
  }else{
    macroData$Longitude[theseData] = unique(macroData$Longitude[theseData],na.rm=T)
  }
}
rm(noRegionSites)
noSiteIDSites = unique(macroData$LawaSiteID[is.na(macroData$SiteID)])
# Date Data Imported	Region Name	Agency	LAWA Site ID	SiteID	CouncilSiteID	Latitude	Longitude	Catchment
# 	Indicator	Indicator Unit of Measure
macroData <- macroData%>%select(Region,Agency,LawaSiteID,SiteID,CouncilSiteID,Latitude,Longitude,Catchment,recLandcover,Indicator,Date,Value)
if(writeOut)write.csv(macroData,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                           "DownloadDatafiles/RiverEcologyMonitoringData_LC.csv"),row.names=F)

if(writeOut)write.csv(macroData%>%filter(Region%in%c("auckland", "bay of plenty", "gisborne","wellington", "hawkes bay", "manawatu-whanganui",  "waikato", "taranaki","northland")),
                      paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                             "DownloadDatafiles/RiverEcologyMonitoringDataNI_LC.csv"),row.names=F)

if(writeOut)write.csv(macroData%>%filter(Region%in%c("canterbury", "southland",  "marlborough","nelson", "otago", "west coast", "tasman")),
                      paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                             "DownloadDatafiles/RiverEcologyMonitoringDataSI_LC.csv"),row.names=F)

macroData%>%split(.$Region)%>%purrr::map(~{if(writeOut)write.csv(.,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                                                                           "DownloadDatafiles/RiverEcologyMonitoringData",unique(.$Region),"_LC.csv"),row.names=F)})



# River ecology STATE results (includes 5-year median,  State NOF band)		####									
# Region Name	Agency	LAWA Site ID	SiteID	CouncilSiteID	Latitude	Longitude	Catchment	Indicator	5-year median	State NOF band	License
# MCI score only	
macroState=tail(dir(path="H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/",
                    pattern="ITEMacroState",recursive=T,full.names = T,ignore.case = T),1)
macroState = read_csv(macroState)%>%as.data.frame
macroState$NOFBand=NA
macroState$NOFBand[macroState$Parameter=="MCI"] <- cut(x = macroState$Median[macroState$Parameter=="MCI"],
                                                               breaks=c(0,90,110,130,200))
macroState$quartile=NA
macroState$quartile[macroState$Parameter=="MCI"]=cut(macroState$Median[macroState$Parameter=="MCI"],
                                                     breaks = quantile(macroState$Median[macroState$Parameter=="MCI"],
                                                                       probs=c(0,0.25,0.5,0.75,1)))
macroState$quartile[macroState$Parameter=="TaxaRichness"]=cut(macroState$Median[macroState$Parameter=="TaxaRichness"],
                                                     breaks = quantile(macroState$Median[macroState$Parameter=="TaxaRichness"],
                                                                       probs=c(0,0.25,0.5,0.75,1)))
macroState$quartile[macroState$Parameter=="PercentageEPTTaxa"]=cut(macroState$Median[macroState$Parameter=="PercentageEPTTaxa"],
                                                     breaks = quantile(macroState$Median[macroState$Parameter=="PercentageEPTTaxa"],
                                                                       probs=c(0,0.25,0.5,0.75,1)))

macroState <- merge(x=macroSiteTable%>%dplyr::select(Agency,Region,LawaSiteID,SiteID,CouncilSiteID,
                                                     Latitde=Lat,Longitude=Long,Catchment,recLandcover),all.x=F,
                    y=macroState%>%dplyr::transmute(LawaSiteID=LAWAID,Indicator=Parameter,Median,NOFBand),all.y = T)
macroState$Agency=as.character(macroState$Agency)
macroState$Agency[is.na(macroState$Agency)] <- macroData$Agency[match(gsub('_niwa','',macroState$LawaSiteID[is.na(macroState$Agency)]),
                                                                      macroData$LawaSiteID)]
macroState$Region[is.na(macroState$Region)] <- macroData$Region[match(gsub('_niwa','',macroState$LawaSiteID[is.na(macroState$Region)]),
                                                                      macroData$LawaSiteID)]
macroState$SiteID[is.na(macroState$SiteID)] <- macroData$SiteID[match(gsub('_niwa','',macroState$LawaSiteID[is.na(macroState$SiteID)]),
                                                                      macroData$LawaSiteID)]
naCSID=which(is.na(macroState$CouncilSiteID))
if(length(naCSID)>0)
macroState$CouncilSiteID[naCSID] <- macroData$CouncilSiteID[match(gsub('_niwa','',macroState$LawaSiteID[naCSID]),
                                                                      macroData$LawaSiteID)]
rm(naCSID)
if(writeOut)write.csv(macroState,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                           "DownloadDatafiles/RiverEcologyStateResults_LC.csv"),row.names=F)
if(writeOut)write.csv(macroState%>%filter(Region%in%c("auckland", "bay of plenty", "gisborne","wellington", "hawkes bay", "manawatu-whanganui",  "waikato", "taranaki","northland")),
                      paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                             "DownloadDatafiles/RiverEcologyStateResultsNI_LC.csv"),row.names=F)
if(writeOut)write.csv(macroState%>%filter(Region%in%c("canterbury", "southland",  "marlborough","nelson", "otago", "west coast", "tasman")),
                      paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                             "DownloadDatafiles/RiverEcologyStateResultsSI_LC.csv"),row.names=F)
macroState%>%split(.$Region)%>%purrr::map(~{if(writeOut)write.csv(.,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                                                                          "DownloadDatafiles/RiverEcologyStateResults",unique(.$Region),"_LC.csv"),row.names=F)})



# River Ecology TREND results (MCI score only) ####
# Region Name	Agency	LAWA Site ID	SiteID	CouncilSiteID	Latitude	Longitude	Catchment
# 	Indicator	TrendPeriod	TrendDataFrequency	TrendScore	Trend description	License
macroTrend10=tail(dir(path="H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/",
                      pattern="ITEMacroTrend10",recursive=T,full.names = T,ignore.case = T),1)
macroTrend10 <- read_csv(macroTrend10)%>%as.data.frame
macroTrend15=tail(dir(path="H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/",
                      pattern="ITEMacroTrend15",recursive=T,full.names = T,ignore.case = T),1)  
macroTrend15 <- read_csv(macroTrend15)%>%as.data.frame
mTrend <- merge(macroTrend10%>%transmute(LawaSiteID=LAWAID,Indicator=Parameter,Trend10y=Trend),
                macroTrend15%>%transmute(LawaSiteID=LAWAID,Indicator=Parameter,Trend15y=Trend),all=T)
mTrend$Trend10y[is.na(mTrend$Trend10y)] <- (-99)
mTrend$Trend15y[is.na(mTrend$Trend15y)] <- (-99)
mTrend$TrendDescription10y=factor(mTrend$Trend10y,levels=c(-99,2,1,0,-1,-2),
                               labels=c("Not determined","Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
mTrend$TrendDescription15y=factor(mTrend$Trend15y,levels=c(-99,2,1,0,-1,-2),
                                  labels=c("Not determined","Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))

mTrend$LawaSiteID = gsub('_niwa','',tolower(mTrend$LawaSiteID))
mTrend <- merge(x=macroSiteTable%>%select(Region,Agency,LawaSiteID,SiteID,CouncilSiteID,Latitude=Lat,Longitude=Long,Catchment,recLandcover),all.x=F,
                y=mTrend%>%select(LawaSiteID,Indicator,Trend10y,TrendDescription10y,Trend15y,TrendDescription15y),all.y=T)
mTrend$Agency=as.character(mTrend$Agency)
mTrend$Agency[is.na(mTrend$Agency)] <- macroData$Agency[match(mTrend$LawaSiteID[is.na(mTrend$Agency)],macroData$LawaSiteID)]
mTrend$Region[is.na(mTrend$Region)] <- macroData$Region[match(mTrend$LawaSiteID[is.na(mTrend$Region)],macroData$LawaSiteID)]
mTrend$SiteID[is.na(mTrend$SiteID)] <- macroData$SiteID[match(mTrend$LawaSiteID[is.na(mTrend$SiteID)],macroData$LawaSiteID)]
mTrend$CouncilSiteID[is.na(mTrend$CouncilSiteID)] <- macroData$CouncilSiteID[match(mTrend$LawaSiteID[is.na(mTrend$CouncilSiteID)],macroData$LawaSiteID)]
mTrend$Latitude[is.na(mTrend$Latitude)] <- macroData$Latitude[match(mTrend$LawaSiteID[is.na(mTrend$Latitude)],macroData$LawaSiteID)]
mTrend$Longitude[is.na(mTrend$Longitude)] <- macroData$Longitude[match(mTrend$LawaSiteID[is.na(mTrend$Longitude)],macroData$LawaSiteID)]
mTrend$Catchment[is.na(mTrend$Catchment)] <- macroData$Catchment[match(mTrend$LawaSiteID[is.na(mTrend$Catchment)],macroData$LawaSiteID)]


if(writeOut)write.csv(mTrend,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                        "DownloadDatafiles/RiverEcologyTrendResults_LC.csv"),row.names=F)
if(writeOut)write.csv(mTrend%>%filter(Region%in%c("auckland", "bay of plenty", "gisborne","wellington", "hawkes bay", "manawatu-whanganui",  "waikato", "taranaki","northland")),
                      paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                             "DownloadDatafiles/RiverEcologyTrendResultsNI_LC.csv"),row.names=F)
if(writeOut)write.csv(mTrend%>%filter(Region%in%c("canterbury", "southland",  "marlborough","nelson", "otago", "west coast", "tasman")),
                      paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                             "DownloadDatafiles/RiverEcologyTrendResultsSI_LC.csv"),row.names=F)
mTrend%>%split(.$Region)%>%purrr::map(~{if(writeOut)write.csv(.,paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/",
                                                                           "DownloadDatafiles/RiverEcologyTrendResults",unique(.$Region),"_LC.csv"),row.names=F)})


  