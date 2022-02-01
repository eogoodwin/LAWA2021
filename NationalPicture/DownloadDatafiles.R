rm(list=ls())
library(tidyverse)
source('H:/ericg/16666LAWA/LAWA2021/scripts/LAWAFunctions.R')
EndYear <- lubridate::isoyear(Sys.Date())-1
startYear15 <- EndYear - 15+1
StartYear10 <- EndYear - 10+1
StartYear5 <- EndYear -   5+1

writeOut=FALSE



# LAKES: ####
lakeSiteTable=loadLatestSiteTableLakes()
#   Lake water quality monitoring dataset (2006-2020) ####
lakeData=tail(dir(path = "H:/ericg/16666LAWA/LAWA2021/Lakes/Data",pattern = "LakesWithMetadata.csv",
                          recursive = T,full.names = T,ignore.case=T),1)
lakeData=read.csv(lakeData,stringsAsFactors = F)

lakeData$Value[which(lakeData$Measurement%in%c('TN','TP'))]=
  lakeData$Value[which(lakeData$Measurement%in%c('TN','TP'))]*1000  #mg/L (g/m3) to mg/m3   #Until 4/10/18 also NH4N
lakeData$month=lubridate::month(lubridate::dmy(lakeData$Date))
lakeData$Year=lubridate::year(lubridate::dmy(lakeData$Date))
lakeData$monYear=paste0(lakeData$month,lakeData$Year)
lakeData=lakeData[which(lakeData$Year>=startYear15 & lakeData$Year<=EndYear),] #72353 to 69652

lakeData$Units = as.character(factor(lakeData$Measurement,
                                     levels=c("CHLA",
                                              "CYANOTOT", "CYANOTOX",
                                              "ECOLI",
                                              "NH4N",
                                              "pH", "Secchi", 
                                              "TN", "TP"),
                                     labels=c("mg/m3",
                                              'mm3/L','mm3/L',
                                              "#/100 mL",
                                              "mg/L",
                                              "-log[H+]","m",
                                              "mg/m3","mg/m3")))

lakeWQmon <- lakeData%>%transmute(DateImported=accessDate,
                                  Region,Agency,LawaSiteID,SiteID,CouncilSiteID,LFENZID,
                                  Latitude=Lat,Longitude=Long,GeomorphicLType,LType,
                                  Indicator=Measurement,SampleDate=Date,Symbol=centype,
                                  CensoredValue=signif(Value,4),Units=Units)
if(writeOut){
  write.csv(lakeWQmon,
          paste0("C:/Users/ericg/Otago Regional Council/",
                 "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                 "LakesNationalPicture/DownloadData/LakeWQMonDat.csv"),row.names = F)
}
rm(lakeWQmon,lakeData)

# Lake TLI (2006-2020) ####
lakeTLI=tail(dir(path="H:/ericg/16666LAWA/LAWA2021/Lakes/",
                 pattern="ITELakeTLI",recursive=T,full.names = T,ignore.case = T),1)
lakeTLI=read.csv(lakeTLI,stringsAsFactors = F)
lakeTLI=lakeTLI[which(lakeTLI$TLIYear>=startYear15 & lakeTLI$TLIYear<=EndYear),] #2064 to 1951

lakeSiteTable$lake=lakeSiteTable$SiteID
lakeSiteTable$lake[which(!is.na(as.numeric(lakeSiteTable$lake)))] <- lakeSiteTable$CouncilSiteID[which(!is.na(as.numeric(lakeSiteTable$lake)))]

lakeTLI <- merge(all.x=F,x=lakeSiteTable%>%select(Region,Agency,lake,LFENZID,MixingPattern=LType,GeomorphicType=GeomorphicLType)%>%distinct,
                 all.y=T,y=lakeTLI%>%select(LFENZID=FENZID,Year=TLIYear,TLI)%>%distinct)%>%
  select(Agency,Region,lake,LFENZID,Year,TLI,MixingPattern,GeomorphicType)
lakeTLI$LFENZID[lakeTLI$LFENZID<0] <- 0
if(writeOut){
  write.csv(lakeTLI,paste0("C:/Users/ericg/Otago Regional Council/",
                                     "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                         "LakesNationalPicture/DownloadData/LakeTLI.csv"),row.names=F)
  }
rm(lakeTLI)

# Lake State ####
lakesState=tail(dir(path="H:/ericg/16666LAWA/LAWA2021/Lakes/",
                       pattern="ITELakeSiteState",recursive=T,full.names = T,ignore.case = T),1)
lakesState=read.csv(lakesState,stringsAsFactors = F)%>%
  dplyr::rename(LawaSiteID=LAWAID,Indicator=Parameter)%>%filter(Indicator!="TLI")
lakesState = merge(all.x=F,x = lakeSiteTable%>%select(Agency,Region,SiteID,CouncilSiteID,LawaSiteID,LFENZID),
                   all.y=T,y = lakesState%>%select(LawaSiteID,Indicator,Year,Median))
lakesState$Units = as.character(factor(lakesState$Indicator,
                                     levels=c("CHLA","CYANOTOT", "CYANOTOX",
                                              "ECOLI","NH4N","pH", "Secchi", 
                                              "TN", "TP"),
                                     labels=c("mg/m3",'mm3/L','mm3/L',
                                              "#/100 mL","mg/L","-log[H+]","m",
                                              "mg/m3","mg/m3")))


lakeNOF=tail(dir(path="H:/ericg/16666LAWA/LAWA2021/Lakes/",
                     pattern="ITELakeSiteNOF[^G]",recursive=T,full.names = T,ignore.case = T),1)
lakeNOF = read.csv(lakeNOF,stringsAsFactors = F)%>%
  dplyr::rename(LawaSiteID=LAWAID,NOFband=NOFMedian)%>%
  mutate(Year=strFrom(Year,'to'))

# lakeNOF <- lakeNOF%>%filter(BandingRule%in%c("Tot_Nitr_Band","Tot_Phos_Band",
#                                              "Ammonia_Toxicity_Band","Med_Clarity_Band",
#                                              "ChlASummaryBand","E_coliSummaryband"))

lakeNOF$Indicator = gsub(pattern ='Band|_Band',
                         # 'Tot_|Med_|Median_|Max_|_Band|_Toxicity|Summary|band|RecHealth|260|540|_band|95|cal|Band',
                         replacement = '',lakeNOF$BandingRule)
lakeNOF$Indicator <- gsub(pattern = 'Ammoniacal|Ammonia',replacement = 'NH4N',lakeNOF$Indicator)#[lakeNOF$Indicator=="Ammonia"] <- "NH4N"
lakeNOF$Indicator <- gsub(pattern = 'ChlA',replacement = 'CHLA',lakeNOF$Indicator)#[lakeNOF$Indicator=="ChlA"] <- "CHLA"
lakeNOF$Indicator <- gsub(pattern = 'Clarity',replacement = 'Secchi',lakeNOF$Indicator)#[lakeNOF$Indicator=="Clarity"] <- "Secchi"
lakeNOF$Indicator <- gsub(pattern = 'E_coli',replacement = 'ECOLI',lakeNOF$Indicator)#[lakeNOF$Indicator=="E_coli"] <- "ECOLI"
lakeNOF$Indicator <- gsub(pattern = 'Nitrogen',replacement = 'TN',lakeNOF$Indicator)#[lakeNOF$Indicator=="Nitr"] <- "TN"
lakeNOF$Indicator <- gsub(pattern = 'Phos|Phosphorus',replacement = 'TP',lakeNOF$Indicator)#[lakeNOF$Indicator=="Phos"] <- "TP"

unique(lakeNOF$Indicator)



lakeNOF = merge(all.x=F,x = lakeSiteTable%>%select(Agency,Region,SiteID,CouncilSiteID,LawaSiteID,LFENZID),
                all.y=T,y = lakeNOF%>%select(LawaSiteID,Year,NOFband,Indicator)%>%drop_na(NOFband))

lakesStateNOF = merge(x=lakesState,all.x=T,
                   y=lakeNOF,all.y=T)%>%
                     select(Region,Agency,LawaSiteID,SiteID,CouncilSiteID,LFENZID,
                            Indicator,Year,Median,Units,NOFband)%>%distinct
if(writeOut){
  write.csv(lakesStateNOF,paste0("C:/Users/ericg/Otago Regional Council/",
                                           "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                           "LakesNationalPicture/DownloadData/LakeState.csv"),row.names=F)
}
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
lst <- merge(x=lakeSiteTable%>%select(Region,Agency,LawaSiteID,CouncilSiteID,SiteID,LFENZID),all.x=F,
             y=lst%>%transmute(LawaSiteID=LAWAID,Indicator=Parameter,Trend5,Trend10,Trend15),all.y=T)
if(writeOut){
  write.csv(lst,paste0("C:/Users/ericg/Otago Regional Council/",
                                 "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                     "LakesNationalPicture/DownloadData/LakeTrend.csv"),row.names=F)
}
rm(lst)
rm(lakeSiteTable)  













#RIVIERAS ####
riverSiteTable=loadLatestSiteTableRiver()
riverSiteTable$Agency=factor(riverSiteTable$Agency,levels=c("ac", "boprc", "ecan", "es", "gdc", "gwrc", "hbrc", "hrc", 
                                      "mdc", "ncc", "niwa", "nrc", "orc", "tdc", "trc", "wcrc", "wrc"),
                  labels=c("Auckland Council","Bay of Plenty Regional Council","Environment Canterbury","Environment Southland","Gisborne District Council","Greater Wellington Regional Council","Hawkes Bay Regional Council","Horizons Regional Council",
                           "Marlborough District Council","Nelson City Council","NIWA","Northland Regional Council","Otago Regional Council","Tasman District Council","Taranaki Regional Council","West Coast Regional Council","Waikato Regional Council"))

riverSiteTable$recLandcover=riverSiteTable$rawRecLandcover
riverSiteTable$recLandcover <- factor(riverSiteTable$recLandcover,
                                      levels=c("if","w","t","s","b",
                                               "ef",
                                               "p","m",
                                               "u"),
                                      labels=c(rep("Native vegetation",5),
                                               "Plantation forest",
                                               "Pasture","Pasture",
                                               "Urban"))

rd <- loadLatestDataRiver()
rd$Agency=factor(rd$Agency,levels=c("ac", "boprc", "ecan", "es", "gdc", "gwrc", "hbrc", "hrc", 
                                      "mdc", "ncc", "niwa", "nrc", "orc", "tdc", "trc", "wcrc", "wrc"),
                  labels=c("Auckland Council","Bay of Plenty Regional Council","Environment Canterbury","Environment Southland","Gisborne District Council","Greater Wellington Regional Council","Hawkes Bay Regional Council","Horizons Regional Council",
                           "Marlborough District Council","Nelson City Council","NIWA","Northland Regional Council","Otago Regional Council","Tasman District Council","Taranaki Regional Council","West Coast Regional Council","Waikato Regional Council"))

rd$recLandcover=riverSiteTable$rawRecLandcover[match(rd$LawaSiteID,riverSiteTable$LawaSiteID)]
rd$recLandcover <- factor(rd$recLandcover,
                          levels=c("if","w","t","s","b",
                                   "ef",
                                   "p","m",
                                   "u"),
                          labels=c(rep("Native vegetation",5),
                                   "Plantation forest",
                                   "Pasture","Pasture",
                                   "Urban"))
rd$Long=riverSiteTable$Long[match(rd$LawaSiteID,riverSiteTable$LawaSiteID)]
rd$Lat=riverSiteTable$Lat[match(rd$LawaSiteID,riverSiteTable$LawaSiteID)]
rd$Catchment = riverSiteTable$Catchment[match(rd$LawaSiteID,riverSiteTable$LawaSiteID)]

rd$SedimentClass = riverSiteTable$SedimentClass[match(rd$LawaSiteID,riverSiteTable$LawaSiteID)]

# River water quality monitoring dataset (2006-2020) ####
# Date Data Imported,	Region Name,	Agency,	LAWA Site ID,	SiteID,	CouncilSiteID,	Latitude,	Longitude,	SWQLandUse,
# 	SWQAltitude,	Catchment,	Indicator,	Indicator Unit of Measure, Sample Date,	Raw Value,	Symbol,	Censored Value,	License
rrd <- rd%>%filter(lubridate::year(lubridate::dmy(Date))>=2006&lubridate::year(lubridate::dmy(Date))<=2020)%>%
  transmute(Region,Agency,LawaSiteID,SiteID,CouncilSiteID,
            Lat,Long,WFSLanduse=SWQLanduse,WFSAltitude=SWQAltitude,recLandcover,
            Catchment,Indicator=Measurement,IndicatorUnitOfMeasure=Units,Date,RawValue,Symbol,Value)

if(writeOut){
  write.csv(rrd,paste0("C:/Users/ericg/Otago Regional Council/",
            "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
            "SWQNationalPicture/DownloadData/RiverWQMonitoringData_LC.csv"),row.names=F)
  }

rrd%>%split(.$Region)%>%purrr::map(~{
  if(writeOut)write.csv(.,paste0("C:/Users/ericg/Otago Regional Council/",
                                 "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                                 "SWQNationalPicture/DownloadData/RiverWQMonitoringData",unique(.$Region),"_LC.csv"),row.names=F)
  })

if(writeOut){
  rrd%>%filter(Region%in%c("auckland", "bay of plenty", "gisborne",
                           "wellington", "hawkes bay", "manawatu-whanganui",
                           "waikato", "taranaki","northland"))%>%
  write.csv(.,paste0("C:/users/ericg/Otago Regional Council/",
                     "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                     "SWQNationalPicture/DownloadData/RiverWQMonitoringDataNI_LC.csv"))
  }
if(writeOut){
  rrd%>%filter(Region%in%c("canterbury", "southland",  "marlborough","nelson", "otago", "west coast", "tasman"))%>%
  write.csv(.,paste0("C:/users/ericg/Otago Regional Council/",
                     "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                     "SWQNationalPicture/DownloadData/RiverWQMonitoringDataSI_LC.csv"))
}
rm(rrd)

# River water quality State ####
# Region 	Agency	LAWA Site ID	SiteID	CouncilSiteID	Latitude	Longitude	SWQLandUse	SWQAltitude	Catchment	Indicator	5-year median
# State quartile (all sites)	State quartile (same land use)	State quartile (same altitude)	State quartile (same land use and altitude)
# State NOF band	License
riverWQState=tail(dir(path="H:/ericg/16666LAWA/LAWA2021/WaterQuality/",
                    pattern="ITERiverState",recursive=T,full.names = T,ignore.case = T),1)
riverWQState = read_csv(riverWQState)%>%
  dplyr::rename(Indicator=Parameter,LawaSiteID=LawaID)

riverWQState$Altitude = factor(riverWQState$Altitude,
                               levels=c("All","Lowland","Upland"),
                               labels=c("","SameAlt","SameAlt"))
riverWQState$Landuse =  factor(riverWQState$Landuse, 
                               levels=c("All","Forest","Rural","Urban"),
                               labels=c('',rep("SameLU",3)))
riverWQStateW = riverWQState%>%
  dplyr::rename(quartile=StateScore)%>%
  pivot_wider(id_cols = c("LawaSiteID","Indicator"),
              names_from = c("Altitude","Landuse"),
              values_from = c("Median","quartile"))%>%
  select(-Median_SameAlt_,-Median__SameLU,-Median_SameAlt_SameLU)%>%
  dplyr::rename(Median_AllSites=Median__,quartile_AllSites=quartile__)

rm(riverWQState)

riverWQStateW <- merge(x=riverSiteTable%>%filter(Agency!='niwa')%>%
                         dplyr::select(Region,Agency,LawaSiteID,SiteID,CouncilSiteID,
                                       Lat,Long,SWQLanduse,SWQAltitude,recLandcover,Catchment,SedimentClass),all.x=F,
                       y=riverWQStateW%>%select(LawaSiteID,Indicator,Median_AllSites,
                                                quartile_AllSites,quartile__SameLU,
                                                quartile_SameAlt_,quartile_SameAlt_SameLU),all.y=T)

niwasiteTable=riverSiteTable%>%filter(tolower(Agency)=='niwa')

for(colFill in c("Region","Agency","SiteID","CouncilSiteID","Lat","Long","SWQLanduse","SWQAltitude",'recLandcover',"Catchment","SedimentClass")){
  naInCol = is.na(riverWQStateW[,colFill])
  cat(sum(naInCol),'\t')
  if(sum(naInCol)>0){
  riverWQStateW[naInCol,colFill] <- niwasiteTable[match(gsub('_niwa','',tolower(riverWQStateW$LawaSiteID[naInCol])),
                                                       tolower(niwasiteTable$LawaSiteID)),colFill]
  }
  rm(naInCol)
}
rm(colFill)

riverWQStateW <- riverWQStateW%>%dplyr::rename(Latitude=Lat,Longitude=Long,WFSLanduse=SWQLanduse,WFSAltitude=SWQAltitude)
riverWQStateW$Indicator[riverWQStateW$Indicator=='BDISC'] <- 'CLAR'


riverNOF=tail(dir(path="H:/ericg/16666LAWA/LAWA2021/WaterQuality/",
                  pattern="ITERiverNOF",recursive=T,full.names = T,ignore.case = T),1)
riverNOF=read_csv(riverNOF,col_types = cols(
  LAWAID = col_character(),
  SiteName = col_character(),
  Year = col_character(),
  Parameter = col_character(),
  Value = col_double(),
  Band = col_character()
))
riverNOF <- riverNOF%>%dplyr::rename(LawaSiteID=LAWAID,Indicator=Parameter,NOFBand=Band)%>%
   dplyr::filter(Indicator%in%c("DIN_Summary_Band","Ammonia_Toxicity_Band","EcoliSummaryband","DRP_Summary_Band","SusSedBand","Nitrate_Toxicity_Band"))%>%
  drop_na(NOFBand)%>%select(-Value)
riverNOF <- riverNOF%>%filter(NOFBand!="NaN")
riverNOF$Indicator = gsub(pattern = '_Band|_band|_|Band|band|Summary|Toxicity',
                         replacement = '',riverNOF$Indicator)
riverNOF$Indicator <- gsub("Ammonia|Ammoniacal","NH4",riverNOF$Indicator)#[riverNOF$Indicator=="Ammonia"] <- "NH4"
riverNOF$Indicator <- gsub("Ecoli","ECOLI",riverNOF$Indicator)#[riverNOF$Indicator=="Ecoli"] <- "ECOLI"
riverNOF$Indicator <- gsub("DRP_","DRP",riverNOF$Indicator)#[riverNOF$Indicator=="DRP_"] <- "DRP"
riverNOF$Indicator <- gsub("DIN_","DIN",riverNOF$Indicator)#[riverNOF$Indicator=="DRP_"] <- "DRP"
riverNOF$Indicator <- gsub("SusSed","CLAR",riverNOF$Indicator)#[riverNOF$Indicator=="SusSed"] <- "CLAR"
riverNOF$Indicator <- gsub("Nitrate","NO3N",riverNOF$Indicator)#[riverNOF$Indicator=="SusSed"] <- "CLAR"



riverWQStateW <- merge(x=riverWQStateW,all.x=T,
                      y=riverNOF%>%dplyr::select(-Year,-SiteName),all.y=F)%>%
  select('Region','Agency','LawaSiteID','SiteID','CouncilSiteID','Latitude','Longitude','Catchment',"SedimentClass",
  'WFSLandCover'='WFSLanduse','WFSAltitude','recLandcover','Indicator','median5yr'='Median_AllSites',
  'quartile_AllSites','quartile_LandCover'='quartile__SameLU',
  'quartile_Altitude'='quartile_SameAlt_',
  'quartile_LandCoverAltitude'='quartile_SameAlt_SameLU','NOFBand')
  

# State NOF band	License
if(writeOut){
  write.csv(riverWQStateW,paste0("C:/Users/ericg/Otago Regional Council/",
                                            "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                              "SWQNationalPicture/DownloadData/RiverWQStateResults_LC.csv"),row.names=F)
  }
if(writeOut){
  write.csv(riverWQStateW%>%filter(Region%in%c("auckland", "bay of plenty", "gisborne","wellington", "hawkes bay", "manawatu-whanganui",  "waikato", "taranaki","northland")),
                      paste0("C:/Users/ericg/Otago Regional Council/",
                             "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                             "SWQNationalPicture/DownloadData/RiverWQStateResultsNI_LC.csv"),row.names=F)
  }
if(writeOut){
  write.csv(riverWQStateW%>%filter(Region%in%c("canterbury", "southland",  "marlborough","nelson", "otago", "west coast", "tasman")),
                      paste0("C:/Users/ericg/Otago Regional Council/",
                             "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                             "SWQNationalPicture/DownloadData/RiverWQStateResultsSI_LC.csv"),row.names=F)
  }
riverWQStateW%>%split(.$Region)%>%purrr::map(~{
  if(writeOut)write.csv(.,paste0("C:/Users/ericg/Otago Regional Council/",
                                 "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                                 "SWQNationalPicture/DownloadData/RiverWQStateResults",unique(.$Region),"_LC.csv"),
                        row.names=F)
  })

rm(riverNOF,riverWQStateW)



# River water quality Trends ####
# Region Name	Agency	LAWA Site ID	SiteID	CouncilSiteID	Latitude	Longitude	Catchment
# 	Indicator	TrendPeriod	TrendDataFrequency	TrendScore	Trend description	License
riverTrend=tail(dir(path="H:/ericg/16666LAWA/LAWA2021/WaterQuality/",
                      pattern="ITERiverTrend",recursive=T,full.names = T,ignore.case = T),1)
riverTrend <- read_csv(riverTrend)%>%as.data.frame
riverTrend$TrendDescription=factor(riverTrend$TrendScore,levels=c(-99,2,1,0,-1,-2),
                                   labels=c("Not determined",
                                            "Very likely improving","Likely improving",
                                            "Indeterminate",
                                            "Likely degrading","Very likely degrading"))
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

riverTrend <- riverTrend%>%select(Region,Agency,LawaSiteID,SiteID,CouncilSiteID,Latitude,Longitude,Catchment,
                                  WFSLanduse,WFSAltitude,recLandcover,Indicator,TrendPeriod,
                                  TrendDataFrequency,TrendScore,TrendDescription)

if(writeOut){
  write.csv(riverTrend,paste0("C:/Users/ericg/Otago Regional Council/",
                              "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                              "SWQNationalPicture/DownloadData/RiverWQTrendResults_LC.csv"),row.names=F)}
if(writeOut){
  write.csv(riverTrend%>%filter(Region%in%c("auckland", "bay of plenty", "gisborne","wellington", "hawkes bay", "manawatu-whanganui",  "waikato", "taranaki","northland")),
            paste0("C:/Users/ericg/Otago Regional Council/",
                   "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                   "SWQNationalPicture/DownloadData/RiverWQTrendResultsNI_LC.csv"),row.names=F)}
if(writeOut){
  write.csv(riverTrend%>%filter(Region%in%c("canterbury", "southland",  "marlborough","nelson", "otago", "west coast", "tasman")),
            paste0("C:/Users/ericg/Otago Regional Council/",
                   "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                   "SWQNationalPicture/DownloadData/RiverWQTrendResultsSI_LC.csv"),row.names=F)}
riverTrend%>%split(.$Region)%>%purrr::map(~{
  if(writeOut)write.csv(.,paste0("C:/Users/ericg/Otago Regional Council/",
                                 "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                                 "SWQNationalPicture/DownloadData/RiverWQTrendResults",unique(.$Region),"_LC.csv"),row.names=F)}
)
rm(riverTrend)



  
#Rolling NOF results ####
NOFSummaryTable=read.csv(tail(dir(path = "h:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis/",
                                  pattern="NOFSummaryTable_Rolling",
                                  recursive=T,full.names = T),1),stringsAsFactors = F)
NOFSummaryTable <- NOFSummaryTable%>%filter(!Year%in%c('2004to2008','2005to2009','2006to2010'))%>%  #Just, you see this way we're left with a single decade
  # select(LawaSiteID:Year,DIN_Summary_Band,NO3N_Toxicity_Band,TON_Toxicity_Band,Ammonia_Toxicity_Band,EcoliSummaryband,DRP_Summary_Band,SusSedBand,Nitrate_Toxicity_Band,NZReach:AltitudeCl)
select(LawaSiteID:Year,ends_with('Band',ignore.case=T),NZReach:AltitudeCl)
# #Drop out NIWA versions of sites that are monitored by NIWA and by council
# nstSiteCount <- NOFSummaryTable%>%group_by(LawaSiteID)%>%summarise(n=n())%>%ungroup
# NOFSummaryTable$n = nstSiteCount$n[match(NOFSummaryTable$LawaSiteID,nstSiteCount$LawaSiteID)]
# toCut = which(NOFSummaryTable$n==20 & NOFSummaryTable$Agency=='niwa')
# NOFSummaryTable = NOFSummaryTable[-toCut,]


NOFSummaryTable$SWQAltitude = pseudo.titlecase(tolower(NOFSummaryTable$SWQAltitude))
NOFSummaryTable$rawRecLandcover = riverSiteTable$rawRecLandcover[match(tolower(NOFSummaryTable$LawaSiteID),tolower(riverSiteTable$LawaSiteID))]
NOFSummaryTable$rawRecLandcover = factor(NOFSummaryTable$rawRecLandcover,levels=c("if","w","t","s","b","ef", "p", "u"))
NOFSummaryTable$recLandcover=NOFSummaryTable$rawRecLandcover
NOFSummaryTable$recLandcover <- factor(NOFSummaryTable$recLandcover,levels=c("if","w","t","s","b",
                                                                 "ef",
                                                                 "p","m",
                                                                 "u"),
                                 labels=c(rep("Native vegetation",5),
                                          "Plantation forest","Pasture","Pasture","Urban"))

# NOFSummaryTable$DIN_Summary_Band=factor(NOFSummaryTable$DIN_Summary_Band,
#                                         levels=c("D", "C", "B", "A", "NA"),labels=c("D", "C", "B", "A", "NA"))
# NOFSummaryTable$DIN_Summary_Band[is.na(NOFSummaryTable$DIN_Summary_Band)] <- "NA"
# NOFSummaryTable$NO3N_Toxicity_Band=factor(NOFSummaryTable$NO3N_Toxicity_Band,
#                                              levels=c("D", "C", "B", "A", "NA"),labels=c("D", "C", "B", "A", "NA"))
# NOFSummaryTable$NO3N_Toxicity_Band[is.na(NOFSummaryTable$NO3N_Toxicity_Band)] <- "NA"
# NOFSummaryTable$TON_Toxicity_Band=factor(NOFSummaryTable$TON_Toxicity_Band,
#                                           levels=c("D", "C", "B", "A", "NA"),labels=c("D", "C", "B", "A", "NA"))
# NOFSummaryTable$TON_Toxicity_Band[is.na(NOFSummaryTable$TON_Toxicity_Band)] <- "NA"
# NOFSummaryTable$Ammonia_Toxicity_Band=factor(NOFSummaryTable$Ammonia_Toxicity_Band,
#                                              levels=c("D", "C", "B", "A", "NA"),labels=c("D", "C", "B", "A", "NA"))
# NOFSummaryTable$Ammonia_Toxicity_Band[is.na(NOFSummaryTable$Ammonia_Toxicity_Band)] <- "NA"
# NOFSummaryTable$EcoliSummaryband=factor(NOFSummaryTable$EcoliSummaryband,
#                                         levels=c("E","D", "C", "B", "A", "NA"),labels=c("E","D", "C", "B", "A", "NA"))
# NOFSummaryTable$EcoliSummaryband[is.na(NOFSummaryTable$EcoliSummaryband)] <- "NA"
# NOFSummaryTable$DRP_Summary_Band=factor(NOFSummaryTable$DRP_Summary_Band,
#                                         levels=c("D", "C", "B", "A", "NA"),labels=c("D", "C", "B", "A", "NA"))
# NOFSummaryTable$DRP_Summary_Band[is.na(NOFSummaryTable$DRP_Summary_Band)] <- "NA"
# NOFSummaryTable$SusSedBand=factor(NOFSummaryTable$SusSedBand,
#                                         levels=c("D", "C", "B", "A", "NA"),labels=c("D", "C", "B", "A", "NA"))
# NOFSummaryTable$SusSedBand[is.na(NOFSummaryTable$SusSedBand)] <- "NA"
# 
# NOFSummaryTable$Nitrate_Toxicity_Band=factor(NOFSummaryTable$Nitrate_Toxicity_Band,
#                                              levels=c("D", "C", "B", "A", "NA"),labels=c("D", "C", "B", "A", "NA"))
# NOFSummaryTable$Nitrate_Toxicity_Band[is.na(NOFSummaryTable$Nitrate_Toxicity_Band)] <- "NA"

NOFSummaryTable$Long=riverSiteTable$Long[match(tolower(NOFSummaryTable$LawaSiteID),tolower(riverSiteTable$LawaSiteID))]
NOFSummaryTable$Lat=riverSiteTable$Lat[match(tolower(NOFSummaryTable$LawaSiteID),tolower(riverSiteTable$LawaSiteID))]

NOFTableOut = NOFSummaryTable%>%select(LawaSiteID:Year,NZReach:Long,
                                       WFSLanduse=Landcover,WFSAltitude=AltitudeCl,recLandcover,
                                       everything())%>%rename(ClarityBand=SusSedBand)
which(apply(NOFTableOut[,14:19],1,FUN=function(x)all(x=="NA")))->toCut
if(length(toCut)>0){
  NOFTableOut = NOFTableOut[-toCut,]
}
rm(toCut,NOFSummaryTable)
if(writeOut){
  write.csv(NOFTableOut,paste0("C:/Users/ericg/Otago Regional Council/",
                                         "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                                         "SWQNationalPicture/DownloadData/RiverNOFHistory_LC.csv"),row.names=F)
  }




################################################################# ####
#MACROS ####
macroSiteTable <- loadLatestSiteTableMacro()
macroSiteTable$Agency=factor(macroSiteTable$Agency,
                             levels=c("ac", "boprc", "ecan", "es", "gdc", "gwrc", "hbrc", "hrc", 
                                      "mdc", "ncc", "niwa", "nrc", "orc", "tdc", "trc", "wcrc", "wrc"),
                             labels=c("Auckland Council","Bay of Plenty Regional Council","Environment Canterbury","Environment Southland","Gisborne District Council","Greater Wellington Regional Council","Hawkes Bay Regional Council","Horizons Regional Council",
                                      "Marlborough District Council","Nelson City Council","NIWA","Northland Regional Council","Otago Regional Council","Tasman District Council","Taranaki Regional Council","West Coast Regional Council","Waikato Regional Council"))

macroSiteTable$recLandcover=macroSiteTable$rawRecLandcover
macroSiteTable$recLandcover <- factor(macroSiteTable$recLandcover,
                                      levels=c("if","w","t","s","b",
                                               "ef",
                                               "p","m",
                                               "u"),
                                      labels=c(rep("Native vegetation",5),
                                               "Plantation forest",
                                               "Pasture","Pasture","Urban"))

  
# River Ecology monitoring data (2006-2020) ####					
# Date Data Imported	Region Name	Agency	LAWA Site ID	SiteID	CouncilSiteID	Latitude	Longitude	Catchment
# 	Indicator	Indicator Unit of Measure
macroData=loadLatestDataMacro()
macroData=tail(dir(path="H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/",
                           pattern="ITEMacroHistoric",recursive=T,full.names = T,ignore.case = T),1)
macroData <- read_csv(macroData)%>%
  filter(year(CollectionDate)>=2006 & year(CollectionDate)<=2020)%>%as.data.frame

# #Drop NIWA versions of dual-monitored sites?
# mdsc = macroData%>%group_by(LAWAID)%>%summarise(nAge=length(unique(Agency)))%>%ungroup
# macroData$nAge = mdsc$nAge[match(macroData$LAWAID,mdsc$LAWAID)]
# toCut = which(macroData$nAge==2&macroData$Agency=='niwa')
# macroData <- macroData[-toCut,]
# 
# rm(toCut)

macroData <- merge(x=macroSiteTable%>%
                     dplyr::select(Region,LawaSiteID,SiteID,CouncilSiteID,
                                   Latitude=Lat,Longitude=Long,Catchment,recLandcover),all.x=F,
                   y=macroData%>%
                     transmute(LawaSiteID=LAWAID,Agency=Agency,Date=CollectionDate,
                               Indicator=Metric,Value=Value),all.y=T)

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
macroData <- macroData%>%select(Region,Agency,LawaSiteID,SiteID,CouncilSiteID,
                                Latitude,Longitude,Catchment,recLandcover,Indicator,Date,Value)
macroData$Region[is.na(macroData$Region)&macroData$Agency=='nrc'] <- "northland"
if(writeOut){write.csv(macroData,paste0("C:/Users/ericg/Otago Regional Council/",
                                        "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                                        "SWQNationalPicture/DownloadData/RiverEcologyMonitoringData_LC.csv"),
                       row.names=F)}

if(writeOut){write.csv(macroData%>%filter(Region%in%c("auckland", "bay of plenty", "gisborne","wellington", "hawkes bay", "manawatu-whanganui",  "waikato", "taranaki","northland")),
                       paste0("C:/Users/ericg/Otago Regional Council/",
                              "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                              "SWQNationalPicture/DownloadData/RiverEcologyMonitoringDataNI_LC.csv"),row.names=F)}

if(writeOut){write.csv(macroData%>%filter(Region%in%c("canterbury", "southland",  "marlborough","nelson", "otago", "west coast", "tasman")),
                       paste0("C:/Users/ericg/Otago Regional Council/",
                              "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                              "SWQNationalPicture/DownloadData/RiverEcologyMonitoringDataSI_LC.csv"),row.names=F)}

macroData%>%split(.$Region)%>%purrr::map(~{
  if(writeOut)write.csv(.,paste0("C:/Users/ericg/Otago Regional Council/",
                                 "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                                 "SWQNationalPicture/DownloadData/RiverEcologyMonitoringData",
                                 unique(.$Region),"_LC.csv"),row.names=F)
})



# River ecology STATE results (includes 5-year median,  State NOF band)		####									
# Region Name	Agency	LAWA Site ID	SiteID	CouncilSiteID	Latitude	Longitude	Catchment	Indicator	5-year median	State NOF band	License
# MCI score only	
macroState=tail(dir(path="H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/",
                    pattern="ITEMacroState",recursive=T,full.names = T,ignore.case = T),1)
macroState = read_csv(macroState)%>%as.data.frame
macroState$NOFBand=NA
mciDat=macroState$Parameter=="MCI"
macroState$NOFBand[mciDat] <- cut(x = macroState$Median[mciDat],
                                  breaks=c(0,90,110,130,200),right=F)
rm(mciDat)
qmciDat=macroState$Parameter=="QMCI"
macroState$NOFBand[qmciDat]=cut(macroState$Median[qmciDat],
                               breaks = c(-1,4.5,5.5,6.5,20),right=F)
rm(qmciDat)
aspmDat=macroState$Parameter=="ASPM"
macroState$NOFBand[aspmDat]=cut(macroState$Median[aspmDat],
                                breaks = c(-1,0.3,0.4,0.6,1),right=F)
rm(aspmDat)



macroState <- merge(x=macroSiteTable%>%dplyr::select(Agency,Region,LawaSiteID,SiteID,CouncilSiteID,
                                                     Latitde=Lat,Longitude=Long,Catchment,recLandcover),all.x=F,
                    y=macroState%>%dplyr::transmute(LawaSiteID=LAWAID,Indicator=Parameter,Median,NOFBand),all.y = T)
macroState$Agency=as.character(macroState$Agency)

noAge=is.na(macroState$Agency)
if(sum(noAge)>0){
  macroState$Agency[noAge] <- macroData$Agency[match(macroState$LawaSiteID[noAge],macroData$LawaSiteID)]
}
rm(noAge)
noReg=is.na(macroState$Region)
if(sum(noReg)>0){
  macroState$Region[noReg] <- macroData$Region[match(macroState$LawaSiteID[noReg],macroData$LawaSiteID)]
}
rm(noReg)
noSID=is.na(macroState$SiteID)
if(sum(noSID)>0){
  macroState$SiteID[noSID] <- macroData$SiteID[match(macroState$LawaSiteID[noSID],macroData$LawaSiteID)]
}
rm(noSID)
naCSID=is.na(macroState$CouncilSiteID)
if(sum(naCSID)>0){
  macroState$CouncilSiteID[naCSID] <- macroData$CouncilSiteID[match(gsub('_niwa','',macroState$LawaSiteID[naCSID]),
                                                                      macroData$LawaSiteID)]
}
rm(naCSID)
if(writeOut){
  write.csv(macroState,paste0("C:/Users/ericg/Otago Regional Council/",
                              "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                           "SWQNationalPicture/DownloadData/RiverEcologyStateResults_LC.csv"),row.names=F)
  write.csv(macroState%>%filter(Region%in%c("auckland", "bay of plenty", "gisborne",
                                            "wellington", "hawkes bay", "manawatu-whanganui",
                                            "waikato", "taranaki","northland")),
                      paste0("C:/Users/ericg/Otago Regional Council/",
                             "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                             "SWQNationalPicture/DownloadData/RiverEcologyStateResultsNI_LC.csv"),row.names=F)
  write.csv(macroState%>%filter(Region%in%c("canterbury", "southland",  "marlborough",
                                            "nelson", "otago", "west coast", "tasman")),
                      paste0("C:/Users/ericg/Otago Regional Council/",
                             "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                             "SWQNationalPicture/DownloadData/RiverEcologyStateResultsSI_LC.csv"),row.names=F)
}
macroState%>%split(.$Region)%>%purrr::map(~{
  if(writeOut)write.csv(.,paste0("C:/Users/ericg/Otago Regional Council/",
                                 "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                                 "SWQNationalPicture/DownloadData/RiverEcologyStateResults",
                                 unique(.$Region),"_LC.csv"),row.names=F)
  })



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
                               labels=c("Not determined",
                                        "Very likely improving","Likely improving",
                                        "Indeterminate",
                                        "Likely degrading","Very likely degrading"))
mTrend$TrendDescription15y=factor(mTrend$Trend15y,levels=c(-99,2,1,0,-1,-2),
                                  labels=c("Not determined",
                                           "Very likely improving","Likely improving",
                                           "Indeterminate",
                                           "Likely degrading","Very likely degrading"))

mTrend$LawaSiteID = gsub('_niwa','',tolower(mTrend$LawaSiteID))
mTrend <- merge(x=macroSiteTable%>%select(Region,Agency,LawaSiteID,SiteID,CouncilSiteID,
                                          Latitude=Lat,Longitude=Long,Catchment,recLandcover),all.x=F,
                y=mTrend%>%select(LawaSiteID,Indicator,
                                  Trend10y,TrendDescription10y,Trend15y,TrendDescription15y),all.y=T)
mTrend$Agency=as.character(mTrend$Agency)
mTrend$Agency[is.na(mTrend$Agency)] <- macroData$Agency[match(mTrend$LawaSiteID[is.na(mTrend$Agency)],macroData$LawaSiteID)]
mTrend$Region[is.na(mTrend$Region)] <- macroData$Region[match(mTrend$LawaSiteID[is.na(mTrend$Region)],macroData$LawaSiteID)]
mTrend$SiteID[is.na(mTrend$SiteID)] <- macroData$SiteID[match(mTrend$LawaSiteID[is.na(mTrend$SiteID)],macroData$LawaSiteID)]
mTrend$CouncilSiteID[is.na(mTrend$CouncilSiteID)] <- macroData$CouncilSiteID[match(mTrend$LawaSiteID[is.na(mTrend$CouncilSiteID)],macroData$LawaSiteID)]
mTrend$Latitude[is.na(mTrend$Latitude)] <- macroData$Latitude[match(mTrend$LawaSiteID[is.na(mTrend$Latitude)],macroData$LawaSiteID)]
mTrend$Longitude[is.na(mTrend$Longitude)] <- macroData$Longitude[match(mTrend$LawaSiteID[is.na(mTrend$Longitude)],macroData$LawaSiteID)]
mTrend$Catchment[is.na(mTrend$Catchment)] <- macroData$Catchment[match(mTrend$LawaSiteID[is.na(mTrend$Catchment)],macroData$LawaSiteID)]


if(writeOut){
  write.csv(mTrend,paste0("C:/Users/ericg/Otago Regional Council/",
                          "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                          "SWQNationalPicture/DownloadData/RiverEcologyTrendResults_LC.csv"),row.names=F)
  write.csv(mTrend%>%filter(Region%in%c("auckland", "bay of plenty", "gisborne","wellington",
                                        "hawkes bay", "manawatu-whanganui",  "waikato", "taranaki","northland")),
            paste0("C:/Users/ericg/Otago Regional Council/",
                   "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                   "SWQNationalPicture/DownloadData/RiverEcologyTrendResultsNI_LC.csv"),row.names=F)
  write.csv(mTrend%>%filter(Region%in%c("canterbury", "southland",  "marlborough",
                                        "nelson", "otago", "west coast", "tasman")),
            paste0("C:/Users/ericg/Otago Regional Council/",
                   "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                   "SWQNationalPicture/DownloadData/RiverEcologyTrendResultsSI_LC.csv"),row.names=F)
}

mTrend%>%split(.$Region)%>%purrr::map(~{
  if(writeOut)write.csv(.,paste0("C:/Users/ericg/Otago Regional Council/",
                                 "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                                 "SWQNationalPicture/DownloadData/RiverEcologyTrendResults",
                                 unique(.$Region),"_LC.csv"),row.names=F)
})


#Groundwater ####
siteTab = read_csv("h:/ericg/16666LAWA/LAWA2021/Groundwater/Metadata/SiteTable.csv")
siteTab$Agency=factor(siteTab$Source,levels=c("Auckland","Bay of Plenty","Canterbury",
                                              "Gisborne","Hawkes Bay","Manawatu",
                                              "Marlborough","Northland","Otago",
                                              "Southland","Taranaki","Tasman",
                                              "Waikato","Wellington","West Coast"),
                             labels=c("Auckland Council","Bay of Plenty Regional Council","Environment Canterbury",
                                      "Gisborne District Council","Hawkes Bay Regional Council","Horizons Regional Council",
                                      "Marlborough District Council","Northland Regional Council","Otago Regional Council",
                                      "Environment Southland","Taranaki Regional Council","Tasman District Council",
                                      "Waikato Regional Council","Greater Wellington Regional Council","West Coast Regional Council"))

siteTab <- siteTab%>%rename(LAWASiteID=LAWA_ID,Region=Source,LAWAWellName=Site_ID)%>%select(-RC_ID)


#Monitoring results
GWdata = read_csv(tail(dir('h:/ericg/16666LAWA/LAWA2021/Groundwater/Data/',
                           'GWdata.csv',recursive=T,full.names=T),1),guess_max = 5000)
GWdata$Agency=factor(GWdata$Region,levels=c("Auckland","Bay of Plenty","Canterbury",
                                              "Gisborne","Hawkes Bay","Manawatu",
                                              "Marlborough","Northland","Otago",
                                              "Southland","Taranaki","Tasman",
                                              "Waikato","Wellington","West Coast"),
                      labels=c("Auckland Council","Bay of Plenty Regional Council","Environment Canterbury",
                               "Gisborne District Council","Hawkes Bay Regional Council","Horizons Regional Council",
                               "Marlborough District Council","Northland Regional Council","Otago Regional Council",
                               "Environment Southland","Taranaki Regional Council","Tasman District Council",
                               "Waikato Regional Council","Greater Wellington Regional Council","West Coast Regional Council"))

GWdata <- GWdata%>%rename(LAWASiteID=LAWA_ID,Indicator=Variable)%>%filter(Year<2021)
GWMonData = merge(GWdata,siteTab%>%select(LAWASiteID,LAWAWellName))%>%
  select(Region,Agency, LAWASiteID, LAWAWellName, Latitude, Longitude,  Indicator,
         Units=Variable_units, Date, RawValue='Result-raw', CenType,CensoredValue='Result-edited')


# Region agency LAWASiteID LAWAWellName Latitude Longitude GWQZone Indicator Units Date RawValue Sumbole CensoredValue
c("plenty|gisborne|wellington|hawke|whanganui|waikato|taranaki|northland")
c("canterbury|southland|marlborough|nelson|otago|west coast|tasman")
write.csv(GWMonData,paste0("C:/Users/ericg/Otago Regional Council/",
                        "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                        "GWQNationalPicture/DownloadData/GWQMonitoringResults.csv"),row.names=F)
write.csv(GWMonData[grep(pattern = c("auckland|plenty|gisborne|wellington|hawke|manawatu|waikato|taranaki|northland"),
                         x = trimws(gsub('region','',tolower(GWMonData$Region)))),],
          paste0("C:/Users/ericg/Otago Regional Council/",
                 "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                 "GWQNationalPicture/DownloadData/GWQMonitoringResultsNI.csv"),row.names=F)
write.csv(GWMonData[grep(pattern = c("canterbury|southland|marlborough|nelson|otago|west|tasman"),
                         x = trimws(gsub('region','',tolower(GWMonData$Region)))),],
          paste0("C:/Users/ericg/Otago Regional Council/",
                 "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                 "GWQNationalPicture/DownloadData/GWQMonitoringResultsSI.csv"),row.names=F)
rm(GWMonData,GWdata)

#State results
GWmedians <- read.csv(tail(dir(path='h:/ericg/16666LAWA/LAWA2021/Groundwater/Analysis/',
                               pattern='ITEGWState',full.names = T,recursive = T),1),stringsAsFactors = F)
GWmedians$Units = "g/m3"
GWmedians$Units[GWmedians$Measurement=="E.coli"]="CFU/100mL"
GWmedians$Units[GWmedians$Measurement=='Electrical conductivity/salinity']="\U00B5S/cm"
#Region Agency LAWASiteID LAWAWellName Latitiude Longitude Indicator Units State
GWState = merge(GWmedians%>%rename(LAWASiteID=LawaSiteID),
                siteTab%>%select(LAWASiteID,LAWAWellName,Region,Agency,Latitude,Longitude))%>%
  select(Region,Agency,LAWASiteID,LAWAWellName,Latitude,Longitude,Indicator=Measurement,Units, State=StateVal)

GWState$State[GWState$Indicator=="E.coli" & GWState$State==1] <- "Detected"
GWState$State[GWState$Indicator=="E.coli" & GWState$State==2] <- "NotDetected"

write.csv(GWState,paste0("C:/Users/ericg/Otago Regional Council/",
                           "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                           "GWQNationalPicture/DownloadData/GWQStateResults.csv"),row.names=F)
write.csv(GWState[grep(pattern = c("auckland|plenty|gisborne|wellington|hawke|manawatu|waikato|taranaki|northland"),
                       x = trimws(gsub('region','',tolower(GWState$Region)))),],
          paste0("C:/Users/ericg/Otago Regional Council/",
                 "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                 "GWQNationalPicture/DownloadData/GWQStateResultsNI.csv"),row.names=F)
write.csv(GWState[grep(pattern = c("canterbury|southland|marlborough|nelson|otago|west|tasman"),
                       x = trimws(gsub('region','',tolower(GWState$Region)))),],
          paste0("C:/Users/ericg/Otago Regional Council/",
                 "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                 "GWQNationalPicture/DownloadData/GWQStateResultsSI.csv"),row.names=F)
rm(GWState,GWmedians)


#Trend results
 GWtrends <- read_csv(tail(dir("h:/ericg/16666LAWA/LAWA2021/Groundwater/Analysis/",
                               "ITEGWTrend[[:digit:]]",full.names=T,recursive = T,ignore.case = T),1))
 GWtrends <- GWtrends%>%rename(LAWASiteID=LawaSiteID,Indicator=Measurement,TrendPeriod=period,TrendDescription=ConfCat)
#Region Agency LAWASiteID LAWAWellName Latitude Longitude Indicator TrendPeriod TrendDescription
GWtrend <- merge(GWtrends,siteTab)%>%
  select(Region, Agency, LAWASiteID, LAWAWellName, Latitude, Longitude, Indicator, TrendPeriod, TrendDescription)
 
 write.csv(GWtrend,paste0("C:/Users/ericg/Otago Regional Council/",
                          "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                          "GWQNationalPicture/DownloadData/GWQTrendResults.csv"),row.names=F)
 write.csv(GWtrend[grep(pattern = c("auckland|plenty|gisborne|wellington|hawke|manawatu|waikato|taranaki|northland"),
                        x = trimws(gsub('region','',tolower(GWtrend$Region)))),],
           paste0("C:/Users/ericg/Otago Regional Council/",
                  "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                  "GWQNationalPicture/DownloadData/GWQTrendResultsNI.csv"),row.names=F)
 write.csv(GWtrend%>%filter(tolower(Region)%in%c("canterbury", "southland",  "marlborough",
                                        "nelson", "otago", "west coast", "tasman")),
           paste0("C:/Users/ericg/Otago Regional Council/",
                  "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                  "GWQNationalPicture/DownloadData/GWQTrendResultsSI.csv"),row.names=F)
 
 
 
 
 
 #Can I do a swim here ####
 load(tail(dir(path="h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data",pattern="RecData2021",recursive = T,full.names = T,ignore.case=T),1),verbose=T)  
 lmsl=read_csv("H:/ericg/16666LAWA/LAWA2021/Metadata/Masterlist of sites in LAWA Umbraco as at 1 June 2021.csv")
 
 recData$Agency = as.character(factor(recData$regionName,
                                      levels=c("bay of plenty","canterbury","gisborne",
                                               "hawkes bay","horizons","marlborough",
                                               "nelson","northland","otago",
                                               "southland","taranaki","tasman",
                                               "waikato","wellington","west coast"),
                                      labels=c("Bay of Plenty Regional Council","Environment Canterbury",
                                               "Gisborne District Council",
                                               "Hawkes Bay Regional Council","Horizons Regional Council",
                                               "Marlborough District Council",
                                               "Nelson City Council","Northland Regional Council","Otago Regional Council",
                                               "Environment Southland","Taranaki Regional Council","Tasman District Council",
                                               "Waikato Regional Council","Greater Wellington Regional Council",
                                               "West Coast Regional Council")))
 
 recData$SwimIcon = "NA"
 recData$SwimIcon[recData$property=="E-coli"] <- 
   as.character(cut(recData$val[recData$property=="E-coli"],
                    breaks = c(0,260,550,Inf),
                    labels = c('green','amber','red')))
 recData$SwimIcon[which(recData$property=="E-coli"&recData$val==0)] <- 'green'
 recData$SwimIcon[recData$property=="Enterococci"] <- 
   as.character(cut(recData$val[recData$property=="Enterococci"],
                    breaks = c(0,140,280,Inf),
                    labels = c('green','amber','red')))
 recData$SwimIcon[which(recData$property=="Enterococci"&recData$val==0)] <- 'green'
 recData$SwimIcon[recData$property=='Cyanobacteria'] <- 
   as.character(factor(recData$val[recData$property=='Cyanobacteria'],
                       levels=c(0,1,2,3),
                       labels=c("No Data","green","amber","red")))
 table(recData$SwimIcon,recData$property,useNA = 'a')
 
 recData$SwimmingGuidelinesTestResultDescription=as.character(factor(recData$SwimIcon,
                                                                     levels=c("green","amber","red"),
                                                                     labels=c("Suitable for swimming","Caution advised",
                                                                              "Unsuitable for swimming")))
 
 recData$siteType[recData$siteType=="Site"] <- "River"
 recData$siteType[recData$siteType=="LakeSite"] <- "Lake"
 recData$siteType[recData$siteType=="Beach"] <- "Coastal"
 

 recData$Latitude = lmsl$Latitude[match(recData$LawaSiteID,lmsl$LAWAID)]
 recData$Longitude = lmsl$Longitude[match(recData$LawaSiteID,lmsl$LAWAID)]
 #Agency  Region  siteName  SiteID  LawaSiteID  SiteType  Latitude  LOngitude  Property  DateCollected  Resample?  Value  SwimIcon  SwimmingSuitability   
 
 rdOut <- recData%>%select(Agency,Region=region,siteName,SiteID,LawaSiteID,siteType,Latitude,Longitude,Property=property,DateCollected=dateCollected,`Resample?`=resample,Value=val,SwimIcon,SwimmingGuidelinesTestResultDescription)
 
 
 write.csv(rdOut,paste0("C:/Users/ericg/Otago Regional Council/",
                        "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                        "CISH summary files/DownloadData/CISHResults.csv"),row.names=F)
 write.csv(rdOut[grep(pattern = c("plenty|gisborne|wellington|hawke|whanganui|waikato|taranaki|northland"),
                      x = trimws(gsub('region','',tolower(rdOut$Region)))),],
           paste0("C:/Users/ericg/Otago Regional Council/",
                  "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                  "CISH summary files/DownloadData/CISHResultsNI.csv"),row.names=F)
 write.csv(rdOut[grep(pattern=c("canterbury|southland|marlborough|nelson|otago|west coast|tasman"),
                      x = trimws(gsub('region','',tolower(rdOut$Region)))),],
           paste0("C:/Users/ericg/Otago Regional Council/",
                  "Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/",
                  "CISH summary files/DownloadData/CISHResultsSI.csv"),row.names=F)
 
 