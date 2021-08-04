rm(list=ls())
library(tidyverse)
library(sysfonts)
library(googleVis)
library(xml2)
library(parallel)
library(doParallel)
source('H:/ericg/16666LAWA/LAWA2021/Scripts/LAWAFunctions.R')

lmsl=read_csv("H:/ericg/16666LAWA/LAWA2021/Masterlist of sites in LAWA Umbraco as at 1 June 2021.csv")

ssm = readxl::read_xlsx(tail(dir(path='h:/ericg/16666LAWA/LAWA2021/CanISwimHere/MetaData/',
                                 pattern='SwimSiteMonitoringResults.*.xlsx',
                                 recursive = T,full.names = T),1),
                        sheet=1)%>%as.data.frame%>%unique

ssm$callID =  NA
ssm$callID[!is.na(ssm$TimeseriesUrl)] <- c(unlist(sapply(X = ssm%>%select(TimeseriesUrl),
                                                         FUN = function(x)unlist(strsplit(x,split='&')))))%>%
  grep('featureofinterest',x = .,ignore.case=T,value=T)%>%
  gsub('featureofinterest=','',x=.,ignore.case = T)%>%
  sapply(.,URLdecode)%>%trimws

RegionTable=data.frame(ssm=unique(ssm$Region),
                       wfs=c("bay of plenty","canterbury","gisborne","hawkes bay",
                             "horizons","marlborough","nelson","northland",
                             "otago","southland","taranaki","tasman",
                             "waikato","wellington","west coast"))


EndYear <- lubridate::year(Sys.Date())
StartYear5 <- EndYear - 5 + 1
# firstYear = min(wqdYear,na.rm=T)
firstYear=2005
yr <- paste0(as.character(StartYear5),'to',as.character(EndYear))
rollyrs=which(grepl('to',yr))
nonrollyrs=which(!grepl('to',yr))
reps <- length(yr)

dir.create(paste0("H:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/",format(Sys.Date(),"%Y-%m-%d")),showWarnings = F,recursive = T)
dir.create(paste0("H:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/",format(Sys.Date(),"%Y-%m-%d")),showWarnings = F,recursive = T)


#Simply reload existing dataset ####


load(tail(dir(path="h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data",pattern="RecData2021",recursive = T,full.names = T,ignore.case=T),1),verbose=T)  
recData <- recData%>%filter(property!="WQ sample")
recData$regionName = RegionTable$wfs[match(tolower(recData$region),tolower(RegionTable$ssm))]
recData$regionName[is.na(recData$regionName)] <- tolower(recData$region[is.na(recData$regionName)])


load(tail(dir(path="h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data",pattern="RecMetaData2021",recursive = T,full.names = T,ignore.case=T),1),verbose=T)
recMetaData$region=as.character(factor(tolower(recMetaData$region),
                                       levels=c("canterbury","gisborne","hawke.s","manawatu.whanganui",
                                                "marlborough","nelson","otago","southland","taranaki","wellington"),
                                       labels=c("Canterbury region","Gisborne region","Hawke's Bay region",
                                                "Manawat\u16b-Whanganui region","Marlborough region",
                                                "Nelson region"   ,
                                                "Otago region","Southland region","Taranaki region",
                                                "Wellington region")))
recMetaData$region[which(recMetaData$region== "Manawatu-Whanganui region")] <- "Manawat\u16b-Whanganui region" 
recMetaData$regionName=RegionTable$wfs[match(tolower(recMetaData$region),tolower(RegionTable$ssm))]

prettyBung = which(apply(recMetaData%>%select(-regionName,-siteName,-property,-SampleDate,-dateCollected,-region),
                         1,function(r)all(is.na(r))))
if(length(prettyBung)>0){
  recMetaData <-  recMetaData[-prettyBung,]
}
rm(prettyBung)

recMetaData <- recMetaData%>%plyr::mutate(week=lubridate::week(dateCollected),
                                          month=lubridate::month(dateCollected),
                                          year=lubridate::year(dateCollected),
                                          YW=paste0(year,week))

if(0){
  #8/10/2020 survey where councils have hteir resample metadata ####
  #The oens that said it was in metadata
  for(SSMregion in c("Gisborne region","Hawke's Bay region","Marlborough region","Northland region","Taranaki region","Wellington region")){
    load(tail(dir(path="h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/",pattern=paste0(SSMregion,"metaData.Rdata"),recursive=T,full.names = T),1),verbose=T)
    if(!is.null(recMetaData)){
      eval(parse(text=paste0(make.names(word(SSMregion,1,1)),"MD=recMetaData")))
    }
    rm(recMetaData)
  } 
  
  '2019 info
Auckland
BayOfPlenty
Canterbury           exclusive URL   add ashley enterococci
Gisborne             metadata URL
HawkesBay            metadata URL
Manawatu(Horizons)   exclusive URL
Marlborough          metadata URL
Nelson              "The Nelson LAWA data.hts file only includes routine samples" PFisher 5/10/18
Northland            metadata URL
Southland            "All samples provided are routine". LHayward 9/10/18
Taranaki             metadata URL
Wellington           metadata URL
WestCoast            all data at the supplied URL is routine (because retests are never done!!)
Tasman               replacement file
Waikato              replacement file
Otago                replacement file'
  
  apply(GisborneMD,2,function(x)any(grepl('re-sampl|resampl|follow[^ing]',x,ignore.case=T)))%>%which  #SampleEventType, Comments
  apply(Hawke.sMD,2,function(x)any(grepl('re-sampl|resampl|follow[^ing]',x,ignore.case=T)))%>%which   #Puddle Comment
  apply(MarlboroughMD,2,function(x)any(grepl('re-sampl|resampl|follow[^ing]',x,ignore.case=T)))%>%which #Project, Other Comments
  apply(NorthlandMD,2,function(x)any(grepl('re-sampl|resampl|follow[^ing]',x,ignore.case=T)))%>%which  #Run type, comment
  apply(Taranaki,2,function(x)any(grepl('re-sampl|resampl|follow[^ing]',x,ignore.case=T)))%>%which
  apply(WellingtonMD,2,function(x)any(grepl('re-sampl|resampl|follow[^ing]',x,ignore.case=T)))%>%which  #Sample Type, comment/
  
  
  
  #Which metadata columns contain "retest" or "routine" or "resample"
  apply(recMetaData,2,function(x)any(grepl('re-sampl|resampl|follow[^ing]',x,ignore.case=T)))%>%which
  
  unique(recMetaData$`Sample Type`)
  #End investigation
}


routineResample = recMetaData%>%select(regionName,siteName,dateCollected,YW,
                                       Project, `Project ID`, `Analysts Comments`,
                                       `Sample Type`,
                                       `Sample Comment`,`SampleComment`,
                                       SampleEventType, Comments,`Other Comments`,Comment,
                                       `Sample type`
)%>%unique
routineResample$region = recData$region[match(routineResample$regionName,recData$regionName)]

routineResample$resample = apply(routineResample[,-c(1,2,3)],1,
                                 FUN=function(r)any(grepl('re-sampl|resampl|follow[^ing|ed]|BB-Ex',r,ignore.case = T)))
exceptions = apply(routineResample[,-c(1,2,3,dim(routineResample)[2])],1,
                  FUN=function(r)any(grepl('follow up not needed|no re-*sample|not re-*sam',r,ignore.case=T)))
routineResample$resample[exceptions]=F
rm(exceptions)


#Which column gave the followup info?
clueSource = apply(routineResample[routineResample$resample,-c(1,2,3)],1,
                   FUN=function(s)paste0(names(routineResample)[3+grep('re-sampl|resampl|follow[^ing|ed]|BB-Ex',s,ignore.case = T)],collapse='&'))
#And what was the followup info given by that column?
clue = apply(routineResample[routineResample$resample,-c(1,2,3)],1,
             FUN=function(s)paste0(grep('re-sampl|resampl|follow[^ing|ed]|BB-Ex',s,ignore.case = T,value = T),collapse='&'))

table(routineResample$regionName[routineResample$resample],clueSource)
table(clue,routineResample$regionName[routineResample$resample])


routineResample$clue = ""
routineResample$clue[routineResample$resample] <- paste0(clueSource,": ",clue)
rm(clueSource,clue)

routineResample <- routineResample%>%distinct



unique(recData$region)[unique(recData$region) %in% unique(routineResample$region[routineResample$resample])]
#8  councils have followup clues.  This leaves the following councils unIDed.

unique(recData$region)[!unique(recData$region) %in% unique(routineResample$region[routineResample$resample])] -> knownMissingRegions
"Bay of Plenty region     Nelson region   Southland region    Tasman region   Waikato region   West Coast region"  
#BoP  -  already filtered out, during load
# Lisa Naysmith emailed 8/10/2020              Max McKay responded with an xl file now in h:/ericg/16666Lawa/LAWA2021/CISH/Data/BOPRC 
#Max MacKay updated this 4/8/2021 at h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/BOPRC Recreational Repeats List 2021 - LAWA.xlsx
#
#
#Manawatu whanganui                               Amber Garnett "Followups are already removed from our dataset"
#Nelson have confirmed they have no idea what a routine vs a resample
#Southland
#Tasman emailed anette.becher 8/10/2020           twice-weekly routine, so no followups
#Waikato  Paul K etc                              The data I access through their KiWIS contains only routine samples
#West Coast  emailed Millie Taylor 8/10/2020      she says they don't do followups!


recData$rsy = paste0(recData$regionName,recData$siteName,recData$YW)
routineResample$rsy = paste0(routineResample$regionName,routineResample$siteName,routineResample$YW)

recData$resample = routineResample$resample[match(recData$rsy,routineResample$rsy)]

# recDataB <- merge(all.x=T,x=recData,
#                  all.y=F,y=routineResample%>%select(regionName,siteName,YW,resample,clue)%>%distinct,
#                  by=c('regionName','siteName','YW'))
# recDataB$resample[recDataB$region%in%knownMissingRegions] <- FALSE

rm(routineResample)
table(recData$region,recData$resample,useNA = 'if')%>%addmargins
#                             FALSE  TRUE  <NA>   Sum
#   Bay of Plenty region          0     0  6472  6472
#   Canterbury region         11887   419     2 12308
#   Gisborne region            4301    36   229  4566
#   Hawke's Bay region         5257   226    30  5513
#   Manawatū-Whanganui region 13991     1   191 14183
#   Marlborough region         2424   103     1  2528
#   Nelson region               245     0  1674  1919
#   Otago region               2496    14  1251  3761
#   Southland region           2651     0     3  2654
#   Taranaki                   5154   173     0  5327
#   Tasman region                 0     0  1082  1082
#   Waikato region                0     0  2544  2544
#   Wellington region         12026    20   402 12448
#   West Coast region             0     0  1514  1514
#   Sum                       60432   992 15395 76819



#2020
#                           FALSE  TRUE  <NA>
#   Bay of Plenty region     8473     0     0
# Canterbury region          2138   268   3164
# Gisborne region            3204    36   362
# Hawke's Bay region         3576   234   1379
# Manawatū-Whanganui region  9038     0   0
# Marlborough region         1710   194   726
# Nelson region              1832     0     0
# Northland region           4081   167  1429
# Otago region               1468    10  1847
# Southland region           2478     0     0
# Taranaki region            5229   189     0
# Tasman region              1097     0     0
# Waikato region             2074     0     0
# Wellington region          9103    25  4149
# West Coast region          1444     0     0


sum(recData$resample,na.rm=T)
#13/10/2020  799
#15/10/2020  1063
#16/10/2020 925
#20/10/2020 913
#28/10/2020 913
#29/10/2020 1123
#03/11/2020 1123
#19/11/2020 1123
#02/07/2021  221
#29/7/2021  1073
#3/8/2021 1349
recData$clue[is.na(recData$resample)] <- ""
recData$clue[!recData$resample] <- ""

recData$LawaSiteID = ssm$LawaId[match(tolower(as.character(recData$siteName)),
                                      tolower(make.names(ssm$callID)))]
recData$siteType = ssm$SiteType[match(tolower(recData$LawaSiteID),tolower(ssm$LawaId))]
recData$SiteID = ssm$SiteName[match(recData$siteName,make.names(ssm$callID))]
recData$SiteID = ssm$SiteName[match(recData$siteName,make.names(ssm$callID))]

table(recData$region,recData$resample,useNA='a')


#Make all yearweeks six characters
recData$week = lubridate::week(recData$dateCollected)
recData$week=as.character(recData$week)
recData$week[nchar(recData$week)==1]=paste0('0',recData$week[nchar(recData$week)==1])

recData$year = lubridate::year(recData$dateCollected)

recData$YW=as.numeric(paste0(recData$year,recData$week))

#Create bathing seasons
bs=strTo(recData$dateCollected,'-')
bs[recData$month>6]=paste0(recData$year[recData$month>6],'/',recData$year[recData$month>6]+1)
bs[recData$month<7]=paste0(recData$year[recData$month<7]-1,'/',recData$year[recData$month<7])
recData$bathingSeason=bs
table(recData$bathingSeason)


#Write individual regional files: data from recData prior to removing followups 
uReg=unique(recData$region)
for(reg in seq_along(uReg)){
  toExport=recData%>%dplyr::filter(region==uReg[reg],dateCollected>(Sys.time()-lubridate::years(5)))
  write.csv(toExport,file=paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                                 "/recData_",
                                 gsub(' ','',gsub(pattern = ' region',replacement = '',x = uReg[reg])),
                                 "_unfiltered.csv"),row.names = F)
}
rm(reg,uReg,toExport)

if(0){
  downloadData = recData%>%filter(YW>paste0(StartYear5-1,"25"))%>%
    dplyr::filter(LawaSiteID!='')%>%drop_na(LawaSiteID)%>%
    dplyr::filter((lubridate::yday(dateCollected)>297|month<4))%>% #bathign season months only plus the last week of October. To catch labour weeknd.
    mutate(bathingSeason = factor(bathingSeason))%>%
    select(region,siteName,SiteID,LawaSiteID,siteType,property,dateCollected,resample,value=val)
  downloadData$resample[is.na(downloadData$resample)] <- FALSE
  
  downloadData$SwimIcon = "NA"
  downloadData$SwimIcon[downloadData$property=="E-coli"] <- 
    as.character(cut(downloadData$value[downloadData$property=="E-coli"],
                     breaks = c(0,260,550,Inf),
                     labels = c('green','amber','red')))
  downloadData$SwimIcon[downloadData$property=="Enterococci"] <- 
    as.character(cut(downloadData$value[downloadData$property=="Enterococci"],
                     breaks = c(0,140,280,Inf),
                     labels = c('green','amber','red')))
  downloadData$SwimIcon[downloadData$property=='Cyanobacteria'] <- 
    as.character(factor(downloadData$value[downloadData$property=='Cyanobacteria'],
                        levels=c(0,1,2,3),
                        labels=c("No Data","green","amber","red")))
  table(downloadData$SwimIcon,downloadData$property)
  
  downloadData$siteType[downloadData$siteType=="Site"] <- "River"
  downloadData$siteType[downloadData$siteType=="LakeSite"] <- "Lake"
  downloadData$siteType[downloadData$siteType=="Beach"] <- "Coastal"
  
  downloadData$Latitude = lmsl$Latitude[match(tolower(downloadData$LawaSiteID),tolower(lmsl$LAWAID))]
  downloadData$Longitude = lmsl$Longitude[match(tolower(downloadData$LawaSiteID),tolower(lmsl$LAWAID))]
  
  write.csv(downloadData%>%select(region:siteType,Latitude,Longitude,property:SwimIcon)%>%
              arrange(region,siteName,dateCollected),
            file = paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/",
                          format(Sys.Date(),'%Y-%m-%d'),
                          "/CISHdownloadWeekly",format(Sys.time(),'%Y-%b-%d'),".csv"),row.names = F) #"weekly" because that's the name of the sheet it'll go into
}

table(recData$resample,useNA = 'a')
#   FALSE  TRUE  <NA> 
#   60432   992 15395 

recDataF <- recData%>%filter(!resample|is.na(resample))%>%select(-resample,-clue)
#85483 to 84684 13/10/2020
#82521 to 81458 15/10/2020
#82629 to 81704 16/10/2020
#81395 to 80482 20-10/20
#78289 to 77376 28/10/20
#78288 to 77165 29/10/20
#78208 to 77085 3/11/20
#78151 to 77028 9/11/20
#101846 to 101625 2/7/2021
#77666 to 76593  29/7/2021
#76819 to 75827 05/08/2021


save(recDataF,file = paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/",format(Sys.Date(),'%Y-%m-%d'),
                            "/RecDataF",format(Sys.time(),'%Y-%b-%d'),".Rdata"))



