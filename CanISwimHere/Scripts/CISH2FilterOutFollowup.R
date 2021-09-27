rm(list=ls())
library(tidyverse)
library(sysfonts)
library(googleVis)
library(xml2)
library(parallel)
library(doParallel)
source('H:/ericg/16666LAWA/LAWA2021/Scripts/LAWAFunctions.R')

# lmsl=read_csv("H:/ericg/16666LAWA/LAWA2021/Masterlist of sites in LAWA Umbraco as at 1 June 2021.csv")

ssm = readxl::read_xlsx(tail(dir(path='h:/ericg/16666LAWA/LAWA2021/CanISwimHere/MetaData/',
                                 pattern='SwimSiteMonitoringResults.*.xlsx',
                                 recursive = T,full.names = T),1),
                        sheet=1)%>%as.data.frame%>%unique
ssm$TimeseriesUrl[ssm$Region=="Bay of Plenty region"] <- gsub(pattern = '@',
                                                              replacement = '&featureOfInterest=',
                                                              x =ssm$TimeseriesUrl[ssm$Region=="Bay of Plenty region"])
ssm$callID =  NA
ssm$callID[!is.na(ssm$TimeseriesUrl)] <- c(unlist(sapply(X = ssm[!is.na(ssm$TimeseriesUrl),]%>%
                                                           select(TimeseriesUrl),
                                                         FUN = function(x){
                                                           unlist(strsplit(x,split='&'))
                                                         })))%>%
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




#Load existing dataset ####

load(tail(dir(path="h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data",pattern="RecData2021",recursive = T,full.names = T,ignore.case=T),1),verbose=T)  
recData <- recData%>%filter(property!="WQ sample")
recData$regionName = RegionTable$wfs[match(tolower(recData$region),tolower(RegionTable$ssm))]
recData$regionName[is.na(recData$regionName)] <- tolower(recData$region[is.na(recData$regionName)])

#Make all yearweeks six characters
recData$wday = wday(recData$dateCollected)
recData$week = lubridate::week(recData$dateCollected)
recData$week=as.character(recData$week)
recData$week[nchar(recData$week)==1]=paste0('0',recData$week[nchar(recData$week)==1])
recData$year = lubridate::year(recData$dateCollected)
recData$YWD=as.numeric(paste0(recData$year,recData$week,recData$wday))


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

recMetaData <- recMetaData%>%plyr::mutate(wday=wday(dateCollected),
                                          week=as.character(lubridate::week(dateCollected)),
                                          month=lubridate::month(dateCollected),
                                          year=lubridate::year(dateCollected))
#Make all yearweeks six characters
recMetaData$week[nchar(recMetaData$week)==1]=paste0('0',recMetaData$week[nchar(recMetaData$week)==1])
recMetaData$YWD=as.numeric(paste0(recMetaData$year,recMetaData$week,recMetaData$wday))



recData <- recData%>%filter(YWD>as.numeric(paste0(StartYear5-1,"251")))
recMetaData <- recMetaData%>%filter(YWD>as.numeric(paste0(StartYear5-1,"251")))





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

# GWRC - WQSample measurement has field "Sample type" Value="R"
routineResample = recMetaData%>%select(regionName,siteName,dateCollected,YWD,
                                       Project, `Project ID`, `Analysts Comments`,
                                       `Sample Type`,
                                       `Sample Comment`,`SampleComment`,
                                       SampleEventType, Comments,`Other Comments`,Comment,
                                       `Sample type`
)%>%unique
routineResample$region = recData$region[match(routineResample$regionName,recData$regionName)]

routineResample$`Sample Comment`[routineResample$regionName=='canterbury'] <- ''

routineResample$resample = apply(routineResample[,-c(1,2,3)],1,
                                 FUN=function(r)any(grepl('re-sampl|resampl|follow[^ing|ed]|BB-Ex',r,ignore.case = T)))
incliptions = which(routineResample$`Sample type`=="F")
routineResample$resample[incliptions]=T
exceptions = apply(routineResample[,-c(1,2,3,dim(routineResample)[2])],1,
                  FUN=function(r)any(grepl('follow *up not samp|follow *up not needed|no re-*sample|not re-*sam',r,ignore.case=T)))
routineResample$resample[exceptions]=F
rm(exceptions,incliptions)


#Which column gave the followup info?
clueSource = apply(routineResample[routineResample$resample,-c(1,2,3)],1,
                   FUN=function(s)paste0(names(routineResample)[3+grep('re-sampl|resampl|follow[^ing|ed]|BB-Ex',s,ignore.case = T)],collapse='&'))
#And what was the followup info given by that column?
clue = apply(routineResample[routineResample$resample,-c(1,2,3)],1,
             FUN=function(s)paste0(grep('re-sampl|resampl|follow[^ing|ed]|BB-Ex',s,ignore.case = T,value = T),collapse='&'))

table(routineResample$regionName[routineResample$resample],clueSource)
# table(clue,routineResample$regionName[routineResample$resample])
clue[routineResample$regionName[routineResample$resample]=="taranaki"]%>%table

routineResample$clue = ""
routineResample$clue[routineResample$resample] <- paste0(clueSource,": ",clue)
rm(clueSource,clue)

routineResample <- routineResample%>%distinct



unique(recData$region)[unique(recData$region) %in% unique(routineResample$region[routineResample$resample])]
#8  councils have followup clues.  This leaves the following councils unIDed.

unique(recData$region)[!unique(recData$region) %in% unique(routineResample$region[routineResample$resample])] -> knownMissingRegions
"Bay of Plenty region     Nelson region   Northland region    Southland region
Tasman region   Waikato region   West Coast region"  
#BoP  -  already filtered out, during load
# Lisa Naysmith emailed 8/10/2020              Max McKay responded with an xl file now in h:/ericg/16666Lawa/LAWA2021/CISH/Data/BOPRC 
#Max MacKay updated this 4/8/2021 at h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/BOPRC Recreational Repeats List 2021 - LAWA.xlsx
#This is done in the load script though, for BoP, at line 650
#
#
#Manawatu whanganui                               Amber Garnett "Followups are already removed from our dataset"
#Nelson have confirmed they have no idea what a routine vs a resample
#Southland
#Tasman emailed anette.becher 8/10/2020           twice-weekly routine, so no followups
#Waikato  Paul K etc                              The data I access through their KiWIS contains only routine samples
#West Coast  emailed Millie Taylor 8/10/2020      she says they don't do followups!


recData$rsy = paste0(recData$regionName,recData$siteName,recData$YWD)
routineResample$rsy = paste0(routineResample$regionName,routineResample$siteName,routineResample$YWD)

recData$resample = routineResample$resample[match(recData$rsy,routineResample$rsy)]

# recDataB <- merge(all.x=T,x=recData,
#                  all.y=F,y=routineResample%>%select(regionName,siteName,YWD,resample,clue)%>%distinct,
#                  by=c('regionName','siteName','YWD'))
# recDataB$resample[recDataB$region%in%knownMissingRegions] <- FALSE

rm(routineResample)
table(recData$region,recData$resample,useNA = 'if')%>%addmargins
#                           FALSE  TRUE  <NA>   Sum
# Bay of Plenty region          0     0  9359  9359
# Canterbury region          8757   453     7  9217
# Gisborne region            4183    51     0  4234
# Hawke's Bay region         3732   233    38  4003
#   Manawatū-Whanganui region 13298     1   406 13705
#   Marlborough region         1710   174     1  1885
#   Nelson region                 4     0  1493  1497
#   Northland region              0     0  3958  3958
#   Otago region               1881    10   942  2833
#   Southland region           2087     0     5  2092
#   Taranaki                   3412   161     0  3573
#   Tasman region                 0     0   779   779
#   Waikato region                0     0  2194  2194
#   Wellington region          7557   701    58  8316
#   West Coast region             0     0  1125  1125
#   Sum                       46621  1784 20365 68770



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
#9/8/2021 2250
#13/8/2021 2267
#19/8/21   2223
#15/9/21  1784
#16/9/21 1800
recData$clue[is.na(recData$resample)] <- ""
recData$clue[!recData$resample] <- ""

recData$LawaSiteID = ssm$LawaId[match(tolower(make.names(recData$siteName)),
                                      tolower(make.names(ssm$callID)))]
recData$siteType = ssm$SiteType[match(tolower(recData$LawaSiteID),tolower(ssm$LawaId))]
recData$SiteID = ssm$SiteName[match(recData$siteName,make.names(ssm$callID))]
recData$SiteID = ssm$SiteName[match(recData$siteName,make.names(ssm$callID))]

#This site manually imported Septebmer 2021, you probably can safely delete these three
recData$LawaSiteID[recData$siteName=="JL348334"] <- "EBOP-00049"
recData$siteType[recData$siteName=="JL348334"] <- "Site"
recData$SiteID[recData$siteName=="JL348334"] <- "Rangitāiki at Te Teko"

#Create bathing seasons
bs=strTo(recData$dateCollected,'-')
bs[recData$month>6]=paste0(recData$year[recData$month>6],'/',recData$year[recData$month>6]+1)
bs[recData$month<7]=paste0(recData$year[recData$month<7]-1,'/',recData$year[recData$month<7])
recData$bathingSeason=bs
table(recData$bathingSeason)

#a oneoff for Hayden Rabel, GWRC, Salt Ecology
recData <- recData[-which(recData$siteName=="Ruamahanga.River.at.Waihenga.Bridge"&
                             recData$dateCollected=="2020-11-09 12:40:00"),]

#Save unfiltered data
# save(recData,file = paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/",format(Sys.Date(),'%Y-%m-%d'),
#                             "/RecData",format(Sys.time(),'%Y-%b-%d'),".Rdata"))

#Write individual regional files: data from recData prior to removing followups 
uReg=unique(recData$region)
for(reg in seq_along(uReg)){
  cat(unique(recData$region)[reg],'\n')
  toExport=recData%>%dplyr::filter(region==uReg[reg],dateCollected>(Sys.time()-lubridate::years(5)))
  write.csv(toExport,file=paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                                 "/recData_",
                                 gsub(' ','',gsub(pattern = ' region',replacement = '',x = uReg[reg])),
                                 "_unfiltered.csv"),row.names = F)
}
rm(reg,uReg,toExport)

table(recData$resample,useNA = 'a')
#   FALSE  TRUE  <NA> 
#   46808  1800 19637 

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
#81971 to 79721 09/08/2021
#81900 to 79633 13/8/21
#65232 to 63481
#65649 to 63864 26/8/21
#65650 to 63865 10/9/21
#66919 to 65135 13/9/21
#68019 to 66235 13/9/21
#68128 to 66344 14/9/21
#68770 to 66986 15/9/21
#68096 to 66312 15/9/21 BoP timezone
#68245 to 66445 16/9/21 GWRC addidiont


save(recDataF,file = paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/",format(Sys.Date(),'%Y-%m-%d'),
                            "/RecDataF",format(Sys.time(),'%Y-%b-%d'),".Rdata"))



