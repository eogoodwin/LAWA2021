## Load libraries ------------------------------------------------
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(xml2)     ### XML library to write hilltop XML
library(readr)
library(parallel)
library(doParallel)
setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")


agency='ac'
translate <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%filter(Agency==agency)
translate$retName = c("NH3+NH4 as N (mg/l)","E. coli (CFU/100ml)","pH (pH units)",
                      "Dis Rx P (mg/l)","Tot N (mg/l)","NO3+NO2 (mg/l)",
                      "NO3-N (mg/l)","Dissolved Inorganic Nitrogen", "Tot P (mg/l)",
                      "Black Disk (m)","Turb","Field Turb")
                      

siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])


workers = makeCluster(8)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  library(dplyr)
  library(tidyr)
})


for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of',length(sites),'\t')
  dir.create(paste0('D:/LAWA/2021/AC/',make.names(sites[i])),recursive=T)
  foreach(j = 1:length(translate$CallName),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    # for(j in 1:length(translate$CallName)){
    url <- paste0("http://aklc.hydrotel.co.nz:8080/KiWIS/KiWIS?datasource=3",
                  "&Procedure=Sample.Results.LAWA&service=SOS&version=2.0.0&request=GetObservation",
                  "&observedProperty=",translate$CallName[j],
                  "&featureOfInterest=",sites[i],
                  "&temporalfilter=om:phenomenonTime,P25Y/2021-01-01")
    url <- URLencode(url)
    url <- gsub(pattern = '\\+',replacement = '%2B',x = url)
    destFile=paste0("D:/LAWA/2021/AC","/",make.names(sites[i]),'/',make.names(translate$CallName[j]),".xml")
    if(!file.exists(destFile)|file.info(destFile)$size<3500){
      if(exists('Data'))rm(Data)
      dl=download.file(url,destfile=destFile,method='wininet',quiet=T)
      if(!'try-error'%in%attr(dl,'class')){
        Data=xml2::read_xml(destFile)
        Data = xml2::as_list(Data)[[1]]
        if(length(Data)>1){
          RetProperty=attr(Data$observationData$OM_Observation$observedProperty,'title')
          RetCID = strFrom(s = attr(Data$observationData$OM_Observation$featureOfInterest,'href'),
                           c = "stations/")
          while(RetProperty!=translate$retName[j]|RetCID!=sites[i]){
            file.rename(from = destFile,to = paste0('XX',make.names(RetProperty),destFile))
            dl=download.file(url,destfile=destFile,method='libcurl',quiet=T)
            Data=xml2::read_xml(destFile)
            Data = xml2::as_list(Data)[[1]]
            if(length(Data)>0){
              RetProperty=attr(Data$observationData$OM_Observation$observedProperty,'title')
              RetCID = strFrom(s = attr(Data$observationData$OM_Observation$featureOfInterest,'href'),
                               c = "stations/")
            }
          }
        }
      }
    }
    return(NULL)
  }->dummyout
}
stopCluster(workers)
rm(workers)





#Check site and measurement returned
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  for(j in 1:12){
    destFile=paste0("D:/LAWA/2021/AC","/",
                    gsub('\\.','',make.names(sites[i])),'/',
                    make.names(translate$CallName[j]),".xml")
    if(file.exists(destFile)&file.info(destFile)$size>2000){
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
      if(length(Data)>0&&names(Data)[1]!='Error'){
        cat('.')
        RetProperty=attr(Data$observationData$OM_Observation$observedProperty,'title')
        RetCID = strFrom(s = attr(Data$observationData$OM_Observation$featureOfInterest,'href'),
                         c = "stations/")
        if(RetProperty!=translate$retName[j]|RetCID!=sites[i]){
          browser()
        }
      }
    }
  }
}





workers = makeCluster(8)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  library(dplyr)
  library(tidyr)
})

acSWQ=NULL
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of',length(sites),'\t')
  foreach(j = 1:length(translate$CallName),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    destFile=paste0("D:/LAWA/2021/AC","/",make.names(sites[i]),'/',make.names(translate$CallName[j]),".xml")
    if(file.exists(destFile)&file.info(destFile)$size>1000){
    Data=NULL
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
      if(length(Data)>1){
        RetProperty=attr(Data$observationData$OM_Observation$observedProperty,'title')
        RetSID = attr(Data$observationData$OM_Observation$featureOfInterest,'title')
        RetCID = strFrom(s = attr(Data$observationData$OM_Observation$featureOfInterest,'href'),
                         c = "stations/")
        Data=Data$observationData$OM_Observation$result$MeasurementTimeseries
        if('defaultPointMetadata'%in%names(Data)){
          units = attributes(Data$defaultPointMetadata$DefaultTVPMeasurementMetadata$uom)$code
        }else{units='unfound'}
        Data=do.call(rbind, unname(sapply(Data,FUN=function(listItem){
          if("MeasurementTVP"%in%names(listItem)){
            retVal=data.frame(time=listItem$MeasurementTVP$time[[1]],value=listItem$MeasurementTVP$value[[1]])
            if("metadata"%in%names(listItem$MeasurementTVP)){
              retVal$Censored=attr(listItem$MeasurementTVP$metadata$TVPMeasurementMetadata$qualifier,'title')
            }else{
              retVal$Censored="F"
            }
            return(retVal)
          }
        })))
        if(!is.null(Data)){
          cat(translate$CallName[j],'\t')
          Data$measurement=translate$CallName[j]
          Data$retProp=RetProperty
          Data$CouncilSiteID = sites[i]
          Data$RetCID=strFrom(s = RetCID,c="stations/")
          Data$RetSID=RetSID
          Data$Units = units
        }
      }
    }
    rm(destFile)
    return(Data)
  }->siteDat
  if(!is.null(siteDat)){
    # siteDat$CouncilSiteID = sites[i]
    acSWQ=bind_rows(acSWQ,unique(siteDat))
  }
  rm(siteDat)
}
stopCluster(workers)
rm(workers)

table(acSWQ$CouncilSiteID==acSWQ$RetCID)
table(acSWQ$retProp==translate$retName[match(acSWQ$measurement,translate$CallName)])

save(acSWQ,file='acSWQraw.rData')  
#load('acSWQraw.rData')
agency='ac'

qualifiers_added <- unique(acSWQ$Censored)

acSWQ$time=as.character(acSWQ$time)
acSWQ$CouncilSiteID=as.character(acSWQ$CouncilSiteID)
acSWQ$Measurement=as.character(acSWQ$measurement)
acSWQ$value=as.character(acSWQ$value)
# acSWQ$Units=as.character(acSWQ$Units)




acSWQ=data.frame(CouncilSiteID=acSWQ$CouncilSiteID,
                 Date=as.character(format(lubridate::ymd_hms(acSWQ$time),'%d-%b-%y')),
                 Value=acSWQ$value,
                 Measurement=acSWQ$Measurement,
                 Units=acSWQ$Units,
                 Censored=acSWQ$Censored,
                 CenType=F,
                 QC=bitwAnd(as.numeric(acSWQ$Censored),255))

acCLAR = read_csv("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/AC210809_Export_KiWQM_LAWA Streams TurbidityConverted.csv")
acCLAR$SiteID = unname(sapply(acCLAR$`Station name`,function(s)strTo(s,c = '/')))
acCLAR$CouncilSiteID = as.character(acCLAR$`Station number`)
acCLAR$Date = as.character(format(lubridate::dmy_hm(acCLAR$`Date/Time`),'%d-%b-%y'))
acCLAR$Value = as.character(acCLAR$Value)
acCLAR$Measurement = "CLAR"
acCLAR$Units='m'
acCLAR$Censored=as.character(acCLAR$`Parameter quality code`)
acCLAR$CenType=F
acCLAR$QC=acCLAR$`Parameter quality code`

acSWQ = bind_rows(acSWQ,acCLAR%>%select(names(acSWQ)))
rm(acCLAR)




if(0){
  unique(acSWQ$Censored)
  sapply(as.numeric(unique(acSWQ$Censored)),FUN=function(x)bitwAnd(x,255))
  sapply(as.numeric(unique(acSWQ$Censored)),FUN=function(x)bitwAnd(x,511))
  sapply(as.numeric(unique(acSWQ$Censored)),FUN=function(x)bitwAnd(x,1023))
  sapply(as.numeric(unique(acSWQ$Censored)),FUN=function(x)bitwAnd(x,2047))
  sapply(as.numeric(unique(acSWQ$Censored)),FUN=function(x)bitwAnd(x,4095))
  sapply(as.numeric(unique(acSWQ$Censored)),FUN=function(x)bitwAnd(x,8191))
  sapply(as.numeric(unique(acSWQ$Censored)),FUN=function(x)bitwAnd(x,16383))
  sapply(as.numeric(unique(acSWQ$Censored)),FUN=function(x)bitwAnd(x,32767))
  sapply(as.numeric(unique(acSWQ$Censored)),FUN=function(x)bitwAnd(x,32767))-sapply(as.numeric(unique(acSWQ$Censored)),FUN=function(x)bitwAnd(x,255))
  
  sapply(c(10, 16394, 43, 151, 30, 8222, 42, 16426, 16427),
         FUN=function(x)paste(sapply(strsplit(paste(rev(intToBits(x))),""),`[[`,2),collapse=""))
  unique(sapply(sapply(c(10, 16394, 43, 151, 30, 8222, 42, 16426, 16427),
                       FUN=function(x)bitwAnd(x,sum(2^(13:14)))),
                FUN=function(x)paste(sapply(strsplit(paste(rev(intToBits(x))),""),`[[`,2),collapse="")))
}

acSWQ = acSWQ%>%filter(!bitwAnd(as.numeric(Censored),255)%in%c(42,151))
#71506 to 70588



acSWQ$CenType=sapply(as.numeric(acSWQ$Censored),FUN=function(cenCode){
  retVal=""
  if(is.na(cenCode)){return(retVal)}
  if(bitwAnd(2^13,cenCode)==2^13){  #8192
    retVal=paste0(retVal,'>')
  }
  if(bitwAnd(2^14,cenCode)==2^14){ #16384
    retVal=paste0(retVal,'<')
  }
  return(retVal)
})

acSWQ$Censored=FALSE
acSWQ$Censored[acSWQ$CenType!=""] <- TRUE

acSWQ$CenType=as.character(factor(acSWQ$CenType,
                                  levels = c('<','','>'),
                                  labels=c('Left','FALSE','Right')))

translate=rbind(translate,c('ac',"CLAR","BDISC","CLAR"))


table(acSWQ$Measurement)
acSWQ$Measurement = factor(acSWQ$Measurement,levels=translate$CallName,labels = translate$LAWAName)
table(acSWQ$Measurement)

acSWQ <- unique(acSWQ)



write.csv(acSWQ,file = paste0("D:/LAWA/2021/ac.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/ac.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/ac.csv"),
          overwrite = T)
rm(acSWQ)




