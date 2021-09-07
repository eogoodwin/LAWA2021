## Load libraries ------------------------------------------------
require(XML)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)
setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality/")
source("H:/ericg/16666LAWA/LAWA2021/scripts/LAWAFunctions.R")
# https://www.lawa.org.nz/media/18225/nrwqn-monitoring-sites-sheet1-sheet1.pdf
# https://hydro-sos.niwa.co.nz/?service=SOS&version=2.0.0&request=GetCapabilities
agency='niwa'

translate=read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%filter(Agency==agency)%>%select(CallName,LAWAName)
translate$retName=c("Ammonia", "Dissolved Reactive Phosphorus",
                            "Escherichia coli Count",  "pH",
                            "Nitrogen Total  (Water)",  "Nitrite and Nitrate as N","NO3N",
                            "Phosphorous (Total)",  "Turbidity (Nephelom)",
                    "Visual Water Clarity")


 siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])




workers = makeCluster(8)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})


if(exists('Data'))rm(Data)
for(i in 1:length(sites)){
  dir.create(paste0("D:/LAWA/2021/NIWA/",make.names(sites[i])),recursive = T,showWarnings = F)
  cat(sites[i],i,'out of',length(sites),'\n')
  foreach(j = 1:length(translate$CallName),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    url <- paste0("https://hydro-sos.niwa.co.nz/?service=SOS&version=2.0.0&request=GetObservation",
                  "&featureOfInterest=",sites[i],
                  "&ObservedProperty=",URLencode(translate$CallName[j],reserved = T),
                  "&TemporalFilter=om:phenomenonTime,2004-01-01/2021-01-01")
    url <- URLencode(url)
    
    destFile=paste0("D:/LAWA/2021/NIWA/",make.names(sites[i]),"/",make.names(translate$CallName[j]),"WQ.xml")
    if(!file.exists(destFile)|file.info(destFile)$size<3500){
      dl=try(download.file(url,destfile=destFile,method='wininet',quiet=T),silent = T)
      if(!'try-error'%in%attr(dl,'class')){
        Data=xml2::read_xml(destFile)
        Data = xml2::as_list(Data)[[1]]
        if(length(Data)>0&&names(Data)[1]!='Exception'){
          RetProperty=attr(Data$observationData$OM_Observation$observedProperty,'title')
          RetCID = attr(Data$observationData$OM_Observation$featureOfInterest,'href')
          
          while(RetProperty!=translate$retName[j]|RetCID!=sites[i]){
            file.rename(from = destFile,to = paste0('XX',make.names(RetProperty),destFile))
            file.remove(destFile)
            dl=download.file(url,destfile=destFile,method='libcurl',quiet=T)
            Data=xml2::read_xml(destFile)
            Data = xml2::as_list(Data)[[1]]
            if(length(Data)>0){
              RetProperty=attr(Data$observationData$OM_Observation$observedProperty,'title')
              RetCID = attr(Data$observationData$OM_Observation$featureOfInterest,'href')
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



workers = makeCluster(8)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})


NIWASWQ=NULL
if(exists('Data'))rm(Data)
for(i in 1:length(sites)){
  cat(sites[i],i,'out of',length(sites),'\n')
  rm(siteDat)
  foreach(j = 1:length(translate$CallName),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    
    destFile=paste0("D:/LAWA/2021/NIWA/",make.names(sites[i]),"/",make.names(translate$CallName[j]),"WQ.xml")
    if(file.exists(destFile)&&file.info(destFile)$size>2000){
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
      if(length(Data)>0){
        RetProperty=attr(Data$observationData$OM_Observation$observedProperty,'title')
        RetCID = attr(Data$observationData$OM_Observation$featureOfInterest,'href')
        if(RetProperty==translate$retName[j]&RetCID==sites[i]){
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
            Data$Units = units
            Data$RetCID = RetCID
          }
        }else(Data=NULL)
      }else{Data=NULL}
    } else{Data=NULL} 
    rm(destFile)
    return(Data)
  }->siteDat
  if(!is.null(siteDat)){
    siteDat$CouncilSiteID = sites[i]
    NIWASWQ=bind_rows(NIWASWQ,siteDat)
  }
  rm(siteDat)
}
stopCluster(workers)
rm(workers)


table(NIWASWQ$CouncilSiteID==NIWASWQ$RetCID)
table(NIWASWQ$measurement==translate$CallName[match(NIWASWQ$retProp,translate$retName)])

save(NIWASWQ,file="NIWASWQRaw.rData")
# load("NIWASWQRaw.rData")
agency='niwa'



NIWASWQ <- NIWASWQ%>%select(CouncilSiteID,Date=time,Value=value,Measurement=measurement,Units)


NIWASWQb=data.frame(CouncilSiteID=NIWASWQ$CouncilSiteID,
                   Date=as.character(format(lubridate::ymd_hms(NIWASWQ$Date),'%d-%b-%y')),
                   Value=NIWASWQ$Value,
                   Measurement=NIWASWQ$Measurement,
                   Units=NIWASWQ$Units,
                   Censored=grepl(pattern = '<|>',x = NIWASWQ$Value),
                   CenType=F,QC=NA)
NIWASWQb$CenType[grep('<',NIWASWQb$Value)] <- 'Left'
NIWASWQb$CenType[grep('>',NIWASWQb$Value)] <- 'Right'

NIWASWQb$Value = readr::parse_number(NIWASWQb$Value)



table(NIWASWQb$Measurement,useNA = 'a')
NIWASWQb$Measurement <- as.character(factor(NIWASWQb$Measurement,
                                            levels =  translate$CallName,
                                           labels = translate$LAWAName))
table(NIWASWQb$Measurement,useNA='a')

NIWASWQb <- unique(NIWASWQb)


NIWASWQb$Value[NIWASWQb$Measurement%in%c("DRP","NH4","TN","TP","TON")]=NIWASWQb$Value[NIWASWQb$Measurement%in%c("DRP","NH4","TN","TP","TON")]/1000

# NIWASWQb <- merge(NIWASWQb,siteTable,by='CouncilSiteID')


write.csv(NIWASWQb,file = paste0("D:/LAWA/2021/NIWA.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/NIWA.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/NIWA.csv"),
          overwrite = T)
rm(NIWASWQ,NIWASWQb)
