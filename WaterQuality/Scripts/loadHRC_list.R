require(xml2)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)
library(parallel)
library(doParallel)

setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")
agency='hrc'

Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",
                           sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName)%>%unname%>%unlist

siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])




workers = makeCluster(7)
registerDoParallel(workers)

clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})


if(exists("Data"))rm(Data)
hrcSWQ=NULL
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  suppressWarnings({rm(siteDat)})
  foreach(j = 1:length(Measurements),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    url <- paste0("http://tsdata.horizons.govt.nz/boo.hts?service=SOS&agency=LAWA&request=GetObservation",
                  "&FeatureOfInterest=",sites[i],
                  "&ObservedProperty=",Measurements[j],
                  "&TemporalFilter=om:phenomenonTime,2004-01-01,2021-01-01")
    url <- URLencode(url)
    
    destFile=paste0("D:/LAWA/2021/hrcpara/tmp",gsub(' ','',URLencode(Measurements[j],reserved = T)),"WQ",sites[i],"hrc.xml")
    dl=try(download.file(url,destfile=destFile,method='wininet',quiet=T),silent = T)
    if(!'try-error'%in%attr(dl,'class')){
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
      if(length(Data)>0){
        Data=Data$observationMember$OM_Observation$result$MeasurementTimeseries
        if('defaultPointMetadata'%in%names(Data)){
          metaData = Data$defaultPointMetadata
          uom=attr(metaData$DefaultTVPMeasurementMetadata$uom,'code')
        }else{
          uom='unfound'
        }
        Data=do.call(rbind, unname(sapply(Data,FUN=function(listItem){
          if("MeasurementTVP"%in%names(listItem)){
            if(length(listItem$MeasurementTVP$value)>0){
              retVal=data.frame(time=unlist(listItem$MeasurementTVP$time),
                                value=unlist(listItem$MeasurementTVP$value),
                                Censored=F)
            }else{
              retVal = data.frame(time=unlist(listItem$MeasurementTVP$time),
                                  value=unlist(listItem$MeasurementTVP$metadata$TVPMeasurementMetadata$qualifier$Quantity$value),
                                  Censored=T)
            }
            return(retVal)
          }
        })))
        if(!is.null(Data)){
          Data$measurement=Measurements[j]
          Data$Units=uom
        }
        # file.remove(destFile)
      }
     rm(destFile)
    }else{
      Data=NULL
    }
    return(Data)
  }->siteDat
  
  if(!is.null(siteDat)){
    rownames(siteDat) <- NULL
    siteDat$CouncilSiteID = sites[i]
    hrcSWQ=bind_rows(hrcSWQ,siteDat)
  }
  rm(siteDat)
}
stopCluster(workers)
rm(workers)



save(hrcSWQ,file = 'hrcSWQraw.rData')
# load('hrcSWQraw.rData')
agency='hrc'




hrcSWQ <- hrcSWQ%>%select(CouncilSiteID,Date=time,Value=value,Measurement=measurement,Units)


hrcSWQb=data.frame(CouncilSiteID=hrcSWQ$CouncilSiteID,
                   Date=as.character(format(lubridate::ymd_hms(hrcSWQ$Date),'%d-%b-%y')),
                   Value=hrcSWQ$Value,
                   Measurement=hrcSWQ$Measurement,
                   Units=hrcSWQ$Units,
                   Censored=grepl(pattern = '<|>',x = hrcSWQ$Value),
                   CenType=F,QC=NA)
hrcSWQb$CenType[grep('<',hrcSWQb$Value)] <- 'Left'
hrcSWQb$CenType[grep('>',hrcSWQb$Value)] <- 'Right'

hrcSWQb$Value = readr::parse_number(hrcSWQb$Value)


translate=read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName,LAWAName)


table(hrcSWQb$Measurement,useNA = 'a')
hrcSWQb$Measurement <- as.character(factor(hrcSWQb$Measurement,
                                           levels = translate$CallName,
                                           labels = translate$LAWAName))
table(hrcSWQb$Measurement,useNA='a')

hrcSWQb <- unique(hrcSWQb)

# hrcSWQb <- merge(hrcSWQb,siteTable,by='CouncilSiteID')



write.csv(hrcSWQb,file = paste0("D:/LAWA/2021/hrc.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/hrc.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/hrc.csv"),
          overwrite = T)
rm(hrcSWQ,hrcSWQb)


