## Load libraries ------------------------------------------------
require(tidyverse)
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(xml2)     ### XML library to write hilltop XML
require(RCurl)
library(parallel)
library(doParallel)

agency='boprc'
setwd("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/")
Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Metadata/boprcMacro_config.csv",sep=',',header=T,stringsAsFactors = F)%>%
  select(Value)%>%unname%>%unlist

if("WQ Sample"%in%Measurements){
  Measurements=Measurements[-which(Measurements=="WQ Sample")]
}

siteTable=loadLatestSiteTableMacro()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])

workers = makeCluster(6)
registerDoParallel(workers)

clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})
startTime=Sys.time()

foreach(i = 1:length(sites),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
  siteDat=NULL
  for(j in 1:length(Measurements)){
    Data=NULL
    
    url <- paste0("http://sos.boprc.govt.nz/service?",
                  "service=SOS&version=2.0.0&request=GetObservation&",
                  "offering=",URLencode(Measurements[j],reserved = T),
                  "@",sites[i],
                  "&temporalfilter=om:phenomenonTime,P15Y/2021-06-01")
    url <- URLencode(url)
    
    destFile=paste0("D:/LAWA/2021/tmp",sites[i],gsub(' ','',Measurements[j]),"Mbop.xml")
    
    dl=try(download.file(url,destfile=destFile,method='curl',quiet=T),silent = T)
    if(!'try-error'%in%attr(dl,'class')){
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
      if(length(Data)>0){
        Data=Data$observationData$OM_Observation$result$MeasurementTimeseries
        if('defaultPointMetadata'%in%names(Data)){
          metaData = Data$defaultPointMetadata
          uom=attr(metaData$DefaultTVPMeasurementMetadata$uom,'code')
        }else{
          uom='unfound'
        }
        Data=do.call(rbind, unname(sapply(Data,FUN=function(listItem){
          if("MeasurementTVP"%in%names(listItem)){
            retVal=data.frame(time=listItem$MeasurementTVP$time[[1]],value=listItem$MeasurementTVP$value[[1]])
            if("metadata"%in%names(listItem$MeasurementTVP)){
              retVal$Censored=attr(listItem$MeasurementTVP$metadata$TVPMeasurementMetadata$qualifier,'title')
            }else{
              retVal$Censored=F
            }
            return(retVal)
          }
        })))
        if(!is.null(Data)){
          Data$measurement=Measurements[j]
          Data$Units=uom
        }
      }
    }
    file.remove(destFile)
    rm(destFile)
    if(!is.null(Data)){
      siteDat=bind_rows(siteDat,Data)
    }
  }
  if(!is.null(siteDat)){
    rownames(siteDat) <- NULL
    siteDat$CouncilSiteID = sites[i]
  }
  return(siteDat)
}->bopM
Sys.time()-startTime
stopCluster(workers)
rm(workers)




save(bopM,file = 'bopMraw.rData')
#load('bopMraw.rData')


bopM <- bopM%>%select(CouncilSiteID,Date=time,Value=value,Measurement=measurement)

bopMb=data.frame(CouncilSiteID=bopM$CouncilSiteID,
                  Date=as.character(format(lubridate::ymd_hms(bopM$Date),'%d-%b-%y')),
                  Value=bopM$Value,
                  Measurement=bopM$Measurement,
                  Censored=grepl(pattern = '<|>',x = bopM$Value),
                  CenType=F,
                  CollMeth=NA,ProcMeth=NA)

bopMb$CenType[grep('<',bopMb$Value)] <- 'Left'
bopMb$CenType[grep('>',bopMb$Value)] <- 'Right'

bopMb$Value = readr::parse_number(bopMb$Value)




bopMb$measName=bopMb$Measurement
table(bopMb$Measurement,useNA = 'a')
bopMb$Measurement <- as.character(factor(bopMb$Measurement,
                                          levels = Measurements,
                                          labels = c("MCI","PercentageEPTTaxa",'TaxaRichness')))
table(bopMb$Measurement,bopMb$measName,useNA='a')
bopMb <- bopMb%>%select(-measName)

# bopMb <- merge(bopMb,siteTable,by='CouncilSiteID')


write.csv(bopMb,file = paste0("D:/LAWA/2021/boprcM.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/boprcM.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/",
                      format(Sys.Date(),"%Y-%m-%d"),"/boprc.csv"),overwrite = T)
rm(bopM,bopMb)



