require(xml2)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)
library(parallel)
library(doParallel)


setwd("H:/ericg/16666LAWA/LAWA2021/Lakes")
agency='boprc'

Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/Lakes/Metadata/boprcLWQ_config.csv",
                           sep=',',header=T,stringsAsFactors = F)%>%select(Value)%>%unname%>%unlist


siteTable=loadLatestSiteTableLakes()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])


workers = makeCluster(3)
registerDoParallel(workers)

clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})

if(exists("Data"))rm(Data)
options(timeout=5)
boprcLWQ=NULL
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  suppressWarnings({rm(siteDat)})
  foreach(j = 1:length(Measurements),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
   url <- paste0("http://sos.boprc.govt.nz/service?",
                  "service=SOS&version=2.0.0&request=GetObservation&",
                  "offering=",URLencode(Measurements[j],reserved = T),
                  "@",sites[i],
                 "&temporalfilter=om:phenomenonTime,P15Y/2021-01-01")
    url <- URLencode(url)
    destFile=paste0("D:/LAWA/2021/tmp",gsub(' ','',Measurements[j]),"LQboprc.xml")
    dl=try(download.file(url,destfile=destFile,method='curl',quiet=T),silent = T)
    if(!'try-error'%in%attr(dl,'class')){
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
      if(!'exception'%in%tolower(names(Data))){
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
        }
      }else{Data=NULL}
    }else{Data=NULL}
    file.remove(destFile)
    rm(destFile)
    return(Data)
  }->siteDat
  
  if(!is.null(siteDat)){
    rownames(siteDat) <- NULL
    siteDat$CouncilSiteID = sites[i]
    boprcLWQ=bind_rows(boprcLWQ,siteDat)
  }
  rm(siteDat)
}

stopCluster(workers)
rm(workers)



save(boprcLWQ,file = 'boprcLWQraw.rData')
agency='boprc'
# load('boprcLWQraw.rData')


boprcLWQ <- boprcLWQ%>%select(CouncilSiteID,Date=time,Value=value,Measurement=measurement,Units)



boprcLWQb=data.frame(CouncilSiteID=boprcLWQ$CouncilSiteID,
                     Date=as.character(format(lubridate::ymd_hms(boprcLWQ$Date),'%d-%b-%y')),
                     Value=boprcLWQ$Value,
                     Method=NA,
                     Measurement=boprcLWQ$Measurement,
                     # Units=boprcLWQ$Units,
                     Censored=grepl(pattern = '<|>',x = boprcLWQ$Value),
                     centype=F,QC=NA,agency='boprc')
boprcLWQb$centype[grep('<',boprcLWQb$Value)] <- 'Left'
boprcLWQb$centype[grep('>',boprcLWQb$Value)] <- 'Right'

boprcLWQb$Value = readr::parse_number(boprcLWQb$Value)


Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/Lakes/Metadata/boprcLWQ_config.csv",
                           sep=',',header=T,stringsAsFactors = F)%>%select(Value)%>%unname%>%unlist

table(boprcLWQb$Measurement,useNA = 'a')
boprcLWQb$Measurement <- as.character(factor(boprcLWQb$Measurement,
                                             levels = Measurements,
                                             labels = c("TN","NH4N","TP","CHLA",
                                                        "ECOLI","pH","Secchi")))
table(boprcLWQb$Measurement,useNA='a')

# boprcLWQb <- merge(boprcLWQb,siteTable,by='CouncilSiteID')


write.csv(boprcLWQb,file = paste0("D:/LAWA/2021/boprcL.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/boprcL.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d"),"/boprc.csv"),
          overwrite = T)
rm(boprcLWQ,boprcLWQb)
