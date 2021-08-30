require(xml2)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)
library(parallel)
library(doParallel)

setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")
agency='wrc'

Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",
                           sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName)%>%unname%>%unlist

siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])


#Duplicate series
# 36768 & 42385 NH4
# 9         60
# 42340 44407 DRP
# 59      98

workers = makeCluster(5)
registerDoParallel(workers)

clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})


if(exists("Data"))rm(Data)
wrcSWQ=NULL
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  rm(siteDat)
  foreach(j = 1:length(Measurements),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    destFile=paste0("D:/LAWA/2021/tmp",gsub(' ','',URLencode(Measurements[j],reserved = T)),"WQwrc.xml")
    
    url <- paste0("http://envdata.waikatoregion.govt.nz:8080/KiWIS/KiWIS?datasource=0&service=SOS&agency=LAWA&",
                  "version=2.0&request=GetObservation&procedure=RERIMP.Sample.Results.P",
                  "&featureOfInterest=",sites[i],
                  "&observedProperty=", Measurements[j],
                  "&temporalfilter=om:phenomenonTime,2004-01-01/2021-01-01")
    url <- URLencode(url)
    
    dl=try(download.file(url,destfile=destFile,method='wininet',quiet=T),silent = T)
    if(!'try-error'%in%attr(dl,'class')){
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
    }
    if('try-error'%in%attr(dl,'class')||length(Data)==0){
      url <- paste0("http://envdata.waikatoregion.govt.nz:8080/KiWIS/KiWIS?datasource=0&service=SOS&agency=LAWA&",
                    "version=2.0&request=GetObservation&procedure=WARIMP.Sample.Results.P",
                    "&featureOfInterest=",sites[i],
                    "&observedProperty=", Measurements[j],
                    "&temporalfilter=om:phenomenonTime,2004-01-01/2021-01-01")
      url <- URLencode(url)
      dl=try(download.file(url,destfile=destFile,method='wininet',quiet=T),silent = T)
      if(!'try-error'%in%attr(dl,'class')){
        Data=xml2::read_xml(destFile)
        Data = xml2::as_list(Data)[[1]]
      }else{
        file.remove(destFile)
        rm(destFile)
        return(NULL)
      }
    }
    
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
                              value=unlist(listItem$MeasurementTVP$value))
          }else{
            retVal = data.frame(time=unlist(listItem$MeasurementTVP$time),
                                value=unlist(attr(listItem$MeasurementTVP$metadata$TVPMeasurementMetadata$qualifier,'title')))
          }
          if(!is.null(listItem$MeasurementTVP$metadata$TVPMeasurementMetadata$qualifier)){
            retVal$qual=attr(listItem$MeasurementTVP$metadata$TVPMeasurementMetadata$qualifier,'title')
          }else{
            retVal$qual=NA
          }
          return(retVal)
        }
      })))
      if(!is.null(Data)){
        Data$measurement=Measurements[j]
        Data$Units=uom
      }
    }else(Data=NULL)
    file.remove(destFile)
    rm(destFile)
    return(Data)
  }->siteDat
  
  if(!is.null(siteDat)){
    rownames(siteDat) <- NULL
    siteDat$CouncilSiteID = sites[i]
    wrcSWQ=bind_rows(wrcSWQ,siteDat)
  }
  rm(siteDat)
}
stopCluster(workers)
rm(workers)



save(wrcSWQ,file = 'wrcSWQraw.rData')
# load('wrcSWQraw.rData')
agency='wrc'

# wrcSWQ <- wrcSWQ%>%filter(!qual%in%c())

#PK at WRC says bit posns 13 and 14 are < and >
table(bitwAnd(2^13,as.numeric(wrcSWQ$qual))==2^13)
table(wrcSWQ$measurement,bitwAnd(2^13,as.numeric(wrcSWQ$qual))==2^13)

# if(bitwAnd(2^13,as.numeric(GWdata$qual))==2^13){}
# if(bitwAnd(2^14,as.numeric(GWdata$qual))==2^14){}
# 
# CensLeft = which(bitwAnd(2^14,as.numeric(GWdata$qual))==2^14)  
# CensRight = which(bitwAnd(2^13,as.numeric(GWdata$qual))==2^13)  
# 
# if(length(CensLeft)>0){
#   with(GWdata[CensLeft,],table(Variable_aggregated,`Result-raw`))
#   GWdata$`Result-raw`[CensLeft]=paste0('<',GWdata$`Result-raw`[CensLeft])
#   GWdata$`Result-prefix`[CensLeft]='<'
# }
# rm(CensLeft)
# 
# if(length(CensRight)>0){
#   GWdata$`Result-raw`[CensRight]=paste0('>',GWdata$`Result-raw`[CensLeft])
#   GWdata$`Result-prefix`[CensLeft]='>'
# }
# rm(CensRight)




by(data = wrcSWQ,INDICES = wrcSWQ$measurement,FUN=function(d){
   table(d$qual[as.numeric(d$value)==min(as.numeric(d$value),na.rm=T)])})

by(data = wrcSWQ,INDICES = wrcSWQ$measurement,FUN=function(d){
  table(d$qual[as.numeric(d$value)==max(as.numeric(d$value),na.rm=T)])})

wrcSWQ <- wrcSWQ%>%select(CouncilSiteID,Date=time,Value=value,Measurement=measurement,Units,qual)

sort(as.numeric(unique(wrcSWQ$qual)))
bitops::bitAnd(sort(as.numeric(unique(wrcSWQ$qual))),255)
wrcSWQ$bcode=bitops::bitAnd(as.numeric(wrcSWQ$qual),255)

by(data = wrcSWQ,INDICES = wrcSWQ$Measurement,FUN=function(d){
  table(d$bcode[as.numeric(d$value)==min(as.numeric(d$value),na.rm=T)])})

by(data = wrcSWQ,INDICES = wrcSWQ$Measurement,FUN=function(d){
  table(d$bcode[as.numeric(d$value)==max(as.numeric(d$value),na.rm=T)])})



bitops::bitAnd(1034,255)  #10
bitops::bitAnd(9426,255)  #210



wrcSWQb=data.frame(CouncilSiteID=wrcSWQ$CouncilSiteID,
                   Date=as.character(format(lubridate::ymd_hms(wrcSWQ$Date),'%d-%b-%y')),
                   Value=wrcSWQ$Value,
                   Measurement=wrcSWQ$Measurement,
                   Units=wrcSWQ$Units,
                   Censored=grepl(pattern = '<|>',x = wrcSWQ$Value),
                   CenType=F,QC=NA)
wrcSWQb$CenType[grep('<',wrcSWQb$Value)] <- 'Left'
wrcSWQb$CenType[grep('>',wrcSWQb$Value)] <- 'Right'

wrcSWQb$Value = readr::parse_number(wrcSWQb$Value)


translate=read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName,LAWAName)


table(wrcSWQb$Measurement,useNA = 'a')
wrcSWQb$Measurement <- as.character(factor(wrcSWQb$Measurement,
                                           levels = translate$CallName,
                                           labels = translate$LAWAName))
table(wrcSWQb$Measurement,useNA='a')

wrcSWQb <- unique(wrcSWQb)

# wrcSWQb <- merge(wrcSWQb,siteTable,by='CouncilSiteID')


write.csv(wrcSWQb,file = paste0("D:/LAWA/2021/wrc.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/wrc.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/wrc.csv"),
          overwrite = T)
rm(wrcSWQ,wrcSWQb)
