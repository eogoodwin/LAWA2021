## Load libraries ------------------------------------------------
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(XML)     ### XML library to write hilltop XML
# require(RCurl)
setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")


agency='ac'
Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName)%>%unname%>%unlist
Measurements=c(Measurements,'WQ Sample')

siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])

acSWQ=NULL
if(exists('Data'))rm(Data)
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of',length(sites),'\t')
  siteDat=NULL
  for(j in 1:length(Measurements)){
    url <- paste0("http://aklc.hydrotel.co.nz:8080/KiWIS/KiWIS?datasource=3",
                  "&Procedure=Sample.Results.LAWA&service=SOS&version=2.0.0&request=GetObservation",
                  "&observedProperty=",Measurements[j],
                  "&featureOfInterest=",sites[i],
                  "&temporalfilter=om:phenomenonTime,P25Y/2021-01-01")
    url <- URLencode(url)
    url <- gsub(pattern = '\\+',replacement = '%2B',x = url)
    dl=try(download.file(url,destfile="D:/LAWA/2021/tmpWQac.xml",method='curl',quiet=T),silent = T)
    Data=xml2::read_xml("D:/LAWA/2021/tmpWQac.xml")
    Data = xml2::as_list(Data)[[1]]
    if(length(Data)>0){
      Data=Data$observationData$OM_Observation$result$MeasurementTimeseries
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
        cat(Measurements[j],'\t')
        Data$measurement=Measurements[j]
        siteDat=rbind(siteDat,Data)
      }
    }
    rm(Data)
  }
  siteDat$CouncilSiteID = sites[i]
  acSWQ=rbind(acSWQ,siteDat)
  rm(siteDat)
}

qualifiers_added <- unique(acSWQ$Censored)

acSWQ$time=as.character(acSWQ$time)
acSWQ$CouncilSiteID=as.character(acSWQ$CouncilSiteID)
acSWQ$Measurement=as.character(acSWQ$Measurement)
acSWQ$value=as.character(acSWQ$value)
# acSWQ$Units=as.character(acSWQ$Units)



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
#65435 to 64588



acSWQ=data.frame(CouncilSiteID=acSWQ$CouncilSiteID,
                 Date=format(lubridate::ymd_hms(acSWQ$time),'%d-%b-%y'),
                 Value=acSWQ$value,
                 Measurement=acSWQ$measurement,
                 Units=NA,
                 Censored=acSWQ$Censored,
                 centype=F,
                 QC=NA)

acSWQ$centype=sapply(as.numeric(acSWQ$Censored),FUN=function(cenCode){
  retVal=""
  if(is.na(cenCode)){return(retVal)}
  if(bitwAnd(2^13,cenCode)==2^13){
    retVal=paste0(retVal,'>')
  }
  if(bitwAnd(2^14,cenCode)==2^14){
    retVal=paste0(retVal,'<')
  }
  return(retVal)
})

acSWQ$Censored=FALSE
acSWQ$Censored[acSWQ$centype!=""] <- TRUE

acSWQ$centype=as.character(factor(acSWQ$centype,
                                  levels = c('<','','>'),
                                  labels=c('Left','FALSE','Right')))

transfers=read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",
                     sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(-Agency)

table(acSWQ$Measurement)
acSWQ$Measurement = factor(acSWQ$Measurement,levels=transfers$CallName,labels = transfers$LAWAName)
table(acSWQ$Measurement)

acSWQ <- merge(acSWQ,siteTable,by='CouncilSiteID')

# By this point, we have all the data downloaded from the council, in a data frame
write.csv(acSWQ,file = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,".csv"),row.names = F)

