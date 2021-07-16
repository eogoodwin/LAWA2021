require(dplyr)   ### dply library to manipulate table joins on dataframes
require(XML)     ### XML library to write hilltop XML
require(RCurl)
source('H:/ericg/16666LAWA/LAWA2021/scripts/LAWAFunctions.R')


agency='ac'

Measurements=c("Chloro a (mg/l)", "Cyanobacteria BioVolume (mm3/L)", "E. coli (CFU/100ml)", 
               "NH3+NH4 as N (mg/l)", "pH (pH units)", "Tot N (mg/l)", "Tot P (mg/l)", 
               "Transpar.-secchi (m)")

siteTable=loadLatestSiteTableLakes(maxHistory = 30)
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])

acLWQ=NULL
setwd("H:/ericg/16666LAWA/LAWA2021/Lakes")
if(exists('Data'))rm(Data)
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of',length(sites),'\n')
  siteDat=NULL
  for(j in 1:length(Measurements)){
    url <- paste0("http://aklc.hydrotel.co.nz:8080/KiWIS/KiWIS?datasource=3&Procedure=Sample.Results.LAWA&",
                  "Service=SOS&version=2.0.0&request=getObservation&", #correct as to 08/07/2021 email VP
                  "&observedProperty=",Measurements[j],
                  "&featureOfInterest=",sites[i],
                  "&temporalfilter=om:phenomenonTime,P25Y/2020-12-31")
    url <- URLencode(url)
    url <- gsub(pattern = '\\+',replacement = '%2B',x = url)
    dl=try(download.file(url,destfile="D:/LAWA/2021/tmpLac.xml",method='curl',quiet=T),silent = T)
    Data=xml2::read_xml("D:/LAWA/2021/tmpLac.xml")
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
      Data$measurement=Measurements[j]
      siteDat=rbind(siteDat,Data)
      }
    }
      rm(Data)
  }
  siteDat$CouncilSiteID = sites[i]
  acLWQ=rbind(acLWQ,siteDat)
}
acLWQ=data.frame(CouncilSiteID=acLWQ$CouncilSiteID,
                 Date=format(lubridate::ymd_hms(acLWQ$time),'%d-%b-%y'),
                 Value=acLWQ$value,
                 Method=NA,
                 Measurement=acLWQ$measurement,
                 Censored=acLWQ$Censored,
                 centype=F,
                 QC=NA)

acLWQ$centype=sapply(as.numeric(acLWQ$Censored),FUN=function(cenCode){
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

acLWQ$Censored=FALSE
acLWQ$Censored[acLWQ$centype!=""] <- TRUE

acLWQ$centype=as.character(factor(acLWQ$centype,
                                  levels = c('<','','>'),
                                  labels=c('Left','FALSE','Right')))

table(acLWQ$Measurement)
acLWQ$Measurement[acLWQ$Measurement == "Transpar.-secchi (m)"] <- "Secchi"
acLWQ$Measurement[acLWQ$Measurement == 'E. coli (CFU/100ml)'] <- "ECOLI"
acLWQ$Measurement[acLWQ$Measurement == 'Tot P (mg/l)'] <- "TP"
acLWQ$Measurement[acLWQ$Measurement == 'NH3+NH4 as N (mg/l)'] <- "NH4N"
acLWQ$Measurement[acLWQ$Measurement == 'Tot N (mg/l)'] <- "TN" 
acLWQ$Measurement[acLWQ$Measurement == 'pH (pH units)'] <- "pH"
acLWQ$Measurement[acLWQ$Measurement == 'Cyanobacteria BioVolume (mm3/L)'] <- "CYANOTOT"
table(acLWQ$Measurement)

# By this point, we have all the data downloaded from the council, in a data frame called Data.
write.csv(acLWQ,file = paste0("H:/ericg/16666LAWA/LAWA2021/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,".csv"),row.names = F)


