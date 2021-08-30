## Load libraries ------------------------------------------------
require(tidyverse)
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(xml2)     ### XML library to write hilltop XML
require(RCurl)
library(parallel)
library(doParallel)

agency='wcrc'
setwd("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/")

Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Metadata/wcrcMacro_config.csv",sep=',',header=T,stringsAsFactors = F)%>%
  select(Value)%>%unname%>%unlist

if("WQ Sample"%in%Measurements){
  Measurements=Measurements[-which(Measurements=="WQ Sample")]
}

siteTable=loadLatestSiteTableMacro()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])

workers = makeCluster(4)
registerDoParallel(workers)

clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})


if(exists("Data"))rm(Data)
wcrcM=NULL
  for(i in 1:length(sites)){
    cat('\n',sites[i],i,'out of ',length(sites))
    suppressWarnings(rm(siteDat))
    foreach(j = 1:length(Measurements),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
      Data=NULL
      url <- paste0("http://hilltop.wcrc.govt.nz/wq.hts?service=Hilltop&request=GetData&agency=LAWA",
                    "&Site=",sites[i],
                    "&Measurement=",Measurements[j],
                    "&From=1990-01-01",
                    "&To=2021-06-01")
      url <- URLencode(url)
      
      destFile=paste0("D:/LAWA/2021/tmp",gsub(' ','',Measurements[j]),"Mwcrc.xml")
      
      dl=try(download.file(url,destfile=destFile,method='curl',quiet=T),silent = T)
      if(!'try-error'%in%attr(dl,'class')){
        Data=xml2::read_xml(destFile)
        Data = xml2::as_list(Data)[[1]]
        dataSource = unlist(Data$Measurement$DataSource)
        Data = Data$Measurement$Data
        if(length(Data)>0){
          Data=data.frame(apply(t(sapply(Data,function(e)bind_rows(unlist(e)))),2,unlist),row.names = NULL)
          Data$Measurement=Measurements[j]
          rownames(Data) <- NULL
        }
      }
      file.remove(destFile)
      rm(destFile)
      return(Data)
    }->siteDat
    
    if(!is.null(siteDat)){
      rownames(siteDat) <- NULL
      siteDat$CouncilSiteID = sites[i]
      wcrcM=bind_rows(wcrcM,siteDat)
    }
    rm(siteDat)
  }
stopCluster(workers)
rm(workers)


save(wcrcM,file = 'wcrcMraw.rData')
# load('wcrcMraw.rData')
agency='wcrc'
Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Metadata/wcrcMacro_config.csv",sep=',',header=T,stringsAsFactors = F)%>%
  select(Value)%>%unname%>%unlist
siteTable=loadLatestSiteTableMacro()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])

wcrcM <- wcrcM%>%select(CouncilSiteID,Date=T,Value=I1,Measurement)

wcrcMb=data.frame(CouncilSiteID=wcrcM$CouncilSiteID,
                  Date=as.character(format(lubridate::ymd_hms(wcrcM$Date),'%d-%b-%y')),
                  Value=wcrcM$Value,
                  Measurement=wcrcM$Measurement,
                  Censored=grepl(pattern = '<|>',x = wcrcM$Value),
                  CenType=F,
                  CollMeth=NA,ProcMeth=NA)
wcrcMb$CenType[grep('<',wcrcMb$Value)] <- 'Left'
wcrcMb$CenType[grep('>',wcrcMb$Value)] <- 'Right'
wcrcMb$Value = readr::parse_number(wcrcMb$Value)




wcrcSQMCI = readxl::read_xlsx('h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/2021WCRCMacroinvertebrate SQMCI data.xlsx',sheet=1)
wcrcSQMCI <- wcrcSQMCI[-which(apply(wcrcSQMCI,1,FUN=function(r)all(is.na(r)))),]

wcrcSQMCI <- wcrcSQMCI%>%filter(`Site Name`%in%sites)


lubridate::hour(wcrcSQMCI$Date) <- lubridate::hour(wcrcSQMCI$Time)
lubridate::minute(wcrcSQMCI$Date) <- lubridate::minute(wcrcSQMCI$Time)
wcrcSQMCI <- data.frame(CouncilSiteID=wcrcSQMCI$`Site Name`,
                        Date=as.character(format(lubridate::ymd_hms(wcrcSQMCI$Date),'%d-%b-%Y')),
                        Value=wcrcSQMCI$`SQMCI [SQMCI]`,
                        Measurement = "QMCI",
                        Censored=grepl(pattern = '<|>',x = wcrcSQMCI$`SQMCI [SQMCI]`),
                        CenType=F,
                        CollMeth=NA,ProcMeth=NA)
wcrcSQMCI$CenType[grep('<',wcrcSQMCI$Value)] <- 'Left'
wcrcSQMCI$CenType[grep('>',wcrcSQMCI$Value)] <- 'Right'
wcrcSQMCI$Value = readr::parse_number(as.character(wcrcSQMCI$Value))

wcrcMb <- rbind(wcrcMb,wcrcSQMCI)
rm(wcrcSQMCI)

wcrcMb$measName=wcrcMb$Measurement
table(wcrcMb$Measurement,useNA = 'a')
wcrcMb$Measurement <- as.character(factor(wcrcMb$Measurement,
                                          levels = Measurements,
                                          labels = c('TaxaRichness',"PercentageEPTTaxa",
                                                     "MCI","MCI","MCI",
                                                     "QMCI","QMCI","QMCI","QMCI",
                                                     "ASPM","ASPM")))
table(wcrcMb$Measurement,wcrcMb$measName,useNA='a')


# wcrcMb <- merge(wcrcMb,siteTable,by='CouncilSiteID')


write.csv(wcrcMb,file = paste0("D:/LAWA/2021/wcrcM.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/wcrcM.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/",
                      format(Sys.Date(),"%Y-%m-%d"),"/wcrc.csv"),overwrite = T)
rm(wcrcM,wcrcMb)



