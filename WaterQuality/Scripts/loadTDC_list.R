require(xml2)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)
library(parallel)
library(doParallel)

setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")
agency='tdc'

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
tdcSWQ=NULL
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  rm(siteDat)
  foreach(j = 1:length(Measurements),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    url <- paste0("http://envdata.tasman.govt.nz/WaterQuality.hts?service=Hilltop&request=GetData",
                  "&Site=",sites[i],
                  "&Measurement=",Measurements[j],
                  "&From=2004-01-01",
                  "&To=2021-01-01")
    url <- URLencode(url)
    
    destFile=paste0("D:/LAWA/2021/tmp",gsub(' ','',URLencode(Measurements[j],reserved = T)),"WQtdc.xml")
    dl=try(download.file(url,destfile=destFile,method='wininet',quiet=T),silent = T)
    if(!'try-error'%in%attr(dl,'class')){
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
      dataSource = unlist(Data$Measurement$DataSource)
      Data = Data$Measurement$Data
      if(length(Data)>0){
        Data=lapply(Data,function(e)bind_rows(unlist(e)))
        Data=do.call(rbind,Data)
        if(any(grepl('unit',names(dataSource),ignore.case=T))){
          Data$Units=dataSource[grep('unit',names(dataSource),ignore.case=T)]
        }else{
          Data$Units=NA
        }
        if(!is.null(Data)){
          Data$Measurement=Measurements[j]
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
    tdcSWQ=bind_rows(tdcSWQ,siteDat)
  }
  rm(siteDat)
}
stopCluster(workers)
rm(workers)



save(tdcSWQ,file = 'tdcSWQraw.rData')
# load('tdcSWQraw.rData')
agency='tdc'



tdcSWQ <- tdcSWQ%>%select(CouncilSiteID,Date=T,Value,Measurement,Units)


tdcSWQb=data.frame(CouncilSiteID=tdcSWQ$CouncilSiteID,
                   Date=as.character(format(lubridate::ymd_hms(tdcSWQ$Date),'%d-%b-%y')),
                   Value=tdcSWQ$Value,
                   Measurement=tdcSWQ$Measurement,
                   Units=tdcSWQ$Units,
                   Censored=grepl(pattern = '<|>',x = tdcSWQ$Value),
                   CenType=F,QC=NA)
tdcSWQb$CenType[grep('<',tdcSWQb$Value)] <- 'Left'
tdcSWQb$CenType[grep('>',tdcSWQb$Value)] <- 'Right'

tdcSWQb$Value = readr::parse_number(tdcSWQb$Value)


translate=read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName,LAWAName)


table(tdcSWQb$Measurement,useNA = 'a')
tdcSWQb$Measurement <- as.character(factor(tdcSWQb$Measurement,
                                           levels = translate$CallName,
                                           labels = translate$LAWAName))
table(tdcSWQb$Measurement,useNA='a')

tdcSWQb <- unique(tdcSWQb)

# tdcSWQb <- merge(tdcSWQb,siteTable,by='CouncilSiteID')


write.csv(tdcSWQb,file = paste0("D:/LAWA/2021/tdc.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/tdc.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/tdc.csv"),
          overwrite = T)
rm(tdcSWQ,tdcSWQb)
