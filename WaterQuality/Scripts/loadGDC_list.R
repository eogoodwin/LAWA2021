require(xml2)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)
library(parallel)
library(doParallel)

setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")
agency='gdc'

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
gdcSWQ=NULL
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  rm(siteDat)
  foreach(j = 1:length(Measurements),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    url <- paste0("http://hilltop.gdc.govt.nz/data.hts?service=Hilltop&request=GetData",
                  "&Site=",sites[i],
                  "&Measurement=",Measurements[j],
                  "&From=2004-01-01",
                  "&To=2021-01-01")
    url <- URLencode(url)
    
    destFile=paste0("D:/LAWA/2021/tmp",gsub(' ','',URLencode(Measurements[j],reserved = T)),"WQgdc.xml")
    dl=try(download.file(url,destfile=destFile,method='wininet',quiet=T),silent = T)
    if(!'try-error'%in%attr(dl,'class')){
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
      dataSource = unlist(Data$Measurement$DataSource)
      Data = Data$Measurement$Data
      if(length(Data)>0){
        Data=lapply(Data,function(e)bind_rows(unlist(e)))
        for(d in seq_along(Data)){
          if(!"QualityCode"%in%names(Data[[d]])){
            Data[[d]]$QualityCode <- NA
          }
        }
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
    gdcSWQ=bind_rows(gdcSWQ,siteDat)
  }
  rm(siteDat)
}
stopCluster(workers)
rm(workers)







save(gdcSWQ,file = 'gdcSWQraw.rData')
# load('gdcSWQraw.rData')
agency='gdc'

# gdcSWQ <- gdcSWQ%>%filter(!QualityCode%in%c(100))

gdcSWQ <- gdcSWQ%>%select(CouncilSiteID,Date=T,Value,Measurement,Units,QualityCode)


gdcSWQb=data.frame(CouncilSiteID=gdcSWQ$CouncilSiteID,
                   Date=as.character(format(lubridate::ymd_hms(gdcSWQ$Date),'%d-%b-%y')),
                   Value=gdcSWQ$Value,
                   Measurement=gdcSWQ$Measurement,
                   Units=gdcSWQ$Units,
                   Censored=grepl(pattern = '<|>',x = gdcSWQ$Value),
                   CenType=F,QC=gdcSWQ$QualityCode)
gdcSWQb$CenType[grep('<',gdcSWQb$Value)] <- 'Left'
gdcSWQb$CenType[grep('>',gdcSWQb$Value)] <- 'Right'

gdcSWQb$Value = readr::parse_number(gdcSWQb$Value)


translate=read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName,LAWAName)


table(gdcSWQb$Measurement,useNA = 'a')
gdcSWQb$Measurement <- as.character(factor(gdcSWQb$Measurement,
                                           levels = translate$CallName,
                                           labels = translate$LAWAName))
table(gdcSWQb$Measurement,useNA='a')

gdcSWQb <- unique(gdcSWQb)

# gdcSWQb <- merge(gdcSWQb,siteTable,by='CouncilSiteID')


write.csv(gdcSWQb,file = paste0("D:/LAWA/2021/gdc.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/gdc.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/gdc.csv"),
          overwrite = T)
rm(gdcSWQ,gdcSWQb)
