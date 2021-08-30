require(xml2)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)
library(parallel)
library(doParallel)

setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")
agency='mdc'

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
mdcSWQ=NULL
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  rm(siteDat)
  foreach(j = 1:length(Measurements),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    url <- paste0("http://hydro.marlborough.govt.nz/LAWA_WQ.hts?service=Hilltop&request=GetData",
                  "&Site=",sites[i],
                  "&Measurement=",Measurements[j],
                  "&From=2004-01-01&To=2021-01-01")
    url <- URLencode(url)
    
    destFile=paste0("D:/LAWA/2021/tmp",gsub(' ','',URLencode(Measurements[j],reserved = T)),"WQmdc.xml")
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
    mdcSWQ=bind_rows(mdcSWQ,siteDat)
  }
  rm(siteDat)
}
stopCluster(workers)
rm(workers)



save(mdcSWQ,file = 'mdcSWQraw.rData')
# load('mdcSWQraw.rData')
agency='mdc'

# mdcSWQ <- mdcSWQ%>%filter(!QualityCode%in%c())

mdcSWQ <- mdcSWQ%>%select(CouncilSiteID,Date=T,Value,Measurement,Units,QualityCode)


mdcSWQb=data.frame(CouncilSiteID=mdcSWQ$CouncilSiteID,
                   Date=as.character(format(lubridate::ymd_hms(mdcSWQ$Date),'%d-%b-%y')),
                   Value=mdcSWQ$Value,
                   Measurement=mdcSWQ$Measurement,
                   Units=mdcSWQ$Units,
                   Censored=grepl(pattern = '<|>',x = mdcSWQ$Value),
                   CenType=F,QC=mdcSWQ$QualityCode)
mdcSWQb$CenType[grep('<',mdcSWQb$Value)] <- 'Left'
mdcSWQb$CenType[grep('>',mdcSWQb$Value)] <- 'Right'

mdcSWQb$Value = readr::parse_number(mdcSWQb$Value)


translate=read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName,LAWAName)


table(mdcSWQb$Measurement,useNA = 'a')
mdcSWQb$Measurement <- as.character(factor(mdcSWQb$Measurement,
                                           levels = translate$CallName,
                                           labels = translate$LAWAName))
table(mdcSWQb$Measurement,useNA='a')

mdcSWQb <- unique(mdcSWQb)

# mdcSWQb <- merge(mdcSWQb,siteTable,by='CouncilSiteID')


write.csv(mdcSWQb,file = paste0("D:/LAWA/2021/mdc.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/mdc.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/mdc.csv"),
          overwrite = T)
rm(mdcSWQ,mdcSWQb)
