## Load libraries ------------------------------------------------
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(xml2)     ### XML library to write hilltop XML
require(RCurl)
library(parallel)
library(doParallel)

setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")
agency='gwrc'
Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName)%>%unname%>%unlist

# Measurements=c(Measurements,'WQ Sample')


siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])

workers = makeCluster(8)
registerDoParallel(workers)

clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})


if(exists("Data"))rm(Data)
gwrcSWQ=NULL
suppressMessages({
  for(i in 1:length(sites)){
    cat('\n',sites[i],i,'out of ',length(sites))
    rm(siteDat)
    foreach(j = 1:length(Measurements),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
      url <- paste0("http://hilltop.gw.govt.nz/Data.hts?service=Hilltop&request=GetData",
                    "&Site=",sites[i],
                    "&Measurement=",Measurements[j],
                    "&From=2004-01-01&To=2021-01-01")
      url <- URLencode(url)
      
      destFile=paste0("D:/LAWA/2021/tmp",gsub(' ','',Measurements[j]),"WQgwrc.xml")
      
      dl=try(download.file(url,destfile=destFile,method='curl',quiet=T),silent = T)
      if(!'try-error'%in%attr(dl,'class')){
        Data=xml2::read_xml(destFile)
        Data = xml2::as_list(Data)[[1]]
        dataSource = unlist(Data$Measurement$DataSource)
        Data = Data$Measurement$Data
        if(length(Data)>0){
          if(length(Data)>1){
            Data = lapply(Data,function(e){
              unlist(e)
            })
            Data = do.call(bind_rows,Data)
          }else{
            Data = bind_rows(unlist(lapply(Data,function(e){
              unlist(e[which(names(e)!='Parameter')])%>%t%>%as.data.frame
            })))
            names(Data) <- gsub("^E\\.","",names(Data))
          }
          Data$Measurement=Measurements[j]
          if(exists('dataSource')){
            if('ItemInfo.Units'%in%names(dataSource)){
              Data$Units = unname(dataSource[which(names(dataSource)=="ItemInfo.Units")])
            }
          }
          
          rownames(Data) <- NULL
        }else{Data=NULL}
      }
      file.remove(destFile)
      rm(destFile)
      return(Data)
    }->siteDat
    
    if(!is.null(siteDat)){
      rownames(siteDat) <- NULL
      siteDat$CouncilSiteID = sites[i]
      gwrcSWQ=bind_rows(gwrcSWQ,siteDat)
    }
    rm(siteDat)
  }
})
stopCluster(workers)
rm(workers)


save(gwrcSWQ,file = 'gwrcSWQraw.rData')
# load('gwrcSWQraw.rData')
agency='gwrc'

# gwrcSWQ <- gwrcSWQ%>%filter(!QualityCode%in%c())


Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName)%>%unname%>%unlist


gwrcSWQ <- gwrcSWQ%>%select(CouncilSiteID,Date=T,Value,Measurement,Units,QC=QualityCode)



gwrcSWQb=data.frame(CouncilSiteID=gwrcSWQ$CouncilSiteID,
                    Date=as.character(format(lubridate::ymd_hms(gwrcSWQ$Date),'%d-%b-%y')),
                    Value=gwrcSWQ$Value,
                    Measurement=gwrcSWQ$Measurement,
                    Units=gwrcSWQ$Units,
                    Censored=grepl(pattern = '<|>',x = gwrcSWQ$Value),
                    CenType=F,
                    QC=gwrcSWQ$QC)
gwrcSWQb$CenType[grep('<',gwrcSWQb$Value)] <- 'Left'
gwrcSWQb$CenType[grep('>',gwrcSWQb$Value)] <- 'Right'

gwrcSWQb$Value = readr::parse_number(gwrcSWQb$Value)

translate=read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName,LAWAName)

table(gwrcSWQb$Measurement,useNA = 'a')
gwrcSWQb$Measurement <- as.character(factor(gwrcSWQb$Measurement,
                                            levels = translate$CallName,
                                            labels = translate$LAWAName))
table(gwrcSWQb$Measurement,useNA='a')
gwrcSWQb <- unique(gwrcSWQb)

# gwrcSWQb <- merge(gwrcSWQb,siteTable,by='CouncilSiteID')


write.csv(gwrcSWQb,file = paste0("D:/LAWA/2021/gwrc.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/gwrc.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/gwrc.csv"),
          overwrite = T)
rm(gwrcSWQ,gwrcSWQb)



