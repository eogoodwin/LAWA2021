require(xml2)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)
library(parallel)
library(doParallel)

setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")
agency='trc'

translate <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",
                           sep=',',header=T,stringsAsFactors = F)%>%filter(Agency==agency)
translate$retName=c("BDISC","DRP","ECOL","NH4","NNN","TON","NO3","DIN","pH","TN","TP","TURBY","TURB")

siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])





for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  dir.create(paste0('D:/LAWA/2021/TRC/',sites[i]),recursive = T,showWarnings = F)
  rm(siteDat)
  for(j in 1:length(translate$CallName)){
    url <- paste0("https://extranet.trc.govt.nz/getdata/LAWA_river_WQ.hts?service=Hilltop&request=GetData",
                  "&Site=",sites[i],
                  "&Measurement=",translate$CallName[j],
                  "&From=2004-01-01",
                  "&To=2021-01-01")
    url <- URLencode(url)
    
    destFile=paste0("D:/LAWA/2021/TRC/",sites[i],"/",translate$CallName[j],".xml")
    if(!file.exists(destFile)||file.info(destFile)$size<2000){
      dl=try(download.file(url,destfile=destFile,method='wininet',quiet=T),silent = T)
      if(file.exists(destFile)&&file.info(destFile)$size>1500){
        cat(',')
        Data=xml2::read_xml(destFile)
        Data = xml2::as_list(Data)[[1]]
        RetCID = attr(Data$Measurement,'SiteName')
        dataSource = unlist(Data$Measurement$DataSource)
        RetProperty=dataSource[grep("ItemInfo.ItemName",names(dataSource),ignore.case=T)]
        Data = Data$Measurement$Data
        if(length(Data)>0){
          while(RetProperty!=translate$retName[j]|RetCID!=sites[i]){
            file.rename(from = destFile,to = paste0('XX',make.names(RetProperty),destFile))
            file.remove(destFile)
            dl=download.file(url,destfile=destFile,method='libcurl',quiet=T)
            Data=xml2::read_xml(destFile)
            Data = xml2::as_list(Data)[[1]]
            if(length(Data)>0){
              RetProperty=attr(Data$observationMember$OM_Observation$procedure,'title')
            }
          }
        }
      }
    }
  }
}
    
    
    

workers = makeCluster(7)
registerDoParallel(workers)

clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})

if(exists("Data"))rm(Data)
trcSWQ=NULL

for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  dir.create(paste0('D:/LAWA/2021/TRC/',sites[i]),recursive = T,showWarnings = F)
  rm(siteDat)
  foreach(j = 1:length(translate$CallName),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    destFile=paste0("D:/LAWA/2021/TRC/",sites[i],"/",translate$CallName[j],".xml")
    if(file.exists(destFile)&&file.info(destFile)$size>1500){
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
      RetCID = attr(Data$Measurement,'SiteName')
      dataSource = unlist(Data$Measurement$DataSource)
      RetProperty=dataSource[grep("ItemInfo.ItemName",names(dataSource),ignore.case=T)]
      Data = Data$Measurement$Data
      if(length(Data)>0&&RetProperty==translate$retName[j]&&RetCID==sites[i]){
        Data=data.frame(apply(t(sapply(Data,function(e)bind_rows(unlist(e)))),2,unlist),row.names = NULL)
        # tags=bind_rows(sapply(Data,function(e){
        #   as.data.frame(sapply(e[which(names(e)=="Parameter")],function(ee){
        #     retVal=data.frame(attributes(ee)$Value)
        #     names(retVal)=attributes(ee)$Name
        #     return(retVal)
        #   }))
        # }))
        # 
        # names(tags) <- gsub('Parameter.','',names(tags))
        
        if(any(grepl('unit',names(dataSource),ignore.case=T))){
          Data$Units=dataSource[grep('unit',names(dataSource),ignore.case=T)]
        }else{
          Data$Units=NA
        }
        Data$Measurement=translate$CallName[j]
      }else{Data=NULL}
    }else{Data=NULL}
    # file.remove(destFile)
    rm(destFile)
    return(Data)
  }->siteDat
  
  if(!is.null(siteDat)){
    rownames(siteDat) <- NULL
    siteDat$CouncilSiteID = sites[i]
    trcSWQ=bind_rows(trcSWQ,siteDat)
  }
  rm(siteDat)
}
stopCluster(workers)
rm(workers)



save(trcSWQ,file = 'trcSWQraw.rData')
# load('trcSWQraw.rData')
agency='trc'




trcSWQ <- trcSWQ%>%select(CouncilSiteID,Date=T,Value,Measurement,Units)


trcSWQb=data.frame(CouncilSiteID=trcSWQ$CouncilSiteID,
                   Date=as.character(format(lubridate::ymd_hms(trcSWQ$Date),'%d-%b-%y')),
                   Value=trcSWQ$Value,
                   Measurement=trcSWQ$Measurement,
                   Units=trcSWQ$Units,
                   Censored=grepl(pattern = '<|>',x = trcSWQ$Value),
                   CenType=F,QC=NA)
trcSWQb$CenType[grep('<',trcSWQb$Value)] <- 'Left'
trcSWQb$CenType[grep('>',trcSWQb$Value)] <- 'Right'

trcSWQb$Value = readr::parse_number(trcSWQb$Value)


translate=read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName,LAWAName)


table(trcSWQb$Measurement,useNA = 'a')
trcSWQb$Measurement <- as.character(factor(trcSWQb$Measurement,
                                           levels = translate$CallName,
                                           labels = translate$LAWAName))
table(trcSWQb$Measurement,useNA='a')


# trcSWQb <- merge(trcSWQb,siteTable,by='CouncilSiteID')
trcSWQb$Measurement[tolower(trcSWQb$Units)=='ntu'&trcSWQb$Measurement=="TURBFNU"]<-"TURB"  #Switched in July 2019
trcSWQb <- unique(trcSWQb)


write.csv(trcSWQb,file = paste0("D:/LAWA/2021/trc.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/trc.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/trc.csv"),
          overwrite = T)
rm(trcSWQ,trcSWQb)
