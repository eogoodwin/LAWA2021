require(xml2)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)
library(parallel)
library(doParallel)

setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")
agency='tdc'

translate <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",
                        sep=',',header=T,stringsAsFactors = F)%>%filter(Agency==agency)
translate$retName=c("Dissolved Reactive Phosphorus","E.coli","E.coli (APHA 9223B)","Nitrate-N + Nitrite-N (Lachat)","Nitrate-N","pH","Total Ammonia","Total Nitrogen","Total Phosphorus","Turbidity","Water Clarity")

siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])





for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  dir.create(paste0('D:/LAWA/2021/tdc/',sites[i]),recursive = T,showWarnings = F)
  for(j in 1:length(translate$CallName)){
    url <- paste0("http://envdata.tasman.govt.nz/WaterQuality.hts?service=Hilltop&request=GetData",
                  "&Site=",sites[i],
                  "&Measurement=",translate$CallName[j],
                  "&From=2004-01-01",
                  "&To=2021-01-01")
    url <- URLencode(url)
    
    destFile=paste0("D:/LAWA/2021/tdc/",sites[i],"/",make.names(translate$CallName[j]),".xml")
    if(!file.exists(destFile)||file.info(destFile)$size<2000){
      dl=try(download.file(url,destfile=destFile,method='wininet',quiet=T),silent = T)
      if(file.exists(destFile)&&file.info(destFile)$size>500){
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
})
#Check site and measurement returned
foreach(i = 1:length(sites),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
  cat('\n',sites[i],i,'out of ',length(sites))
  for(j in 1:11){
    destFile=paste0("D:/LAWA/2021/tdc/",sites[i],"/",make.names(translate$CallName[j]),".xml")
    if(file.exists(destFile)&file.info(destFile)$size>500){
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
      if(length(Data)>0&&names(Data)[1]!='Error'){
        RetCID = attr(Data$Measurement,'SiteName')
        RetProperty=attr(Data$Measurement$DataSource,"Name")
        
        if(RetProperty!=translate$retName[j]|RetCID!=sites[i]){
          stop("site",i,' var',j)
        }
      }
    }
  }
  return(NULL)
}->dummyout
stopCluster(workers)
rm(workers,dummyout)





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
  dir.create(paste0('D:/LAWA/2021/tdc/',sites[i]),recursive = T,showWarnings = F)
  rm(siteDat)
  foreach(j = 1:length(translate$CallName),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    destFile=paste0("D:/LAWA/2021/tdc/",sites[i],"/",make.names(translate$CallName[j]),".xml")
    if(file.exists(destFile)&&file.info(destFile)$size>500){
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
      if(length(Data)>0&&names(Data)[1]!='Error'){
        RetCID = attr(Data$Measurement,'SiteName')
        RetProperty=attr(Data$Measurement$DataSource,"Name")
        uom = unlist(Data$Measurement$DataSource$ItemInfo$Units)
        Data = Data$Measurement$Data
        if(length(Data)>0){
          Data=lapply(Data,function(e)bind_rows(unlist(e,recursive = F)))
          
          for(d in seq_along(Data)){
            if(!"QualityCode"%in%names(Data[[d]])){
              Data[[d]]$QualityCode <- NA
            }
          }
          Data=do.call(rbind,Data)
          if(!is.null(Data)){
            Data$Measurement=translate$CallName[j]
            Data$retProp=RetProperty
            Data$CouncilSiteID = sites[i]
            Data$retCID = RetCID
            Data$units=uom
          }
        }else{Data=NULL}
      }else{Data=NULL}
    }else{Data=NULL}
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
# load('tdcSWQraw.rData') #18713
agency='tdc'




tdcSWQ <- tdcSWQ%>%select(CouncilSiteID,Date=T,Value,Measurement,units,QualityCode)


tdcSWQb=data.frame(CouncilSiteID=tdcSWQ$CouncilSiteID,
                   Date=as.character(format(lubridate::ymd_hms(tdcSWQ$Date),'%d-%b-%y')),
                   Value=tdcSWQ$Value,
                   Measurement=tdcSWQ$Measurement,
                   Units=tdcSWQ$units,
                   Censored=grepl(pattern = '<|>',x = tdcSWQ$Value),
                   CenType=F,QC=tdcSWQ$QualityCode)
tdcSWQb$CenType[grep('<',tdcSWQb$Value)] <- 'Left'
tdcSWQb$CenType[grep('>',tdcSWQb$Value)] <- 'Right'

tdcSWQb$Value = readr::parse_number(tdcSWQb$Value)


table(tdcSWQb$Measurement,useNA = 'a')
tdcSWQb$Measurement <- as.character(factor(tdcSWQb$Measurement,
                                           levels = translate$CallName,
                                           labels = translate$LAWAName))
table(tdcSWQb$Measurement,useNA='a')

tdcSWQb <- unique(tdcSWQb)

write.csv(tdcSWQb,file = paste0("D:/LAWA/2021/tdc.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/tdc.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/tdc.csv"),
          overwrite = T)
rm(tdcSWQ,tdcSWQb)
