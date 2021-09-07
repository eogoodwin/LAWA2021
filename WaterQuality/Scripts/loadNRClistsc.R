require(xml2)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)
library(parallel)
library(doParallel)

setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")
agency='nrc'

translate <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",
                        sep=',',header=T,stringsAsFactors = F)%>%filter(Agency==agency)
translate$retName=c("DIN","Total Oxidised Nitrogen","Total Nitrogen","Nitrate Nitrogen","Ammoniacal Nitrogen","DRP","TP","Secchi Depth","Ecoli","pH","Turbidity")

siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])





for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  dir.create(paste0('D:/LAWA/2021/nrc/',sites[i]),recursive = T,showWarnings = F)
  for(j in 1:length(translate$CallName)){
    url <- paste0("http://hilltop.nrc.govt.nz/SOEFinalArchive.hts?service=Hilltop&request=GetData&agency=LAWA",
                  "&Site=",sites[i],
                  "&Measurement=",translate$CallName[j],
                  "&From=2004-01-01",
                  "&To=2021-01-01")
    url <- URLencode(url)
    
    destFile=paste0("D:/LAWA/2021/nrc/",sites[i],"/",make.names(translate$CallName[j]),".xml")
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
    destFile=paste0("D:/LAWA/2021/nrc/",sites[i],"/",make.names(translate$CallName[j]),".xml")
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
nrcSWQ=NULL

for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  dir.create(paste0('D:/LAWA/2021/nrc/',sites[i]),recursive = T,showWarnings = F)
  rm(siteDat)
  foreach(j = 1:length(translate$CallName),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    destFile=paste0("D:/LAWA/2021/nrc/",sites[i],"/",make.names(translate$CallName[j]),".xml")
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
    nrcSWQ=bind_rows(nrcSWQ,siteDat)
  }
  rm(siteDat)
}
stopCluster(workers)
rm(workers)


save(nrcSWQ,file = 'nrcSWQraw.rData')
# load('nrcSWQraw.rData') #62237
agency='nrc'




nrcSWQ <- nrcSWQ%>%select(CouncilSiteID,Date=T,Value,Measurement,units,QualityCode)


nrcSWQb=data.frame(CouncilSiteID=nrcSWQ$CouncilSiteID,
                   Date=as.character(format(lubridate::ymd_hms(nrcSWQ$Date),'%d-%b-%y')),
                   Value=nrcSWQ$Value,
                   Measurement=nrcSWQ$Measurement,
                   Units=nrcSWQ$units,
                   Censored=grepl(pattern = '<|>',x = nrcSWQ$Value),
                   CenType=F,QC=nrcSWQ$QualityCode)
nrcSWQb$CenType[grep('<',nrcSWQb$Value)] <- 'Left'
nrcSWQb$CenType[grep('>',nrcSWQb$Value)] <- 'Right'

nrcSWQb$Value = readr::parse_number(nrcSWQb$Value)


table(nrcSWQb$Measurement,useNA = 'a')
nrcSWQb$Measurement <- as.character(factor(nrcSWQb$Measurement,
                                           levels = translate$CallName,
                                           labels = translate$LAWAName))
table(nrcSWQb$Measurement,useNA='a')

nrcSWQb <- unique(nrcSWQb)

write.csv(nrcSWQb,file = paste0("D:/LAWA/2021/nrc.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/nrc.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/nrc.csv"),
          overwrite = T)
rm(nrcSWQ,nrcSWQb)
