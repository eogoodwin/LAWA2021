require(xml2)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
library(parallel)
library(doParallel)
# Quality coding to NEMS is a work in progress here (except for Surface Water Quantity who have been doing it for some time).
#  Currently, River Water Quality is quality coded using a different process based on an early draft of NEMS, 
#  but we are aiming to have all our datasets quality coded to NEMS in the near future!
#   
#   Information on quality codes is available from our Hilltop server:

#Also "method" available same place

setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")

agency='ncc'
translate <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%filter(Agency==agency)
translate$retName=c("Ammoniacal Nitrogen","Black Disc Clarity","Dissolved Inorganic Nitrogen","Dissolved Reactive Phosphorus","E. coli CFU","pH","Nitrate Nitrogen","Total Nitrogen","Total Oxidised Nitrogen","Total Phosphorus","Turbidity")

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
nccSWQ=NULL


for(i in (1:length(sites))){
  dir.create(paste0("D:/LAWA/2021/NCC/",make.names(sites[i])),recursive = T,showWarnings = F)
  cat('\n',sites[i],i,'out of ',length(sites),'\t')
  siteDat=NULL
  foreach(j = 1:length(translate$CallName),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    url <- paste0("http://envdata.nelson.govt.nz/",
    # url <- paste0("http://202.27.104.190/",
                  "data.hts?service=Hilltop&request=GetData",
                  "&Site=",sites[i],
                  "&Measurement=",translate$CallName[j],
                  "&From=2004-01-01",
                  "&To=2021-01-01")
    url <- URLencode(url)
    destFile=paste0("D:/LAWA/2021/NCC/",make.names(sites[i]),"/",make.names(translate$CallName[j]),"WQ.xml")
    if(!file.exists(destFile)|file.info(destFile)$size<3500){
      dl=try(download.file(url,destfile=destFile,method='wininet',quiet=T),silent = T)
      if(!'try-error'%in%attr(dl,'class')){
        Data=xml2::read_xml(destFile)
        Data = xml2::as_list(Data)[[1]]
        if(length(Data)>0&&names(Data)[1]!='Error'){
          RetCID = attr(Data$Measurement,'SiteName')
          RetProperty=attr(Data$Measurement$DataSource,"Name")
          
          while(RetProperty!=translate$retName[j]|RetCID!=sites[i]){
            file.rename(from = destFile,to = paste0('XX',make.names(RetProperty),destFile))
            file.remove(destFile)
            dl=download.file(url,destfile=destFile,method='libcurl',quiet=T)
            Data=xml2::read_xml(destFile)
            Data = xml2::as_list(Data)[[1]]
            if(length(Data)>0){
              RetCID = attr(Data$Measurement,'SiteName')
              RetProperty=attr(Data$Measurement$DataSource,"Name")
            }
          }
        }
      }
    }
    return(NULL)
  }->dummyout
}
stopCluster(workers)
rm(workers)




#Check site and measurement returned
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  for(j in 1:14){
    destFile=paste0("D:/LAWA/2021/NCC/",make.names(sites[i]),"/",make.names(translate$CallName[j]),".xml")
    if(file.exists(destFile)&file.info(destFile)$size>1000){
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
      if(length(Data)>0&&names(Data)[1]!='Error'){
        cat('.')
        RetProperty=Data$Measurement$DataSource$ItemInfo$ItemName[[1]]
        RetCID = attr(Data$Measurement,'SiteName')
        
        if(RetProperty!=translate$retName[j]|RetCID!=sites[i]){
          browser()
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


nccSWQ=NULL
if(exists('Data'))rm(Data)
for(i in 1:length(sites)){
  cat(sites[i],i,'out of',length(sites),'\n')
  rm(siteDat)
  foreach(j = 1:length(translate$CallName),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    
    destFile=paste0("D:/LAWA/2021/NCC/",make.names(sites[i]),"/",make.names(translate$CallName[j]),"WQ.xml")
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
    nccSWQ=bind_rows(nccSWQ,siteDat)
  }
  rm(siteDat)
}

stopCluster(workers)
rm(workers)

table(nccSWQ$CouncilSiteID==nccSWQ$retCID)
table(nccSWQ$Measurement==translate$CallName[match(nccSWQ$retProp,translate$retName)])


save(nccSWQ,file = 'nccSWQraw.rData')
# load('nccSWQraw.rData') 25258
agency='ncc'



nccSWQb=data.frame(CouncilSiteID=nccSWQ$CouncilSiteID,
                   Date=as.character(format(lubridate::ymd_hms(nccSWQ$T),'%d-%b-%y')),
                   Value=nccSWQ$Value,
                   Measurement=nccSWQ$Measurement,
                   Units = ifelse('units'%in%names(nccSWQ),nccSWQ$units,NA),
                    Censored=grepl(pattern = '<|>',x = nccSWQ$Value),
                    CenType=F,
                    QC=nccSWQ$QualityCode)
nccSWQb$CenType[grep('<',nccSWQb$Value)] <- 'Left'
nccSWQb$CenType[grep('>',nccSWQb$Value)] <- 'Right'

nccSWQb$Value=readr::parse_number(nccSWQb$Value)


table(nccSWQb$Measurement,useNA = 'a')
nccSWQb$Measurementb <- as.character(factor(nccSWQb$Measurement,
                                             levels = translate$CallName,
                                             labels = translate$LAWAName))
table(nccSWQb$Measurement,nccSWQb$Measurementb,useNA='a')
nccSWQb$Measurement=nccSWQb$Measurementb
nccSWQb <- nccSWQb%>%select(-Measurementb)

nccSWQb <- unique(nccSWQb)



write.csv(nccSWQb,file = paste0("D:/LAWA/2021/ncc.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/ncc.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/ncc.csv"),
          overwrite = T)
rm(nccSWQ,nccSWQb)
