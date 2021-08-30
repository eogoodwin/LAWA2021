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
translate$retName=c("Ammoniacal Nitrogen","Black Disc Clarity","Dissolved Reactive Phosphorus","E. coli CFU","pH","Nitrate Nitrogen","Total Nitrogen","Total Phosphorus","Turbidity")

siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])

workers = makeCluster(6)
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
    url <- paste0("http://envdata.nelson.govt.nz/data.hts?service=Hilltop&request=GetData",
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
        if(length(Data)>0&&names(Data)[1]!='Exception'){
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

    
    
    
workers = makeCluster(8)
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
    if(file.exists(destFile)&&file.info(destFile)$size>2000){
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
      if(length(Data)>0&&names(Data)[1]!='Exception'){
        RetCID = attr(Data$Measurement,'SiteName')
        RetProperty=attr(Data$Measurement$DataSource,"Name")
        Data = Data$Measurement$Data
        if(length(Data)>0){
          Data=lapply(Data,function(e)bind_rows(unlist(e,recursive = F)))
          
          for(d in seq_along(Data)){
            if(!"QualityCode"%in%names(Data[[d]])){
              Data[[d]]$QualityCode <- NA
            }
          }
          Data=do.call(rbind,Data)
          tags=bind_rows(sapply(Data,function(e){
            as.data.frame(sapply(e[which(names(e)=="Parameter")],function(ee){
              retVal=data.frame(attributes(ee)$Value)
              names(retVal)=attributes(ee)$Name
              return(retVal)
            }))
          }))
          names(tags) <- gsub('Parameter.','',names(tags))
          if(any(grepl('unit',names(dataSource),ignore.case=T))){
            Data$Units=dataSource[grep('unit',names(dataSource),ignore.case=T)]
          }else{
            Data$Units=NA
          }
          if(!is.null(Data)){
            Data$Measurement=translate$CallName[j]
            Data$retProp=RetProperty
            Data$CouncilSiteID = sites[i]
            Data$retCID = RetCID
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
# load('nccSWQraw.rData')
agency='ncc'



nccSWQb=data.frame(CouncilSiteID=nccSWQ$CouncilSiteID,
                   Date=as.character(format(lubridate::ymd_hms(nccSWQ$T),'%d-%b-%y')),
                   Value=nccSWQ$Value,
                   # Method=nccSWQ%>%select(matches('meth'))%>%apply(.,2,FUN=function(r)paste(r)),
                   Measurement=nccSWQ$Measurement,
                   Units = ifelse('Units'%in%names(nccSWQ),nccSWQ$Units,NA),
                    Censored=grepl(pattern = '<|>',x = nccSWQ$Value),
                    CenType=F,
                    QC=NA)
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
