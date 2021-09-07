## Load libraries ------------------------------------------------
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(xml2)     ### XML library to write hilltop XML
require(RCurl)
library(parallel)
library(doParallel)

agency='ecan'
setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")
translate <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%filter(Agency==agency)
translate$retName=c("Ammoniacal Nitrogen","Black Disc Clarity","Dissolved Reactive Phosphorus","E. coli","Nitrate-N Nitrite-N","pH","Total Nitrogen","Nitrate Nitrogen","Total Phosphorus","Dissolved Inorganic Nitrogen","Turbidity")


siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])
which(sites == siteTable$CouncilSiteID[siteTable$LawaSiteID%in%c('ecan-00018')])
which(sites == siteTable$CouncilSiteID[siteTable$LawaSiteID%in%c('ecan-00072')])


workers = makeCluster(6)
registerDoParallel(workers)

clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})


if(exists("Data"))rm(Data)
  for(i in 1:length(sites)){
    dir.create(paste0("D:/LAWA/2021/ECAN/",make.names(sites[i])),recursive = T,showWarnings = F)
    cat('\n',sites[i],i,'out of ',length(sites))
    
    if(exists('siteDat'))rm(siteDat)
    foreach(j = 1:length(translate$CallName),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
      url <- paste0("http://wateruse.ecan.govt.nz/wqlawa.hts?service=Hilltop&Agency=LAWA&request=GetData",
                    "&Site=",sites[i],
                    "&Measurement=",translate$CallName[j],
                    "&From=2004-01-01",
                    "&To=2021-01-01")
      url <- URLencode(url)
      destFile=paste0("D:/LAWA/2021/ECAN/",make.names(sites[i]),"/",make.names(translate$CallName[j]),".xml")
      if(!file.exists(destFile)|file.info(destFile)$size<3500){
        dl=try(download.file(url,destfile=destFile,method='wininet',quiet=T),silent = T)
        if(!'try-error'%in%attr(dl,'class')){
          Data=try(xml2::read_xml(destFile),silent=T)
          while('try-error'%in%attr(Data,'class')){
            dl=try(download.file(url,destfile=destFile,method='wininet',quiet=T),silent = T)
            if(!'try-error'%in%attr(dl,'class')){
              Data=try(xml2::read_xml(destFile),silent=T)
            }else{return(NULL)}
          }
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

      
      

workers = makeCluster(7)
registerDoParallel(workers)

clusterCall(workers,function(){
  # library(magrittr)  
  # library(dplyr)
  # library(tidyr)
})
#Check site and measurement returned
foreach(i = 1:length(sites),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
  cat('\n',sites[i],i,'out of ',length(sites))
  for(j in 1:11){
    destFile=paste0("D:/LAWA/2021/ECAN/",make.names(sites[i]),"/",make.names(translate$CallName[j]),".xml")
    if(file.exists(destFile)&file.info(destFile)$size>2000){
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
      if(length(Data)>0&&names(Data)[1]!='Error'){
        cat('.')
        RetProperty=Data$Measurement$DataSource$ItemInfo$ItemName[[1]]
        RetCID = attr(Data$Measurement,'SiteName')
        
        if(RetProperty!=translate$retName[j]|RetCID!=sites[i]){
          stop("site",i,' var',j)
        }
      }
    }
  }
  return(NULL)
}->dummyout
stopCluster(workers)
rm(workers)




workers = makeCluster(7)
registerDoParallel(workers)

clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})
rm(Data,datasource,RetProperty,RetCID)
ecanSWQ=NULL
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  foreach(j = 1:length(translate$CallName),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    destFile=paste0("D:/LAWA/2021/ECAN/",make.names(sites[i]),"/",make.names(translate$CallName[j]),".xml")
    if(file.exists(destFile)){#&&file.info(destFile)$size>1000){
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
      RetProperty=Data$Measurement$DataSource$ItemInfo$ItemName[[1]]
      RetCID = attr(Data$Measurement,'SiteName')
      dataSource = unlist(Data$Measurement$DataSource)
      Data = Data$Measurement$Data
      
      if(length(Data)>0){
        if(length(Data)>1){
          Data=data.frame(apply(t(sapply(Data,function(e){
            bind_rows(unlist(e))
          })),2,unlist),row.names = NULL)
        }else{
          Data = bind_rows(unlist(lapply(Data,function(e){
            unlist(e[which(names(e)!='Parameter')])%>%t%>%as.data.frame
          })))
          names(Data) <- gsub("^E\\.","",names(Data))
        }
        if(any(grepl('unit',names(dataSource),ignore.case=T))){
          Data$Units=dataSource[grep('unit',names(dataSource),ignore.case=T)]
        }else{
          Data$Units=NA
        }
        Data$CouncilSiteID=sites[i]
        Data$Measurement=translate$CallName[j]
        Data$RetProp=RetProperty
        Data$RetCID = RetCID
        rownames(Data) <- NULL
      }
    }else{Data=NULL}
    rm(destFile)
    return(Data)
  }->siteDat
  
  if(!is.null(siteDat)){
    rownames(siteDat) <- NULL
    ecanSWQ=bind_rows(ecanSWQ,siteDat)
  }
  rm(siteDat)
}

stopCluster(workers)
rm(workers)

table(ecanSWQ$CouncilSiteID==ecanSWQ$RetCID)
table(ecanSWQ$Measurement==translate$CallName[match(ecanSWQ$RetProp,translate$retName)])



save(ecanSWQ,file = 'ecanSWQraw.rData')
# load('ecanSWQraw.rData') #222638
agency='ecan'

ecanSWQ <- ecanSWQ%>%select(CouncilSiteID,Date=T,Value,Measurement,Units,QC=QualityCode)
  
  
ecanSWQb=data.frame(CouncilSiteID=ecanSWQ$CouncilSiteID,
                    Date=as.character(format(lubridate::ymd_hms(ecanSWQ$Date),'%d-%b-%y')),
                    Value=ecanSWQ$Value,
                    Measurement=ecanSWQ$Measurement,
                    Units=ecanSWQ$Units,
                    Censored=grepl(pattern = '<|>',x = ecanSWQ$Value),
                    CenType=F,
                    QC=ecanSWQ$QC)
ecanSWQb$CenType[grep('<',ecanSWQb$Value)] <- 'Left'
ecanSWQb$CenType[grep('>',ecanSWQb$Value)] <- 'Right'

ecanSWQb$Value = readr::parse_number(ecanSWQb$Value)


table(ecanSWQb$Measurement,useNA = 'a')
ecanSWQb$Measurement <- as.character(factor(ecanSWQb$Measurement,
                                             levels = translate$CallName,
                                             labels = translate$LAWAName))
table(ecanSWQb$Measurement,useNA='a')
ecanSWQb <- unique(ecanSWQb)

# ecanSWQb <- merge(ecanSWQb,siteTable,by='CouncilSiteID')


write.csv(ecanSWQb,file = paste0("D:/LAWA/2021/ecan.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/ecan.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/ecan.csv"),
          overwrite = T)
rm(ecanSWQ,ecanSWQb)



