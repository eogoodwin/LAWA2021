## Load libraries ------------------------------------------------
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(xml2)     ### XML library to write hilltop XML
require(RCurl)
library(parallel)
library(doParallel)

setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")
agency='gwrc'
translate <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%filter(Agency==agency)
translate$retName=c("Ammoniacal Nitrogen","Black Disc","Dissolved Inorganic Nitrogen",
                    "Dissolved Reactive Phosphorus","E-Coli","Nitrite-Nitrate Nitrogen",
                    "Nitrate Nitrogen","pH (Field)(X)","Total Nitrogen",
                    "Total Phosphorus","Turbidity (Lab)(X)")


siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])

workers = makeCluster(7)
registerDoParallel(workers)

clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})


  for(i in 1:length(sites)){
    dir.create(paste0("D:/LAWA/2021/GWRC/",make.names(sites[i])),recursive = T,showWarnings = F)
    cat('\n',sites[i],i,'out of ',length(sites))
    foreach(j = 1:length(translate$CallName),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
      url <- paste0("http://hilltop.gw.govt.nz/Data.hts?service=Hilltop&request=GetData",
                    "&Site=",sites[i],
                    "&Measurement=",translate$CallName[j],
                    "&From=2004-01-01&To=2021-01-01")
      url <- URLencode(url)
      destFile=paste0("D:/LAWA/2021/GWRC/",make.names(sites[i]),"/",make.names(translate$CallName[j]),".xml")
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
  })
  #Check site and measurement returned
  foreach(i = 1:length(sites),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    cat('\n',sites[i],i,'out of ',length(sites))
    for(j in 1:11){
      destFile=paste0("D:/LAWA/2021/GWRC/",make.names(sites[i]),"/",make.names(translate$CallName[j]),".xml")
      if(file.exists(destFile)&file.info(destFile)$size>2000){
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
  rm(workers)
  
  
  
  
  
  workers = makeCluster(7)
  registerDoParallel(workers)
  
  clusterCall(workers,function(){
    library(magrittr)  
    library(dplyr)
    library(tidyr)
  })
  rm(Data,datasource,RetProperty,RetCID)
  gwrcSWQ=NULL
  for(i in 1:length(sites)){
    cat('\n',sites[i],i,'out of ',length(sites))
    foreach(j = 1:length(translate$CallName),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
      destFile=paste0("D:/LAWA/2021/GWRC/",make.names(sites[i]),"/",make.names(translate$CallName[j]),".xml")
      if(file.exists(destFile)){#&&file.info(destFile)$size>1000){    
        Data=xml2::read_xml(destFile)
        Data = xml2::as_list(Data)[[1]]
        RetCID = attr(Data$Measurement,'SiteName')
        RetProperty=attr(Data$Measurement$DataSource,"Name")
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
          Data$Measurement=translate$CallName[j]
          Data$RetProp=RetProperty
          Data$CouncilSiteID=sites[i]
          Data$RetCID=RetCID
          if(exists('dataSource')){
            if('ItemInfo.Units'%in%names(dataSource)){
              Data$Units = unname(dataSource[which(names(dataSource)=="ItemInfo.Units")])
            }
          }
          rownames(Data) <- NULL
        }else{Data=NULL}
      }else{Data=NULL}
      rm(destFile)
      return(Data)
    }->siteDat
    
    if(!is.null(siteDat)){
      rownames(siteDat) <- NULL
      gwrcSWQ=bind_rows(gwrcSWQ,siteDat)
    }
    rm(siteDat)
  }
stopCluster(workers)
rm(workers)

table(gwrcSWQ$CouncilSiteID==gwrcSWQ$RetCID)
table(gwrcSWQ$Measurement==translate$CallName[match(gwrcSWQ$RetProp,translate$retName)])



save(gwrcSWQ,file = 'gwrcSWQraw.rData')
# load('gwrcSWQraw.rData') 89988
agency='gwrc'

 gwrcSWQ <- gwrcSWQ%>%filter(!QualityCode%in%c(400))




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



write.csv(gwrcSWQb,file = paste0("D:/LAWA/2021/gwrc.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/gwrc.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/gwrc.csv"),
          overwrite = T)
rm(gwrcSWQ,gwrcSWQb)



