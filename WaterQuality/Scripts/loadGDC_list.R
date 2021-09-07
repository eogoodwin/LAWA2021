require(xml2)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)
library(parallel)
library(doParallel)

setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")
agency='gdc'

# gdc,Secchi Black Disc (metres) water clarity,BDISC


translate <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",
                           sep=',',header=T,stringsAsFactors = F)%>%filter(Agency==agency)
translate$retName=c("Ammoniacal Nitrogen as N","Clarity Tube (cm)-Field","Clarity Tube (cm)-Field",
                    "Dissolved Inorganic Nitrogen (DIN)","Dissolved Reactive Phosphorus","E.Coli CFU/100mL",
                    "pH (Field)","Nitrate as N","Nitrogen Total",
                    "Total Oxidised Nitrogen","Total Phosphorus","Turbidity (Lab - NTU)",
                    "Turbidity (Lab - FNU)")

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
  dir.create(paste0("D:/LAWA/2021/GDC/",make.names(sites[i])),recursive = T,showWarnings = F)
  cat('\n',sites[i],i,'out of ',length(sites))
  foreach(j = 1:length(translate$CallName),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    url <- paste0("http://hilltop.gdc.govt.nz/data.hts?service=Hilltop&request=GetData",
                  "&Site=",sites[i],
                  "&Measurement=",translate$CallName[j],
                  "&From=2004-01-01",
                  "&To=2021-01-01")
    url <- URLencode(url)
    destFile=paste0("D:/LAWA/2021/GDC/",make.names(sites[i]),"/",make.names(translate$CallName[j]),".xml")
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
        }else{file.remove(destFile)}
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
  rm(siteDat)
  for(j in 1:13){
    destFile=paste0("D:/LAWA/2021/GDC/",make.names(sites[i]),"/",make.names(translate$CallName[j]),".xml")
    if(file.exists(destFile)&file.info(destFile)$size>2000){
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

rm(Data,datasource,RetProperty,RetCID)
gdcSWQ=NULL
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  foreach(j = 1:length(translate$CallName),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    destFile=paste0("D:/LAWA/2021/GDC/",make.names(sites[i]),"/",make.names(translate$CallName[j]),".xml")
    if(file.exists(destFile)&&file.info(destFile)$size>1000){
      
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
      RetProperty=Data$Measurement$DataSource$ItemInfo$ItemName[[1]]
      RetCID = attr(Data$Measurement,'SiteName')
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
          Data$Measurement=translate$CallName[j]
          Data$RetProp=RetProperty
          Data$RetCID = RetCID
        }
      }else{Data=NULL}
    }else{Data=NULL}
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



table(gdcSWQ$Measurement==translate$CallName[match(gdcSWQ$RetProp,translate$retName)])
table(gdcSWQ$CouncilSiteID==gdcSWQ$RetCID)


save(gdcSWQ,file = 'gdcSWQraw.rData')
# load('gdcSWQraw.rData') #44296


gdcSWQ <- gdcSWQ%>%filter(!QualityCode%in%c(400))

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
