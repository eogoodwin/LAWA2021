## Load libraries ------------------------------------------------
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(xml2)     ### XML library to write hilltop XML
require(RCurl)
library(parallel)
library(doParallel)

agency='es'
setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")

translate <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)

translate$CallName[translate$CallName== "Black Disc (Field)[Clarity (Black Disc Field)]"] <-
  "Black Disc (Field)[Clarity (Black Disc, Field)]"

translate$retName=c("Clarity (Black Disc, Field)","E-Coli <CFU>","Nitrogen (Nitrate Nitrite)","Nitrogen (Total Ammoniacal)","Nitrogen (Total)","Nitrogen (Nitrate)","pH (Lab)","Phosphorus (Dissolved Reactive)","Phosphorus (Total)","Turbidity (Lab)" )


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
esSWQ=NULL
for(i in 1:length(sites)){
  dir.create(paste0("D:/LAWA/2021/ES/",make.names(sites[i])),recursive = T,showWarnings = F)
  cat('\n',sites[i],i,'out of ',length(sites))
  foreach(j = 1:length(translate$CallName),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    url <- paste0("http://odp.es.govt.nz/WQ.hts?service=Hilltop&request=GetData",
                  "&Site=",sites[i],
                  "&Measurement=",translate$CallName[j],
                  "&From=2006-01-01",
                  "&To=2021-01-01")
    url <- URLencode(url)
    destFile=paste0("D:/LAWA/2021/ES/",make.names(sites[i]),"/",make.names(translate$CallName[j]),"WQ.xml")
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




#Check site and measurement returned
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  for(j in 1:10){
    destFile=paste0("D:/LAWA/2021/ES/",make.names(sites[i]),"/",make.names(translate$CallName[j]),"WQ.xml")
    if(file.exists(destFile)&file.info(destFile)$size>2000){
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
      if(length(Data)>0&&names(Data)[1]!='Error'){
        cat('.')
        RetCID = attr(Data$Measurement,'SiteName')
        RetProperty=attr(Data$Measurement$DataSource,"Name")
        
        if(RetProperty!=translate$retName[j]|RetCID!=sites[i]){
          browser()
        }
      }
    }
  }
}




workers = makeCluster(8)
registerDoParallel(workers)

clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})

rm(Data,datasource,RetProperty,RetCID)
esSWQ=NULL
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  rm(siteDat)
  foreach(j = 1:length(translate$CallName),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    destFile=paste0("D:/LAWA/2021/ES/",make.names(sites[i]),"/",make.names(translate$CallName[j]),"WQ.xml")
    if(file.exists(destFile)&&file.info(destFile)$size>1000){
      Data=try(xml2::read_xml(destFile),silent=T)
      if('try-error'%in%attr(Data,'class')){
        file.remove(destFile)
        return(NULL)
      }
        Data = xml2::as_list(Data)[[1]]
      if(length(Data)>0&&names(Data)[1]!='Exception'){
        RetCID = attr(Data$Measurement,'SiteName')
        RetProperty=attr(Data$Measurement$DataSource,"Name")
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
    rownames(siteDat) <- NULL
    siteDat$CouncilSiteID = sites[i]
    esSWQ=bind_rows(esSWQ,siteDat)
  }
  rm(siteDat)
}

stopCluster(workers)
rm(workers)

  save(esSWQ,file = 'esSWQraw.rData')
# load('esSWQraw.rData')  
agency='es'

esSWQ <- esSWQ%>%filter(!QualityCode%in%c(400,403,404,406))
  
DINextra = readxl::read_xlsx("h:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/ES_DIN for LAWA 1210730.xlsx",sheet=1)
DINextra$CouncilSiteID = siteTable$CouncilSiteID[match(tolower(DINextra$`Site Name`),tolower(siteTable$SiteID))]
DINextra <- DINextra%>%transmute(T=as.character(Time),Value=`DIN (LAWA)`,
                                 QualityCode=NA,Units=as.character(DINextra[1,4]),
                                 Measurement="DIN",CouncilSiteID)
DINextra = DINextra[-1,]

esSWQ=bind_rows(esSWQ,DINextra)
rm(DINextra)


esSWQ <- esSWQ%>%select(CouncilSiteID,Date=T,Value,Measurement,Units,QualityCode)

esSWQb=data.frame(CouncilSiteID=esSWQ$CouncilSiteID,
                    Date=as.character(format(lubridate::ymd_hms(esSWQ$Date),'%d-%b-%y')),
                    Value=esSWQ$Value,
                    Measurement=esSWQ$Measurement,
                    Units=esSWQ$Units,
                    Censored=grepl(pattern = '<|>',x = esSWQ$Value),
                    CenType=F,QC=esSWQ$QualityCode)
esSWQb$CenType[grep('<',esSWQb$Value)] <- 'Left'
esSWQb$CenType[grep('>',esSWQb$Value)] <- 'Right'

esSWQb$Value = readr::parse_number(esSWQb$Value)



table(esSWQb$Measurement,useNA = 'a')
esSWQb$Measurement <- as.character(factor(esSWQb$Measurement,
                                            levels = c(translate$CallName,"DIN"),
                                            labels = c(translate$LAWAName,"DIN")))
table(esSWQb$Measurement,useNA='a')
esSWQb <- unique(esSWQb)



write.csv(esSWQb,file = paste0("D:/LAWA/2021/es.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/es.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/es.csv"),
          overwrite = T)
rm(esSWQ,esSWQb)




