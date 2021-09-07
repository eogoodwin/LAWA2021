require(xml2)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)
library(parallel)
library(doParallel)

setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")
agency='wrc'

translate <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",
                        sep=',',header=T,stringsAsFactors = F)%>%filter(Agency==agency)

translate$retName=c("Ammoniacal Nitrogen","DIN Calculated","Dissolved Reactive Phosphorus","E-Coli MPN","Nitrite/Nitrate Nitrogen","pH (Lab)","NO3N","Total Nitrogen","Total Phosphorus","Turbidity","Turbidity (X)")

siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])





for(i in 1:length(sites)){
  dir.create(paste0('D:/LAWA/2021/wrc/',sites[i]),recursive = T,showWarnings = F)
  cat('\n',sites[i],i,'out of ',length(sites))
  for(j in 1:length(translate$CallName)){
    destFile=paste0("D:/LAWA/2021/wrc/",sites[i],"/",make.names(translate$CallName[j]),".xml")
    if(!file.exists(destFile)||file.info(destFile)$size<2000){
      
        url <- paste0("http://envdata.waikatoregion.govt.nz:8080/KiWIS/KiWIS?datasource=0&service=SOS&agency=LAWA&",
                  "version=2.0&request=GetObservation&procedure=RERIMP.Sample.Results.P",
                  "&featureOfInterest=",sites[i],
                  "&observedProperty=", translate$CallName[j],
                  "&temporalfilter=om:phenomenonTime,2004-01-01/2021-01-01")
    url <- URLencode(url)
    
    dl=try(download.file(url,destfile=destFile,method='wininet',quiet=T),silent = T)
    if(!'try-error'%in%attr(dl,'class')){
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
    }
    if('try-error'%in%attr(dl,'class')||length(Data)==0){
      url <- paste0("http://envdata.waikatoregion.govt.nz:8080/KiWIS/KiWIS?datasource=0&service=SOS&agency=LAWA&",
                    "version=2.0&request=GetObservation&procedure=WARIMP.Sample.Results.P",
                    "&featureOfInterest=",sites[i],
                    "&observedProperty=", translate$CallName[j],
                    "&temporalfilter=om:phenomenonTime,2004-01-01/2021-01-01")
      url <- URLencode(url)
      dl=try(download.file(url,destfile=destFile,method='wininet',quiet=T),silent = T)
      if(!'try-error'%in%attr(dl,'class')){
        Data=xml2::read_xml(destFile)
        Data = xml2::as_list(Data)[[1]]
      }else{
        file.remove(destFile)
        rm(destFile)
        next
        }
    }
    
        cat(',')
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
    destFile=paste0("D:/LAWA/2021/wrc/",sites[i],"/",make.names(translate$CallName[j]),".xml")
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
rm(workers,dummyout)





workers = makeCluster(7)
registerDoParallel(workers)

clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})

if(exists("Data"))rm(Data)
wrcSWQ=NULL

for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  dir.create(paste0('D:/LAWA/2021/wrc/',sites[i]),recursive = T,showWarnings = F)
  rm(siteDat)
  foreach(j = 1:length(translate$CallName),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    destFile=paste0("D:/LAWA/2021/wrc/",sites[i],"/",make.names(translate$CallName[j]),".xml")
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
    wrcSWQ=bind_rows(wrcSWQ,siteDat)
  }
  rm(siteDat)
}
stopCluster(workers)
rm(workers)



save(wrcSWQ,file = 'wrcSWQraw.rData')
# load('wrcSWQraw.rData') #76883
agency='wrc'




wrcSWQ <- wrcSWQ%>%select(CouncilSiteID,Date=T,Value,Measurement,units)


wrcSWQb=data.frame(CouncilSiteID=wrcSWQ$CouncilSiteID,
                   Date=as.character(format(lubridate::ymd_hms(wrcSWQ$Date),'%d-%b-%y')),
                   Value=wrcSWQ$Value,
                   Measurement=wrcSWQ$Measurement,
                   Units=wrcSWQ$units,
                   Censored=grepl(pattern = '<|>',x = wrcSWQ$Value),
                   CenType=F,QC=NA)
wrcSWQb$CenType[grep('<',wrcSWQb$Value)] <- 'Left'
wrcSWQb$CenType[grep('>',wrcSWQb$Value)] <- 'Right'

wrcSWQb$Value = readr::parse_number(wrcSWQb$Value)


table(wrcSWQb$Measurement,useNA = 'a')
wrcSWQb$Measurement <- as.character(factor(wrcSWQb$Measurement,
                                           levels = translate$CallName,
                                           labels = translate$LAWAName))
table(wrcSWQb$Measurement,useNA='a')

wrcSWQb <- unique(wrcSWQb)

write.csv(wrcSWQb,file = paste0("D:/LAWA/2021/wrc.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/wrc.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/wrc.csv"),
          overwrite = T)
rm(wrcSWQ,wrcSWQb)
