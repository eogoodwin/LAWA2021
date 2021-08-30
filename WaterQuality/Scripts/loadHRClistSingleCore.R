


require(xml2)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)
library(parallel)
library(doParallel)

setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")
agency='hrc'

translate=read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%filter(Agency==agency)%>%select(CallName,LAWAName)

translate$retName = c("Ammoniacal-N (HRC)", "Black Disc (HRC)", "Dissolved Reactive Phosphorus (HRC)", 
                        "E. coli by MPN (HRC)", "Field pH (HRC)", "Total Nitrogen (HRC)", 
                        "Nitrate (HRC)", "Total Oxidised Nitrogen (HRC)","Total Nitrogen (HRC)",
                      "Total Phosphorus (HRC)","Soluble Inorganic Nitrogen (HRC)", "Turbidity EPA (HRC)",
                      "Turbidity ISO (HRC)")

# Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",
#                            sep=',',header=T,stringsAsFactors = F)%>%
#   filter(Agency==agency)%>%select(CallName)%>%unname%>%unlist

siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])


workers = makeCluster(7)
registerDoParallel(workers)

clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})
i=1
for(i in i:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  dir.create(paste0("D:/LAWA/2021/HRC/",make.names(sites[i])),showWarnings = F)
  # for(j in 1:length(translate$CallName)){
    foreach(j = 1:length(translate$CallName),.combine = c,.errorhandling = 'remove',.inorder = FALSE)%dopar%{
      
      url <- paste0("http://tsdata.horizons.govt.nz/boo.hts?service=SOS&agency=LAWA&request=GetObservation",
                    "&FeatureOfInterest=",sites[i],
                    "&ObservedProperty=",translate$CallName[j],
                    "&TemporalFilter=om:phenomenonTime,2004-01-01,2021-01-01")
      url <- URLencode(url)
      
      destFile=paste0("D:/LAWA/2021/HRC/",make.names(sites[i]),"/",make.names(translate$CallName[j]),"WQ","hrc.xml")
      if(!file.exists(destFile)|file.info(destFile)$size<3500){
      dl=download.file(url,destfile=destFile,method='wininet',quiet=T)
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
      if(length(Data)>1){
        RetProperty=attr(Data$observationMember$OM_Observation$procedure,'title')
        RetCID = attr(Data$observationData$OM_Observation$featureOfInterest,'href')
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
    destFile=paste0("D:/LAWA/2021/HRC","/",
                    gsub('\\.','',make.names(sites[i])),'/',
                    make.names(translate$CallName[j]),".xml")
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



workers = makeCluster(8)
registerDoParallel(workers)

clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})

if(exists("Data"))rm(Data)
hrcSWQ=NULL
foreach(i = 1:length(sites),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
  cat('\n',sites[i],i,'out of ',length(sites))
  siteDat=NULL
  # foreach(j = 1:length(translate$CallName),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
  for(j in 1:length(translate$CallName)){
    destFile=paste0("D:/LAWA/2021/HRC/",make.names(sites[i]),"/",make.names(translate$CallName[j]),"WQ","hrc.xml")
    if(file.exists(destFile)&file.info(destFile)$size>1000){
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
      if(length(Data)>0){
        RetProperty=attr(Data$observationMember$OM_Observation$procedure,'title')
        RetSID = attr(Data$observationMember$OM_Observation$featureOfInterest,'title')
        
        Data=Data$observationMember$OM_Observation$result$MeasurementTimeseries
        if('defaultPointMetadata'%in%names(Data)){
          metaData = Data$defaultPointMetadata
          uom=attr(metaData$DefaultTVPMeasurementMetadata$uom,'code')
          rm(metaData)
        }else{
          uom='unfound'
        }
        Data=do.call(rbind, unname(sapply(Data,FUN=function(listItem){
          if("MeasurementTVP"%in%names(listItem)){
            if(length(listItem$MeasurementTVP$value)>0){
              retVal=data.frame(time=unlist(listItem$MeasurementTVP$time),
                                value=unlist(listItem$MeasurementTVP$value),
                                Censored=F)
            }else{
              retVal = data.frame(time=unlist(listItem$MeasurementTVP$time),
                                  value=unlist(listItem$MeasurementTVP$metadata$TVPMeasurementMetadata$qualifier$Quantity$value),
                                  Censored=T)
            }
            return(retVal)
          }
        })))
        if(!is.null(Data)){
          Data$measurement=translate$CallName[j]
          Data$retProp=RetProperty
          Data$CouncilSiteID = sites[i]
          Data$RetCID=strFrom(s = RetSID,c="stations/")
          Data$Units=uom
        }
      }
      rm(destFile)
    }else{
      Data=NULL
    }
    # return(Data)
    siteDat=bind_rows(siteDat,Data)
  }
  
  # if(!is.null(siteDat)){
  #   rownames(siteDat) <- NULL
  #   hrcSWQ=bind_rows(hrcSWQ,siteDat)
  # }
  # rm(siteDat)
  return(siteDat)
}->hrcSWQ
stopCluster(workers)
rm(workers)


save(hrcSWQ,file = 'hrcSWQraw.rData')
# load('hrcSWQraw.rData')
agency='hrc'

#Audit retuned peroperty against requested
table(translate$LAWAName[match(hrcSWQ$measurement,translate$CallName)] == translate$LAWAName[match(hrcSWQ$retProp,translate$retName)])

hrcSWQ%>%filter(translate$LAWAName[match(hrcSWQ$measurement,translate$CallName)] != translate$LAWAName[match(hrcSWQ$retProp,translate$retName)])




hrcSWQ <- hrcSWQ%>%select(CouncilSiteID,Date=time,Value=value,Measurement=measurement,Units)


hrcSWQb=data.frame(CouncilSiteID=hrcSWQ$CouncilSiteID,
                   Date=as.character(format(lubridate::ymd_hms(hrcSWQ$Date),'%d-%b-%y')),
                   Value=hrcSWQ$Value,
                   Measurement=hrcSWQ$Measurement,
                   Units=hrcSWQ$Units,
                   Censored=grepl(pattern = '<|>',x = hrcSWQ$Value),
                   CenType=F,QC=NA)
hrcSWQb$CenType[grep('<',hrcSWQb$Value)] <- 'Left'
hrcSWQb$CenType[grep('>',hrcSWQb$Value)] <- 'Right'

hrcSWQb$Value = readr::parse_number(hrcSWQb$Value)


# translate=read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
#   filter(Agency==agency)%>%select(CallName,LAWAName)


table(hrcSWQb$Measurement,useNA = 'a')
hrcSWQb$Measurement <- as.character(factor(hrcSWQb$Measurement,
                                           levels = translate$CallName,
                                           labels = translate$LAWAName))
table(hrcSWQb$Measurement,useNA='a')


hrcSWQb <- unique(hrcSWQb)

# hrcSWQb <- merge(hrcSWQb,siteTable,by='CouncilSiteID')



write.csv(hrcSWQb,file = paste0("D:/LAWA/2021/hrc.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/hrc.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/hrc.csv"),
          overwrite = T)
rm(hrcSWQ,hrcSWQb)


if(0){




todaySiteCount = table(paste(tolower(hrcSWQb$CouncilSiteID),tolower(hrcSWQb$Measurement)))
disksiteCount = table(paste(tolower(mfl$CouncilSiteID),tolower(mfl$Measurement)))

siteCountComp = data.frame(comb = names(todaySiteCount))
siteCountComp$today = todaySiteCount[match(siteCountComp$comb ,names(todaySiteCount))]
siteCountComp$disc = disksiteCount[match(siteCountComp$comb ,names(disksiteCount))]

table(siteCountComp$today==siteCountComp$disc)
siteCountComp%>%filter(today<disc)%>%head

agencyFiles = rev(dir(path = "H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",
                      pattern = '^hrc.csv',
                      full.names = T,recursive = T,ignore.case = T))

par(mfrow=c(7,1),mar=c(3,2,2,1))
for(af in agencyFiles){
  oldhrc=read_csv(af)
  cat(af,'\t',dim(oldhrc),'\n')
cat('\n',length(unique(oldhrc$CouncilSiteID)),'\t')
  }


with(mfl%>%filter(tolower(CouncilSiteID)=='arawhata drain at hokio beach road',tolower(Measurement)=="din"),
     plot(lubridate::dmy(Date),Value))
with(hrcSWQb%>%filter(tolower(CouncilSiteID)=='arawhata drain at hokio beach road',tolower(Measurement)=="din"),
     points(lubridate::dmy(Date),Value,pch=16,cex=0.75,col='red'))







paraFiles = dir("D:/LAWA/2021/hrcpara/")
scFiles = dir("D:/LAWA/2021/hrc")


scFiles[!scFiles%in%paraFiles]
paraFiles[!paraFiles%in%scFiles]

fileInfos=data.frame(files=scFiles,size = sapply(scFiles,function(f)file.info(paste0("D:/LAWA/2021/hrc/",f))$size))

paraInfos = data.frame(files=paraFiles,size = sapply(paraFiles,function(f)file.info(paste0("D:/LAWA/2021/hrcpara/",f))$size))


fileInfos$ParaSize = paraInfos$size[match(fileInfos$files,paraInfos$files)]

table(fileInfos$size==fileInfos$ParaSize)

fileInfos%>%filter(size!=ParaSize)%>%head



todaySiteCount = table(paste(hrcSWQb$CouncilSiteID,hrcSWQb$Measurement))

disksiteCount = table(paste(mfl$CouncilSiteID,mfl$Measurement))

siteCountComp = data.frame(comb = names(todaySiteCount))
siteCountComp$today = todaySiteCount[match(siteCountComp$comb ,names(todaySiteCount))]
siteCountComp$disc = disksiteCount[match(siteCountComp$comb ,names(disksiteCount))]

table(siteCountComp$today==siteCountComp$disc)
siteCountComp%>%filter(today<disc)%>%head

}