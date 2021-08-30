## Load libraries ------------------------------------------------
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(xml2)     ### XML library to write hilltop XML
require(RCurl)
library(parallel)
library(doParallel)

setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")
agency='wcrc'
translate <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%filter(Agency==agency)
translate$retName =c("Ammonia Total","Dissolved Reactive Phosphorus","Dissolved Inorganic Nitrogen",
                     "E.coli (Mem Filtration)","Nitrate + Nitrite","Nitrogen (Total)",
                     "Nitrate-N","pH","Phosphorous (Total)",
                     "Turbidity","Turbidity","Water Clarity (Black Disc)",
                     "Water Clarity (Clarity Tube)")


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
wcrcSWQ=NULL
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  dir.create(paste0('D:/LAWA/2021/WCRC/',gsub('\\.','',make.names(sites[i]))),recursive=T,showWarnings = F)
  foreach(j = 1:length(translate$CallName),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    url <- paste0("http://hilltop.wcrc.govt.nz/wq.hts?service=Hilltop&request=GetData",
                  "&Site=",sites[i],
                  "&Measurement=",URLencode(translate$CallName[j],reserved = T),
                  "&From=2004-01-01",
                  "&To=2021-01-01")
    url <- URLencode(url)
    
    destFile=paste0("D:/LAWA/2021/WCRC","/",
                    gsub('\\.','',make.names(sites[i])),'/',
                    make.names(translate$CallName[j]),".xml")
    if(!file.exists(destFile)|file.info(destFile)$size<3500){
      dl=try(download.file(url,destfile=destFile,method='wininet',quiet=T),silent = T)
      if(!'try-error'%in%attr(dl,'class')){
        if(exists('Data'))rm(Data)
        Data=xml2::read_xml(destFile)
        Data = xml2::as_list(Data)[[1]]
        if(length(Data)>0&&names(Data)[1]!='Error'){
          cat('.')
          RetProperty=Data$Measurement$DataSource$ItemInfo$ItemName[[1]]
          RetCID = attr(Data$Measurement,'SiteName')
          while(RetProperty!=translate$retName[j]|RetCID!=sites[i]){
            file.rename(from = destFile,to = paste0(make.names(RetProperty),destFile))
            dl=download.file(url,destfile=destFile,method='libcurl',quiet=T)
            Data=xml2::read_xml(destFile)
            Data = xml2::as_list(Data)[[1]]
            if(length(Data)>0&&names(Data)[1]!='Error'){
              RetProperty=Data$Measurement$DataSource$ItemInfo$ItemName[[1]]
              RetCID = attr(Data$Measurement,'SiteName')
            }
          }
        }
      }
    }
  }
}

stopCluster(workers)
rm(workers)

#Check site and measurement returned
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  rm(siteDat)
  for(j in 1:13){
    destFile=paste0("D:/LAWA/2021/WCRC","/",
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
wcrcSWQ=NULL
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  rm(siteDat)
  foreach(j = 1:length(translate$CallName),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    
    destFile=paste0("D:/LAWA/2021/WCRC","/",
                    gsub('\\.','',make.names(sites[i])),'/',
                    make.names(translate$CallName[j]),".xml")
    if(file.exists(destFile)&file.info(destFile)$size>150){
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
      if(length(Data)>0&&names(Data)[1]!='Error'){
        cat('.')
        RetProperty=Data$Measurement$DataSource$ItemInfo$ItemName[[1]]
        RetCID = attr(Data$Measurement,'SiteName')
        if(length(Data$Measurement$DataSource$ItemInfo$Units)>0){
          RetUnit = Data$Measurement$DataSource$ItemInfo$Units[[1]]
        }else{
          RetUnit="unspecified"
        }
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
          Data$RetProperty=RetProperty
          Data$CouncilSiteID=sites[i]
          Data$RetCID = RetCID
          Data$Units = RetUnit
          rownames(Data) <- NULL
        }else{Data=NULL}
      }else{Data=NULL}
    }else{Data=NULL}
    rm(destFile)
    return(Data)
  }->siteDat
  
  if(!is.null(siteDat)){
    rownames(siteDat) <- NULL
    wcrcSWQ=bind_rows(wcrcSWQ,siteDat)
  }
  rm(siteDat)
}

stopCluster(workers)
rm(workers)

save(wcrcSWQ,file = 'wcrcSWQraw.rData')
# load('wcrcSWQraw.rData') #29503
agency='wcrc'


# wcrcSWQ <- wcrcSWQ%>%filter(!QualityCode%in%c())




wcrcSWQ <- wcrcSWQ%>%select(CouncilSiteID,Date=T,Value,Measurement,Units,QualityCode)



wcrcSWQb=data.frame(CouncilSiteID=wcrcSWQ$CouncilSiteID,
                    Date=as.character(format(lubridate::ymd_hms(wcrcSWQ$Date),'%d-%b-%y')),
                    Value=wcrcSWQ$Value,
                    Measurement=wcrcSWQ$Measurement,
                    Units=wcrcSWQ$Units,
                    Censored=grepl(pattern = '<|>',x = wcrcSWQ$Value),
                    CenType=F,
                    QC=wcrcSWQ$QualityCode)
wcrcSWQb$CenType[grep('<',wcrcSWQb$Value)] <- 'Left'
wcrcSWQb$CenType[grep('>',wcrcSWQb$Value)] <- 'Right'

wcrcSWQb$Value = readr::parse_number(wcrcSWQb$Value)

translate=read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName,LAWAName)

wcrcSWQb$measname=wcrcSWQb$Measurement
table(wcrcSWQb$Measurement,useNA = 'a')
wcrcSWQb$Measurement <- as.character(factor(wcrcSWQb$Measurement,
                                            levels = translate$CallName,
                                            labels = translate$LAWAName))
table(wcrcSWQb$measname,wcrcSWQb$Measurement,useNA='a')






#Censor the data there that came in.
# From: Emma Perrin-Smith <emmaps@wcrc.govt.nz> 
#   Sent: Wednesday, 25 August 2021 12:15
# To: Eric Goodwin <Eric.Goodwin@cawthron.org.nz>
#   Subject: Detection limits
# 
# Good afternoon Eric,
# 
# We have just been reviewing the water quality data we are using for our SoE trend analysis and due to changes in lab detection limits over time we are adjusting some of our parameters to be consistent.
# I’m not sure if LAWA has been doing any of this? I think at some stage in the past we provided info on detection level changes?
#   
#   Might be too late to re-run LAWA analyses but for your information we are adjusting:
#   Ammonia – 0.005
# TP – 0.005
# DRP – 0.004
# N03 – 0.002            - revised later to 0.01
# TN – 0.1
# 
# All values below these numbers will be adjusted up to this value.
# 
# Thanks,
# Emma
# 
cenNH4 = which(wcrcSWQb$Measurement=="NH4"&wcrcSWQb$Value<=0.005)
cenTP =  which(wcrcSWQb$Measurement=="TP"&wcrcSWQb$Value<=0.005)
cenDRP = which(wcrcSWQb$Measurement=="DRP"&wcrcSWQb$Value<=0.004)
cenNO3 = which(wcrcSWQb$Measurement=='NO3N'&wcrcSWQb$Value<=0.01)
cenTN =  which(wcrcSWQb$Measurement=="TN"&wcrcSWQb$Value<=0.1)


if(length(cenNH4)>0){
  wcrcSWQb$Censored[cenNH4] <- TRUE
  wcrcSWQb$CenType[cenNH4] <- "Left"
  wcrcSWQb$Value[cenNH4] <- 0.005
}
if(length(cenTP)>0){
  wcrcSWQb$Censored[cenTP] <- TRUE
  wcrcSWQb$CenType[cenTP] <- "Left"
  wcrcSWQb$Value[cenTP] <- 0.005
}
if(length(cenDRP)>0){
  wcrcSWQb$Censored[cenDRP] <- TRUE
  wcrcSWQb$CenType[cenDRP] <- "Left"
  wcrcSWQb$Value[cenDRP] <- 0.004
}
if(length(cenNO3)>0){
  wcrcSWQb$Censored[cenNO3] <- TRUE
  wcrcSWQb$CenType[cenNO3] <- "Left"
  wcrcSWQb$Value[cenNO3] <- 0.01
}
if(length(cenTN)>0){
  wcrcSWQb$Censored[cenTN] <- TRUE
  wcrcSWQb$CenType[cenTN] <- "Left"
  wcrcSWQb$Value[cenTN] <- 0.1
}

rm(cenTN,cenNH4,cenDRP,cenTP,cenNO3)


wcrcSWQb <- unique(wcrcSWQb%>%select(-measname))

# wcrcSWQb <- merge(wcrcSWQb,siteTable,by='CouncilSiteID')


write.csv(wcrcSWQb,file = paste0("D:/LAWA/2021/wcrc.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/wcrc.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/wcrc.csv"),
          overwrite = T)
rm(wcrcSWQ,wcrcSWQb)



