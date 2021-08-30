## Load libraries ------------------------------------------------
require(tidyverse)
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(xml2)     ### XML library to write hilltop XML
require(RCurl)
library(parallel)
library(doParallel)

agency='orc'
setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality/")
Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName)%>%unname%>%unlist


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
orcSWQ=NULL
suppressMessages({
  for(i in 1:length(sites)){
    cat('\n',sites[i],i,'out of ',length(sites))
    suppressWarnings(rm(siteDat))
    foreach(j = 1:length(Measurements),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
      url <- paste0("http://gisdata.orc.govt.nz/hilltop/ORCWQ.hts?service=Hilltop&request=GetData&agency=LAWA",
                    "&Site=",sites[i],
                    "&Measurement=",Measurements[j],
                    "&From=2004-01-01",
                    "&To=2021-01-01")
      url <- URLencode(url)
      
      destFile=paste0("D:/LAWA/2021/tmp",gsub('[[:punct:]]| ','',Measurements[j]),"orc.xml")
      
      dl=try(download.file(url,destfile=destFile,method='curl',quiet=T),silent = T)
      if(!'try-error'%in%attr(dl,'class')){
        Data=xml2::read_xml(destFile)
        Data = xml2::as_list(Data)[[1]]
        dataSource = unlist(Data$Measurement$DataSource)
        Data = Data$Measurement$Data
        if(length(Data)>0){
          mayHaveTags=which(sapply(Data,function(e)'parameter'%in%tolower(names(e))))
          if(length(mayHaveTags)>0){
            tags=data.frame(t(sapply(mayHaveTags,function(li){
              as.data.frame(sapply(Data[[li]][which(names(Data[[li]])=="Parameter")],function(ee){
                retVal=data.frame(attributes(ee)$Value)
                names(retVal)=attributes(ee)$Name
                return(retVal)
              }))
            })))
            if(!any(grepl('parameter',names(tags),ignore.case=T))){
              #dealing with Error: dim(tags)[1] == dim(Data)[1] is not TRUE
              tags = do.call(bind_rows,sapply(mayHaveTags,function(li){
                pivot_wider(data.frame(t(sapply(Data[[li]][which(names(Data[[li]])=='Parameter')],function(ee){
                  data.frame(attributes(ee))
                }))),names_from=Name,values_from=Value,values_fn = unlist)%>%as.data.frame
              }))
            }else{
              names(tags) <- gsub('Parameter.','',names(tags))
            }
            tags$matchIndex = mayHaveTags
          }
 
          mayHaveQC = which(sapply(Data,function(e)any(grepl('qual',tolower(names(e))))))
          if(length(mayHaveQC)>0){
            QC = data.frame(QC=unname(unlist(sapply(mayHaveQC,function(li)unlist(Data[[li]]$QualityCode)))))
            QC$matchIndex = mayHaveQC
          }
          rm(mayHaveQC)
          Data=data.frame(apply(t(sapply(Data,function(e)bind_rows(unlist(e[1:2])))),2,unlist),row.names = NULL)
          
          Data$Measurement=Measurements[j]
          Data$matchIndex = 1:dim(Data)[1]
          if(exists('tags')){
            Data=merge(x=Data,tags,by='matchIndex',all.x=T)
            rm(tags)
          }
          if(exists('QC')){
            Data = merge(x=Data,QC,by='matchIndex',all.x=T)
            rm(QC)
          }
          if(exists('dataSource')){
            if('ItemInfo.Units'%in%names(dataSource)){
              Data$Units = unname(dataSource[which(names(dataSource)=="ItemInfo.Units")])
            }
          }
          rownames(Data) <- NULL
        }
      }else{
        Data=NULL
        }
      file.remove(destFile)
      rm(destFile)
      return(Data)
    }->siteDat
    
    if(!is.null(siteDat)){
      rownames(siteDat) <- NULL
      siteDat$CouncilSiteID = sites[i]
      orcSWQ=bind_rows(orcSWQ,siteDat)
    }
    rm(siteDat)
  }
})
stopCluster(workers)
rm(workers)



save(orcSWQ,file = 'orcSWQraw.rData')
# load('orcSWQraw.rData')
agency='orc'


# orcSWQ <- orcSWQ%>%filter(!QC%in%c())

 
Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency=='orc')%>%select(CallName)%>%unname%>%unlist

orcSWQ <- orcSWQ%>%select(CouncilSiteID,Date=T,Value,Measurement,Units,matches('units'),matches('QC'))



orcSWQb=data.frame(CouncilSiteID=orcSWQ$CouncilSiteID,
                  Date=as.character(format(lubridate::ymd_hms(orcSWQ$Date),'%d-%b-%y')),
                  Value=orcSWQ$Value,
                  Measurement=orcSWQ$Measurement,
                  Units = orcSWQ$Units,
                  Censored=grepl(pattern = '<|>',x = orcSWQ$Value),
                  CenType=F,QC=F)
orcSWQb$CenType[grep('<',orcSWQb$Value)] <- 'Left'
orcSWQb$CenType[grep('>',orcSWQb$Value)] <- 'Right'

orcSWQb$Value = readr::parse_number(orcSWQb$Value)




orcSWQb$measName=orcSWQb$Measurement
table(orcSWQb$Measurement,useNA = 'a')
orcSWQb$Measurement <- as.character(factor(orcSWQb$Measurement,
                                          levels = Measurements,
                                          labels = c("NH4","DIN","DRP",
                                                     "ECOLI","TON","PH",
                                                     "NO3N","TN","TP",
                                                     "TURB","TURB")))
table(orcSWQb$Measurement,orcSWQb$measName,useNA='a')
table(orcSWQb$Measurement,useNA = 'a')

orcSWQb <- unique(orcSWQb%>%select(-measName))

 # orcSWQb <- merge(orcSWQb,siteTable,by='CouncilSiteID')


write.csv(orcSWQb,file = paste0("D:/LAWA/2021/orc.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/orc.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/orc.csv"),
          overwrite = T)
rm(orcSWQ,orcSWQb)



