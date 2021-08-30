require(xml2)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)
library(parallel)
library(doParallel)


# Just to add a bit of complexity (sorry…), it is correct to filter on Run Type = RWQMN for 
# all sites with the exception of the five sites below, they all used to be 
# Catchment sites (Run type = Catchment) from 2014 to September 2020 
# and RWQMN since then (i.e. Oct 2020).
# 
# LOC.105972 Hatea at Whangarei Falls
# LOC.304589 Waiaruhe at Puketona
# LOC.304641 Oruaiti at Windust Road
# LOC.304709 Raumanga at Bernard Street
# LOC.306641 Peria at Honeymoon Valley Road
# 
# Please let me know if it is a problem.

setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")
agency='nrc'

Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName)%>%unname%>%unlist
# Measurements=c(Measurements,'WQ Sample')

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
nrcSWQ=NULL
suppressMessages({
  for(i in 1:length(sites)){
    cat('\n',sites[i],i,'out of ',length(sites))
    rm(siteDat)
    foreach(j = 1:length(Measurements),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
      url <- paste0("http://hilltop.nrc.govt.nz/SOEFinalArchive.hts?service=Hilltop&request=GetData&agency=LAWA",
                    "&Site=",sites[i],
                    "&Measurement=",Measurements[j],
                    "&From=2004-01-01",
                    "&To=2021-01-01")
      url <- URLencode(url)
      
      destFile=paste0("D:/LAWA/2021/tmp",gsub('[[:punct:]]| ','',Measurements[j]),"WQnrc.xml")
      
      dl=try(download.file(url,destfile=destFile,method='wininet',quiet=T),silent = T)
      if(!'try-error'%in%attr(dl,'class')){
        Data=xml2::read_xml(destFile)
        Data = xml2::as_list(Data)[[1]]
        dataSource = unlist(Data$Measurement$DataSource)
        Data = Data$Measurement$Data
        if(length(Data)>0){
          Data=apply(t(sapply(Data,function(e)bind_rows(unlist(e)))),2,unlist)
          if('character'%in%class(Data)){
            Data=data.frame(as.list(Data),row.names = NULL)
          }else{
            Data=data.frame(Data,row.names=NULL)
          }
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
          Data$Measurement=Measurements[j]
          
          if('Run.Type'%in%names(tags)){
            keeps = which(tolower(tags$Run.Type)=='rwqmn')
            if(sites[i]%in%c("105972","304589","304641","304709","306641")){
              keeps = c(keeps,which(tolower(tags$Run.Type)=='catchment'))
            }
            Data = Data[keeps,]
          }
        }
      }
      file.remove(destFile)
      rm(destFile)
      return(Data)
    }->siteDat
    
    if(!is.null(siteDat)){
      rownames(siteDat) <- NULL
      siteDat$CouncilSiteID = sites[i]
      nrcSWQ=bind_rows(nrcSWQ,siteDat)
    }
    rm(siteDat)
  }
})

stopCluster(workers)
rm(workers)




mtCols = which(apply(nrcSWQ,2,function(c)all(is.na(c))))
if(length(mtCols)>0){
  mtCols=mtCols[which(mtCols>6)]
  if(length(mtCols)>0){
    nrcSWQ=nrcSWQ[,-mtCols]
  }
}
rm(mtCols)
nrcSWQ <- nrcSWQ%>%select(CouncilSiteID,Date=T,Value,Measurement,Units)


save(nrcSWQ,file = 'nrcSWQraw.rData')
# load('nrcSWQraw.rData')
agency='nrc'



Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName)%>%unname%>%unlist


nrcSWQb=data.frame(CouncilSiteID=nrcSWQ$CouncilSiteID,
                   Date=as.character(format(lubridate::ymd_hms(nrcSWQ$Date),'%d-%b-%y')),
                   Value=nrcSWQ$Value,
                   Measurement=nrcSWQ$Measurement,
                   Units=nrcSWQ$Units,
                   Censored=grepl(pattern = '<|>',x = nrcSWQ$Value),
                   CenType=F,QC=NA)
nrcSWQb$CenType[grep('<',nrcSWQb$Value)] <- 'Left'
nrcSWQb$CenType[grep('>',nrcSWQb$Value)] <- 'Right'

nrcSWQb$Value = readr::parse_number(nrcSWQb$Value)


translate=read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName,LAWAName)%>%arrange(LAWAName)
nrcSWQb <- nrcSWQb%>%filter(Measurement%in%translate$CallName)

table(nrcSWQb$Measurement,useNA = 'a')
nrcSWQb$Measurement <- as.character(factor(nrcSWQb$Measurement,
                                           levels = translate$CallName,
                                           labels = translate$LAWAName))
table(nrcSWQb$Measurement,useNA='a')

nrcSWQb <- unique(nrcSWQb)

# nrcSWQb <- merge(nrcSWQb,siteTable,by='CouncilSiteID')


write.csv(nrcSWQb,file = paste0("D:/LAWA/2021/nrc.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/nrc.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/nrc.csv"),
          overwrite = T)
rm(nrcSWQ,nrcSWQb)
