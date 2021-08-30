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

setwd("H:/ericg/16666LAWA/LAWA2021/Lakes")
agency='nrc'

df <- read.csv("H:/ericg/16666LAWA/LAWA2021/Lakes/Metadata/nrcLWQ_config.csv",sep=",",stringsAsFactors=FALSE)
Measurements <- subset(df,df$Type=="Measurement")[,1]

siteTable=loadLatestSiteTableLakes()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])

workers = makeCluster(6)
registerDoParallel(workers)

clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})


if(exists("Data"))rm(Data)
nrcLWQ=NULL
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
            keeps = which(tolower(tags$Run.Type)=='lwqmn')
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
      nrcLWQ=bind_rows(nrcLWQ,siteDat)
    }
    rm(siteDat)
  }
})

stopCluster(workers)
rm(workers)



save(nrcLWQ,file = 'nrcLWQraw.rData')
# load('nrcLWQraw.rData')
agency='ncr'



nrcLWQ <- nrcLWQ%>%select(CouncilSiteID,Date=T,Value,Measurement)

nrcLWQb=data.frame(CouncilSiteID=nrcLWQ$CouncilSiteID,
                   Date=as.character(format(lubridate::ymd_hms(nrcLWQ$Date),'%d-%b-%y')),
                   Value=nrcLWQ$Value,
                   Method="",
                   Measurement=nrcLWQ$Measurement,
                   Censored=grepl(pattern = '<|>',x = nrcLWQ$Value),
                   centype=F,QC=NA)
nrcLWQb$centype[grep('<',nrcLWQb$Value)] <- 'Left'
nrcLWQb$centype[grep('>',nrcLWQb$Value)] <- 'Right'

nrcLWQb$Value = readr::parse_number(nrcLWQb$Value)

if(mean(nrcLWQb$Value[nrcLWQb$Measurement=="Chlorophyll a"])<1){
  nrcLWQb$Value[nrcLWQb$Measurement=="Chlorophyll a"] = nrcLWQb$Value[nrcLWQb$Measurement=="Chlorophyll a"]*1000
}


table(nrcLWQb$Measurement,useNA = 'a')
nrcLWQb$Measurement <- as.character(factor(nrcLWQb$Measurement,
                                           levels = Measurements,
                                           labels = c("TP","NH4N","TN","Secchi","Secchi","CHLA","CHLA",
                                                      "pH","ECOLI","CYANOTOT","CYANOTOX")))
table(nrcLWQb$Measurement,useNA='a')

# nrcLWQb <- merge(nrcLWQb,siteTable,by='CouncilSiteID')


write.csv(nrcLWQb,file = paste0("D:/LAWA/2021/nrcL.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/nrcL.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d"),"/nrc.csv"),
          overwrite = T)
# write.csv(nrcLWQb,file = paste0("H:/ericg/16666LAWA/LAWA2021/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,".csv"),row.names = F)
rm(nrcLWQ,nrcLWQb)

lawaset=c("TN","NH4N","TP","CHLA","pH","Secchi","ECOLI","CYANOTOT","CYANOTOX")

