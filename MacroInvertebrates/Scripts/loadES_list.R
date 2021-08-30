## Load libraries ------------------------------------------------
require(tidyverse)
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(xml2)     ### XML library to write hilltop XML
require(RCurl)
library(parallel)
library(doParallel)
setwd("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/")

agency='es'
Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Metadata/esMacro_config.csv",sep=',',header=T,stringsAsFactors = F)%>%
  select(Value)%>%unname%>%unlist

if("WQ Sample"%in%Measurements){
  Measurements=Measurements[-which(Measurements=="WQ Sample")]
}

siteTable=loadLatestSiteTableMacro()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])

workers = makeCluster(8)
registerDoParallel(workers)

clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})


if(exists("Data"))rm(Data)
esM=NULL
suppressMessages({
  for(i in 1:length(sites)){
    cat('\n',sites[i],i,'out of ',length(sites))
    suppressWarnings(rm(siteDat))
    foreach(j = 1:length(Measurements),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
      url <- paste0("http://odp.es.govt.nz/MI.hts?service=Hilltop&request=GetData",
                    "&Site=",URLencode(sites[i]),
                    "&Measurement=",URLencode(Measurements[j]),
                    "&From=1990-01-01",
                    "&To=2021-06-01")
      url <- URLencode(url)
      
      destFile=paste0("D:/LAWA/2021/tmp",gsub('[[:punct:]]| ','',Measurements[j]),"WQes.xml")
      
      dl=try(download.file(url,destfile=destFile,method='wininet',quiet=T),silent = T)
      if(!'try-error'%in%attr(dl,'class')){
        Data=xml2::read_xml(destFile)
        Data = xml2::as_list(Data)[[1]]
        # dataSource = unlist(Data$Measurement$DataSource)
        Data = Data$Measurement$Data
        if(length(Data)>0){
          tags=data.frame(t(sapply(Data,function(e){
            as.data.frame(sapply(e[which(names(e)=="Parameter")],function(ee){
              retVal=data.frame(attributes(ee)$Value)
              names(retVal)=attributes(ee)$Name
              return(retVal)
            }))
          })))
          names(tags) <- gsub('Parameter.','',names(tags))
          
          Data=data.frame(apply(t(sapply(Data,function(e)bind_rows(unlist(e)))),2,unlist),row.names = NULL)
          Data$Measurement=Measurements[j]
          stopifnot(dim(tags)[1]==dim(Data)[1])
          Data=cbind(Data,tags)
          rownames(Data) <- NULL
          rm(tags)
        }
      }else(Data=NULL)
      file.remove(destFile)
      rm(destFile)
      return(Data)
    }->siteDat
    
    if(!is.null(siteDat)){
      rownames(siteDat) <- NULL
      siteDat$CouncilSiteID = sites[i]
      esM=bind_rows(esM,siteDat)
    }
    rm(siteDat)
  }
})



stopCluster(workers)
rm(workers)

save(esM,file = 'esMraw.rData')
load('esMraw.rData')

mtCols = which(apply(esM,2,function(c)all(is.na(c))))
if(length(mtCols)>0){
  mtCols=mtCols[which(mtCols>6)]
  if(length(mtCols)>0){
    esM=esM[,-mtCols]
  }
}
rm(mtCols)



esM <- esM%>%dplyr::transmute(CouncilSiteID,Date=T,Value,Measurement,
                    CollMeth=unlist(Collection.Method),ProcMeth=unlist(Processing.Method))



esMb=data.frame(CouncilSiteID=esM$CouncilSiteID,
                Date=as.character(format(lubridate::ymd_hms(esM$Date),'%d-%b-%y')),
                Value=esM$Value,
                Measurement=esM$Measurement,
                Censored=grepl(pattern = '<|>',x = esM$Value),
                CenType=F,
                CollMeth=esM$CollMeth,
                ProcMeth=esM$ProcMeth)
esMb$CenType[grep('<',esMb$Value)] <- 'Left'
esMb$CenType[grep('>',esMb$Value)] <- 'Right'

esMb$Value = readr::parse_number(esMb$Value)






esMb$measName=esMb$Measurement
table(esMb$Measurement,useNA = 'a')
esMb$Measurement <- as.character(factor(esMb$Measurement,
                                          levels = Measurements,
                                          labels = c("ASPM","PercentageEPTTaxa","MCI",'TaxaRichness',"QMCI")))
table(esMb$Measurement,esMb$measName,useNA='a')


# esMb <- merge(esMb,siteTable,by='CouncilSiteID')


write.csv(esMb,file = paste0("D:/LAWA/2021/esM.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/esM.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/",format(Sys.Date(),"%Y-%m-%d"),"/es.csv"),
          overwrite = T)
# write.csv(esMb,file = paste0("H:/ericg/16666LAWA/LAWA2021/macroinvertebrates/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,".csv"),row.names = F)
rm(esM,esMb)



