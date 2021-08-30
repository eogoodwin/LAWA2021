## Load libraries ------------------------------------------------
require(tidyverse)
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(xml2)     ### XML library to write hilltop XML
require(RCurl)
library(parallel)
library(doParallel)

agency='ecan'
setwd("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/")
Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Metadata/ecanMacro_config.csv",sep=',',header=T,stringsAsFactors = F)%>%
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
ecanM=NULL
suppressMessages({
  for(i in 1:length(sites)){
    cat('\n',sites[i],i,'out of ',length(sites))
    suppressWarnings(rm(siteDat))
    foreach(j = 1:length(Measurements),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
      url <- paste0("http://wateruse.ecan.govt.nz/wqlawa.hts?service=Hilltop&Agency=LAWA&request=GetData",
                    "&Site=",sites[i],
                    "&Measurement=",Measurements[j],
                    "&From=2004-01-01",
                    "&To=2021-06-01")
      url <- URLencode(url)
      
      destFile=paste0("D:/LAWA/2021/tmp",gsub(' ','',Measurements[j]),"Mecan.xml")
      
      dl=try(download.file(url,destfile=destFile,method='curl',quiet=T),silent = T)
      if(!'try-error'%in%attr(dl,'class')){
        Data=xml2::read_xml(destFile)
        Data = xml2::as_list(Data)[[1]]
        dataSource = unlist(Data$Measurement$DataSource)
        Data = Data$Measurement$Data
        if(length(Data)>0){
          tags=data.frame(t(sapply(Data,function(e){
            as.data.frame(sapply(e[which(names(e)=="Parameter")],function(ee){
              retVal=data.frame(attributes(ee)$Value)
              names(retVal)=attributes(ee)$Name
              return(retVal)
            }))
          })))
          if(!any(grepl('parameter',names(tags),ignore.case=T))){
            #dealing with Error: dim(tags)[1] == dim(Data)[1] is not TRUE
            tags = do.call(bind_rows,sapply(Data,function(e){
               pivot_wider(data.frame(t(sapply(e[which(names(e)=='Parameter')],function(ee){
                data.frame(attributes(ee))
              }))),names_from=Name,values_from=Value,values_fn = unlist)%>%as.data.frame
            }))
          }else{
            names(tags) <- gsub('Parameter.','',names(tags))
          }
          Data=data.frame(apply(t(sapply(Data,function(e)bind_rows(unlist(e)))),2,unlist),row.names = NULL)
          Data$Measurement=Measurements[j]
          stopifnot(dim(tags)[1]==dim(Data)[1])
          Data=cbind(Data,tags)
          rownames(Data) <- NULL
          rm(tags)
        }
      }
      file.remove(destFile)
      rm(destFile)
      return(Data)
    }->siteDat
    
    if(!is.null(siteDat)){
      rownames(siteDat) <- NULL
      siteDat$CouncilSiteID = sites[i]
      ecanM=bind_rows(ecanM,siteDat)
    }
    rm(siteDat)
  }
})
stopCluster(workers)
rm(workers)



mtCols = which(apply(ecanM,2,function(c)all(is.na(c))))
if(length(mtCols)>0){
  mtCols=mtCols[which(mtCols>6)]
  if(length(mtCols)>0){
    ecanM=ecanM[,-mtCols]
  }
}
rm(mtCols)

save(ecanM,file = 'ecanMraw.rData')
# load('ecanMraw.rData')


CollMeth=ecanM$Collection.Method
CMindices = which(lengths(CollMeth)==1)
CollMeth = unname(unlist(CollMeth))

ProcMeth=ecanM$Processing.Method
PMindices = which(lengths(ProcMeth)==1)
ProcMeth = unname(unlist(ProcMeth))

ecanM <- ecanM%>%transmute(CouncilSiteID,Date=T,Value,Measurement)
ecanM$CollMeth = NA
ecanM$CollMeth[CMindices] <- CollMeth
rm(CollMeth,CMindices)

ecanM$ProcMeth = NA
ecanM$ProcMeth[PMindices] <- ProcMeth
rm(ProcMeth,PMindices)

ecanMb=data.frame(CouncilSiteID=ecanM$CouncilSiteID,
                  Date=as.character(format(lubridate::ymd_hms(ecanM$Date),'%d-%b-%y')),
                  Value=ecanM$Value,
                  Measurement=ecanM$Measurement,
                  Censored=grepl(pattern = '<|>',x = ecanM$Value),
                  CenType=F,
                  CollMeth=ecanM$CollMeth,
                  ProcMeth=ecanM$ProcMeth)

ecanMb$CenType[grep('<',ecanMb$Value)] <- 'Left'
ecanMb$CenType[grep('>',ecanMb$Value)] <- 'Right'

ecanMb$Value = readr::parse_number(ecanMb$Value)




ecanMb$measName=ecanMb$Measurement
table(ecanMb$Measurement,useNA = 'a')
ecanMb$Measurement <- as.character(factor(ecanMb$Measurement,
                                            levels = Measurements,
                                            labels = c("MCI","PercentageEPTTaxa",'TaxaRichness',"QMCI","ASPM")))
table(ecanMb$Measurement,ecanMb$measName,useNA='a')


# ecanMb <- merge(ecanMb,siteTable,by='CouncilSiteID')


write.csv(ecanMb,file = paste0("D:/LAWA/2021/ecanM.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/ecanM.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/",format(Sys.Date(),"%Y-%m-%d"),"/ecan.csv"),
          overwrite = T)
# write.csv(ecanMb,file = paste0("H:/ericg/16666LAWA/LAWA2021/macroinvertebrates/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,".csv"),row.names = F)
rm(ecanM,ecanMb)



