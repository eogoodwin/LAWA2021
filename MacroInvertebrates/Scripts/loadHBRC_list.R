## Load libraries ------------------------------------------------
require(tidyverse)
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(xml2)     ### XML library to write hilltop XML
require(RCurl)
library(parallel)
library(doParallel)

agency='hbrc'
setwd("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/")
Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Metadata/hbrcMacro_config.csv",sep=',',header=T,stringsAsFactors = F)%>%
  select(Value)%>%unname%>%unlist

if("WQ Sample"%in%Measurements){
  Measurements=Measurements[-which(Measurements=="WQ Sample")]
}

siteTable=loadLatestSiteTableMacro()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])



workers = makeCluster(6)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})


if(exists("Data"))rm(Data)
hbrcM=NULL
  for(i in 1:length(sites)){
    cat('\n',sites[i],i,'out of ',length(sites))
    suppressWarnings(rm(siteDat))
    foreach(j = 1:length(Measurements),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
      if(Measurements[j]%in%c("Reported MCI","Reported QMCI","ASPM")){
        url <- paste0("https://data.hbrc.govt.nz/Envirodata/EMAR.hts?service=Hilltop&request=GetData",
                      "&Site=",sites[i],
                      "&Measurement=",Measurements[j],
                      "&From=1990-01-01",
                      "&To=2021-06-01")
      }else{
        url <- paste0("https://data.hbrc.govt.nz/Envirodata/EMARDiscreteGood.hts?service=Hilltop&request=GetData",
                      "&Site=",sites[i],
                      "&Measurement=",Measurements[j],
                      "&From=1990-01-01",
                      "&To=2021-06-01")
        
      }
      url <- URLencode(url)
      destFile=paste0("D:/LAWA/2021/tmp",gsub(' ','',Measurements[j]),"WQhbrc.xml")
      dl=try(download.file(url,destfile=destFile,method='wininet',quiet=T),silent = T)
      
      if(!'try-error'%in%attr(dl,'class')){
        Data=xml2::read_xml(destFile)
        Data = xml2::as_list(Data)[[1]]
        # dataSource = unlist(Data$Measurement$DataSource)
        Data = Data$Measurement$Data
        
        if(length(Data)>0){
          tags=bind_rows(lapply(Data,function(e){
            as.data.frame(sapply(e[which(names(e)=="Parameter")],function(ee){
              pivot_wider(data = data.frame(attributes(ee)),names_from = 'Name',values_from = 'Value')
            }))
          }))
          if(length(Data)>1){
            Data=data.frame(apply(t(sapply(Data,function(e){
              bind_rows(unlist(e))
            })),2,unlist),row.names = NULL)
          }else{
            Data = bind_rows(unlist(lapply(Data,function(e){
              unlist(e[which(names(e)!='Parameter')])%>%t%>%as.data.frame
            })))
            names(Data) <- gsub("^E\\.","",names(Data))
          }
          Data$Measurement=Measurements[j]
          
          if(dim(tags)[2]>0&!is.null(tags[1,1])){
            stopifnot(dim(tags)[1]==dim(Data)[1])
            names(tags) <- gsub('Parameter.','',names(tags))
            Data=cbind(Data,tags)
          }
          rownames(Data) <- NULL
          rm(tags)
        }
      }
      file.remove(destFile)
      rm(destFile)
      return(Data)
    }->siteDat
    if(any(grepl('^E\\.',names(siteDat)))){browser()}
    
    if(!is.null(siteDat)){
      rownames(siteDat) <- NULL
      siteDat$CouncilSiteID = sites[i]
     hbrcM=bind_rows(hbrcM,siteDat)
    }
    rm(siteDat)
  }

stopCluster(workers)
rm(workers)



mtCols = which(apply(hbrcM,2,function(c)all(is.na(c))))
if(length(mtCols)>0){
  mtCols=mtCols[which(mtCols>6)]
  if(length(mtCols)>0){
   hbrcM=hbrcM[,-mtCols]
  }
}
rm(mtCols)

save(hbrcM,file = 'hbrcMraw.rData')
#load('hbrcMraw.rData')

hbrcM <-hbrcM%>%dplyr::transmute(CouncilSiteID,Date=T,Value,Measurement,
                              CollMeth=CollectionMethod,ProcMeth=ProcessMethod)



hbrcMb=data.frame(CouncilSiteID=hbrcM$CouncilSiteID,
                Date=as.character(format(lubridate::ymd_hms(hbrcM$Date),'%d-%b-%y')),
                Value=hbrcM$Value,
                Measurement=hbrcM$Measurement,
                Censored=grepl(pattern = '<|>',x =hbrcM$Value),
                CenType=F,
                CollMeth=hbrcM$CollMeth,
                ProcMeth=hbrcM$ProcMeth)
hbrcMb$CenType[grep('<',hbrcMb$Value)] <- 'Left'
hbrcMb$CenType[grep('>',hbrcMb$Value)] <- 'Right'

hbrcMb$Value = readr::parse_number(hbrcMb$Value)






hbrcMb$measName=hbrcMb$Measurement
table(hbrcMb$Measurement,useNA = 'a')
hbrcMb$Measurement <- as.character(factor(hbrcMb$Measurement,
                                        levels = Measurements,
                                        labels = c("PercentageEPTTaxa","ASPM",'TaxaRichness',"MCI","QMCI")))
table(hbrcMb$Measurement,hbrcMb$measName,useNA='a')


#hbrcMb <- merge(hbrcMb,siteTable,by='CouncilSiteID')


write.csv(hbrcMb,file = paste0("D:/LAWA/2021/hbrcM.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/hbrcM.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/",format(Sys.Date(),"%Y-%m-%d"),"/hbrc.csv"),
          overwrite = T)
# write.csv(hbrcMb,file = paste0("H:/ericg/16666LAWA/LAWA2021/macroinvertebrates/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,".csv"),row.names = F)
rm(hbrcM,hbrcMb)



