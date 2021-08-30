## Load libraries ------------------------------------------------
require(tidyverse)
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(xml2)     ### XML library to write hilltop XML
require(RCurl)
library(parallel)
library(doParallel)

agency='nrc'
setwd("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/")
Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Metadata/nrcMacro_config.csv",sep=',',header=T,stringsAsFactors = F)%>%
  select(Value)%>%unname%>%unlist


if("WQ Sample"%in%Measurements){
  Measurements=Measurements[-which(Measurements=="WQ Sample")]
}

Measurements = c("Number of Taxa","Macroinvertebrate Community Index","Percentage of EPT taxa",
                "Quantitative Macroinvertebrate Community Index","Macroinvertebrate Average Score Per Metric")

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
nrcM=NULL
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  suppressWarnings(rm(siteDat))
  foreach(j = 1:length(Measurements),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    url <- paste0("http://hilltop.nrc.govt.nz/SOEMacroinvertebrates.hts?service=Hilltop&request=GetData",
                  "&Site=",sites[i],
                  "&Measurement=",Measurements[j],
                  "&From=1999-01-01",
                  "&To=2021-06-01")
    url <- URLencode(url)
    destFile=paste0("D:/LAWA/2021/tmp",gsub(' ','',Measurements[j]),"WQnrc.xml")
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
   nrcM=bind_rows(nrcM,siteDat)
  }
  rm(siteDat)
}

stopCluster(workers)
rm(workers)



mtCols = which(apply(nrcM,2,function(c)all(is.na(c))))
if(length(mtCols)>0){
  mtCols=mtCols[which(mtCols>6)]
  if(length(mtCols)>0){
   nrcM=nrcM[,-mtCols]
  }
}
rm(mtCols)

save(nrcM,file = 'nrcMraw.rData')
load('nrcMraw.rData')
agency='nrc'



nrcM <-nrcM%>%dplyr::transmute(CouncilSiteID,Date=T,Value,Measurement,
                                  CollMeth=Collection.Method,ProcMeth=Processing.Method)

nrcMb=data.frame(CouncilSiteID=nrcM$CouncilSiteID,
                  Date=as.character(format(lubridate::ymd_hms(nrcM$Date),'%d-%b-%y')),
                  Value=nrcM$Value,
                  Measurement=nrcM$Measurement,
                  Censored=grepl(pattern = '<|>',x =nrcM$Value),
                  CenType=F,
                  CollMeth=nrcM$CollMeth,
                  ProcMeth=nrcM$ProcMeth)
nrcMb$CenType[grep('<',nrcMb$Value)] <- 'Left'
nrcMb$CenType[grep('>',nrcMb$Value)] <- 'Right'

nrcMb$Value = readr::parse_number(nrcMb$Value)


#Exclude out the pre-2007 macro data
#email from Gail Townsend 25August2021
#2.	Macroivertebrate data pre 2007 – we are in the processing of re-processing, QAing and recalculating MCIs for all our data pre-2007.  Unfortunately we’re not going to make it in time though. Can we not display this data on LAWA in the meantime please? 
  
nrcMb <- nrcMb%>%dplyr::filter(lubridate::year(lubridate::dmy(Date))>=2007)




nrcMb$measName=nrcMb$Measurement
table(nrcMb$Measurement,useNA = 'a')
nrcMb$Measurement <- as.character(factor(nrcMb$Measurement,
                                           levels = Measurements,
                                           labels = c('TaxaRichness',"MCI","PercentageEPTTaxa","QMCI","ASPM")))
table(nrcMb$Measurement,nrcMb$measName,useNA='a')


#nrcMb <- merge(nrcMb,siteTable,by='CouncilSiteID')


write.csv(nrcMb,file = paste0("D:/LAWA/2021/nrcM.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/nrcM.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/",format(Sys.Date(),"%Y-%m-%d"),"/nrc.csv"),
          overwrite = T)
# write.csv(nrcMb,file = paste0("H:/ericg/16666LAWA/LAWA2021/macroinvertebrates/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,".csv"),row.names = F)
rm(nrcM,nrcMb)



