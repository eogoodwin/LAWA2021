## Load libraries ------------------------------------------------
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(xml2)     ### XML library to write hilltop XML
require(RCurl)
library(parallel)
library(doParallel)

agency='ecan'
setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")
Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName)%>%unname%>%unlist

# Measurements=c(Measurements,'WQ Sample')

siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])
which(sites == siteTable$CouncilSiteID[siteTable$LawaSiteID%in%c('ecan-00018')])
which(sites == siteTable$CouncilSiteID[siteTable$LawaSiteID%in%c('ecan-00072')])


workers = makeCluster(6)
registerDoParallel(workers)

clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})


if(exists("Data"))rm(Data)
ecanSWQ=NULL
  for(i in 1:length(sites)){
    cat('\n',sites[i],i,'out of ',length(sites))
    if(exists('siteDat'))rm(siteDat)
    foreach(j = 1:length(Measurements),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
      url <- paste0("http://wateruse.ecan.govt.nz/wqlawa.hts?service=Hilltop&Agency=LAWA&request=GetData",
                    "&Site=",sites[i],
                    "&Measurement=",Measurements[j],
                    "&From=2004-01-01",
                    "&To=2021-01-01")
      url <- URLencode(url)
      
      destFile=paste0("D:/LAWA/2021/tmp",gsub(' ','',Measurements[j]),"WQecan.xml")
      
      dl=try(download.file(url,destfile=destFile,method='curl',quiet=T),silent = T)
      if(!'try-error'%in%attr(dl,'class')){
        Data=xml2::read_xml(destFile)
        Data = xml2::as_list(Data)[[1]]
        dataSource = unlist(Data$Measurement$DataSource)
        Data = Data$Measurement$Data
        
        if(length(Data)>0){
          # tags=bind_rows(lapply(Data,function(e){
          #   as.data.frame(sapply(e[which(names(e)=="Parameter")],function(ee){
          #     pivot_wider(data = data.frame(attributes(ee)),names_from = 'Name',values_from = 'Value')
          #   }))
          # }))
          # if(length(tags)>0){
          #   tags$matchIndex = unname(which((sapply(Data,function(e)'Parameter'%in%names(e)))))
          # }
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
            if(any(grepl('unit',names(dataSource),ignore.case=T))){
              Data$Units=dataSource[grep('unit',names(dataSource),ignore.case=T)]
            }else{
              Data$Units=NA
            }
          Data$Measurement=Measurements[j]
          
          # if(dim(tags)[2]>0&!is.null(tags[1,1])){
          #   names(tags) <- gsub('Parameter.','',names(tags))
          #   Data$matchIndex=1:(dim(Data)[1])
          #   Data=merge(x=Data,y=tags,by='matchIndex',all=T)
          #   Data=Data%>%select(-matchIndex)
          # }
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
      ecanSWQ=bind_rows(ecanSWQ,siteDat)
    }
    rm(siteDat)
  }

    stopCluster(workers)
rm(workers)


save(ecanSWQ,file = 'ecanSWQraw.rData')
# load('ecanSWQraw.rData')
agency='ecan'

ecanSWQ <- ecanSWQ%>%select(CouncilSiteID,Date=T,Value,Measurement,Units,QC=QualityCode)
  
  
ecanSWQb=data.frame(CouncilSiteID=ecanSWQ$CouncilSiteID,
                    Date=as.character(format(lubridate::ymd_hms(ecanSWQ$Date),'%d-%b-%y')),
                    Value=ecanSWQ$Value,
                    Measurement=ecanSWQ$Measurement,
                    Units=ecanSWQ$Units,
                    Censored=grepl(pattern = '<|>',x = ecanSWQ$Value),
                    CenType=F,
                    QC=ecanSWQ$QC)
ecanSWQb$CenType[grep('<',ecanSWQb$Value)] <- 'Left'
ecanSWQb$CenType[grep('>',ecanSWQb$Value)] <- 'Right'

ecanSWQb$Value = readr::parse_number(ecanSWQb$Value)

translate=read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName,LAWAName)

table(ecanSWQb$Measurement,useNA = 'a')
ecanSWQb$Measurement <- as.character(factor(ecanSWQb$Measurement,
                                             levels = translate$CallName,
                                             labels = translate$LAWAName))
table(ecanSWQb$Measurement,useNA='a')
ecanSWQb <- unique(ecanSWQb)

# ecanSWQb <- merge(ecanSWQb,siteTable,by='CouncilSiteID')


write.csv(ecanSWQb,file = paste0("D:/LAWA/2021/ecan.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/ecan.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/ecan.csv"),
          overwrite = T)
rm(ecanSWQ,ecanSWQb)



