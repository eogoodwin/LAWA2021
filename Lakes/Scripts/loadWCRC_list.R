require(XML)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)
source('H:/ericg/16666LAWA/LAWA2021/scripts/LAWAFunctions.R')

agency='wcrc'

df <- read.csv(paste0("H:/ericg/16666LAWA/LAWA2021/Lakes/Metadata/",agency,"LWQ_config.csv"),sep=",",stringsAsFactors=FALSE)
Measurements <- subset(df,df$Type=="Measurement")[,1]

siteTable=loadLatestSiteTableLakes(maxHistory=30)
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])
lakeDataColumnLabels=NULL


workers = makeCluster(8)
registerDoParallel(workers)

clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})


if(exists("Data"))rm(Data)
wcrcLWQ=NULL

    
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  rm(siteDat)
  foreach(j = 1:length(Measurements),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    url <- paste0("http://hilltop.wcrc.govt.nz/wq.hts?service=Hilltop&request=GetData",
                  "&Site=",sites[i],
                  "&Measurement=",Measurements[j],
                  "&From=2004-01-01",
                  "&To=2021-01-01")
    url <- URLencode(url)
    
    destFile=paste0("D:/LAWA/2021/tmp",gsub(' ','',Measurements[j]),"WQwcrc.xml")
    
    dl=try(download.file(url,destfile=destFile,method='curl',quiet=T),silent = T)
    if(!'try-error'%in%attr(dl,'class')){
      Data=xml2::read_xml(destFile)
      if(!'error'%in%tolower(names(Data))){
        Data = xml2::as_list(Data)[[1]]
        dataSource = unlist(Data$Measurement$DataSource)
        Data = Data$Measurement$Data
        
        if(length(Data)>0){
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
      
          rownames(Data) <- NULL
        }
        
      }else{Data=NULL}
    }
    file.remove(destFile)
    rm(destFile)
    return(Data)
  }->siteDat
  
  if(!is.null(siteDat)){
    rownames(siteDat) <- NULL
    siteDat$CouncilSiteID = sites[i]
    wcrcLWQ=bind_rows(wcrcLWQ,siteDat)
  }
  rm(siteDat)
}
stopCluster(workers)
rm(workers)

save(wcrcLWQ,file = 'wcrcLWQraw.rData')
# load('wcrcLWQraw.rData')
agency='wcrc'

if("I1"%in%names(wcrcLWQ)){
 wcrcLWQ$Value[is.na(wcrcLWQ$Value)] <- wcrcLWQ$I1[is.na(wcrcLWQ$Value)]
}

wcrcLWQ <- wcrcLWQ%>%select(CouncilSiteID,Date=T,Value,Measurement)


wcrcLWQb=data.frame(CouncilSiteID=wcrcLWQ$CouncilSiteID,
                    Date=as.character(format(lubridate::ymd_hms(wcrcLWQ$Date),'%d-%b-%y')),
                    Value=wcrcLWQ$Value,
                    Method=NA,
                    Measurement=wcrcLWQ$Measurement,
                    # Units=NA,
                    Censored=grepl(pattern = '<|>',x = wcrcLWQ$Value),
                    centype=F,
                    QC=NA,
                    agency='wcrc')
wcrcLWQb$centype[grep('<',wcrcLWQb$Value)] <- 'Left'
wcrcLWQb$centype[grep('>',wcrcLWQb$Value)] <- 'Right'

wcrcLWQb$Value = readr::parse_number(wcrcLWQb$Value)

Measurements=read_csv("H:/ericg/16666LAWA/LAWA2021/Lakes/Metadata/wcrcLWQ_config.csv")%>%filter(Type=="Measurement")%>%select(Value)%>%unlist%>%unname

table(wcrcLWQb$Measurement,useNA='a')
wcrcLWQb$Measurement <- as.character(factor(wcrcLWQb$Measurement,
                                            levels = Measurements,
                                            labels = c("TP","NH4N","TN",
                                                       "Secchi","Secchi","CHLA",
                                                       "pH","ECOLI","TP")))
table(wcrcLWQb$Measurement,useNA='a')

# wcrcLWQb <- merge(wcrcLWQb,siteTable,by='CouncilSiteID')

write.csv(wcrcLWQb,file = paste0("D:/LAWA/2021/wcrcL.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/wcrcL.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d"),"/wcrc.csv"),
          overwrite = T)
rm(wcrcLWQ,wcrcLWQb)
