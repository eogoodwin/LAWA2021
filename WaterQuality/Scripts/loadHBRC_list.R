require(xml2)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
library(parallel)
library(doParallel)
# Quality coding to NEMS is a work in progress here (except for Surface Water Quantity who have been doing it for some time).
#  Currently, River Water Quality is quality coded using a different process based on an early draft of NEMS, 
#  but we are aiming to have all our datasets quality coded to NEMS in the near future!
#   
#   Information on quality codes is available from our Hilltop server:
  
#Also "method" available same place

setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")

agency='hbrc'
translate <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%filter(Agency==agency)

translate$retName=c("Total Ammoniacal-N" ,"Black Disc" ,"Reported DIN" ,"Dissolved Reactive Phosphorus" ,"E. Coli" ,"Nitrate Nitrogen" ,"Nitrate + Nitrite Nitrogen" ,"pH (Field)" ,"pH (Lab)" ,"Total Nitrogen" ,"Total Phosphorus" ,"Turbidity (Field)" ,"Turbidity (Lab)" ,"Turbidity FNU (Lab)")

siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])



workers = makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})

if(exists("Data"))rm(Data)
hbrcSWQ=NULL
for(i in 1:length(sites)){
  dir.create(paste0("D:/LAWA/2021/HBRC/",make.names(sites[i])),recursive = T,showWarnings = F)
  cat('\n',sites[i],i,'out of ',length(sites),'\t')
  siteDat=NULL
  foreach(j = 1:length(translate$CallName),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    if(translate$CallName[j]=="Reported DIN"){
      url <- paste0("https://data.hbrc.govt.nz/Envirodata/EMAR.hts?",
                    "service=Hilltop&request=GetData",
                    "&Site=",sites[i],
                    "&Measurement=",translate$CallName[j],
                    "&From=2004-01-01&To=2021-01-01")
    }else{
      url <- paste0("https://data.hbrc.govt.nz/Envirodata/EMARDiscrete.hts?",
                    "service=Hilltop&request=GetData",
                    "&Site=",sites[i],
                    "&Measurement=",translate$CallName[j],
                    "&From=2004-01-01&To=2021-01-01")
    }
    url <- URLencode(url)
    
    
    destFile=paste0("D:/LAWA/2021/HBRC/",make.names(sites[i]),"/",make.names(translate$CallName[j]),".xml")
    
    if(!file.exists(destFile)|file.info(destFile)$size<3500){
      dl=try(download.file(url,destfile=destFile,method='wininet',quiet=T),silent = T)
      if(!'try-error'%in%attr(dl,'class')){
        Data=try(xml2::read_xml(destFile),silent=T)
        while('try-error'%in%attr(Data,'class')){
          dl=try(download.file(url,destfile=destFile,method='wininet',quiet=T),silent = T)
          if(!'try-error'%in%attr(dl,'class')){
            Data=try(xml2::read_xml(destFile),silent=T)
          }else{return(NULL)}
        }
        Data = xml2::as_list(Data)[[1]]
        if(length(Data)>0&&names(Data)[1]!='Error'){
          RetCID = attr(Data$Measurement,'SiteName')
          RetProperty=attr(Data$Measurement$DataSource,"Name")
          
          while(RetProperty!=translate$retName[j]|RetCID!=sites[i]){
            file.rename(from = destFile,to = paste0('XX',make.names(RetProperty),destFile))
            file.remove(destFile)
            dl=download.file(url,destfile=destFile,method='libcurl',quiet=T)
            Data=xml2::read_xml(destFile)
            Data = xml2::as_list(Data)[[1]]
            if(length(Data)>0){
              RetCID = attr(Data$Measurement,'SiteName')
              RetProperty=attr(Data$Measurement$DataSource,"Name")
            }
          }
        }
      }
    }
    return(NULL)
  }->dummyout
}
stopCluster(workers)
rm(workers)





#Check site and measurement returned
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  for(j in 1:14){
    destFile=paste0("D:/LAWA/2021/HBRC/",make.names(sites[i]),"/",make.names(translate$CallName[j]),".xml")
    if(file.exists(destFile)&file.info(destFile)$size>1000){
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





workers = makeCluster(7)
registerDoParallel(workers)

clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})

rm(Data,datasource,RetProperty,RetCID)
hbrcSWQ=NULL
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  foreach(j = 1:length(translate$CallName),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    destFile=paste0("D:/LAWA/2021/HBRC/",make.names(sites[i]),"/",make.names(translate$CallName[j]),".xml")
    if(file.exists(destFile)&&file.info(destFile)$size>1000){
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
      RetProperty=Data$Measurement$DataSource$ItemInfo$ItemName[[1]]
      RetCID = attr(Data$Measurement,'SiteName')
      dataSource = unlist(Data$Measurement$DataSource)
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
        Data$CouncilSiteID = sites[i]
        Data$RetProp=RetProperty
        Data$RetCID =RetCID
        if(exists('dataSource')){
          if('ItemInfo.Units'%in%names(dataSource)){
            Data$Units = unname(dataSource[which(names(dataSource)=="ItemInfo.Units")])
          }
        }
        rownames(Data) <- NULL
      }else{Data=NULL}
    }
    rm(destFile)
    return(Data)
  }->siteDat
  
  if(!is.null(siteDat)){
    hbrcSWQ=bind_rows(hbrcSWQ,siteDat)
  }
  rm(siteDat)
}
stopCluster(workers)
rm(workers)


table(hbrcSWQ$CouncilSiteID==hbrcSWQ$RetCID)
table(hbrcSWQ$Measurement==translate$CallName[match(hbrcSWQ$RetProp,translate$retName)])

save(hbrcSWQ,file = 'hbrcSWQraw.rData')
#load('hbrcSWQraw.rData') 113162
  agency='hbrc'
  
  
  
hbrcSWQ <- hbrcSWQ%>%filter(QualityCode!=40)
    
hbrcSWQb=data.frame(CouncilSiteID=hbrcSWQ$CouncilSiteID,
                    Date=as.character(format(lubridate::ymd_hms(hbrcSWQ$T),'%d-%b-%y')),
                    Value=hbrcSWQ$Value,
                    # Method=hbrcSWQ%>%select(matches('meth'))%>%apply(.,2,FUN=function(r)paste(r)),
                    Measurement=hbrcSWQ$Measurement,
                    Units = hbrcSWQ$Units,
                    Censored=grepl(pattern = '<|>',x = hbrcSWQ$Value),
                    CenType=F,
                    QC=hbrcSWQ$QualityCode)
hbrcSWQb$CenType[grep('<',hbrcSWQb$Value)] <- 'Left'
hbrcSWQb$CenType[grep('>',hbrcSWQb$Value)] <- 'Right'
hbrcSWQb$Value=readr::parse_number(hbrcSWQb$Value)


table(hbrcSWQb$Measurement,useNA = 'a')
hbrcSWQb$Measurementb <- as.character(factor(hbrcSWQb$Measurement,
                                             levels = translate$CallName,
                                             labels = translate$LAWAName))
table(hbrcSWQb$Measurement,hbrcSWQb$Measurementb,useNA='a')
hbrcSWQb$Measurement <- hbrcSWQb$Measurementb
hbrcSWQb <- hbrcSWQb%>%select(-Measurementb)


labMeasureNames=unique(grep(pattern = 'lab',x = hbrcSWQb$Measurement,ignore.case=T,value = T))
fieldMeasureNames=unique(grep(pattern = 'field',x = hbrcSWQb$Measurement,ignore.case=T,value = T))
if(length(labMeasureNames)>0 & length(fieldMeasureNames)>0){
  if(any(gsub(pattern = 'Lab|lab|LAB',replacement = '',x = labMeasureNames)%in%
         gsub(pattern = 'Field|field|FIELD',replacement = '',x = fieldMeasureNames))){
    matchy=gsub(pattern = 'Lab|lab|LAB',
                replacement = '',
                x = labMeasureNames)[gsub(pattern = 'Lab|lab|LAB',
                                          replacement = '',
                                          x = labMeasureNames)%in%gsub(pattern = 'Field|field|FIELD',
                                                                       replacement = '',
                                                                       x = fieldMeasureNames)]
    matchy=gsub(pattern = "[[:punct:]]|[[:space:]]",replacement='',x = matchy)
    cat(agency,'\t',matchy,'\n')
    for(mm in seq_along(matchy)){
      LAWAName=unique(transfers$LAWAName[transfers$CallName==matchy[mm]])
      transfers=rbind(transfers,c(agency,matchy[mm],LAWAName))
      forsub=hbrcSWQb[grep(pattern = paste0(matchy[mm],' *\\('),x = hbrcSWQb$Measurement,ignore.case = T),]
      teppo = forsub%>%group_by(CouncilSiteID,Date,Measurement)%>%
        dplyr::summarise(.groups='keep',
                         Value=mean(as.numeric(Value),na.rm=T),
                         Censored=any(Censored),
                         CenType=ifelse(all(CenType=="FALSE"),
                                        'FALSE',
                                        ifelse(any(tolower(CenType)=='left'),
                                               'Left',
                                               ifelse(any(tolower(CenType)=='right'),
                                                      'Right',NA))),
                         QC=paste0(unique(QC),collapse='&'),
                         Units=paste0(unique(Units),collapse='&'))%>%
        pivot_wider(names_from=Measurement,values_from = Value)
      LabCol=grep(paste0(matchy[mm]," *\\(Lab\\)"),names(teppo),ignore.case=T)
      FieldCol=grep(paste0(matchy[mm]," *\\(Field\\)"),names(teppo),ignore.case=T)
      teppo$Value=unlist(teppo[,LabCol])
      teppo$Value[is.na(teppo$Value)]=unlist(teppo[is.na(teppo$Value),FieldCol])
      teppo=teppo[,-c(LabCol,FieldCol)]
      teppo$Measurement=matchy[mm]
      teppo <- left_join(teppo,unique(hbrcSWQb%>%select(-(Date:QC))),by="CouncilSiteID")%>%as.data.frame
      without=hbrcSWQb[-grep(pattern = paste0(matchy[mm],' *\\('),
                           x = hbrcSWQb$Measurement,ignore.case = T),]
      hbrcSWQb=rbind(without,teppo)
      rm(without,teppo)
    }
  }
}      

hbrcSWQb <- unique(hbrcSWQb)



write.csv(hbrcSWQb,file = paste0("D:/LAWA/2021/hbrc.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/hbrc.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/hbrc.csv"),
          overwrite = T)
rm(hbrcSWQ,hbrcSWQb)

