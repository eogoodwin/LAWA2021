require(XML)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
library(parallel)
library(doParallel)
# Quality coding to NEMS is a work in progress here (except for Surface Water Quantity who have been doing it for some time).
#  Currently, River Water Quality is quality coded using a different process based on an early draft of NEMS, 
#  but we are aiming to have all our datasets quality coded to NEMS in the near future!
#   
#   Information on quality codes is available from our Hilltop server:
  
#Also "method" available same place

agency='hbrc'
setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")

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
hbrcSWQ=NULL


for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites),'\t')
  siteDat=NULL
  foreach(j = 1:length(Measurements),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
    if(Measurements[j]=="Reported DIN"){
      url <- paste0("https://data.hbrc.govt.nz/Envirodata/EMAR.hts?service=Hilltop&request=GetData",
                    "&Site=",sites[i],
                    "&Measurement=",Measurements[j],
                    "&From=2004-01-01",
                    "&To=2021-01-01")
    }else{
      url <- paste0("https://data.hbrc.govt.nz/Envirodata/EMARDiscreteGood.hts?service=Hilltop&request=GetData",
                    "&Site=",sites[i],
                    "&Measurement=",Measurements[j],
                    "&From=2004-01-01",
                    "&To=2021-01-01")
    }
    url <- URLencode(url)
    destFile=paste0("D:/LAWA/2021/tmp",Measurements[j],"WQhbrc.xml")
    dl=try(download.file(url,destfile=destFile,method='curl',quiet=T),silent = T)
    Data=xml2::read_xml(destFile)
    Data = xml2::as_list(Data)[[1]]
    Data = Data$Measurement$Data
    if(length(Data)>0){
      cat('.')
      Data=lapply(seq_along(Data),function(listIndex){
        liout <- unlist(Data[[listIndex]][c("T","Value")])
        liout=data.frame(Name=names(liout),Value=unname(liout))
        if(length(Data[[listIndex]])>2){
          attrs=sapply(seq_along(Data[[listIndex]])[-c(1,2)],
                       FUN=function(inListIndex){
                         as.data.frame(attributes(Data[[listIndex]][inListIndex]$Parameter))
                       })
          if(class(attrs)=="list"&&any(lengths(attrs)==0)){
            attrs[[which(lengths(attrs)==0)]] <- NULL
            liout=rbind(liout,bind_rows(attrs))
          }else{
            attrs=t(attrs)
            liout=rbind(liout,attrs)
          }
        }
        return(liout)
      })
      allNames = unique(unlist(sapply(Data,function(li)li$Name)))
      
      eval(parse(text=paste0("Datab=data.frame(`",paste(allNames,collapse='`=NA,`'),"`=NA)")))
      for(dd in seq_along(Data)){
        newrow=Data[[dd]]
        newrow=pivot_wider(newrow,names_from = 'Name',values_from = 'Value')%>%as.data.frame
        newrow=apply(newrow,2,unlist)%>%as.list%>%data.frame
        Datab <- merge(Datab,newrow,all=T)
        rm(newrow)
      }
      rm(dd)
      
      mtRows=which(apply(Datab,1,FUN=function(r)all(is.na(r))))
      if(length(mtRows)>0){Datab=Datab[-mtRows,]}
      
      Data=Datab
      rm(Datab,mtRows)
      
      if(!is.null(Data)){
        Data$Measurement=Measurements[j]
        # siteDat=bind_rows(siteDat,Data)
      }
    }else{
      cat("No",Measurements[j],'\t')
    }
    # rm(Data)
    file.remove(destFile)
    rm(destFile)
    return(Data)
  }->siteDat
  if(!is.null(siteDat)){
    siteDat$CouncilSiteID = sites[i]
    hbrcSWQ=bind_rows(hbrcSWQ,siteDat)
  }
  rm(siteDat)
}


stopCluster(workers)
rm(workers)

mtCols = which(apply(hbrcSWQ,2,function(c)all(is.na(c))))
if(length(mtCols)>0){
  hbrcSWQ=hbrcSWQ[,-mtCols]
}
rm(mtCols)


if(0){
  save(hbrcSWQ,file = 'hbrcSWQraw.rData')
}

hbrcSWQb=data.frame(CouncilSiteID=hbrcSWQ$CouncilSiteID,
                    Date=as.character(format(lubridate::ymd_hms(hbrcSWQ$$T),'%d-%b-%y')),
                    Value=hbrcSWQ$Value,
                    # Method=hbrcSWQ%>%select(matches('meth'))%>%apply(.,2,FUN=function(r)paste(r)),
                    Measurement=hbrcSWQ$Measurement,
                    Units = ifelse('Units'%in%names(hbrcSWQ),hbrcSWQ$Units,NA),
                    Censored=grepl(pattern = '<|>',x = hbrcSWQ$Value),
                    CenType=F,
                    QC=NA)
hbrcSWQb$CenType[grep('<',hbrcSWQb$Value)] <- 'Left'
hbrcSWQb$CenType[grep('>',hbrcSWQb$Value)] <- 'Right'

hbrcSWQb$Value=readr::parse_number(hbrcSWQb$Value)

table(hbrcSWQb$Measurement)
hbrcSWQb$Measurement <- as.character(factor(hbrcSWQb$Measurement,
                                            levels = c("Total Ammoniacal-N", "Black Disc", "Reported DIN", 
                                                       "Dissolved Reactive Phosphorus", 
                                                       "E. Coli", "Nitrate Nitrogen", "Nitrate + Nitrite Nitrogen", 
                                                       "pH (Field)", "pH (Lab)", "Total Nitrogen", "Total Phosphorus", 
                                                       "Turbidity (Field)", "Turbidity (Lab)", "Turbidity FNU (Lab)", 
                                                       "WQ Sample"),
                                            labels=c("NH4","BDISC","DIN","DRP",
                                                     "ECOLI","NO3N","TON","PH","PH","TON","TP",
                                                     "TURB(FIELD)","TURB(LAB)","TURBFNU","WQSAMPLE")))
table(hbrcSWQb$Measurement)
transfers=read.table("h:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/transfers_plain_english_view.txt",
                     sep=',',header = T,stringsAsFactors = F)

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


hbrcSWQb <- merge(hbrcSWQb,siteTable,by='CouncilSiteID')


write.csv(hbrcSWQb,file = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/hbrc.csv"),row.names = F)
rm(hbrcSWQ,hbrcSWQb)
