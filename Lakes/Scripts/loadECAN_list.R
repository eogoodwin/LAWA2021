## Load libraries ------------------------------------------------
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(xml2)     
require(RCurl)
source('H:/ericg/16666LAWA/LAWA2021/scripts/LAWAFunctions.R')

agency='ecan'

df <- read.csv("H:/ericg/16666LAWA/LAWA2021/Lakes/Metadata/ecanLWQ_config.csv",sep=",",stringsAsFactors=FALSE)
Measurements <- subset(df,df$Type=="Measurement")[,1]
Measurements <- as.vector(Measurements)

siteTable=loadLatestSiteTableLakes(maxHistory = 30)
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])
lakeDataColumnLabels=NULL


ecanLWQ=NULL
i=1
for(i in i:length(sites)){
  cat('\n',sites[i],i,'out of',length(sites),'\n')  
  siteDat=data.frame(T=NA,Measurement=NA)
  for(j in 1:length(Measurements)){
    
    url <- paste0("http://wateruse.ecan.govt.nz/wqlawa.hts?service=Hilltop&Agency=LAWA&request=GetData",
                  "&Site=",sites[i],
                  "&Measurement=",Measurements[j],
                  "&From=2004-01-01",
                  "&To=2021-01-01")
    url <- URLencode(url)
    dl=try(download.file(url,destfile="D:/LAWA/2021/tmpLecan.xml",method='curl',quiet=T),silent = T)
    Data=xml2::read_xml("D:/LAWA/2021/tmpLecan.xml")
    Data = xml2::as_list(Data)[[1]]
    if(length(Data)>0&&!'Error'%in%names(Data)){
      cat(Measurements[j])
      Data=Data$Measurement$Data
      
      Data=lapply(seq_along(Data),function(listIndex){
        # return(c(time=listItem$T[[1]],value=listItem$Value[[1]]))
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
        siteDat=bind_rows(siteDat,Data)
      }
    }
    rm(Data)
    file.remove('D:/LAWA/2021/tmpLecan.xml')
  }
  if(!is.null(siteDat)){
    siteDat$CouncilSiteID = sites[i]
    ecanLWQ=bind_rows(ecanLWQ,siteDat)
  }
  rm(siteDat)
}
 
mtCols = which(apply(ecanLWQ,2,function(c)all(is.na(c))))
if(length(mtCols)>0){
  ecanLWQ=ecanLWQ[,-mtCols]
}
rm(mtCols)

mtRows = which(apply(ecanLWQ[,c(1,3)],1,function(r){
  is.na(r[1])&is.na(r[2])
}))
if(length(mtRows)>0){
  ecanLWQ = ecanLWQ[-mtRows,]
}
rm(mtRows)
   
  save(ecanLWQ,file = 'ecanLWQraw.rData')

ecanLWQ=data.frame(CouncilSiteID=ecanLWQ$CouncilSiteID,
                 Date=format(lubridate::ymd_hms(ecanLWQ$T),'%d-%b-%y'),
                 Value=as.numeric(gsub('<|>','',ecanLWQ$Value)),
                 Method=apply(ecanLWQ%>%select(matches('meth'))%>%as.data.frame,
                              1,FUN=function(r)paste(r[!is.na(r)],collapse='')),
                 Measurement=ecanLWQ$Measurement,
                 Censored=grepl(pattern = '<|>',x = ecanLWQ$Value),
                 centype=F,
                 QC=NA)
ecanLWQ$centype[grep('<',ecanLWQ$Value)] <- 'Left'
ecanLWQ$centype[grep('>',ecanLWQ$Value)] <- 'Right'

table(ecanLWQ$Measurement)
ecanLWQ$Measurement[ecanLWQ$Measurement == 'Ammoniacal Nitrogen'] <- "NH4N"
ecanLWQ$Measurement[ecanLWQ$Measurement == "CHL a"] <- "CHLA"
ecanLWQ$Measurement[ecanLWQ$Measurement == 'E. coli'] <- "ECOLI"
ecanLWQ$Measurement[ecanLWQ$Measurement == 'pH'] <- "pH"
ecanLWQ$Measurement[ecanLWQ$Measurement == 'Potentially toxic cyanobacteria biovolume'] <- "CYANOTOX"
ecanLWQ$Measurement[ecanLWQ$Measurement == 'Total cyanobacteria biovolume'] <- "CYANOTOT" 
ecanLWQ$Measurement[ecanLWQ$Measurement == 'Secchi Disk'] <- "Secchi"
ecanLWQ$Measurement[ecanLWQ$Measurement == 'Total Nitrogen'] <- "TN" 
ecanLWQ$Measurement[ecanLWQ$Measurement == 'Total Phosphorus'] <- "TP" 
table(ecanLWQ$Measurement)

if(any(is.na(ecanLWQ$Date)&is.na(ecanLWQ$Value))){
  ecanLWQ <- ecanLWQ[-which(is.na(ecanLWQ$Date)&is.na(ecanLWQ$Value)),]
}

write.csv(ecanLWQ,file = paste0("H:/ericg/16666LAWA/LAWA2021/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,".csv"),row.names = F)

