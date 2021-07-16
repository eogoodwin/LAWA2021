rm(list = ls())
library(tidyverse)
library(parallel)
library(doParallel)
library(xml2)
setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality/")
source("H:/ericg/16666LAWA/LAWA2021/scripts/LAWAFunctions.R")
source('K:/R_functions/nzmg2WGS.r')
source('K:/R_functions/nztm2WGS.r')

# https://www.lawa.org.nz/media/18225/nrwqn-monitoring-sites-sheet1-sheet1.pdf

dir.create(paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/", format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)

urls          <- read.csv("H:/ericg/16666LAWA/LAWA2021/Metadata/CouncilWFS.csv",stringsAsFactors=FALSE)


#The first on the list prioritises the ID to be used and match every else to it.
vars <- c("CouncilSiteID","SiteID","LawaSiteID",
          "NZReach","NZSegment","SWQAltitude","SWQLanduse",
          "Region","Agency","Catchment")

if(exists('siteTable')){
  rm(siteTable)
}

#For each council, find the "emar:SWQuality" sites that are switched on, and get their details
startTime=Sys.time()
workers <- makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(XML)
  library(dplyr)
})
foreach(h = 1:length(urls$URL),.combine = rbind,.errorhandling = "stop")%dopar%{
  if(grepl("^x", urls$Agency[h])){ #allow agency switch-off by 'x' prefix
    return(NULL)
  } 
  if(!nzchar(urls$URL[h])){  ## returns false if the URL string is missing
    return(NULL)
  }
  # if(urls$Agency[h]=="NCC"){
  #   return(NULL)
  # }
  
  xmldata <- try(ldWFSlist(urlIn = urls$URL[h],agency=urls$Agency[h],dataLocation = urls$Source[h],case.fix = TRUE))
  if(is.null(xmldata)||'try-error'%in%attr(xmldata,'class')){#||grepl(pattern = '^501|error',
                                                             #      x = xmlValue(getNodeSet(xmldata,'/')[[1]]),
                                                              #     ignore.case=T)){
    cat('Failed for ',urls$Agency[h],'\n')
    return(NULL)
  }
  
  if(urls$Source[h]=="file" & grepl("csv$",urls$URL[h])){
    cc(urls$URL[h])
    tmp <- read.csv(urls$URL[h],stringsAsFactors=FALSE,strip.white = TRUE,sep=",")
    
    tmp <- tmp[,c(5,7,8,9,30,10,28,29,20,21,22)]
    tmp$Lat <- sapply(strsplit(as.character(tmp$pos),' '), "[",1)
    tmp$Long <- sapply(strsplit(as.character(tmp$pos),' '), "[",2)
    tmp <- tmp[-11]
    if(!exists("siteTable")){
      siteTable<-as.data.frame(tmp,stringsAsFactors=FALSE)
    } else{
      siteTable<-rbind.data.frame(siteTable,tmp,stringsAsFactors=FALSE)
    }
    rm(tmp)
  }else {
       # Determine number of records in a wfs with module before attempting to extract all the necessary columns needed
 wqNodes=unname(which(sapply(xmldata,FUN=function(li){
      'SWQuality'%in%names(li)&&li$SWQuality%in%c("Yes","yes","YES","Y","y","TRUE","true","T","t","True")})))
    wqData=lapply(wqNodes,FUN=function(li){
      liout <- unlist(xmldata[[li]][vars])
      })
    if(length(wqNodes)==0){
      cat(urls$Agency[h],"has no records for <emar:SWQuality>\n")
    } else {
    if(!(all(lengths(wqData)==length(vars)))){
      missingvars=which(lengths(wqData)<length(vars))
      for(mv in missingvars){
        missingVarNames = vars[!vars%in%names(wqData[[mv]])]
        for(mvn in missingVarNames){
          eval(parse(text=paste0("wqData[[mv]]=c(wqData[[mv]],",mvn,"=NA)")))
        }
      }
    }
      wqData <- bind_rows(wqData)
      wqData <- as.data.frame(wqData,stringsAsFactors=FALSE)
      
      #Do lat longs separately from other WQParams because they are value pairs and need separating
      if("shape"%in%tolower(names(xmldata[[wqNodes[1]]]))){
        wqPoints=t(sapply(wqNodes,FUN=function(li){
          liout <- unlist(xmldata[[li]]$Shape$Point$pos)
          if(is.null(liout)){liout <- unlist(xmldata[[li]]$SHAPE$Point$pos)}
          as.numeric(unlist(strsplit(liout,' ')))
        }))
        wqPoints=as.data.frame(wqPoints,stringsAsFactors=F)
        names(wqPoints)=c("Lat","Long")
        
        wqData <- cbind(wqData,wqPoints)
        rm(wqPoints)
      }else{
        wqData$Lat=NA
        wqData$Long=NA
      }
      wqData$accessDate=format(Sys.Date(),"%d-%b-%Y")
      wqData$Agency=urls$Agency[h]
      rm(xmldata)
      return(wqData)
    }
  }
}->siteTable
stopCluster(workers)
rm(workers)

Sys.time()-startTime  #20s
#980

siteTable$Lat = as.numeric(siteTable$Lat)
siteTable$Long = as.numeric(siteTable$Long)

siteTable$SiteID=as.character(siteTable$SiteID)
siteTable$LawaSiteID=as.character(siteTable$LawaSiteID)
siteTable$Region=tolower(as.character(siteTable$Region))
siteTable$Agency=tolower(as.character(siteTable$Agency))

#Add NIWA sites separately ####
if(file.exists("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/NIWALawaSiteIDs.csv")){
  niwaSub=read.csv("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/NIWALawaSiteIDs.csv",stringsAsFactors=F)
}else{
  df <- read.csv(paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/MetaData/niwaSWQ_config.csv"),sep=",",stringsAsFactors=FALSE)
  sites=df$Value[which(df$Type=='site')]
  niwaSub=data.frame(CouncilSiteID=sites,LawaSiteID=NA,SiteID=NA,NZReach=NA,
                     Region=NA,Agency='niwa',Catchment=NA,Lat=NA,Long=NA,accessDate=Sys.Date(),stringsAsFactors=F)
  #Get WFS infos
  for(niwaSite in seq_along(sites)){
    urlwfs = paste0("https://hydro-sos.niwa.co.nz/?service=SOS&version=2.0.0&request=GetFeatureOfInterest",
                    "&featureOfInterest=",sites[niwaSite])
    urlwfs <- URLencode(urlwfs)
    xmlfile <- ldWFS(urlIn=urlwfs,dataLocation = 'web',agency = 'niwa',case.fix = F,method = 'curl')
    niwaSub$CouncilSiteID[niwaSite]=sapply(getNodeSet(doc=xmlfile,path = "//gml:identifier",namespaces=c(gml="http://www.opengis.net/gml/3.2")),xmlValue)
    niwaSub$SiteID[niwaSite]=sapply(getNodeSet(doc=xmlfile,path = "//gml:name",namespaces=c(gml="http://www.opengis.net/gml/3.2")),xmlValue)
    pos=sapply(getNodeSet(doc=xmlfile,path = "//ns:pos",namespaces=c(ns="http://www.opengis.net/gml/3.2")),xmlValue)
    pos=sapply(strsplit(pos,' '),readr::parse_number)%>%as.numeric()
    niwaSub$Long[niwaSite]=pos[2]
    niwaSub$Lat[niwaSite]=pos[1]
    rm(urlwfs,xmlfile)
  }
  rm(sites,df)
  
  # Fix NIWA region assignment
  plot(niwaSub$Long,niwaSub$Lat)
  for(ns in seq_along(niwaSub$SiteID)){
    dists=sqrt((niwaSub$Long[ns]-siteTable$Long)^2+(niwaSub$Lat[ns]-siteTable$Lat)^2)
    closest=which.min(dists)
    segments(x0 = niwaSub$Long[ns],y0=niwaSub$Lat[ns],
             x1 = siteTable$Long[closest],y1=siteTable$Lat[closest])
    if(dists[closest]<0.71){
      niwaSub$Region[ns]=siteTable$Region[closest]
    }else{
      browser()
    }
  }
  
  # monList = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2019/Metadata/Monitoring Sites - 8 July 2019.xlsx',guess_max = 5000)
  monList = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2021/Metadata/LAWA Masterlist of Umbraco Sites as at 30 July 2020.xlsx',
                              sheet=1)
  lawaMasterList = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2021/Metadata/LAWA Site Master List.xlsx',sheet=2)
  # lawaMasterList = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2021/Metadata/LAWA Masterlist of Umbraco Sites as at 15 June 2020.xlsx',
                                     # sheet=1)
  
  #Text-basd distace
  for(ns in seq_along(niwaSub$SiteID)){
    mondists = adist(tolower(strTo(niwaSub$SiteID[ns],' \\(')),trimws(tolower(monList$Name)))
    clM = which.min(mondists)
    masdists = adist(tolower(strTo(niwaSub$SiteID[ns],'\\(')),trimws(tolower(lawaMasterList$`Site Name`)))
    clMas = which.min(masdists)
    # geoDist=sqrt((niwaSub$Long[ns]-monList$Long[clM])^2+(niwaSub$Lat[ns]-monList$Lat[clM])^2)
    cat(tolower(strTo(niwaSub$SiteID[ns],' \\(')),'\t',monList$Name[clM],'\t',min(dists),'\n')
    cat(tolower(strTo(niwaSub$SiteID[ns],' \\(')),'\t',lawaMasterList$`Site Name`[clMas],'\t',min(dists),'\n')
    check=readline()
    if(check==""){
      niwaSub$LawaSiteID[ns]=monList$LAWAID[clM]
    }
    segments(x0=niwaSub$Long[ns],y0=niwaSub$Lat[ns],
             x1=monList$Long[clM],y1=monList$Lat[clM],col='blue',lwd=20)
  }
  sum(is.na(niwaSub$LawaSiteID))
  niwaSub[is.na(niwaSub$LawaSiteID),]
  these=which(is.na(niwaSub$LawaSiteID))

  
  
  # monList = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2019/Metadata/Monitoring Sites - 8 July 2019.xlsx',guess_max = 5000)
  monList = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2021/Metadata/LAWA Masterlist of Umbraco Sites as at 30 July 2020.xlsx',
                                     sheet=1)
  # monList = rbind(monList,newmonList)
  lawaMasterList = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2021/Metadata/LAWA Site Master List.xlsx',sheet=2)
  
  
  lawaMasterList$Long = nztm2wgs(ce = as.numeric(lawaMasterList$NZTM_Easting),cn = as.numeric(lawaMasterList$NZTM_Northing))[,2]
  lawaMasterList$Lat = nztm2wgs(ce = as.numeric(lawaMasterList$NZTM_Easting),cn = as.numeric(lawaMasterList$NZTM_Northing))[,1]
  lawaMasterList$Lat[lawaMasterList$Long>200] <- NA
  lawaMasterList$Long[lawaMasterList$Long>200] <- NA
  
  lawaMasterList$Long[is.na(lawaMasterList$Long)] <- siteTable$Long[match(lawaMasterList$`LAWA ID`[is.na(lawaMasterList$Long)],
                                                                          siteTable$LawaSiteID)]
  lawaMasterList$Lat[is.na(lawaMasterList$Lat)] <- siteTable$Lat[match(lawaMasterList$`LAWA ID`[is.na(lawaMasterList$Lat)],
                                                                       siteTable$LawaSiteID)]
  lawaMasterList$Long[is.na(lawaMasterList$Long)] <- siteTable$Long[match(lawaMasterList$`Site Name`[is.na(lawaMasterList$Long)],
                                                                          siteTable$LawaSiteID)]
  lawaMasterList$Lat[is.na(lawaMasterList$Lat)] <- siteTable$Lat[match(lawaMasterList$`Site Name`[is.na(lawaMasterList$Lat)],
                                                                       siteTable$LawaSiteID)]
  
  monList$Long = lawaMasterList$Long[match(monList$LAWAID,lawaMasterList$`LAWA ID`)]
  monList$Lat = lawaMasterList$Lat[match(monList$LAWAID,lawaMasterList$`LAWA ID`)]
  monList$Long = siteTable$Long[match(tolower(monList$LAWAID),tolower(siteTable$LawaSiteID))]
  monList$Lat = siteTable$Lat[match(tolower(monList$LAWAID),tolower(siteTable$LawaSiteID))]
  
  
  plot(niwaSub$Long[these],niwaSub$Lat[these],xlim=range(monList$Long,na.rm=T),ylim=range(monList$Lat,na.rm=T))
  for(ns in seq_along(these)){
    dists=sqrt((niwaSub$Long[these[ns]]-siteTable$Long)^2+(niwaSub$Lat[these[ns]]-siteTable$Lat)^2)
    closest=which.min(dists)
    segments(x0 = niwaSub$Long[these[ns]],y0=niwaSub$Lat[these[ns]],
             x1 = siteTable$Long[closest],y1=siteTable$Lat[closest],col='blue',lwd=2)
    cat(these[ns],'\t',niwaSub$SiteID[these[ns]],'\n')
    cat("siteTable\t","distance\t",min(dists,na.rm=T),'\t',siteTable$SiteID[closest],'\n')
    
    dists=sqrt((niwaSub$Long[these[ns]]-monList$Long)^2+(niwaSub$Lat[these[ns]]-monList$Lat)^2)
    closest=which.min(dists)
    segments(x0 = niwaSub$Long[these[ns]],y0=niwaSub$Lat[these[ns]],
             x1 = monList$Long[closest],y1=monList$Lat[closest],col='red',lwd=2)
    cat("monList\t\t","distance\t",min(dists,na.rm=T),'\t',monList$Name[closest],'\n')
  }
  
  #Try filling gaps from old NIWA data
  load('H:/ericg/16666LAWA/2018/WaterQuality/1.Imported/NIWAwqData.rData',verbose=T)  #NIWAdataInB
  cbind(niwaSub$SiteID[is.na(niwaSub$LawaSiteID)],
        NIWAdataInB[match(niwaSub$CouncilSiteID[is.na(niwaSub$LawaSiteID)],
                          NIWAdataInB$SiteName),c("CouncilSiteID","SiteName","LawaSiteID")])
  
  niwaSub$LawaSiteID[is.na(niwaSub$LawaSiteID)] = 
    NIWAdataInB$LawaSiteID[match(niwaSub$CouncilSiteID[is.na(niwaSub$LawaSiteID)],
                                 NIWAdataInB$SiteName)]
  #Check names match
  cbind(niwaSub$SiteID,NIWAdataInB[match(niwaSub$CouncilSiteID,NIWAdataInB$SiteName),c("CouncilSiteID","SiteName")])
  
  niwaSub <- left_join(niwaSub,NIWAdataInB%>%select(LawaSiteID,SWQAltitude,SWQLanduse)%>%distinct,by="LawaSiteID")
  rm(NIWAdataInB)
  niwaSub$LawaSiteID=tolower(niwaSub$LawaSiteID)
  write.csv(niwaSub,"H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/NIWALawaSiteIDs.csv",row.names=F)
}##
siteTable=merge(x=siteTable,y=niwaSub,all.x=T,all.y=T)
rm(niwaSub)



table(siteTable$Region,siteTable$Agency,useNA = 'a')
table(siteTable$Region,useNA = 'a')
siteTable$Region=tolower(siteTable$Region)
# siteTable$Region[siteTable$Region=='auckland council'] <- 'auckland'
siteTable$Region[siteTable$Region=='orc'|siteTable$Agency=='orc'] <- 'otago'
siteTable$Region[siteTable$Region=='gdc'|siteTable$Agency=='gdc'] <- 'gisborne'
siteTable$Region[siteTable$Region=='wcrc'|siteTable$Agency=='wcrc'] <- 'west coast'
siteTable$Region[siteTable$Region=='ncc'|siteTable$Agency=='ncc'] <- 'nelson'
siteTable$Region[siteTable$Region=='gwrc'|siteTable$Agency=='gwrc'] <- 'wellington'
siteTable$Region[siteTable$Region=='nrc'|siteTable$Agency=='nrc'] <- 'northland'
siteTable$Region[siteTable$Region=='mdc'|siteTable$Agency=='mdc'] <- 'marlborough'
siteTable$Region[siteTable$Region=='tdc'|siteTable$Agency=='tdc'] <- 'tasman'
siteTable$Region[siteTable$Region=='trc'|siteTable$Agency=='trc'] <- 'taranaki'
siteTable$Region[siteTable$Region=='horizons'] <- 'manawat\u16b-whanganui'
siteTable$Region[siteTable$Region=='manawatu-whanganui'] <- 'manawat\u16b-whanganui'
table(siteTable$Region,useNA = 'a')


table(siteTable$Agency)
siteTable$Agency[siteTable$Agency%in%c('arc','auckland','auckland council')] <- 'ac'
siteTable$Agency[siteTable$Agency=='christchurch'] <- 'ecan'
siteTable$Agency[siteTable$Agency=='environment canterbury'] <- 'ecan'
siteTable$Agency[siteTable$Agency%in%c('boprc/niwa','niwa/boprc')] <- 'boprc'
table(siteTable$Agency)

agencies= c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc")
(agencies[!agencies%in%siteTable$Agency]->missingAgencies)
unique(siteTable$Agency[!siteTable$Agency%in%agencies])


#https://www.stat.auckland.ac.nz/~paul/Reports/maori/maori.html
siteTable$SiteID=as.character(siteTable$SiteID)
grep(pattern = "[^a-z,A-Z, ,/,@,0-9,.,-,[:punct:]]",x = siteTable$SiteID,value=T,ignore.case = T)


plot(siteTable$Long,siteTable$Lat,col=as.numeric(factor(siteTable$Region)))
points(siteTable$Long[siteTable$Agency=='niwa'],
       siteTable$Lat[siteTable$Agency=='niwa'],pch=16,cex=0.5,
       col=as.numeric(factor(siteTable$Region[siteTable$Agency=='niwa'],levels=sort(unique(siteTable$Region)))))




siteTable=unique(siteTable)  
#14/7/2021 1057 


#Pull in land use from LCDB
source('K:/R_functions/nzmg2WGS.r')
source('K:/R_functions/nztm2WGS.r')
fwgr <- read_csv('d:/RiverData/FWGroupings.txt')
rec=read_csv("d:/RiverData/RECnz.txt")
rec$easting=fwgr$easting[match(rec$NZREACH,fwgr$NZREACH)]
rec$northing=fwgr$northing[match(rec$NZREACH,fwgr$NZREACH)]
rm(fwgr)
latLong=nzmg2wgs(East = rec$easting,North = rec$northing)
rec$Long=latLong[,2]
rec$Lat=latLong[,1]
rm(latLong)

#Derive rec sediment classes
rec$ClimateCluster=rec$CLIMATE
rec$ClimateCluster[rec$CLIMATE%in%c("WW","WX")] <- "WW"
rec$ClimateCluster[rec$CLIMATE%in%c("CW","CX")] <- "CW"
rec$SourceFlowCluster=rec$SRC_OF_FLW
rec$SourceFlowCluster[rec$SRC_OF_FLW%in%c("M","GM")] <- "M"
rec$GeologyCluster=rec$GEOLOGY
rec$GeologyCluster[rec$GEOLOGY%in%c("SS","Pl","M")] <- "SS"
rec$GeologyCluster[rec$GEOLOGY%in%c("VB","VA")] <- "VA"
rec$CScluster=paste(rec$ClimateCluster,rec$SourceFlowCluster,sep='_')
rec$CSGcluster = paste(rec$ClimateCluster,rec$SourceFlowCluster,rec$GeologyCluster,sep = '_')
rec$CSGcluster[which(rec$CSGcluster=="WD_H_VA")] <- "CW_H_VA"

rec$SedimentClass=NA
rec$SedimentClass[rec$CSGcluster%in%c("CD_L_HS", "WW_L_VA", "WW_H_VA", "CD_L_Al", "CW_H_SS", "CW_M_SS",
                  "CW_H_VA", "CD_H_SS", "CD_H_VA", "CD_L_VA", "CW_L_VA", "CW_M_VA", "CW_M_HS",
                  "CD_M_Al", "CW_H_Al", "CW_M_Al", "WD_L_Al")] <- 1
rec$SedimentClass[rec$CSGcluster%in%c("CD_L_SS", "WW_L_HS", "WW_L_SS", "WW_H_HS", "WW_H_SS", "WW_L_Al",
                                      "WD_L_SS", "WD_L_HS", "WD_L_VA")|
                    rec$CScluster%in%c("WD_Lk")] <- 2
rec$SedimentClass[rec$CSGcluster%in%c("CW_H_HS", "CW_L_HS","CW_L_Al", "CD_H_HS", "CD_H_Al", "CD_M_HS", "CD_M_SS",
                                      "CD_M_VA")|
                    rec$CScluster%in%c("CW_Lk","CD_Lk","WW_Lk")] <- 3
rec$SedimentClass[rec$CSGcluster%in%c("CW_L_SS")] <- 4

rec2=read_csv('D:/RiverData/River_Environment_Classification_(REC2)_New_Zealand.csv')


latLong=nztm2wgs(ce = rec2$upcoordX,cn = rec2$upcoordY)
rec2$Long=latLong[,2]
rec2$Lat=latLong[,1]
rm(latLong)

siteTable$NZReach=as.numeric(siteTable$NZReach)
# siteTable$NZReach[which(siteTable$NZReach<min(rec$NZREACH)|siteTable$NZReach>max(rec$NZREACH))] <- NA


#Assign Longitude and Latitude from rec based on NZREACH, if Long and Lat are missing
table(is.na(siteTable$Long),siteTable$Agency)
table(is.na(siteTable$NZReach),siteTable$Agency)
table(is.na(siteTable$NZReach)&is.na(siteTable$Long),siteTable$Agency)

siteTable$Long[which(is.na(siteTable$Long)&siteTable$NZReach%in%rec$NZREACH)] <- rec$Long[match(siteTable$NZReach[which(is.na(siteTable$Long)&siteTable$NZReach%in%rec$NZREACH)],
                                                                                                rec$NZREACH)]
siteTable$Lat[which(is.na(siteTable$Lat)&siteTable$NZReach%in%rec$NZREACH)] <- rec$Lat[match(siteTable$NZReach[which(is.na(siteTable$Lat)&siteTable$NZReach%in%rec$NZREACH)],
                                                                                             rec$NZREACH)]

siteTable$Landcover=NA #rec
siteTable$SedimentClass=NA #derived from rec groups, tables 23 and 26 in NPSFM
siteTable$Altitude=NA  #rec2
# siteTable$Order=NA       #rec
# siteTable$StreamOrder=NA #rec2
st=1
for(st in st:dim(siteTable)[1]){
  #REC (1)
  #Assign an NZREACH number if its missing
  if(is.na(siteTable$NZReach[st])){
    cat(st,'No NZREACH number\t')
    if(!is.na(siteTable$NZSegment[st])&siteTable$NZSegment[st]%in%rec2$nzsegment){
      siteTable$NZReach[st]=rec2$nzreach_re[match(siteTable$NZSegment[st],rec2$nzsegment)]
      cat('Assigning',siteTable$NZReach[st],'based on NZSegment match\n')
    }else{
    dists=sqrt((siteTable$Long[st]-rec$Long)^2+(siteTable$Lat[st]-rec$Lat)^2)
    mindist=which.min(dists)
    siteTable$NZReach[st] = rec$NZREACH[mindist]
    cat('Assigning',siteTable$NZReach[st],'from',min(dists,na.rm=T),'\n')
    rm(dists,mindist)
    }
  }
  recMatch = which(rec$NZREACH==siteTable$NZReach[st])
  if(length(recMatch)==0){
    cat(st,'No REC reachmatch\t',siteTable$NZReach[st],'\t')
    dists=sqrt((siteTable$Long[st]-rec$Long)^2+(siteTable$Lat[st]-rec$Lat)^2)
    recMatch=which.min(dists)
    cat('Assigning',rec$NZREACH[recMatch],'from',min(dists,na.rm=T),'\n')
    siteTable$NZReach[st]=rec$NZREACH[recMatch]
  }
  # siteTable$Order[st]=paste(unique(rec$ORDER_[recMatch]),collapse='&')
  siteTable$Landcover[st]=paste(unique(rec$LANDCOVER[recMatch]),collapse='&')
  siteTable$SedimentClass[st]=rec$SedimentClass[recMatch]
  rm(recMatch)
  
  #REC2
  #Assign an NZSegment number if its missing
  if(is.na(siteTable$NZSegment[st])){
    cat(st,'No NZSegment number\t')
    if(siteTable$NZReach[st]%in%rec2$nzreach_re){
      siteTable$NZSegment[st] = rec2$nzsegment[match(siteTable$NZReach[st],rec2$nzreach_re)]
      cat('Assigning',siteTable$NZSegment[st],'based on NZREACH match\n')
    }else{
    dists=sqrt((siteTable$Long[st]-rec2$Long)^2+(siteTable$Lat[st]-rec2$Lat)^2)
    mindist=which.min(dists)
    siteTable$NZSegment[st] = rec2$nzsegment[mindist]
    cat('Assigning',siteTable$NZsegment[st],'from',min(dists,na.rm=T),'\n')
    rm(dists,mindist)
    }
  }
  rec2match = which(rec2$nzsegment == siteTable$NZSegment[st]) 
  if(length(rec2match)==0){
    cat(st,'No REC2 segment match\t',siteTable$NZSegment[st],'\t')
    dists=sqrt((siteTable$Long[st]-rec2$Long)^2+(siteTable$Lat[st]-rec2$Lat)^2)
    rec2match=which.min(dists)
    cat('Assigning',rec2$nzsegment[rec2match],'from',min(dists,na.rm=T),'\n')
  }
  # siteTable$StreamOrder[st]=paste(unique(rec2$StreamOrde[rec2match]),collapse='&')
  siteTable$Altitude[st]=mean(rec2$upElev[rec2match],na.rm=T)
  rm(rec2match)
}


if(!all(agencies%in%unique(siteTable$Agency))){  #actually if you need to pull sites in from an old WFS sesh, like if one of them doesn respond
  oldsiteTable = read.csv("H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data/2020-09-24/SiteTable_River24Sep20.csv",stringsAsFactors = F)
  missingCouncils = agencies[!agencies%in%unique(siteTable$Agency)]
  oldsiteTable=oldsiteTable%>%filter(Agency%in%missingCouncils)
  if(dim(oldsiteTable)[1]>0){
    siteTable = merge(siteTable,oldsiteTable%>%select(names(siteTable)[names(siteTable)%in%names(oldsiteTable)]),all=T)
  }
  rm(oldsiteTable)
}

# write.csv(siteTable,'h:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/SiteTableRawLandUse.csv',row.names=F)

#Categorise landcover and altitude
siteTable$SWQLanduse=tolower(as.character(siteTable$SWQLanduse))
siteTable$SWQLanduse[tolower(siteTable$SWQLanduse)%in%c("unstated","")] <- NA
siteTable$rawSWQLanduse=siteTable$SWQLanduse
siteTable$SWQLanduse[tolower(siteTable$SWQLanduse)%in%c("reference","forest","forestry","native","exotic","natural")] <- "Forest"
siteTable$SWQLanduse=pseudo.titlecase(siteTable$SWQLanduse)

table(siteTable$SWQLanduse,siteTable$Landcover)

#Landcover's from REC, SWQLanduse is coucnil-provided
siteTable$Landcover=tolower(as.character(siteTable$Landcover))
siteTable$rawRecLandcover=siteTable$Landcover
siteTable$Landcover[siteTable$Landcover%in%c('if','ef','s','t','w','b')] <- 'Forest' #indigenous forest, exotic forest, scrub, tussock, wetland
siteTable$Landcover[siteTable$Landcover%in%c('p','m')] <- 'Rural'            #pastoral, miscellaneous
siteTable$Landcover[siteTable$Landcover%in%c('u')] <- 'Urban'                    #urban

table(siteTable$SWQLanduse,siteTable$Landcover)


#A per-council audit of conflincts between their WFSed land uses and those proposed by REC.
perRegionAudit=siteTable%>%dplyr::select(-accessDate)%>%filter(SWQLanduse!=Landcover)%>%
  dplyr::select(Region,Agency,CouncilSiteID:NZReach,Lat,Long,SWQLanduse,Landcover)%>%split(.$Region)

for(ag in seq_along(perRegionAudit)){
  thagency=unique(perRegionAudit[[ag]]$Agency)
  if('niwa'%in%thagency){thagency=thagency[thagency!='niwa']}
  write.csv(perRegionAudit[[ag]],paste0('h:/ericg/16666LAWA/LAWA2021/WaterQuality/Audit/',thagency,'LandUseAudit.csv'),row.names=F)
}


siteTable$SWQAltitude[is.na(siteTable$SWQAltitude)|siteTable$SWQAltitude==""] <- "unstated"
siteTable$SWQAltitude=tolower(siteTable$SWQAltitude)
by(data = siteTable$Altitude,INDICES = siteTable$SWQAltitude,FUN = summary)
plot(siteTable$Altitude~factor(tolower(siteTable$SWQAltitude)))

altroc=pROC::roc(response=droplevels(factor(tolower(siteTable$SWQAltitude[siteTable$SWQAltitude!='unstated']))),
                 predictor=siteTable$Altitude[siteTable$SWQAltitude!='unstated'])
highlow=pROC::coords(roc=altroc,'best')[1]$threshold
rm(altroc)
abline(h=highlow,lty=2,lwd=2)
lowland = which(siteTable$Altitude<highlow)
siteTable$AltitudeCl='Upland'
siteTable$AltitudeCl[lowland]='Lowland'
rm(lowland)

table(siteTable$AltitudeCl,siteTable$SWQAltitude)

#Per-council audit of altitude
perRegAltAudit=siteTable%>%dplyr::select(-accessDate)%>%filter(tolower(SWQAltitude)!=tolower(AltitudeCl))%>%
  dplyr::select(Region,Agency,CouncilSiteID:NZReach,Lat,Long,SWQAltitude,AltitudeCl,Altitude)%>%split(.$Region)
sapply(perRegAltAudit,dim)
for(ag in seq_along(perRegAltAudit)){
  thagency=unique(perRegAltAudit[[ag]]$Agency)
  if('niwa'%in%thagency){thagency=thagency[thagency!='niwa']}
  write.csv(perRegAltAudit[[ag]],paste0('h:/ericg/16666LAWA/LAWA2021/WaterQuality/Audit/',thagency,'AltitudeAudit.csv'),row.names=F)
}


siteTable$SWQAltitude[siteTable$SWQAltitude=='unstated']=siteTable$AltitudeCl[siteTable$SWQAltitude=='unstated']
siteTable$SWQLanduse[is.na(siteTable$SWQLanduse)]=siteTable$Landcover[is.na(siteTable$SWQLanduse)]


# siteTable$CouncilSiteID = gsub(pattern = 'Wairaki River at',replacement = "Wairaki River ds",x = siteTable$CouncilSiteID)
# siteTable$CouncilSiteID = gsub(pattern = 'Dipton Rd',replacement = "Dipton Road",x = siteTable$CouncilSiteID)
# siteTable$CouncilSiteID = gsub(pattern = 'Makarewa Confl$',replacement = "Makarewa Confluence",x = siteTable$CouncilSiteID)

for(c in 1:dim(siteTable)[2]){
  if(all(is.character(siteTable[,c]))){
    siteTable[,c]=trimws(siteTable[,c])
  }
}

write.csv(x = siteTable,
          file = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",
                        format(Sys.Date(),"%Y-%m-%d"),
                        "/SiteTable_River",format(Sys.Date(),"%d%b%y"),".csv"),row.names = F)







#Get numbers of sites per agency ####
AgencyRep=table(factor(tolower(siteTable$Agency),levels=c("ac", "boprc", "ecan", "es", "gdc", "gwrc", "hbrc","hrc", "mdc", 
                                                          "ncc","niwa", "nrc", "orc", "tdc", "trc", "wcrc", "wrc")))
AgencyRep=data.frame(agency=names(AgencyRep),count=as.numeric(AgencyRep))
names(AgencyRep)[2]=format(Sys.Date(),"%d%b%y")

WQWFSsiteFiles=dir(path = "H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",
                   pattern = 'SiteTable_River',
                   recursive = T,full.names = T)
for(wsf in WQWFSsiteFiles){
  stin=read.csv(wsf,stringsAsFactors=F)
  agencyRep=table(factor(stin$Agency,levels=c("ac", "boprc", "ecan", "es", "gdc", "gwrc", "hbrc","hrc", "mdc", 
                                              "ncc","niwa", "nrc", "orc", "tdc", "trc", "wcrc", "wrc")))
  AgencyRep = cbind(AgencyRep,as.numeric(agencyRep))
  names(AgencyRep)[dim(AgencyRep)[2]] = strTo(strFrom(wsf,'River'),'.csv')
  rm(agencyRep)
}
AgencyRep=AgencyRep[,-2]
rm(WQWFSsiteFiles)

#     agency 29Jun21 01Jul21    08Jul21  16Jul21
# 1      ac      35      35      35 35    35
# 2   boprc      50      50      50 50    50
# 3    ecan     184     191     191 190   190
# 4      es      60      60      60 60   60
# 5     gdc      39      39      39 39   39
# 6    gwrc      43      43      43 43   43
# 7    hbrc      96      96      91 94   94
# 8     hrc     137     136     136 136   136
# 9     mdc      32      32      32 32   32
# 10    ncc      25      25      25 25   25
# 11   niwa      77      77      77 77   77
# 12    nrc      32      32      32 32   32
# 13    orc      50      50      50 50   50
# 14    tdc      26      26      26 26   26
# 15    trc      22      22      22 22   22
# 16   wcrc      38      38      38 38   38
# 17    wrc     108     108     108 108   108
   

plot(x=as.numeric(AgencyRep[1,-1]),type='l',ylim=c(20,200),log='y')
apply(AgencyRep[,-1],1,function(x)lines(x))
text(rep(par('usr')[2]*0.95,dim(AgencyRep)[1]),AgencyRep[,dim(AgencyRep)[2]],AgencyRep[,1])

