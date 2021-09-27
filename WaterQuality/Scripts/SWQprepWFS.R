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
  xmldata <- try(ldWFSlist(urlIn = urls$URL[h],agency=urls$Agency[h],dataLocation = urls$Source[h],case.fix = TRUE))
  if(is.null(xmldata)||'try-error'%in%attr(xmldata,'class')){
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
#982

siteTable$Lat = as.numeric(siteTable$Lat)
siteTable$Long = as.numeric(siteTable$Long)

siteTable$SiteID=as.character(siteTable$SiteID)
siteTable$LawaSiteID=as.character(siteTable$LawaSiteID)
siteTable$Region=tolower(as.character(siteTable$Region))
siteTable$Agency=tolower(as.character(siteTable$Agency))

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



agencies= c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","nrc","orc","tdc","trc","wcrc","wrc")

all(agencies%in%unique(siteTable$Agency))
if(!all(agencies%in%unique(siteTable$Agency))){  #actually if you need to pull sites in from an old WFS sesh, like if one of them doesn respond
  oldsiteTable = read.csv("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/2021-08-12/SiteTable_River12Aug21.csv",stringsAsFactors = F)
  missingCouncils = agencies[!agencies%in%unique(siteTable$Agency)]
  oldsiteTable=oldsiteTable%>%filter(Agency%in%missingCouncils)
  if(dim(oldsiteTable)[1]>0){
    siteTable = merge(siteTable,oldsiteTable%>%select(names(siteTable)[names(siteTable)%in%names(oldsiteTable)]),all=T)
  }
  rm(oldsiteTable)
}



#Add NIWA sites separately ####
if(!'niwa'%in%unique(siteTable$Agency)){
if(file.exists("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/NIWALawaSiteIDs.csv")){
  niwaSub=read.csv("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/NIWALawaSiteIDs.csv",stringsAsFactors=F)%>%drop_na(LawaSiteID)
  plot(niwaSub$Long,niwaSub$Lat,lwd=2,col='red')
  points(siteTable$Long,siteTable$Lat,pch=16,cex=0.25)
  for(ns in seq_along(niwaSub$SiteID)){
    dists=sqrt((niwaSub$Long[ns]-siteTable$Long)^2+(niwaSub$Lat[ns]-siteTable$Lat)^2)*111.111
    closest=which.min(dists)
    segments(x0 = niwaSub$Long[ns],y0=niwaSub$Lat[ns],
             x1 = siteTable$Long[closest],y1=siteTable$Lat[closest])
    if(dists[closest]<(0.71*111.111)){ #80 km
      niwaSub$Region[ns]=siteTable$Region[closest]
    }else{
      browser()
    }
  }
  # niwaSub$LawaSiteID=paste0(niwaSub$LawaSiteID,'_niwa')
}else{
  df <- read.csv(paste0("H:/ericg/16666LAWA/LAWA2019/WaterQuality/MetaData/niwaSWQ_config.csv"),sep=",",stringsAsFactors=FALSE)
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
  
  #Info from MareePaterson
  niwaIDs=read.csv("h:/ericg/16666LAWA/LAWA2021/Metadata/NRWQN site ID's.csv")
  
  for(ns in 1:77){
    matchUp = grep(paste0(niwaSub$CouncilSiteID[ns],'$'),niwaIDs$Comments)
    if(length(matchUp)==1){
      niwaSub$Region[ns] = niwaIDs$Region[matchUp]
      niwaSub$LawaSiteID[ns] = niwaIDs$LAWAID[matchUp]
      niwaSub$NZReach[ns] = niwaIDs$NZREACH[matchUp]
    }
    if(length(matchUp)>1){browser()}
  }


  # monList = read_csv('H:/ericg/16666LAWA/LAWA2021/Metadata/Masterlist of sites in LAWA Umbraco as at 1 June 2021.csv')%>%filter(grepl('nrwqn',monList$LAWAID,ignore.case = T))
  # 
  # # monList = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2021/Metadata/LAWA Masterlist of Umbraco Sites as at 30 July 2020.xlsx',sheet=1)
  # lawaMasterList = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2021/Metadata/LAWA Site Master List.xlsx',sheet=2)%>%filter(grepl('nrwqn',lawaMasterList$`LAWA ID`,ignore.case=T))
  # 
  # lawaMasterList$Long = nztm2wgs(ce = as.numeric(lawaMasterList$NZTM_Easting),cn = as.numeric(lawaMasterList$NZTM_Northing))[,2]
  # lawaMasterList$Lat = nztm2wgs(ce = as.numeric(lawaMasterList$NZTM_Easting),cn = as.numeric(lawaMasterList$NZTM_Northing))[,1]
  # lawaMasterList$Lat[lawaMasterList$Long>200] <- NA
  # lawaMasterList$Long[lawaMasterList$Long>200] <- NA
  # 
  # lawaMasterList$Long[is.na(lawaMasterList$Long)] <- siteTable$Long[match(lawaMasterList$`LAWA ID`[is.na(lawaMasterList$Long)],
  #                                                                         siteTable$LawaSiteID)]
  # lawaMasterList$Lat[is.na(lawaMasterList$Lat)] <- siteTable$Lat[match(lawaMasterList$`LAWA ID`[is.na(lawaMasterList$Lat)],
  #                                                                      siteTable$LawaSiteID)]
  # lawaMasterList$Long[is.na(lawaMasterList$Long)] <- siteTable$Long[match(lawaMasterList$`Site Name`[is.na(lawaMasterList$Long)],
  #                                                                         siteTable$LawaSiteID)]
  # lawaMasterList$Lat[is.na(lawaMasterList$Lat)] <- siteTable$Lat[match(lawaMasterList$`Site Name`[is.na(lawaMasterList$Lat)],
  #                                                                      siteTable$LawaSiteID)]
  # 
  # monList$Long = as.numeric(monList$Longitude)
  # monList$Lat = as.numeric(monList$Latitude)
  
  # monList$Long[is.na(monList$Long)] = lawaMasterList$Long[match(monList$LAWAID[is.na(monList$Long)],lawaMasterList$`LAWA ID`)]
  # monList$Lat[is.na(monList$Lat)] = lawaMasterList$Lat[match(monList$LAWAID[is.na(monList$Lat)],lawaMasterList$`LAWA ID`)]
  # monList$Long[is.na(monList$Long)] = siteTable$Long[match(tolower(monList$LAWAID[is.na(monList$Long)]),tolower(siteTable$LawaSiteID))]
  # monList$Lat[is.na(monList$Lat)] = siteTable$Lat[match(tolower(monList$LAWAID[is.na(monList$Lat)]),tolower(siteTable$LawaSiteID))]
  
  # getCore <- function(s){
  #   s <- gsub('river','',s,ignore.case=T)
  #   s <- gsub('hw|highway','',s,ignore.case=T)
  #   s <- gsub('br|bridge','',s,ignore.case=T)
  #   s <- gsub('below','',s,ignore.case=T)
  #   
  # }
  # 

  
  # #Text-basd distace
  # for(ns in seq_along(niwaSub$SiteID)){
  #   
  #   mondists = adist(trimws(getCore(tolower(strTo(niwaSub$SiteID[ns],' \\(')))),
  #                    trimws(getCore(tolower(monList$Name))))
  #   clM = which.min(mondists)
  #   # masdists = adist(tolower(strTo(niwaSub$SiteID[ns],'\\(')),trimws(tolower(lawaMasterList$`Site Name`)))
  #   # clMas = which.min(masdists)
  #   geoDist=sqrt((niwaSub$Long[ns]-monList$Long[clM])^2+(niwaSub$Lat[ns]-monList$Lat[clM])^2)*111.111
  #   cat(tolower(strTo(niwaSub$SiteID[ns],' \\(')),'\t',monList$Name[clM],'\t',geoDist,'\n')
  #   # cat(tolower(strTo(niwaSub$SiteID[ns],' \\(')),'\t',lawaMasterList$`Site Name`[clMas],'\n')
  #   check=readline()
  #   if(check==""){
  #     niwaSub$LawaSiteID[ns]=monList$LAWAID[clM]
  #   segments(x0=niwaSub$Long[ns],y0=niwaSub$Lat[ns],
  #            x1=monList$Long[clM],y1=monList$Lat[clM],col='blue',lwd=5)
  #   }
  # }
  # sum(is.na(niwaSub$LawaSiteID))
  niwaSub[is.na(niwaSub$LawaSiteID),c(1,2,3,8,9)]
  these=which(is.na(niwaSub$LawaSiteID))

  # monowai below gates nrwqn-00034
  
  plot(niwaSub$Long,niwaSub$Lat,pch=16,cex=0.25)
  points(niwaSub$Long[these],niwaSub$Lat[these])
  # for(ns in seq_along(these)){
  #   cat('\n',niwaSub$SiteID[these[ns]],'\n')
  #   dists=sqrt((niwaSub$Long[these[ns]]-siteTable$Long)^2+(niwaSub$Lat[these[ns]]-siteTable$Lat)^2)
  #   closest=which.min(dists)
  #   segments(x0 = niwaSub$Long[these[ns]],y0=niwaSub$Lat[these[ns]],
  #            x1 = siteTable$Long[closest],y1=siteTable$Lat[closest],col='magenta',lwd=2)
  #   cat("siteTable\t","distance\t",round(min(dists,na.rm=T)*111,1),'\t',siteTable$LawaSiteID[closest], siteTable$SiteID[closest],'\n')
  # 
  #   dists=sqrt((niwaSub$Long[these[ns]]-monList$Long)^2+(niwaSub$Lat[these[ns]]-monList$Lat)^2)
  #   closest=which.min(dists)
  #   segments(x0 = niwaSub$Long[these[ns]],y0=niwaSub$Lat[these[ns]],
  #            x1 = monList$Long[closest],y1=monList$Lat[closest],col='red',lwd=2)
  #   cat("monList\t\t","distance\t",round(min(dists,na.rm=T)*111,1),'\t',monList$LAWAID[closest],monList$Name[closest],'\n')
  #   niwaSub$LawaSiteID[these[ns]] <- monList$LAWAID[closest]
  # }
  
  #Try filling gaps from old NIWA data
  # load('H:/ericg/16666LAWA/2018/WaterQuality/1.Imported/NIWAwqData.rData',verbose=T)  #NIWAdataInB
  # cbind(niwaSub$SiteID[is.na(niwaSub$LawaSiteID)],
  #       NIWAdataInB[match(niwaSub$CouncilSiteID[is.na(niwaSub$LawaSiteID)],
  #                         NIWAdataInB$SiteName),c("CouncilSiteID","SiteName","LawaSiteID")])
  # 
  # niwaSub$LawaSiteID[is.na(niwaSub$LawaSiteID)] = 
  #   NIWAdataInB$LawaSiteID[match(niwaSub$CouncilSiteID[is.na(niwaSub$LawaSiteID)],
  #                                NIWAdataInB$SiteName)]
  # #Check names match
  # cbind(niwaSub$SiteID,NIWAdataInB[match(niwaSub$CouncilSiteID,NIWAdataInB$SiteName),c("CouncilSiteID","SiteName")])
  # 
  # niwaSub <- left_join(niwaSub,NIWAdataInB%>%select(LawaSiteID,SWQAltitude,SWQLanduse)%>%distinct,by="LawaSiteID")
  # rm(NIWAdataInB)
  # niwaSub$LawaSiteID=tolower(niwaSub$LawaSiteID)
  write.csv(niwaSub,"H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/NIWALawaSiteIDs.csv",row.names=F)
}##
siteTable=merge(x=siteTable,y=niwaSub,all.x=T,all.y=T)
rm(niwaSub)
}


table(siteTable$Region,siteTable$Agency,useNA = 'a')



#https://www.stat.auckland.ac.nz/~paul/Reports/maori/maori.html
siteTable$SiteID=as.character(siteTable$SiteID)
grep(pattern = "[^a-z,A-Z, ,/,@,0-9,.,-,[:punct:]]",x = siteTable$SiteID,value=T,ignore.case = T)


plot(siteTable$Long,siteTable$Lat,col=as.numeric(factor(siteTable$Region)))
points(siteTable$Long[siteTable$Agency=='niwa'],
       siteTable$Lat[siteTable$Agency=='niwa'],pch=16,cex=0.5,
       col=as.numeric(factor(siteTable$Region[siteTable$Agency=='niwa'],levels=sort(unique(siteTable$Region)))))


siteTable$NZReach=as.integer(siteTable$NZReach)
siteTable$NZSegment=as.integer(siteTable$NZSegment)

siteTable=unique(siteTable)  
#27/7/2021 1055
#20/8/21 1056
#8/9/2021 1038


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


# riverSiteTable$SedimentClass[is.na(riverSiteTable$SedimentClass)] <- rec$SedimentClass[match(riverSiteTable$NZReach[is.na(siteTable$SedimentClass)],rec$NZREACH)]



rec2=read_csv('D:/RiverData/River_Environment_Classification_(REC2)_New_Zealand.csv')
latLong=nztm2wgs(ce = rec2$upcoordX,cn = rec2$upcoordY)
rec2$Long=latLong[,2]
rec2$Lat=latLong[,1]
rm(latLong)

siteTable$NZReach=as.numeric(siteTable$NZReach)
siteTable$NZSegment=as.numeric(siteTable$NZSegment)
# siteTable$NZReach[which(siteTable$NZReach<min(rec$NZREACH)|siteTable$NZReach>max(rec$NZREACH))] <- NA


#Assign Longitude and Latitude from rec based on NZREACH, if Long and Lat are missing
table(siteTable$Agency,is.na(siteTable$NZSegment))
table(siteTable$Agency,is.na(siteTable$NZReach))
table(siteTable$Agency,is.na(siteTable$NZReach)&is.na(siteTable$NZSegment))
table(siteTable$Agency,is.na(siteTable$Long))
table(siteTable$Agency,is.na(siteTable$NZReach)&is.na(siteTable$Long))

siteTable$Long[which(is.na(siteTable$Long)&siteTable$NZReach%in%rec$NZREACH)] <- rec$Long[match(siteTable$NZReach[which(is.na(siteTable$Long)&siteTable$NZReach%in%rec$NZREACH)],rec$NZREACH)]
siteTable$Lat[which(is.na(siteTable$Lat)&siteTable$NZReach%in%rec$NZREACH)] <- rec$Lat[match(siteTable$NZReach[which(is.na(siteTable$Lat)&siteTable$NZReach%in%rec$NZREACH)],rec$NZREACH)]

reachButNotSeg=which(is.na(siteTable$NZSegment)&!is.na(siteTable$NZReach))
segButNotReach=which(is.na(siteTable$NZReach)&!is.na(siteTable$NZSegment))

siteTable$NZSegment[reachButNotSeg] <- rec2$nzsegment[match(siteTable$NZReach[reachButNotSeg],rec2$nzreach_re)]
siteTable$NZReach[segButNotReach] <- rec2$nzreach_re[match(siteTable$NZSegment[segButNotReach],rec2$nzsegment)]

rm(segButNotReach,reachButNotSeg)

siteTable$Landcover=NA #rec
siteTable$SedimentClass=NA #derived from rec groups, tables 23 and 26 in NPSFM
siteTable$Altitude=NA  #rec2
# siteTable$Order=NA       #rec
# siteTable$StreamOrder=NA #rec2

missingSomething = which(is.na(siteTable$NZReach)|is.na(siteTable$NZSegment)) #Not an exclusive or

table(siteTable$Agency[missingSomething])

for(st in missingSomething){
  #REC (1)
  #Assign an NZREACH number if its missing
  if(is.na(siteTable$NZReach[st])||tolower(siteTable$NZReach[st])=='unstated'){
    cat('AANo NZREACH number\t')
    dists=sqrt((siteTable$Long[st]-rec$Long)^2+(siteTable$Lat[st]-rec$Lat)^2)
    mindist=which.min(dists)
    siteTable$NZReach[st] = rec$NZREACH[mindist]
    cat('CCAssigning',siteTable$NZReach[st],'from',round(min(dists,na.rm=T)*111000,1),' m\n')
    rm(dists,mindist)
  }
  recMatch = which(rec$NZREACH==siteTable$NZReach[st])
  if(length(recMatch)==0){
    cat('DDNo REC reachmatch\t',siteTable$NZReach[st],'\t')
    dists=sqrt((siteTable$Long[st]-rec$Long)^2+(siteTable$Lat[st]-rec$Lat)^2)
    recMatch=which.min(dists)
    cat('EEAssigning',rec$NZREACH[recMatch],'from',round(min(dists,na.rm=T)*111000,1),' m\n')
    siteTable$NZReach[st]=rec$NZREACH[recMatch]
  }
  rm(recMatch)
  
  #REC2
  #Assign an NZSegment number if its missing
  if(is.na(siteTable$NZSegment[st])||tolower(siteTable$NZSegment[st])=='unstated'){
    cat('FFNo NZSegment number\t')
    if(siteTable$NZReach[st]%in%rec2$nzreach_re){
      siteTable$NZSegment[st] = rec2$nzsegment[match(siteTable$NZReach[st],rec2$nzreach_re)]
      cat('GGAssigning',siteTable$NZSegment[st],'based on NZREACH match\n')
    }else{
    dists=sqrt((siteTable$Long[st]-rec2$Long)^2+(siteTable$Lat[st]-rec2$Lat)^2)
    mindist=which.min(dists)
    siteTable$NZSegment[st] = rec2$nzsegment[mindist]
    cat('HHAssigning',siteTable$NZsegment[st],'from',round(min(dists,na.rm=T)*111000,1),'\n')
    rm(dists,mindist)
    }
  }
  rec2match = which(rec2$nzsegment == siteTable$NZSegment[st]) 
  if(length(rec2match)==0){
    cat('HHNo REC2 segment match\t',siteTable$NZSegment[st],'\t')
    dists=sqrt((siteTable$Long[st]-rec2$Long)^2+(siteTable$Lat[st]-rec2$Lat)^2)
    rec2match=which.min(dists)
    cat('IIAssigning',rec2$nzsegment[rec2match],'from',round(min(dists,na.rm=T)*111000,1),'\n')
  }
  rm(rec2match)
}

siteTable$Landcover = rec$LANDCOVER[match(siteTable$NZReach,rec$NZREACH)]
siteTable$SedimentClass = rec$SedimentClass[match(siteTable$NZReach,rec$NZREACH)]
siteTable$Altitude = rec2$upElev[match(siteTable$NZSegment,rec2$nzsegment)]





# write.csv(siteTable,'h:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/SiteTableRawLandUse.csv',row.names=F)

#Categorise landcover and altitude
siteTable$SWQLanduse=tolower(as.character(siteTable$SWQLanduse))
if(any(siteTable$SWQLanduse%in%c("unstated",""))){
  siteTable$SWQLanduse[which(siteTable$SWQLanduse%in%c("unstated",""))] <- NA
}
siteTable$rawSWQLanduse=siteTable$SWQLanduse
siteTable$SWQLanduse[tolower(siteTable$SWQLanduse)%in%c("reference","forest","forestry","native","exotic","natural")] <- "Forest"
siteTable$SWQLanduse=pseudo.titlecase(siteTable$SWQLanduse)

table(siteTable$SWQLanduse,siteTable$Landcover,useNA='a')%>%addmargins



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


siteTable$SWQAltitude=tolower(siteTable$SWQAltitude)
siteTable$SWQAltitude[siteTable$SWQAltitude%in%c("","unstated")] <- NA
by(data = siteTable$Altitude,INDICES = siteTable$SWQAltitude,FUN = summary)
plot(siteTable$Altitude~factor(tolower(siteTable$SWQAltitude)))

altroc=pROC::roc(response=droplevels(factor(tolower(siteTable$SWQAltitude[siteTable$SWQAltitude!='unstated']))),
                 predictor=siteTable$Altitude[siteTable$SWQAltitude!='unstated'])
highlow=pROC::coords(roc=altroc,'best')[1]$threshold
rm(altroc)
abline(h=highlow,lty=2,lwd=2)        #139m
lowland = which(siteTable$Altitude<highlow)
siteTable$AltitudeCl='Upland'
siteTable$AltitudeCl[lowland]='Lowland'
rm(lowland)

table(siteTable$AltitudeCl,siteTable$SWQAltitude,useNA='a')%>%addmargins()

#Per-council audit of altitude
perRegAltAudit=siteTable%>%dplyr::select(-accessDate)%>%filter(tolower(SWQAltitude)!=tolower(AltitudeCl))%>%
  dplyr::select(Region,Agency,CouncilSiteID:NZReach,Lat,Long,SWQAltitude,AltitudeCl,Altitude)%>%split(.$Region)
sapply(perRegAltAudit,dim)
for(ag in seq_along(perRegAltAudit)){
  thagency=unique(perRegAltAudit[[ag]]$Agency)
  if('niwa'%in%thagency){thagency=thagency[thagency!='niwa']}
  write.csv(perRegAltAudit[[ag]],paste0('h:/ericg/16666LAWA/LAWA2021/WaterQuality/Audit/',thagency,'AltitudeAudit.csv'),row.names=F)
}

if(any(is.na(siteTable$SWQAltitude)|siteTable$SWQAltitude=='unstated')){
  siteTable$SWQAltitude[which(is.na(siteTable$SWQAltitude)|siteTable$SWQAltitude=='unstated')]=siteTable$AltitudeCl[which(is.na(siteTable$SWQAltitude)|siteTable$SWQAltitude=='unstated')]
}
siteTable$SWQLanduse[is.na(siteTable$SWQLanduse)]=siteTable$Landcover[is.na(siteTable$SWQLanduse)]



for(c in 1:dim(siteTable)[2]){
  if(all(is.character(siteTable[,c]))){
    siteTable[,c]=trimws(siteTable[,c])
  }
}

siteTable$LawaSiteID = tolower(siteTable$LawaSiteID)
siteTable$Region[siteTable$Region%in%c("horizons","manawatu-whanganui")] <- "manawatū-whanganui"

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

# agency 29Jun21 01Jul21 08Jul21 14Jul21 16Jul21 23Jul21 27Jul21 30Jul21 05Aug21 09Aug21 11Aug21 12Aug21 20Aug21 27Aug21
#    ac      35      35      35      35      35      35      35      35      35      35      35      35      35      35
# boprc      50      50      50      50      50      50      49      49      49      49      49      49      49      49
#  ecan     184     191     191     190     190     190     190     190     190     190     190     190     190     190
#    es      60      60      60      60      60      60      60      60      60      60      60      60      60      60
#   gdc      39      39      39      39      39      39      39      39      39      39      39      39      39      39
#  gwrc      43      43      43      43      43      43      43      43      43      43      43      43      43      46
#  hbrc      96      96      91      94      94      94      94      94      94      94      94      94      94      94
#   hrc     137     136     136     136     136     136     136     136     136     136     136     136     136     136
#   mdc      32      32      32      32      32      32      32      32      32      32      32      32      32      32
#   ncc      25      25      25      25      25      25      25      25      25      25      25      25      25      25
#  niwa      77      77      77      77      77      77      77      77      77      77      77      77      77      77
#   nrc      32      32      32      32      32      32      40      40      40      40      40      41      41      41
#   orc      50      50      50      50      50      50      50      50      50      50      50      50      50      50
#   tdc      26      26      26      26      26      26      26      26      26      26      26      26      26      26
#   trc      22      22      22      22      22      22      22      22      13      13      13      13      13      13
#  wcrc      38      38      38      38      38      38      38      38      38      38      38      38      38      38
#   wrc     108     108     108     108     108     108     108     108     108     108     108     108     108     108
   

# agency 27Aug21  08Sep21
#    ac      35       35
# boprc      49       49
#  ecan     190      190
#    es      60       60
#   gdc      39       39
#  gwrc      46       46
#  hbrc      94       94
#   hrc     136      136
#   mdc      32       32
#   ncc      25       25
#  niwa      77       57
#   nrc      41       41
#   orc      50       49
#   tdc      26       26
#   trc      13       13
#  wcrc      38       38
#   wrc     108      108

plot(x=as.numeric(AgencyRep[1,-1]),type='l',ylim=c(10,200),log='y')
apply(AgencyRep[,-1],1,function(x)lines(x))
text(rep(par('usr')[2]*0.95,dim(AgencyRep)[1]),AgencyRep[,dim(AgencyRep)[2]],AgencyRep[,1])

