rm(list=ls())
library(XML)
library(tidyverse)
library(parallel)
library(doParallel)
library(lubridate)
source('H:/ericg/16666LAWA/LAWA2021/Scripts/LAWAFunctions.R')
dir.create(paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Audit/", format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)
StartYear5 <- lubridate::year(Sys.Date())-5  #2016
EndYear <- lubridate::year(Sys.Date())-1    #2020

urls          <- read.csv("H:/ericg/16666LAWA/LAWA2021/Metadata/CouncilWFS.csv",stringsAsFactors=FALSE)

wqdata=loadLatestDataRiver()
siteTable=loadLatestSiteTableRiver()



wqAudit=data.frame(agency=NULL,xmlAge=NULL,var=NULL,nMeas=NULL,nSite=NULL,earliest=NULL,latest=NULL,minMeas=NULL,meanMeas=NULL,maxMeas=NULL,nNA=NULL)
for(agency in c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","nrc","orc","tdc","trc","wcrc","wrc")){
   # xmlAge = checkXMLageRiver(agency)
   csvAge= checkCSVageRiver(agency)
  agencyWQdata=loadLatestCSVRiver(agency)
  if(!is.null(agencyWQdata)&&dim(agencyWQdata)[1]>0){
    nPar=length(unique(agencyWQdata$Measurement))
    newRows=data.frame(agency=rep(agency,nPar),
                       # xmlAge=rep(xmlAge,nPar),
                       csvAge=rep(csvAge,nPar),
                       var=sort(unique(agencyWQdata$Measurement)),
                       nMeas=rep(NA,nPar),
                       nMeas5=rep(NA,nPar),
                       nSite=rep(NA,nPar),
                       earliest=rep("",nPar),
                       latest=rep("",nPar),
                       minMeas=rep(NA,nPar),
                       meanMeas=rep(NA,nPar),
                       maxMeas=rep(NA,nPar),
                       nNA=rep(NA,nPar),
                       stringsAsFactors = F)
    rm(nPar)
    for(v in 1:dim(newRows)[1]){
      these=which(agencyWQdata$Measurement==newRows$var[v] & !is.na(agencyWQdata$Value))
      these5=which(agencyWQdata$Measurement==newRows$var[v] & lubridate::year(lubridate::dmy(agencyWQdata$Date))>StartYear5&!is.na(agencyWQdata$Value))
      newRows$earliest[v]=format(min(dmy(agencyWQdata$Date[these])),'%d-%b-%Y')
      newRows$latest[v]=format(max(dmy(agencyWQdata$Date[these])),'%d-%b-%Y')
      newRows$nMeas[v]=length(these)
      newRows$nMeas5[v]=length(these5)
      newRows$nSite[v]=length(unique(agencyWQdata$CouncilSiteID[which(agencyWQdata$Measurement==newRows$var[v] & !is.na(agencyWQdata$Value))]))
      newRows$meanMeas[v]=round(mean(agencyWQdata$Value[these],na.rm=T),1)
      newRows$maxMeas[v]=round(max(agencyWQdata$Value[these],na.rm=T),1)
      newRows$minMeas[v]=round(min(agencyWQdata$Value[these],na.rm=T),1)
      newRows$nNA[v]=sum(is.na(agencyWQdata$Value[these]))
    }
    wqAudit <- rbind.data.frame(wqAudit,newRows)
    
#Per agency audit site/measurement start, stop, n and range ####
    nvar=length(uvars <- unique(agencyWQdata$Measurement))
    nsite=length(usites <- unique(agencyWQdata$CouncilSiteID))
    siteTerm="CouncilSiteID"
    agencyDeets=as.data.frame(matrix(nrow=nvar*nsite,ncol=7))
    names(agencyDeets)=c("Site","Var","StartDate","EndDate","nMeas","MinVal","MaxVal")
    r=1
    for(ns in 1:nsite){
      for(nv in 1:nvar){
        these=which(agencyWQdata[,siteTerm]==usites[ns]&agencyWQdata$Measurement==uvars[nv])
        agencyDeets[r,]=c(usites[ns],                               #Site
                          uvars[nv],                                #Var
                          as.character(min(dmy(agencyWQdata$Date[these]))),  #StartDate
                          as.character(max(dmy(agencyWQdata$Date[these]))),  #EndDate
                          length(these),                            #nMeas
                          min(agencyWQdata$Value[these],na.rm=T),            #minval
                          max(agencyWQdata$Value[these],na.rm=T))            #maxval
        r=r+1
      }
    }
    write.csv(agencyDeets,paste0("h:/ericg/16666LAWA/LAWA2021/WaterQuality/Audit/",format(Sys.Date(),"%Y-%m-%d"),
                                 "/",agency,format(Sys.Date(),'%d%b%y'),"audit.csv"),row.names = F)
    
  }
}
write.csv(wqAudit,paste0("h:/ericg/16666LAWA/LAWA2021/WaterQuality/Audit/",format(Sys.Date(),"%Y-%m-%d"),
                     "/WQAudit",format(Sys.Date(),'%d%b%y'),".csv"),row.names = F)


wqAudit%>%dplyr::group_by(agency)%>%
  dplyr::summarise(csvAge=mean(csvAge,na.rm=T),
                   endDate=max(lubridate::dmy(latest),na.rm=T))%>%knitr::kable(format='rst')




#Audit plots to allow comparison between agencies - check units consistency etc  ####
# wqd=doBy::summaryBy(data=wqdata,
#                     formula=Value~LawaSiteID+Measurement+Date,
#                     id=~Agency,
#                     FUN=median)
# wqds = pivot_wider(data = wqd,names_from = Measurement,values_from = Value.median)%>%as.data.frame

wqds <- wqdata%>%
  group_by(LawaSiteID,Measurement,Date)%>%
  dplyr::summarise(.groups='keep',
            Agency=first(Agency),
            median=median(Value,na.rm=T))%>%ungroup%>%
  pivot_wider(names_from = Measurement,values_from = median)%>%as.data.frame


params=unique(wqdata$Measurement)
for(param in 1:length(params)){
  png(filename = paste0("h:/ericg/16666LAWA/LAWA2021/WaterQuality/Audit/",format(Sys.Date(),"%Y-%m-%d"),"/",
                        names(wqds)[param+3],format(Sys.Date(),'%d%b%y'),".png"),
       width = 12,height=9,units='in',res=300,type='cairo')
  if(names(wqds)[param+3]!="PH"){
    plot(as.factor(wqds$Agency[wqds[,param+3]>0]),wqds[wqds[,param+3]>0,param+3],
         ylab=names(wqds)[param+3],main=names(wqds)[param+3],log='y')
  }else{
    plot(as.factor(wqds$Agency),wqds[,param+3],ylab=names(wqds)[param+3],main=names(wqds)[param+3])
  }
  if(names(dev.cur())=='png'){dev.off()}
}




#And the ubercool html summary audit report doncuments per council! ####
# wqdata=loadLatestDataRiver()
# wqdata$Date[wqdata$Agency=='ac']=as.character(format(lubridate::ymd_hms(wqdata$Date[wqdata$Agency=='ac']),'%d-%b-%y'))
urls          <- read.csv("H:/ericg/16666LAWA/LAWA2021/Metadata/CouncilWFS.csv",stringsAsFactors=FALSE)
startTime=Sys.time()
 for(agency in c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","nrc","orc","tdc","trc","wcrc","wrc")){#
          # if(length(dir(path = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Audit/",format(Sys.Date(),"%Y-%m-%d")),
          #       pattern = paste0('^',agency,".*audit\\.csv"),
          #       recursive = T,full.names = T,ignore.case = T))>0){
            rmarkdown::render('H:/ericg/16666LAWA/LAWA2021/WaterQuality/Scripts/AuditDocument.Rmd',
                              params=list(agency=agency,
                                          sos=urls$SOSwq[which(tolower(urls$Agency)==agency)]),
                              output_dir = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Audit/",format(Sys.Date(),"%Y-%m-%d")),
                              output_file = paste0(toupper(agency),"Audit",format(Sys.Date(),'%d%b%y'),".html"),
                              envir = new.env())
          # }
}
Sys.time()-startTime
#20 minutes 16/7/2021
