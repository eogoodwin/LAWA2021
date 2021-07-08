#July 2021
#This file relies on a list of SSM sites provided by Effect, but then iterates across that list, trying to pull the actual data from the council servers direct.

rm(list=ls())
library(tidyverse)
library(sysfonts)
library(googleVis)
source('H:/ericg/16666LAWA/LAWA2021/Scripts/LAWAFunctions.R')
dir.create(paste0("H:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/",format(Sys.Date(),"%Y-%m-%d")),showWarnings = F,recursive = T)
dir.create(paste0("H:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/",format(Sys.Date(),"%Y-%m-%d")),showWarnings = F,recursive = T)

lmsl=readxl::read_xlsx("H:/ericg/16666LAWA/LAWA2020/Metadata/LAWA Masterlist of Umbraco Sites as at 30 July 2020.xlsx")

checkXMLFile <- function(regionName,siteName,propertyName){
  fname=paste0('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/',regionName,siteName,propertyName,'.xml')
  xmlfile<-read_xml(fname)
  if('ows'%in%names(xml_ns(xmlfile)) && length(xml_find_all(xmlfile,'ows:Exception'))>0){
    #Dont wanna keep exception files
    file.remove(fname)
    return(1);#next
  }
  #Got Data
  if('wml2'%in%names(xml_ns(xmlfile))){
    mvals <- xml_find_all(xmlfile,'//wml2:value')
    if(length(mvals)>0){
      return(2);#hoorah break
    }else{
      #Dont wanna keep empty files
      file.remove(fname)
    }
    return(1);#next
  }else{
    if(propertyName=="WQ.sample"){
      pvals=xml_find_all(xmlfile,'//Parameter')
      if(length(pvals)>0){
        return(2);
      }else{
        file.remove(fname)
        return(1);
      }
      rm(pvals)
    }else{
      #Remove non WML files
      if(regionName!="Taranaki"){
        file.remove(fname)
        return(1);#next
      }else{
        if(file.info(fname)$size>2000){
          return(2)
        }else{
          file.remove(fname)
          return(1);#next
        }
      }
    }
  }
}
readXMLFile <- function(regionName,siteName,propertyName,property){
  fname=paste0('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/',regionName,siteName,propertyName,'.xml')
  require(xml2)
  xmlfile<-read_xml(fname)
  #Check for exceptions
  if('ows'%in%names(xml_ns(xmlfile)) && length(xml_find_all(xmlfile,'ows:Exception'))>0){
    cat('-')
    excCode <- try(xml_find_all(xmlfile,'//ows:Exception'))
    if(length(excCode)>0){
      commentToAdd=xml_text(excCode)
    }else{
      commentToAdd='exception'
    }
    #Dont wanna keep exception files
    file.remove(fname)
    return(1);#next
  }
  #Check for Data
  if('wml2'%in%names(xml_ns(xmlfile))){
    mvals <- xml_find_all(xmlfile,'//wml2:value')
    if(length(mvals)>0){
      cat('*')
      mvals=xml_text(mvals)
      faceVals=as.numeric(mvals)
      if(any(lCens<-unlist(lapply(mvals,FUN = function(x)length(grepRaw(pattern = '<',x=x))>0)))){
        faceVals[lCens]=readr::parse_number(mvals[lCens])
      }
      if(any(rCens<-unlist(lapply(mvals,FUN = function(x)length(grepRaw(pattern = '>',x=x))>0)))){
        faceVals[rCens]=readr::parse_number(mvals[rCens])
      }
      mT<-xml_find_all(xmlfile,'//wml2:time')
      mT<-xml_text(mT)  # get time text
      mT <- strptime(mT,format='%Y-%m-%dT%H:%M:%S')
      siteDat=data.frame(region=SSMregion,
                         siteName=siteName,
                         propertyName=propertyName,
                         property=property,
                         dateCollected=mT,
                         val=faceVals,
                         lCens=lCens,
                         rCens=rCens)%>%
        plyr::mutate(week=lubridate::week(mT),
                     month=lubridate::month(mT),
                     year=lubridate::year(mT),
                     YW=paste0(year,week))
      eval(parse(text=paste0(make.names(paste0(regionName,siteName,propertyName)),"<<-siteDat")))
      dataToCombine <<- c(dataToCombine,make.names(paste0(regionName,siteName,propertyName)))
      rm(siteDat)
      return(2)#        break
    }else{
      #Dont wanna keep empty files
      file.remove(fname)
    }
    return(1);#next
  }else{
    #Check is it WQSample
    if(propertyName=="WQ.sample"){
      xmlfile=xmlParse(fname)
      xmltop<-xmlRoot(xmlfile)
      m<-xmltop[['Measurement']]
      if(!is.null(m)){
        dtV <- xpathSApply(m,"//T",xmlValue)
        # dtV <- unlist(dtV)
        siteMetaDat=data.frame(regionName=regionName,siteName=siteName,property=property,SampleDate=dtV)
        for(k in 1:length(dtV)){
          p <- m[["Data"]][[k]]
          c <- length(xmlSApply(p, xmlSize))
          if(c==1){next}
          newDF=data.frame(regionName=regionName,siteName=siteName,property=property,SampleDate=dtV[k])
          for(n in 2:c){   
            if(!is.null(p[[n]])&!any(xmlToList(p[[n]])=="")){
              metaName  <- as.character(xmlToList(p[[n]])[1])   ## Getting the name attribute
              metaValue <- as.character(xmlToList(p[[n]])[2])   ## Getting the value attribute
              metaValue <- gsub('\\"|\\\\','',metaValue)
              eval(parse(text=paste0("newDF$`",metaName,"`=\"",metaValue,"\"")))
              rm(metaName,metaValue)
            }
          }
          siteMetaDat=merge(siteMetaDat,newDF,all = T)
          rm(newDF)
        }
      }
      rm(c,k,p,m,n)
      mtCols = which(apply(siteMetaDat,2,function(x)all(is.na(x)|x=="NA")))
      if(length(mtCols)>0){
        siteMetaDat=siteMetaDat[,-mtCols]
      }
      eval(parse(text=paste0(make.names(paste0(regionName,siteName,propertyName)),"<<-siteMetaDat")))
      metaToCombine <<- c(metaToCombine,make.names(paste0(regionName,siteName,propertyName)))
      rm(siteMetaDat)
      return(2)#        break
    }else{
      if(regionName!="Taranaki"){
        file.remove(fname)
        return(1);#next
      }else{
        #Curstom read code for taranaki needs to get a siteDat and siteMetaDat together
        xmlfile=xmlParse(fname)
        xmltop<-xmlRoot(xmlfile)
        m<-xmltop[['Measurement']]
        if(!is.null(m)){
          dtV <- xpathSApply(m,"//T",xmlValue)
          dtV <- strptime(dtV,format='%Y-%m-%dT%H:%M:%S')
          siteMetaDat=NULL
          for(k in 1:length(dtV)){
            p <- m[["Data"]][[k]]
            c <- length(xmlSApply(p, xmlSize))
            if(c==1){next}
            newDF=data.frame(regionName=regionName,siteName=siteName,property=property,SampleDate=dtV[k])
            for(n in 2:c){   
              if(!is.null(p[[n]])&!any(xmlToList(p[[n]])=="")){
                if(length(xmlToList(p[[n]]))==1){
                  metaName  <- "Value"
                  metaValue <- as.character(xmlToList(p[[n]])[1])   ## Getting the value attribute
                }else{
                  metaName  <- as.character(xmlToList(p[[n]])[1])   ## Getting the name attribute
                  metaValue <- as.character(xmlToList(p[[n]])[2])   ## Getting the value attribute
                }
                metaValue <- gsub('\\"|\\\\','',metaValue)
                eval(parse(text=paste0("newDF$`",metaName,"`=\"",metaValue,"\"")))
                rm(metaName,metaValue)
              }
            }
            siteMetaDat=merge(siteMetaDat,newDF,all = T)
            rm(newDF)
          }
          mtCols = which(apply(siteMetaDat,2,function(x)all(is.na(x)|x=="NA")))
          if(length(mtCols)>0){
            siteMetaDat=siteMetaDat[,-mtCols]
          }
          eval(parse(text=paste0(make.names(paste0(regionName,siteName,propertyName,"MD")),"<<-siteMetaDat")))
          metaToCombine <<- c(metaToCombine,make.names(paste0(regionName,siteName,propertyName,"MD")))
          
          siteDat=siteMetaDat%>%dplyr::select(region=regionName,siteName,property=property,
                                              dateCollected=SampleDate,val=Value)
          siteDat$lCens<-sapply(siteDat$val,FUN = function(x)length(grepRaw(pattern = '<',x=x))>0)
          siteDat$rCens<-sapply(siteDat$val,FUN = function(x)length(grepRaw(pattern = '>',x=x))>0)
          siteDat$val = readr::parse_number(siteDat$val)
          
          siteDat <- siteDat%>%
            plyr::mutate(week=lubridate::week(dateCollected),
                         month=lubridate::month(dateCollected),
                         year=lubridate::year(dateCollected),
                         YW=paste0(year,week))
          eval(parse(text=paste0(make.names(paste0(regionName,siteName,propertyName)),"<<-siteDat")))
          dataToCombine <<- c(dataToCombine,make.names(paste0(regionName,siteName,propertyName)))
          rm(siteMetaDat)
          rm(siteDat)
          return(2)#        break
        }else{
          file.remove(fname)
          return(1);#next
        }
      }
    }
  }
}


if(0){
  '
  Auckland
  BayOfPlenty
  Canterbury           exclusive URL   add ashley enterococci
  Gisborne             metadata URL
  HawkesBay            metadata URL
  Manawatu(Horizons)   exclusive URL
  Marlborough          metadata URL
  Nelson              "The Nelson LAWA data.hts file only includes routine samples" PFisher 5/10/18
  Northland            metadata URL
  Southland            "All samples provided are routine". LHayward 9/10/18
  Taranaki             metadata URL
  Wellington           metadata URL
  WestCoast            exclusive URL
  Tasman               replacement file
  Waikato              replacement file
  Otago                replacement file'
}


# ssm = read.csv('h:/ericg/16666LAWA/LAWA2021/CanISwimHere/MetaData/SwimSiteMonitoringResults-2021-08-01.csv',stringsAsFactors = F)
# ssm = readxl::read_xlsx('h:/ericg/16666LAWA/LAWA2021/CanISwimHere/MetaData/SwimSiteMonitoringResults-2021-10-06.xlsx',sheet=1)
# ssm = readxl::read_xlsx('c:/users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021/CanISwimhere/Daily swim results/SwimSiteMonitoringResults-2021-11-03.xlsx',sheet=1)
ssm = readxl::read_xlsx('c:/users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2020/CanISwimhere/Daily swim results/SwimSiteMonitoringResults-2020-11-09.xlsx',sheet=1)

ssm$callID =  NA
ssm$callID[!is.na(ssm$TimeseriesUrl)] <- c(unlist(sapply(X = ssm%>%select(TimeseriesUrl),
                                          FUN = function(x)unlist(strsplit(x,split='&')))))%>%
  grep('featureofinterest',x = .,ignore.case=T,value=T)%>%
  gsub('featureofinterest=','',x=.,ignore.case = T)%>%
  sapply(.,URLdecode)%>%trimws

RegionTable=data.frame(ssm=unique(ssm$Region),
                       wfs=c("bay of plenty","canterbury","gisborne","hawkes bay",
                             "horizons","marlborough","nelson","northland",
                             "otago","southland","taranaki","tasman",
                             "waikato","wellington","west coast"))


#Download data to XML files ####
library(xml2)
agencyURLtable=data.frame(region="",server="",property='',feature='')
library(parallel)
library(doParallel)
startTime=Sys.time()
workers <- makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
  dataToCombine=NULL
  library(magrittr)
  library(doBy)
  library(plyr)
  library(dplyr)
  library(stringr)
  library(xml2)
})
foreach(SSMregion = unique(ssm$Region),.combine=rbind,.errorhandling="stop")%dopar%{
  regionName=make.names(word(SSMregion,1,1))
  # WFSregion=RegionTable$wfs[RegionTable$ssm==SSMregion]
  cat('\n\n\n',SSMregion,'\n')
  #Find agency URLs

  agURLs <-     sapply(X = ssm$TimeseriesUrl[ssm$Region==SSMregion],
                       FUN = function(x)unlist(strsplit(x,split='&')))%>%
    unlist%>%
    grep('http',x = .,ignore.case=T,value=T)%>%
    table%>%
    sort(decr=T)%>%names
  
  agURLs = unique(gsub(pattern = 'Service','service',agURLs))
  agURLs%>%paste(collapse='\t')%>%cat
  if(any(!grepl('^http',agURLs))){
    agURLs = agURLs[grepl('^http',agURLs)]
  }
  if(regionName=="Bay"){
    agURLs = paste0(agURLs,'&version=2.0.0')
  }
  if(regionName=="Manawatu.Whanganui"){
    agURLs = gsub(pattern = 'hilltopserver',replacement = 'maps',x = agURLs)
  }
  # if(regionName=='Canterbury'){
  #   agURLs = 'http://wateruse.ecan.govt.nz/WQRecLawa.hts?Service=Hilltop'
  # }
  # if(regionName=="West"){
  #   agURLs = gsub(pattern = 'data.wcrc.govt.nz:9083',replacement = 'hilltop.wcrc.govt.nz',x = agURLs)
  # }
  # if(regionName=="Waikato"){
  #   agURLs = paste0(agURLs,'&service=SOS')
  # }
  if(regionName=="Waikato"){
    agURLs = gsub(pattern="datasource=0$",replacement = "datasource=0&service=SOS&version=2.0.0&procedure=CBACTO.Sample.Results.P",agURLs)
  }
  # if(regionName=="Hawke's"){
  #   agURLs = gsub(pattern="EMAR.hts",replacement = "Recreational_WQ.hts",agURLs)%>%unique
  # }
  if(regionName=='Taranaki'){
    # https://extranet.trc.govt.nz/getdata/LAWA_rec_WQ.hts?Service=Hilltop&Request=GetData&Site=SEA901033&Measurement=ECOL&From=1/11/2015&To=1/4/2016
  }
  
  #Find agency properties from the SSM file
  observedProperty <- sapply(X = ssm%>%filter(Region==SSMregion,TimeseriesUrl!='')%>%select(TimeseriesUrl),
                             FUN = function(x){gsub('(.+)http.+','\\1',x=x)%>%
                                 strsplit(split='&')%>%unlist%>%
                                 grep('observedproperty',x = .,ignore.case=T,value=T)%>%
                                 gsub('observedproperty=','',x=.,ignore.case = T)})%>%
    sapply(.,URLdecode)%>%unname#%>%tolower
  propertyType = ssm%>%
    filter(Region==SSMregion & TimeseriesUrl!='')%>%
    select(Property)%>%
    unlist%>%
    unname
  stopifnot(length(observedProperty)==length(propertyType))
  agProps=unique(data.frame(observedProperty,propertyType,stringsAsFactors=F))
  agProps=rbind(agProps,c('WQ sample','WQ sample'))
  rm(observedProperty,propertyType)
  
  if(lubridate::year(Sys.time())<2021){
    # #Find agency Sites from the WFS reportage and add SSM-sourced sites if necessary
    # agSites = WFSsiteTable%>%
    #   dplyr::filter(Region==WFSregion)%>%dplyr::select(CouncilSiteID)%>%
    #   unlist%>%trimws%>%unique%>%sort
    # if(addSSMsites){
    #   agSites <- c(agSites,
    #                c(unlist(sapply(X = ssm%>%filter(Region==SSMregion)%>%select(TimeseriesUrl),FUN = function(x)unlist(strsplit(x,split='&')))))%>%
    #                  grep('featureofinterest',x = .,ignore.case=T,value=T)%>%gsub('featureofinterest=','',x=.,ignore.case = T)%>%sapply(.,URLdecode)%>%trimws%>%tolower%>%unique)
    # }
  }else{
    #Nothing from WFS, just from the SSM from Effect
    agSites <- c(unlist(sapply(X = ssm%>%filter(Region==SSMregion)%>%select(TimeseriesUrl),
                               FUN = function(x)unlist(strsplit(x,split='&')))))%>%
      grep('featureofinterest',x = .,ignore.case=T,value=T)%>%
      gsub('featureofinterest=','',x=.,ignore.case = T)%>%
      sapply(.,URLdecode)%>%trimws%>%unique
    
    if(0){
      siteComp=data.frame(siteName=ssm$SiteName[ssm$Region==SSMregion],
                          featureofInt=c(unlist(sapply(X = ssm%>%filter(Region==SSMregion)%>%select(TimeseriesUrl),
                                                       FUN = function(x)unlist(strsplit(x,split='&')))))%>%
                            grep('featureofinterest',x = .,ignore.case=T,value=T)%>%
                            gsub('featureofinterest=','',x=.,ignore.case = T)%>%
                            sapply(.,URLdecode)%>%trimws)
      rownames(siteComp) <- NULL
      rm(siteComp)
    }
    }
  
    #Find agency format keys
  (agFormats <- sort(unique(c(unlist(sapply(X = ssm$TimeseriesUrl[ssm$Region==SSMregion],
                                            FUN = function(x)unlist(strsplit(x,split='&')))))))%>%
      grep('format',x = .,ignore.case=T,value=T))%>%cat
  
  #Load each site/property combo, by trying each known URL for that council.  Already-existing files will not be reloaded
  cat('\n',regionName,'\n')
  cat(length(agSites)*dim(agProps)[1]*length(agURLs),'\n')
  for(agSite in seq_along(agSites)){
    siteName=make.names(agSites[agSite])
    for(ap in seq_along(agProps$observedProperty)){
      propertyName=make.names(tolower(agProps$observedProperty[ap]))
      
      fname = paste0('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/',regionName,siteName,propertyName,'.xml')
      if(file.exists(fname)&&file.info(fname)$size>2000){
        next
      }else{
        for(ur in seq_along(agURLs)){
          if(propertyName=="WQ.sample"|SSMregion=='Taranaki region'){
            urlToTry = paste0(gsub('SOS','Hilltop',agURLs[ur]),
                              '&request=GetData',
                              '&Site=',agSites[agSite],
                              '&Measurement=',agProps$observedProperty[ap],
                              '&From=1/6/2010&To=1/7/2021')
          }else{
            urlToTry = paste0(agURLs[ur],
                              '&request=GetObservation',
                              '&featureOfInterest=',agSites[agSite],
                              '&observedProperty=',agProps$observedProperty[ap],
                              '&temporalfilter=om:phenomenonTime,P10Y/2021-06-01')
          }
         
          if(length(agFormats)==1){
            urlToTry = paste0(urlToTry,'&',agFormats[1])
          }
          urlToTry=URLencode(urlToTry)
          dlTry=try(curl::curl_fetch_disk(urlToTry,path = fname),silent=T)
          if('try-error'%in%attr(x = dlTry,which = 'class')){
            file.remove(fname)
            cat('-')
            rm(dlTry)
            next
          }
          if(dlTry$status_code%in%c(400,500,501,503,404)){
            file.remove(fname)
            cat('.')
            rm(dlTry)
            next
          }
          if(dlTry$status_code==408){
            file.remove(fname)
            cat(agURLS[ur],'TimeOut\n')
            rm(dlTry)
            next
          }
          rm(dlTry)
          response=checkXMLFile(regionName,siteName,propertyName)
          if(response==1){next}  #try the other URL
          if(response==2){
            if(file.info(fname)$size<2000){
              file.remove(fname)
            }else{
              break #got a data
              }
            }
          rm(response)
        }
        rm(ur)
      }
    }
    rm(ap)
  }
  rm(agSite)
  rm(regionName)
  return(NULL)
}
stopCluster(workers)
Sys.time()-startTime  #30mins 2/7/2021
rm(startTime)
rm(workers)
rm(SSMregion)
if(0){
#Investigate the yield of metadata files ####
loadedFiles = dir('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/')
grep(pattern = 'WQ\\.sample',x = loadedFiles,ignore.case = F,value = T)->WQSf
sapply(make.names(word(unique(ssm$Region),1,1)),FUN=function(s)sum(grepl(pattern = paste0('^',s),x = loadedFiles,ignore.case = T)))->Dfcount
sapply(make.names(word(unique(ssm$Region),1,1)),FUN=function(s)sum(grepl(pattern = paste0('^',s),x = WQSf,ignore.case = T)))->WQfcount

siteCount=ssm%>%group_by(Region=make.names(word(Region,1,1)))%>%dplyr::summarise(nSites = length(unique(LawaId)))
siteCount$nWQf = WQfcount[match(siteCount$Region,names(WQfcount))]
siteCount$nDf = Dfcount[match(siteCount$Region,names(Dfcount))]
rm(siteCount,WQfcount,Dfcount,loadedFiles)
}





#Load XML files and combine ####
agencyURLtable=data.frame(region="",server="",property='',feature='')
library(parallel)
library(doParallel)
workers <- makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
  dataToCombine=NULL
  library(magrittr)
  library(doBy)
  library(plyr)
  library(dplyr)
  library(stringr)
  library(xml2)
  library(XML)
})
startTime=Sys.time()
  foreach(SSMregion = unique(ssm$Region))%dopar%{
    if(exists('recData')){rm(recData)}
    if(exists('recMetaData')){rm(recMetaData)}
    dataToCombine=NULL
    metaToCombine=NULL
    regionName=make.names(word(SSMregion,1,1))
  WFSregion=RegionTable$wfs[RegionTable$ssm==SSMregion]
  cat('\n',regionName,'\t')
  
  #Find agency properties from the SSM file
  observedProperty <- sapply(X = ssm%>%filter(Region==SSMregion,TimeseriesUrl!='')%>%select(TimeseriesUrl),
                             FUN = function(x){gsub('(.+)http.+','\\1',x=x)%>%
                                 strsplit(split='&')%>%unlist%>%
                                 grep('observedproperty',x = .,ignore.case=T,value=T)%>%
                                 gsub('observedproperty=','',x=.,ignore.case = T)})%>%
                      sapply(.,URLdecode)%>%unname%>%tolower
  propertyType = ssm%>%
    filter(Region==SSMregion&TimeseriesUrl!='')%>%
    select(Property)%>%
    unlist%>%
    unname
  stopifnot(length(observedProperty)==length(propertyType))
  agProps=unique(data.frame(observedProperty,propertyType,stringsAsFactors=F))
  agProps=rbind(agProps,c('WQ sample','WQ sample'))
  rm(observedProperty,propertyType)
  
  #Find agency Sites from the  SSM
  agSites <- c(unlist(sapply(X = ssm%>%filter(Region==SSMregion)%>%select(TimeseriesUrl),
                             FUN = function(x)unlist(strsplit(x,split='&')))))%>%
    grep('featureofinterest',x = .,ignore.case=T,value=T)%>%
    gsub('featureofinterest=','',x=.,ignore.case = T)%>%
    sapply(.,URLdecode)%>%trimws%>%unique
  
  #Step through sites and properties, loading XML files to memory for subsequent combination
  for(agSite in seq_along(agSites)){
    cat('.')
    siteName=make.names(agSites[agSite])
    for(ap in seq_along(agProps$observedProperty)){
      property=agProps$propertyType[ap]
      propertyName=make.names(agProps$observedProperty[ap])
      if(file.exists(paste0('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/',regionName,siteName,propertyName,'.xml'))&
         file.info(paste0('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/',regionName,siteName,propertyName,'.xml'))$size>2000){
        readXMLFile(regionName,siteName,propertyName,property) #returns 1 or 2, and builds 'dataToCombine'
      }
    }
  }
  rm(agProps,agSites)
  
  #Combine region's metadata
  mdlist <- lapply(grep(regionName,metaToCombine,ignore.case = T,value=T),get)
  suppressMessages({recMetaData<<-Reduce(function(x,y) dplyr::full_join(x,y),mdlist)})
  if(!is.null(recMetaData)){
    save(recMetaData,file=paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/",format(Sys.Date(),'%Y-%m-%d'),
                               "/",SSMregion,"metaData.Rdata"))
  }
  rm(list=metaToCombine)
  rm(recMetaData,mdlist)

  # combine region's data
  dlist <- lapply(grep(regionName,dataToCombine,ignore.case=T,value=T),get)
  suppressMessages({recData<<-Reduce(function(x,y) dplyr::full_join(x,y),dlist)})
  if(!is.null(recData)){
    save(recData,file=paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/",format(Sys.Date(),'%Y-%m-%d'),
                               "/",SSMregion,"Data.Rdata"))
  }
  rm(list=dataToCombine)
  rm(recData,dlist,regionName)
}
stopCluster(workers)
rm(workers)

Sys.time()-startTime  #3.35 mins 6/10/2020
                      #1.02 mins 16/10/20
                      #1.03 3/11/2020
                      #38 s 2/7/2021
rm(startTime)





#Combine each region to a nationwide ####
#First load each regions same-named data, and rename to a region-specific
for(SSMregion in unique(ssm$Region)){
  suppressWarnings(rm(recMetaData,recData,mdFile,dFile))
  mdFile=tail(dir(path="h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/",
                  pattern=paste0(SSMregion,"metaData.Rdata"),recursive = T,full.names = T),1)
  if(length(mdFile)==1)load(mdFile,verbose=T)
  
  
  dFile=tail(dir(path="h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/",
                 pattern=paste0(SSMregion,"Data.Rdata"),recursive = T,full.names = T),1)
  if(length(dFile)==1)load(dFile,verbose=T)
  
  if(exists('recData')){
    recData <- recData%>%filter(dateCollected>Sys.time()-lubridate::years(7))
    eval(parse(text=paste0(make.names(word(SSMregion,1,1)),"D=recData")))
  }
  if(exists('recMetaData')){
    recMetaData <- recMetaData%>%filter(SampleDate>Sys.time()-lubridate::years(7))
    recMetaData <- recMetaData%>%filter(!apply(recMetaData[,-c(1,2,3,4)],1,FUN=function(r)all(is.na(r))))
    
    recMetaData$dateCollected = as.POSIXct(lubridate::ymd_hms(recMetaData$SampleDate,tz = 'Pacific/Auckland'))
    eval(parse(text=paste0(make.names(word(SSMregion,1,1)),"MD=recMetaData")))
    # browser()
  }
  
  if(exists('recMetaData'))rm(recMetaData)
  if(exists('recData'))rm(recData)
  rm(mdFile)
  rm(dFile)
}

#Filter BoP data by the file that MaxMcKay sent through
BayD <- BayD%>%filter(propertyName!='E.coli_LabResult')   #15/10/2020 in respone to observation of repeats introduced by presence
                                                        #of one labresult propertytype from umbraco
BayMD = readxl::read_xlsx("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/BOPRC Recreational Repeats List - LAWA.xlsx",sheet=1)%>%
  filter(Parameter%in%c("E coli","Enterococci"))%>%mutate(Parameter=ifelse(Parameter=="E coli","E-coli",Parameter))
lubridate::tz(BayMD$Time)<-'Pacific/Auckland' #to specify what time zone the time was in
BayMD$dateCollected = BayMD$Time - lubridate::hours(12)
BayMD$regionName="Bay"
BayMD <-BayMD%>%dplyr::rename(property=Parameter,siteName=LocationName)

BayD = left_join(x=BayD,y=BayMD%>%select(-siteName,-Unit,-LawaID,-Value),
                  by=c("siteName"="Site","property","dateCollected"))%>%
  filter(is.na(Qualifiers))%>% #This excludes "Recreational repeats"
  select(-Qualifiers,-Time)
rm(BayMD)
tz(BayD$dateCollected) <- ""



CanterburyMD <- CanterburyMD%>%select(-Rain,-`Rain Previously`,-starts_with('Number of'),-`Birds on Beach and/or Water`,-`Wave height (m)`,
                                      -starts_with('Foam '),-`Dead Marine Life`,-Seaweed,-`Site is dry`,-`Site is Dry`,-starts_with('High '),
                                      -starts_with('Visual'),-`Water Colour Marine`,-`Water Clarity`,-`Water Colour`,-`Sent to Lab`,-`Wind Speed Average (m/s)`,
                                      -`Wind Direction`,-`Wind Strength`,-`Bed is Visible`,-starts_with('YSI'),-`Meter Number`,
                                      -`Cloud Cover`,-`Auto Archived`,-Archived,-`Cost Code`)
SouthlandMD <- SouthlandMD%>%select(-Weather,-Tide,-`Digital Photo`,-`Lake Conditions`,-`Wind Speed`,-`Entered By`,-`Checked By`,
                                    -`Method of Collection`,-Odour,-FieldFiltered,-starts_with('Isotope'),-`Checked Time`,-`Entered Time`,
                                    -`Meter Number`,-`Black Disc Size`,-Clarity,-`Wind Direction`,-`Flow Value`,-`Water Level`,-`Field Technician`,
                                    -`Collected By`,-Archived,-`Cost Code`)
OtagoMD <- OtagoMD%>%select(-`Source of Water`,-Observer,-`Field Technician`,-Odour,-Weather,-Colour,-Clarity,-`Wind Speed`,-`Wind Direction`,
                            -`Flow Rate`,-Tide,-`Tide Direction`,-`Grid Reference`,-`Collection Method`,-`Samples from`,-Archived,-`Cost Code`)
NorthlandMD <- NorthlandMD%>%select(-`Field Technician`,-`Run Number`,-`Received at Lab`,-`Wind Strength`,-`Wind Direction`,-`Meter number`,
                                    -Rainfall,-`Weather Affected`,-`Tidal State`,-`Vehicles on beach`,-`Cloud Cover`,-`Cloud cover`,
                                    -Tide,-Weather,-`Photo taken`,-`Cyanobacteria warning sign`,-Archived,-`Cost Centre`)
Manawatu.WhanganuiMD <- Manawatu.WhanganuiMD%>%select(-`Source Type`,-`Input By`,-`Sampling point`,-Fieldsheet,-`Sampling Method`,
                                                      -`Observed Colour`,-SampledBy,-Weather,-MeterID,-`Observed Clarity`,-`Observed Velocity`,
                                                      -`Observed Flow`,-`Compliance Prosecution`,-`Data Audited`,-`Cyanobacteria Detached`,
                                                      -`Cyanobacteria Exposed`,-`Cyanobacterial Cover`,-Archived,-`Cost Code`)
WellingtonMD <- WellingtonMD%>%select(-`Detached Cyanobacteria Mats`,-`Rubbish amount`,-Rainfall,-Weather,-`Sewage Overflow`,
                                      -`Wind Direction`,-`Wind strength`,-`GWRC Programme`,-`Seaweed %`,-Tide,-`Tidal height`)
MarlboroughMD <- MarlboroughMD%>%select(-`Climate Field Meter Used`,-Weather,-Water,-`Tide Flow`,-`Sea State`,-`Field Technician`,
                                        -`Cloud Cover`,-`Meter Used`,-`Auto Archived`,-Archived,-`Cost Code`)
Hawke.sMD <- Hawke.sMD%>%select(-`Puddle verified`,-`Puddle verified by`,-`Puddle verified date`,-`Sampling Issue`,-Observer)
GisborneMD <- GisborneMD%>%select(-`Sample ID`,-Archived,-Lab,-EnteredBy,-`Field Technician`,-`Auto Archived`,-Tide,-`Received at Lab`,-`Lab Batch ID`,
                                  -`Lab Batch Id`,-`Lab Sample ID`,-QAAgency,-CollectionMethod,-QAMethod,-`Cost Code`)
NelsonMD <- NelsonMD%>%select(-Archived)
TaranakiMD$SampleDate = as.character(TaranakiMD$SampleDate)

#Then combine all region-specifics to a nationwide overall 
MDlist <- sapply(ls(pattern='MD$'),get,simplify = FALSE)
MDlist[sapply(MDlist,is.null)] <- NULL
sapply(MDlist,names)
recMetaData=Reduce(function(x,y) dplyr::full_join(x,y%>%distinct),MDlist)
print(dim(recMetaData))
save(recMetaData,file = paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/",format(Sys.Date(),'%Y-%m-%d'),
                           "/RecMetaData",format(Sys.time(),'%Y-%b-%d'),".Rdata"))
rm(list=ls(pattern='MD$'))
rm(MDlist)

Dlist <- sapply(ls(pattern='[^M]D$'),FUN=function(s)get(s),simplify=FALSE)
Dlist[sapply(Dlist,is.null)] <- NULL
sapply(Dlist,names)
recData=Reduce(function(x,y) dplyr::full_join(x,y%>%distinct),Dlist)
print(dim(recData))
save(recData,file = paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/",format(Sys.Date(),'%Y-%m-%d'),
                               "/RecData",format(Sys.time(),'%Y-%b-%d'),".Rdata"))
rm(list=ls(pattern='D$'))
rm(Dlist)





#Simply reload existing dataset ####

if(0){
 load(tail(dir(path="h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data",pattern="RecData2021",recursive = T,full.names = T,ignore.case=T),1),verbose=T)  
  recData <- recData%>%filter(property!="WQ sample")
 load(tail(dir(path="h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data",pattern="RecMetaData2021",recursive = T,full.names = T,ignore.case=T),1),verbose=T)  
 }


if(0){
  #8/10/2020 survey where councils have hteir resample metadata ####
  #The oens that said it was in metadata
  for(SSMregion in c("Gisborne region","Hawke's Bay region","Marlborough region","Northland region","Taranaki region","Wellington region")){
    load(tail(dir(path="h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/",pattern=paste0(SSMregion,"metaData.Rdata"),recursive=T,full.names = T),1),verbose=T)
    if(!is.null(recMetaData)){
      eval(parse(text=paste0(make.names(word(SSMregion,1,1)),"MD=recMetaData")))
    }
    rm(recMetaData)
  } 
  
  '2019 info
Auckland
BayOfPlenty
Canterbury           exclusive URL   add ashley enterococci
Gisborne             metadata URL
HawkesBay            metadata URL
Manawatu(Horizons)   exclusive URL
Marlborough          metadata URL
Nelson              "The Nelson LAWA data.hts file only includes routine samples" PFisher 5/10/18
Northland            metadata URL
Southland            "All samples provided are routine". LHayward 9/10/18
Taranaki             metadata URL
Wellington           metadata URL
WestCoast            all data at the supplied URL is routine (because retests are never done!!)
Tasman               replacement file
Waikato              replacement file
Otago                replacement file'
  
  apply(GisborneMD,2,function(x)any(grepl('re-sampl|resampl|follow[^ing]',x,ignore.case=T)))%>%which  #SampleEventType, Comments
  apply(Hawke.sMD,2,function(x)any(grepl('re-sampl|resampl|follow[^ing]',x,ignore.case=T)))%>%which   #Puddle Comment
  apply(MarlboroughMD,2,function(x)any(grepl('re-sampl|resampl|follow[^ing]',x,ignore.case=T)))%>%which #Project, Other Comments
  apply(NorthlandMD,2,function(x)any(grepl('re-sampl|resampl|follow[^ing]',x,ignore.case=T)))%>%which  #Run type, comment
  apply(Taranaki,2,function(x)any(grepl('re-sampl|resampl|follow[^ing]',x,ignore.case=T)))%>%which
  apply(WellingtonMD,2,function(x)any(grepl('re-sampl|resampl|follow[^ing]',x,ignore.case=T)))%>%which  #Sample Type, comment/
  
  
  
  #Which metadata columns contain "retest" or "routine" or "resample"
  apply(recMetaData,2,function(x)any(grepl('re-sampl|resampl|follow[^ing]',x,ignore.case=T)))%>%which
  # Project        Analysts Comments       Sample Type    Sample Comment       Cloud Cover   SampleEventType          Comments 
  # Puddle Comment    Other Comments          Run Type           Comment          Resample       Sample type 
  
  unique(recMetaData$Resample)
  grep('re-sampl|resampl|follow[^ing]',recMetaData$`Sample Comment`,ignore.case = T,value = T)%>%unique
  
  #End investigation
}


routineResample = recMetaData%>%select(regionName,siteName,dateCollected,
                                       # Project, `Project ID`, `Analysts Comments`,
                                       `Sample Type`
                                       # `Sample Comment`, SampleEventType, Comments,`Other Comments`,`Run Type`,Comment,
                                       # `Sample type`
                                       )#%>%unique
routineResample$region = recData$region[match(routineResample$regionName,make.names(word(recData$region,1,1)))]

routineResample$resample = apply(routineResample[,-c(1,2,3)],1,
                                 FUN=function(s)any(grepl('re-sampl|resampl|follow[^ing|ed]|BB-Ex',s,ignore.case = T)))
manualFix = apply(routineResample[,-c(1,2,3,dim(routineResample)[2])],1,
                  FUN=function(r)any(grepl('follow up not needed',r,ignore.case=T)))
routineResample$resample[manualFix]=F

#Which column gave the followup info?
clueSource = apply(routineResample[routineResample$resample,-c(1,2,3)],1,
                   FUN=function(s)paste0(names(routineResample)[3+grep('re-sampl|resampl|follow[^ing|ed]|BB-Ex',s,ignore.case = T)],collapse='&'))
#And what was the followup info given by that column?
clue = apply(routineResample[routineResample$resample,-c(1,2,3)],1,
                                FUN=function(s)paste0(grep('re-sampl|resampl|follow[^ing|ed]|BB-Ex',s,ignore.case = T,value = T),collapse='&'))
routineResample$clue = ""
routineResample$clue[routineResample$resample] <- paste0(clueSource,": ",clue)
rm(clueSource,clue)
rm(manualFix)

routineResample <- routineResample%>%distinct



unique(recData$region)[unique(make.names(word(recData$region,1,1)))%in%unique(routineResample$regionName[routineResample$resample])]
#8 councils have followup clues.  This leaves the following councils unIDed.
unique(recData$region)[!unique(make.names(word(recData$region,1,1))) %in% unique(routineResample$regionName[routineResample$resample])] -> knownMissingRegions
"Bay of Plenty region   Manawatū-Whanganui region     Nelson region    Southland region    Tasman region     Waikato region      West Coast region" 
#BoP Lisa Naysmith emailed 8/10/2020              Max McKay responded with an xl file now in h:/ericg/16666Lawa/LAWA2021/CISH/Data/BOPRC 
#Manawatu whanganui                               Amber Garnett "Followups are already removed from our dataset"
#Nelson have confirmed they have no idea what a routine vs a resample
#Southland
#Tasman emailed anette.becher 8/10/2020           twice-weekly routine, so no followups
#Waikato  Paul K etc                              The data I access through their KiWIS contains only routine samples
#West Coast  emailed Millie Taylor 8/10/2020      she says they don't do followups!


recData <- merge(all.x=T,x=recData,
                  all.y=F,y=routineResample%>%select(region,siteName,dateCollected,resample,clue)%>%distinct,
                  by=c('region','siteName','dateCollected'))
recData$resample[recData$region%in%knownMissingRegions] <- FALSE

rm(routineResample)
table(recData$region,recData$resample,useNA = 'if')
#                           FALSE  TRUE  <NA>
#   Bay of Plenty region     8473     0     0
# Canterbury region          2138   268   3164
# Gisborne region            3204    36   362
# Hawke's Bay region         3576   234   1379
# Manawatū-Whanganui region  9038     0   0
# Marlborough region         1710   194   726
# Nelson region              1832     0     0
# Northland region           4081   167  1429
# Otago region               1468    10  1847
# Southland region           2478     0     0
# Taranaki region            5229   189     0
# Tasman region              1097     0     0
# Waikato region             2074     0     0
# Wellington region          9103    25  4149
# West Coast region          1444     0     0


sum(recData$resample,na.rm=T)  #13/10/2020  799
                               #15/10/2020  1063
                               #16/10/2020 925
                               #20/10/2020 913
                               #28/10/2020 913
                               #29/10/2020 1123
                               #03/11/2020 1123
                               #19/11/2020 1123
                               #02/07/2021  221
recData$clue[is.na(recData$resample)] <- ""
recData$clue[!recData$resample] <- ""

recData$LawaSiteID = ssm$LawaId[match(tolower(as.character(recData$siteName)),
                                      tolower(make.names(ssm$callID)))]
recData$siteType = ssm$SiteType[match(tolower(recData$LawaSiteID),tolower(ssm$LawaId))]
recData$SiteID = ssm$SiteName[match(recData$siteName,make.names(ssm$callID))]
recData$SiteID = ssm$SiteName[match(recData$siteName,make.names(ssm$callID))]

table(recData$region,recData$resample)

#                         FALSE  TRUE
# Bay of Plenty region    8473     0
# Canterbury region       23186    0
# Gisborne region         9039     0
# Hawke's Bay region      9867     0
#   Marlborough region    5377     0
#   Nelson region         2087     0
#   Otago region          6210     0
#   Southland region      4974     0
#   Taranaki              5112   221
#   Tasman region         1082     0
#   Wellington region    24704     0
#   West Coast region     1514     0


# Diatnostics on merge
if(0){
mmRD = recData[!recData$siteName%in%recMetaData$siteName,]
mmRD <- mmRD[!mmRD$region%in%knownMissingRegions,]
table(mmRD$region) #This is the regions where site names are in the data but not in rec data, excluding the known-missing regions
unique(mmRD$siteName)
rm(mmRD)

#Failed site lodas
table(unique(tolower(make.names(ssm$callID)))%in%tolower(make.names(recData$siteName)))

failedSites = ssm[!tolower(make.names(ssm$callID))%in%tolower(make.names(recData$siteName)),]

recData%>%dplyr::group_by(region)%>%dplyr::summarise(nSite=length(unique(siteName)))%>%ungroup

#End diagnostic
}


#Deal with censored data
#Anna Madarasz-Smith 1/10/2018
#Hey guys,
# We (HBRC) have typically used the value with the >reference 
# (eg >6400 becomes 6400), and half the detection of the less than values (e.g. <1 becomes 0.5). 
# I think realistically you can treat these any way as long as its consistent beccuase 
# it shouldn?t matter to your 95th%ile.  
# These values should be in the highest and lowest of the range.
# Hope this helps Cheers
# Anna
recData$val=as.numeric(recData$val)
recData$fVal=recData$val
recData$fVal[recData$lCens]=recData$val[recData$lCens]/2
recData$fVal[recData$rCens]=recData$val[recData$rCens]
table(recData$lCens)
table(recData$rCens)


#Make all yearweeks six characters
recData$week = lubridate::week(recData$dateCollected)
recData$week=as.character(recData$week)
recData$week[nchar(recData$week)==1]=paste0('0',recData$week[nchar(recData$week)==1])

recData$year = lubridate::year(recData$dateCollected)

recData$YW=as.numeric(paste0(recData$year,recData$week))

#Create bathing seasons
bs=strTo(recData$dateCollected,'-')
bs[recData$month>6]=paste0(recData$year[recData$month>6],'/',recData$year[recData$month>6]+1)
bs[recData$month<7]=paste0(recData$year[recData$month<7]-1,'/',recData$year[recData$month<7])
recData$bathingSeason=bs
table(recData$bathingSeason)


#Write individual regional files: data from recData prior to removing followups 
uReg=unique(recData$region)
for(reg in seq_along(uReg)){
  toExport=recData%>%filter(region==uReg[reg],dateCollected>(Sys.time()-lubridate::years(5)))
  write.csv(toExport,file=paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                                 "/recData_",
                                 gsub(' ','',gsub(pattern = ' region',replacement = '',x = uReg[reg])),
                                 "_unfiltered.csv"),row.names = F)
}
rm(reg,uReg,toExport)


downloadData = recData%>%filter(YW>201525)%>%
  dplyr::filter(LawaSiteID!='')%>%drop_na(LawaSiteID)%>%
  dplyr::filter((yday(dateCollected)>297|month<4))%>% #bathign season months only plus the last week of October. To catch labour weeknd.
  mutate(bathingSeason = factor(bathingSeason))%>%
  select(region,siteName,SiteID,LawaSiteID,siteType,property,dateCollected,resample,value=val)
downloadData$resample[is.na(downloadData$resample)] <- FALSE

downloadData$SwimIcon = "NA"
downloadData$SwimIcon[downloadData$property=="E-coli"] <- 
  as.character(cut(downloadData$value[downloadData$property=="E-coli"],
      breaks = c(0,260,550,Inf),
      labels = c('green','amber','red')))
downloadData$SwimIcon[downloadData$property=="Enterococci"] <- 
  as.character(cut(downloadData$value[downloadData$property=="Enterococci"],
      breaks = c(0,140,280,Inf),
      labels = c('green','amber','red')))
downloadData$SwimIcon[downloadData$property=='Cyanobacteria'] <- 
  as.character(factor(downloadData$value[downloadData$property=='Cyanobacteria'],
                      levels=c(0,1,2,3),
                      labels=c("No Data","green","amber","red")))
table(downloadData$SwimIcon,downloadData$property)

downloadData$siteType[downloadData$siteType=="Site"] <- "River"
downloadData$siteType[downloadData$siteType=="LakeSite"] <- "Lake"
downloadData$siteType[downloadData$siteType=="Beach"] <- "Coastal"

downloadData$Latitude = lmsl$Latitude[match(tolower(downloadData$LawaSiteID),tolower(lmsl$LAWAID))]
downloadData$Longitude = lmsl$Longitude[match(tolower(downloadData$LawaSiteID),tolower(lmsl$LAWAID))]

write.csv(downloadData%>%select(region:siteType,Latitude,Longitude,property:SwimIcon)%>%
            arrange(region,siteName,dateCollected),
          file = paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/",
                                  format(Sys.Date(),'%Y-%m-%d'),
                                  "/CISHdownloadWeekly",format(Sys.time(),'%Y-%b-%d'),".csv"),row.names = F) #"weekly" because that's the name of the sheet it'll go into



recData <- recData%>%filter(!resample|is.na(resample))%>%select(-resample,-clue)
#85483 to 84684 13/10/2020
#82521 to 81458 15/10/2020
#82629 to 81704 16/10/2020
#81395 to 80482 20-10/20
#78289 to 77376 28/10/20
#78288 to 77165 29/10/20
#78208 to 77085 3/11/20
#78151 to 77028 9/11/20
#101846 to 101625 2/7/2021

graphData <- recData%>%
   filter(YW>201525)%>%
  dplyr::filter(LawaSiteID!='')%>%drop_na(LawaSiteID)%>%
  dplyr::filter(property%in%c("E-coli","Enterococci"))%>%
  dplyr::filter((yday(dateCollected)>297|month<4))#%>% #bathign season months only plus the last week of October. To catch labour weeknd.
  # dplyr::group_by(LawaSiteID,YW,property)%>%
  # dplyr::arrange(YW)%>%                   
  # dplyr::summarise(dateCollected=first(dateCollected),
                   # region=unique(region),
                   # n=length(fVal),            #Count number of weeks recorded per season
                   # # fVal=first(fVal),
                   # bathingSeason=unique(bathingSeason))%>%
  # ungroup->graphData
#45239  15/10/20
#45465 16/10/2020
#45577 20/10/20
#50452 28/10/20
#50277 29/10/20
#50254 3/11/20
#50316 9/11/20
#47460 2/7/21
graphData$bathingSeason <- factor(graphData$bathingSeason)


# Calculate CISHSiteSummary ####
###############################
CISHsiteSummary <- graphData%>%
  select(-dateCollected)%>%
  group_by(LawaSiteID,property)%>%            #For each site
  dplyr::summarise(.groups = 'keep',
                   region=unique(region),
                   nBS=length(unique(bathingSeason)),
                   nPbs=paste(as.numeric(table(bathingSeason)),collapse=','),
                   uBS=paste(sort(unique(bathingSeason)),collapse=','),
                   totSampleCount=n(),
                   min=min(fVal,na.rm=T),
                   max=max(fVal,na.rm=T),
                   haz95=quantile(fVal,probs = 0.95,type = 5,na.rm = T),
                   haz50=quantile(fVal,probs = 0.5,type = 5,na.rm = T))%>%ungroup
#709 15/10/20
#708 16/10/2020
#709 20/10/20
#708 28/10/20
#708 29-10-20
#707 3*11*20
#708  9/11/20
#535 2/7/21

#https://www.mfe.govt.nz/sites/default/files/microbiological-quality-jun03.pdf    actually a new one shows a change from 550 to 540
#For enterococci, marine:                      E.coli, freshwater
#table D1 A is 95th %ile <= 40       table E1     <= 130
#         B is 95th %ile <41-200                  131 - 260
#         C is 95th %ile <201-500                 261 - 540
#         D is 95th %ile >500                       >540
# marineEnts=CISHsiteSummary$property=='Enterococci'

# NPSFM tables 9 and 22

# Table 9    p        q       r      s
# A Blue    <5%     <20%    <=130  <=540
# B Green   5-10%   20-30%  <=130  <=1000
# C Yellow  10-20%  30-34%  <=130  <=1200
# D Orange  20-30%  >34%    >130   >1200
# E Red     >30%    >50%    >260   >1200
# p = % exceedances over 540/100mL
# q = % exceedances over 260/100mL
# r = Median conc / 100 mL
# s = 95th% of E.coli/100 mL
# 
# Table 22
# Exc   <=130
# Good  >130 & <=260
# Fair  >260 & <=540
# Poor  >540

#### 17/Nov2020 NOF Definitions ####
library(parallel)
library(doParallel)
source("h:/ericg/16666LAWA/LAWA2021/WaterQuality/scripts/SWQ_NOF_Functions.R")
NOFbandDefinitions <- read.csv("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/NOFbandDefinitions3.csv", header = TRUE, stringsAsFactors=FALSE)
NOFbandDefinitions <- NOFbandDefinitions[,1:11]
uLAWAids = unique(graphData$LawaSiteID)

EndYear <- lubridate::year(Sys.Date())
StartYear5 <- EndYear - 5 + 1
# firstYear = min(wqdYear,na.rm=T)
firstYear=2005
yr <- paste0(as.character(StartYear5),'to',as.character(EndYear))
rollyrs=which(grepl('to',yr))
nonrollyrs=which(!grepl('to',yr))
reps <- length(yr)

if(exists("NOFSummaryTable")) { rm("NOFSummaryTable") }
startTime=Sys.time()
workers <- makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  # library(doBy)
  library(plyr)
  library(dplyr)
  source('H:/ericg/16666LAWA/LAWA2021/scripts/LAWAFunctions.R')
})
startTime=Sys.time()

foreach(i = 1:length(uLAWAids),.combine=rbind,.errorhandling="stop",.inorder=F)%dopar%{
  
# for(i in 1:length(uLAWAids)){
  Com_NOF <- data.frame (LawaSiteID               = rep(uLAWAids[i],reps),
                         Year                     = yr,
                         EcoliMed                 = as.numeric(rep(NA,reps)),
                         EcoliMed_Band            = rep(as.character(NA),reps),
                         Ecoli95                  = as.numeric(rep(NA,reps)),
                         Ecoli95_Band             = rep(as.character(NA),reps),
                         EcoliRecHealth540        = as.numeric(rep(NA,reps)),
                         EcoliRecHealth540_Band   = rep(as.character(NA),reps),
                         EcoliRecHealth260        = as.numeric(rep(NA,reps)),
                         EcoliRecHealth260_Band   = rep(as.character(NA),reps),
                         EcoliAnalysisNote        = rep('',reps),
                         stringsAsFactors = FALSE)
  
  suppressWarnings(rm(ecosite,rightSite,value,val)  )
  rightSite <- graphData%>%
    dplyr::filter(LawaSiteID==uLAWAids[i])%>%
    tidyr::drop_na(val)%>%
    mutate(Year=format(dateCollected,'%Y'))%>%mutate(Value=val)
  rightSite$YearQuarter=paste0(quarters(rightSite$dateCollected),year(rightSite$dateCollected))
  
  nBS=length(unique(rightSite$bathingSeason))
  nPbs=paste(as.numeric(table(rightSite$bathingSeason)),collapse=',')
  uBS=paste(sort(unique(rightSite$bathingSeason)),collapse=',')
  totSampleCount=sum(!is.na(rightSite$val))
  nInLatestBS = as.numeric(tail(unlist(strsplit(nPbs,',')),1))
  nInPreviousBS = as.numeric(head(tail(unlist(strsplit(nPbs,',')),2),1))
  notRecentlyMonitored = nInLatestBS==0 & nInPreviousBS==0
  notEnough50 = totSampleCount<50
  tooFew = any(c(notRecentlyMonitored,notEnough50)) #,notEnoughBS
  
  suppressWarnings(rm(cnEc_Band,cnEc95_Band,cnEcRecHealth540_Band,cnEcRecHealth260_Band,ecosite,ecosite))
  ecosite=rightSite%>%dplyr::filter(property=="E-coli")
  
  #E coli median and  95th percentile
  if(!tooFew){
    Com_NOF$EcoliMed = quantile(ecosite$val,prob=0.5,type=5,na.rm=T)
    Com_NOF$EcoliMed_Band=NOF_FindBand(Com_NOF$EcoliMed,bandColumn = NOFbandDefinitions$E..coli)
    Com_NOF$Ecoli95 = quantile(ecosite$val,prob=0.95,type=5,na.rm=T)
    Com_NOF$Ecoli95_Band=NOF_FindBand(Com_NOF$Ecoli95,bandColumn = NOFbandDefinitions$Ecoli95)
    ecv = ecosite$val[!is.na(ecosite$val)]
    Com_NOF$EcoliRecHealth540=sum(ecv>540)/length(ecv)*100
    Com_NOF$EcoliRecHealth260=sum(ecv>260)/length(ecv)*100
    
    Com_NOF$EcoliRecHealth540_Band <- NOF_FindBand(Com_NOF$EcoliRecHealth540,bandColumn=NOFbandDefinitions$EcoliRec540)
    Com_NOF$EcoliRecHealth260_Band <- NOF_FindBand(Com_NOF$EcoliRecHealth260,bandColumn=NOFbandDefinitions$EcoliRec260)
  }
  
  rm(ecosite)
  rm(rightSite) 
  return(Com_NOF)
}->NOFSummaryTable
stopCluster(workers)
rm(workers)
cat(Sys.time()-startTime)  

# 1.4s 2/7/21

# NOFSummaryTable$EcoliMed_Band <- sapply(NOFSummaryTable$EcoliMed,NOF_FindBand,bandColumn=NOFbandDefinitions$E..coli)

#These contain the best case out of these scorings, the worst of which contributes.
suppressWarnings(cnEc_Band <- sapply(NOFSummaryTable$EcoliMed_Band,FUN=function(x){
  min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEc95_Band <- sapply(NOFSummaryTable$Ecoli95_Band,FUN=function(x){
  min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEcRecHealth540_Band <- sapply(NOFSummaryTable$EcoliRecHealth540_Band,FUN=function(x){
  min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEcRecHealth260_Band <- sapply(NOFSummaryTable$EcoliRecHealth260_Band,FUN=function(x){
  min(unlist(strsplit(x,split = '')))}))  
NOFSummaryTable$EcoliSummaryband = as.character(apply(cbind(pmax(cnEc_Band,cnEc95_Band,cnEcRecHealth540_Band,cnEcRecHealth260_Band)),1,max))
rm("cnEc_Band","cnEc95_Band","cnEcRecHealth540_Band","cnEcRecHealth260_Band")

NOFSummaryTable$siteType = graphData$siteType[match(NOFSummaryTable$LawaSiteID,graphData$LawaSiteID)]

with(NOFSummaryTable%>%filter(Year=="2017to2021"),table(siteType,EcoliSummaryband,useNA='if')%>%addmargins())
#### \17/Nov/2020


CISHsiteSummary$MACmarineEnt=cut(x=CISHsiteSummary$haz95,breaks = c(-0.1,40,200,500,Inf),labels=c("A","B","C","D"))
CISHsiteSummary$MACmarineEnt[CISHsiteSummary$property!='Enterococci'] <- NA
CISHsiteSummary$NOFfwEcoli=cut(x=CISHsiteSummary$haz95,breaks = c(-0.1,130,260,540,Inf),labels=c("A","B","C","D"))
CISHsiteSummary$NOFfwEcoli[CISHsiteSummary$property!='E-coli'] <- NA

table(CISHsiteSummary$MACmarineEnt)
# A   B   C   D 
# 36 110  72  33    8/10/20
# 55 156 85 42      15/10/20
# 55 156 85 42      16/10/20
# 55 156 85 42      20/10/20
# 53 154 92 39      28/10/20
# 53 156 92 37      29
# 53 156 92 37      3/11
# 53 156 92 37
# 53 156 92 37    9/11
# 43 114 82 33     2/7/21
table(CISHsiteSummary$NOFfwEcoli)
# A   B   C   D 
# 24  27  33 176    8/10/20
# 52 34  63 222    15/10/20
# 52 32 67 219     16/10/20 
# 53 32 66 220      20/10/20
# 48 35 65 222     28/10/20
# 48 35 66 221    29
# 48 35 66 220    3/11/20
# 48 35 63 223
# 48 35 63 223   9/11
# 36 32 53 142     2/7/21

#NOF table 22
#Excellent                          <=130
#Good                               >130 <=260
#Fair                               >260 <=540
#Poor                               >540


#For LAWA bands in 2020
#50 needed over 5 seasons 
#Sites must be still recently monitored - if no data fro 19/20, 
#then must have been last monitored in 18/19. 
#Anythign older not considered
#              marine               fresh
#              enterococci          e.coli
#A              <=40                 <=130
#B             41 - 200              131<260
#C             201-500              261-540
#D                >500                 >540

CISHsiteSummary$LawaBand = ifelse(is.na(CISHsiteSummary$MACmarineEnt),
                                        as.character(CISHsiteSummary$NOFfwEcoli),
                                        as.character(CISHsiteSummary$MACmarineEnt))

table(CISHsiteSummary$LawaBand)
# A   B   C   D 
# 128  84  90 209   8/10/2020
# 107 190 148 264   15/10/20
# 107 188 152 261   16/10/20
# 108 188 151 262   20/10/20
# 101 189 157 261   28/10/20
# 101 191 158 257    3/11/20
# 101 191 155 260    4/11/20
# 101 191 155 260    9/11/20
# 79  146 135 175    2/7/21


#Calculate data abundance
#number per bathing season
#This is the old previous rules, about needing ten per season, and every season represented
if(lubridate::year(Sys.time())<2021){
#Email, Abi L 15/10/2019
#2)	Greater Wellington and minimum sample size
#   So as a starter for 10, can we propose alternative rule if the above isn?t met, that:
#   If in one year, there is less than 10 but at least 7 data points, 
#   then as long as the total number of data points over 3 years are at least 35, 
#   then these will be eligible for an overall bacto risk grade?  
#   This would ensure that the majority of sites in the spreadsheet attached would be eligible for a grade - 
#   but wanted to test with you that this is a statistically robust approach before we sign off on this.

# nPbs=do.call(rbind,lapply(CISHsiteSummary$nPbs,FUN = function(x)lapply(strsplit(x,split = ','),as.numeric))) #get the per-season counts
# tooFew = which(do.call('rbind',lapply(nPbs,function(x){
#   any(x<10)|length(x)<3
# })))  #see if any per-season counts are below ten, or there are fewer than three seasons
# tooFewNew = which(do.call('rbind',lapply(nPbs,function(x){
#   (any(x<10)&sum(x,na.rm=T)<35)|(length(x)<3)|any(x<7)
# }))) 
# 
# CISHsiteSummary$Region[tooFew[!tooFew%in%tooFewNew]]
# cbind(CISHsiteSummary$Region[tooFew[!tooFew%in%tooFewNew]],t(sapply((tooFew[!tooFew%in%tooFewNew]),FUN=function(x)nPbs[[x]])))
# 
# 
# 
# CISHsiteSummarySiteName = recData$SiteName[match(CISHsiteSummary$LawaId,recData$LawaId)]
# CISHsiteSummarySiteName[tooFew[!tooFew%in%tooFewNew]]
# 
# CISHsiteSummary$LawaBand[tooFewNew]=NA #set those data-poor sites' grade to NA
}else{
  #50 needed over 5 seasons 
  #Sites must be still recently monitored - if no data fro 19/20, 
  #then must have been last monitored in 18/19. 
  #Anythign older not considered
  nInLatestBS = unname(sapply(CISHsiteSummary$nPbs,FUN=function(s)as.numeric(tail(unlist(strsplit(s,',')),1))))
  nInPreviousBS = unname(sapply(CISHsiteSummary$nPbs,FUN=function(s)as.numeric(head(tail(unlist(strsplit(s,',')),2),1))))
  notRecentlyMonitored = which(nInLatestBS==0 & nInPreviousBS==0)
  notEnough50 = which(CISHsiteSummary$totSampleCount<50)
  # notEnoughBS = which(CISHsiteSummary$nBS<5)
  tooFew = unique(c(notRecentlyMonitored,notEnough50)) #  INDICES, NOT LOGICALS
  rm(nInLatestBS,nInPreviousBS,notRecentlyMonitored,notEnough50)
  table(CISHsiteSummary$LawaBand[-tooFew])
  table(CISHsiteSummary$LawaBand[tooFew])
  CISHsiteSummary$MACmarineEnt[tooFew]=NA
  CISHsiteSummary$NOFfwEcoli[tooFew]=NA
  CISHsiteSummary$LawaBand[tooFew]=NA
}
table(CISHsiteSummary$LawaBand,useNA='if')
#    A    B    C    D <NA> 
#   69  178  126  201  135  20/10/20
#   64  179  136  203  126  28/10/20
#   64  181  135  201  127  29/10/20   
#   64  181  135  201  126   3/11/20
#   64  181  132  204  126   4/11/20
#   64  181  132  204  126   9/11/20
#   57  139  117  141  81    2/7/21

CISHsiteSummary$siteName = recData$siteName[match(CISHsiteSummary$LawaSiteID,recData$LawaSiteID)]
CISHsiteSummary$siteType = recData$siteType[match(CISHsiteSummary$LawaSiteID,recData$LawaSiteID)]
CISHsiteSummary$SiteID = ssm$SiteName[match(CISHsiteSummary$siteName,make.names(ssm$callID))]


#Export here 
downloadSummary = CISHsiteSummary%>%select(region,siteName,SiteID,LawaSiteID,
                                           siteType,property,totSampleCount,
                                           minVal=min,maxVal=max,Hazen95=haz95,
                                           LongTermGrade=NOFfwEcoli)
#LongTermGrade is set to Ecoli just above, set it to enterococci for enterocicci rows
downloadSummary$LongTermGrade[downloadSummary$property=='Enterococci']=
  CISHsiteSummary$MACmarineEnt[CISHsiteSummary$property=="Enterococci"]
  #Dont save out yet, need to add the "GradeSubstitue" column


CISHsiteSummary <- CISHsiteSummary%>%select(LawaSiteID,SiteID,siteName,region,property,siteType,
                                            nBS,nPbs,uBS,totSampleCount,min,max,haz95,haz50,LawaBand)



#Simplify. ####
#  If sites have different bands for multiple properties then keep them, otherwise,
# let's just you know, we dont need to keep both do we
singleProp <- CISHsiteSummary%>%
  group_by(LawaSiteID)%>%
  dplyr::mutate(nProp=length(unique(property)))%>%
  ungroup%>%
  filter(nProp==1)%>%select(-nProp)
#535 3/11
#371 2/7/21
multiProp <- CISHsiteSummary%>%
  group_by(LawaSiteID)%>%
  dplyr::mutate(nProp=length(unique(property)),
                nNA=sum(is.na(LawaBand)))%>%
  ungroup%>%
  filter(nProp>1)%>%
  filter(!(.$nNA==1&is.na(.$LawaBand)))%>%  #we're only looking at sites that have two FIBs, so,
                                            #if a row is NA in lawaband, and has only 1 NA< the other FIB must be good.
  group_by(LawaSiteID)%>%
  dplyr::mutate(nProp=length(unique(property)),
                nNA=sum(is.na(LawaBand))) #113 3/11
moresingles = multiProp%>%filter(nProp==1)%>%select(-nProp,-nNA)%>%distinct
#59 3/11
#110 2/7/21

singleProp=full_join(singleProp,moresingles)  #425
rm(moresingles)

multiProp <- multiProp%>%filter(nProp!=1)%>%
  group_by(LawaSiteID)%>%
  dplyr::mutate(combGr=paste0(unique(LawaBand),collapse=''),
                ncg=nchar(combGr))%>%
  ungroup #56 2/7/21

moresingles=multiProp%>%filter(ncg==1|combGr=="NA")%>%select(-combGr,-nProp,-nNA,-ncg)%>%distinct
#26 2/7

#These are now same grade in ecoli and enterococci, but we do need to keep only the appropriate numeric values
moresingles <- moresingles%>%filter((property=="E-coli"&siteType=='Site')|(property=="Enterococci"&siteType=='Beach'))
#13 2/7

singleProp=full_join(singleProp,moresingles) #438
rm(moresingles)

multiProp <- multiProp%>%  dplyr::filter(ncg>1&combGr!='NA') #30

multiProp%>%select(LawaSiteID,region,siteType,combGr)%>%distinct

#Some of these, the appropriate FIB will be the worst case, in which case, all good, right?

moresingles = multiProp%>%select(LawaSiteID:region,siteType,property,nBS:LawaBand)%>%
  pivot_wider(id_cols = LawaSiteID:siteType,
              names_from = property,values_from = nBS:LawaBand)%>%
  filter((siteType=='Site'&`LawaBand_E-coli`>=LawaBand_Enterococci)|
           (siteType=='Beach'&`LawaBand_E-coli`<=LawaBand_Enterococci))
#Have to keep this two-step I think
moresingles = multiProp%>%filter(LawaSiteID%in%moresingles$LawaSiteID)%>%
  filter((siteType=='Beach'&property=="Enterococci")|
           (siteType=='Site'&property=="E-coli"))%>%
  select(-(nProp:ncg))
#4 2/7/21

multiProp <- multiProp%>%filter(!LawaSiteID%in%moresingles$LawaSiteID) #22 (==11 sites)

singleProp=full_join(singleProp,moresingles) #442
rm(moresingles)

storeForCouncilInfo=multiProp


#There are 11 sites here.  Give them each their worst grade, regardless of water type
stopifnot(all(multiProp$siteType=='Beach'))
moresingles <- multiProp%>%filter(property=='Enterococci')%>%
  mutate(LawaBand = sapply(combGr,FUN=function(s){max(unlist(strsplit(s,split = '')))}))%>%
  select(-(nProp:ncg)) #11

singleProp = full_join(singleProp,moresingles)%>% #453
  arrange(region)
rm(moresingles,multiProp)




CISHsiteSummary = singleProp          #535 rows to 453

# rm(singleProp)


#Can we check here for sites that have different LawaBand in the CISH than in the download
downloadSummary <- left_join(downloadSummary,
                             CISHsiteSummary%>%select(LawaSiteID,property,LawaBand))
downloadSummary$GradeSubstituted = !downloadSummary$LongTermGrade==downloadSummary$LawaBand
downloadSummary <- downloadSummary%>%select(-LawaBand)
downloadSummary$GradeSubstituted[is.na(downloadSummary$GradeSubstituted)] <- FALSE

downloadSummary$siteType[downloadSummary$siteType=="Site"] <- "River"
downloadSummary$siteType[downloadSummary$siteType=="LakeSite"] <- "Lake"
downloadSummary$siteType[downloadSummary$siteType=="Beach"] <- "Coastal"



LAWAPalette=c("#95d3db","#74c0c1","#20a7ad","#007197",
              "#e85129","#ff8400","#ffa827","#85bb5b","#bdbcbc")

AbisPivotData = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2020/CanISwimHere/Data//LAWA Recreational water quality monitoring dataset_Nov2020_pivot draft.xlsx',sheet= "Weekly monitoring dataset")

AbisPivotData <- AbisPivotData %>% filter(property!='Cyanobacteria')%>%droplevels()

AbisPivotData$Grade = factor(AbisPivotData$`Swim guidelines test result description`,
                             levels=c("Suitable for swimming","Caution advised","Unsuitable for swimming","Not available"))

RiverGrades=AbisPivotData%>%
  dplyr::filter(siteType=='River',Grade!="Not available")%>%droplevels%>%
  dplyr::select(Grade)%>%table%>%as.data.frame
LakeGrades=AbisPivotData%>%
  dplyr::filter(siteType=='Lake',Grade!="Not available")%>%droplevels%>%
  dplyr::select(Grade)%>%table%>%as.data.frame
CoastalGrades=AbisPivotData%>%
  dplyr::filter(siteType=='Coastal',Grade!="Not available")%>%droplevels%>%
  dplyr::select(Grade)%>%table%>%as.data.frame

library(showtext)
if(!'source'%in%font_families()){
  sysfonts::font_add_google("Source Sans Pro",family='source')
}


tiff('H:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/Summary.tif',
     width=15,height=12,res=300,compression='lzw',type='cairo',units='in')
showtext::showtext_begin()
layout(matrix(c(1,1,1,1,1,1,1,1,1,
              2,2,2,3,3,3,4,4,4,
              2,2,2,3,3,3,4,4,4,
              5,5,5,6,6,6,7,7,7),nrow=4,byrow=T))
par(family='source',mar=c(0,0,0,0),bg='white')
par(xpd=NA)
plot(0,0,bty='n',type='n',xlab='',ylab='',xaxt='n',yaxt='n',xlim=c(0,10),ylim=c(0,10))
text(5,6,"New Zealand swim spot water quality summary",cex=20)
text(5,3,expression('Recreational water quality over five years'^'*'),cex=15)
pie(RiverGrades$Freq,labels=NA,col=c('#85bb5b','#ffa827','#e85129'),border='white',radius=0.9)
title(main = 'River',line=-4,cex.main=12)
points(0,0,pch=16,cex=50,col='white')
pie(LakeGrades$Freq,labels=NA,col=c('#85bb5b','#ffa827','#e85129'),border='white',radius=0.9)
title(main = 'Lake',line=-4,cex.main=12)
points(0,0,pch=16,cex=50,col='white')
pie(CoastalGrades$Freq,labels=NA,col=c('#85bb5b','#ffa827','#e85129'),border='white',radius=0.9)
title(main = 'Coastal',line=-4,cex.main=12)
points(0,0,pch=16,cex=50,col='white')

plot(0,0,bty='n',type='n',xlab='',ylab='',xaxt='n',yaxt='n',xlim=c(0,10),ylim=c(0,10))
text(1,7+3,paste0(round(RiverGrades$Freq[1]/sum(RiverGrades$Freq)*100,0),'%'),col='#85bb5b',cex=12)
text(1,5+3,paste0(round(RiverGrades$Freq[2]/sum(RiverGrades$Freq)*100,0),'%'),col='#ffa827',cex=12)
text(1,3+3,paste0(round(RiverGrades$Freq[3]/sum(RiverGrades$Freq)*100,0),'%'),col='#e85129',cex=12)
text(2,7+3,"Suitable for swimming",cex=8,pos = 4)
text(2,5+3,"Caution advised",cex=8,pos = 4)
text(2,3+3,"Unsuitable for swimming",cex=8,pos = 4)
text(1,1+3,paste0("Summary from ",sum(RiverGrades$Freq)," samples."),cex=6,pos=4)
plot(0,0+3,bty='n',type='n',xlab='',ylab='',xaxt='n',yaxt='n',xlim=c(0,10),ylim=c(0,10))
text(1,7+3,paste0(round(LakeGrades$Freq[1]/sum(LakeGrades$Freq)*100,0),'%'),col='#85bb5b',cex=12)
text(1,5+3,paste0(round(LakeGrades$Freq[2]/sum(LakeGrades$Freq)*100,0),'%'),col='#ffa827',cex=12)
text(1,3+3,paste0(round(LakeGrades$Freq[3]/sum(LakeGrades$Freq)*100,0),'%'),col='#e85129',cex=12)
text(2,7+3,"Suitable for swimming",cex=8,pos = 4)
text(2,5+3,"Caution advised",cex=8,pos = 4)
text(2,3+3,"Unsuitable for swimming",cex=8,pos = 4)
text(1,1+3,paste0("Summary from ",sum(LakeGrades$Freq)," samples."),cex=6,pos=4)
text(5,2,expression(""^'*'*"Faecal indicator bacterial test results (excludes predicted data) supplied to LAWA. Data were collected over"),cex=8)
text(5,1,"the recreational bathing season (last week Oct – end of Mar) during 2015 – 2021  from regularly monitored swim sites.",cex=8)
plot(0,0+3,bty='n',type='n',xlab='',ylab='',xaxt='n',yaxt='n',xlim=c(0,10),ylim=c(0,10))
text(1,7+3,paste0(round(CoastalGrades$Freq[1]/sum(CoastalGrades$Freq)*100,0),'%'),col='#85bb5b',cex=12)
text(1,5+3,paste0(round(CoastalGrades$Freq[2]/sum(CoastalGrades$Freq)*100,0),'%'),col='#ffa827',cex=12)
text(1,3+3,paste0(round(CoastalGrades$Freq[3]/sum(CoastalGrades$Freq)*100,0),'%'),col='#e85129',cex=12)
text(2,7+3,"Suitable for swimming",cex=8,pos = 4)
text(2,5+3,"Caution advised",cex=8,pos = 4)
text(2,3+3,"Unsuitable for swimming",cex=8,pos = 4)
text(1,1+3,paste0("Summary from ",sum(CoastalGrades$Freq)," samples."),cex=6,pos=4)
showtext::showtext_end()
if(names(dev.cur())=='tiff')dev.off()

riverpie = gvisPieChart(data=RiverGrades,
                          options=list(
                            title='River',
                            width=1800,height=200,
                            pieHole=0.6,
                            colors="[ '#85bb5b','#ffa827','#e85129']",
                            pieSliceText='none',
                            pieStartAngle=90,
                            fontSize=20,
                            fontName='arial',
                            chartArea="{left:10,top:50,width:'40%',height:'90%'}",
                            legend="{position:'right',alignment:'center',maxLines:3}"),
                        chartid='riverCISHgrades')
lakepie = gvisPieChart(data=LakeGrades,
                        options=list(
                                     title="Lake",
                                     width=1800,height=200,
                                     pieHole=0.6,
                                     colors="[ '#85bb5b','#ffa827','#e85129']",
                                     pieSliceText='none',
                                     pieStartAngle=90,
                                     fontSize=20,
                                     fontName='arial',
                                     chartArea="{left:10,top:40,width:'40%',height:'90%'}",
                                     legend="{position:'righh',alignment:'center',maxLines:3}"),
                       chartid='lakeCISHgrades')
coastalpie = gvisPieChart(data=CoastalGrades,
                        options=list(
                                     title="Coastal",
                                     width=1800,height=200,
                                     pieHole=0.6,
                                     colors="[ '#85bb5b','#ffa827','#e85129']",
                                     pieSliceText='none',
                                     pieStartAngle=90,
                                     fontSize=20,
                                     fontName='arial',
                                     chartArea="{left:10,top:30,width:'40%',height:'90%'}",
                                     legend="{position:'right',alignment:'center',maxLines:3}"),
                        chartid='coastalCISHgrades')

# legendpie = gvisBarChart(data=RiverGrades,options=list(
#   title="Legend",
#   width=400,height=400,piehole=0.9,
#   fontName='arial',
#   chartArea="{left:100,top:100,width:'10%',height:'100%'}",
#   pieSliceText='none',fontSize=20,legend="{position:'top',alignment:'start',maxLines:5}"
# ),
# chartid='legendOnly')
# 
# plot()

rlpie = gvisMerge(riverpie,lakepie,horizontal=F)
rlcpie=gvisMerge(rlpie,coastalpie,horizontal=F,chartid='CISHgrades')
plot(rlcpie)


print(rlcpie,file=paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/",
                           format(Sys.Date(),'%Y-%m-%d'),
                           "/SwimSpot.html"))



write.csv(downloadSummary,
          file = paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/",
                        format(Sys.Date(),'%Y-%m-%d'),
                        "/CISHdownloadSummary",format(Sys.time(),'%Y-%b-%d'),".csv"),row.names = F) 


write.csv(CISHsiteSummary,file = paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                                        "/CISHsiteSummary",format(Sys.time(),'%Y-%b-%d'),".csv"),row.names = F)
# CISHsiteSummary=read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/",pattern="CISHsiteSummary",recursive = T,full.names = T,ignore.case = T),1),stringsAsFactors = F)
file.copy(from=paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                      "/CISHsiteSummary",format(Sys.time(),'%Y-%b-%d'),".csv"),
          to = "c:/users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021/Can I Swim here/Analysis/",overwrite=T)


#Write individual regional files: data from recData and scores from CISHsiteSummary 
uReg=unique(recData$region)
for(reg in seq_along(uReg)){
  toExport=recData%>%filter(region==uReg[reg],dateCollected>(Sys.time()-lubridate::years(5)))
  write.csv(toExport,file=paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                                 "/recData_",
                                 gsub(' ','',gsub(pattern = ' region',replacement = '',x = uReg[reg])),
                                 ".csv"),row.names = F)
  toExport=CISHsiteSummary%>%filter(region==uReg[reg])%>%as.data.frame
  write.csv(toExport,file=paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                                 "/recScore_",
                                 gsub(' ','',gsub(pattern = ' region',replacement = '',x = uReg[reg])),
                                 ".csv"),row.names = F)
}
rm(reg,uReg,toExport)







#Write export file for ITEffect to use for the website

recDataITE <- CISHsiteSummary%>%
  transmute(LAWAID=LawaSiteID,
            Region=region,
            Site=siteName,
            Hazen=haz95,
            NumberOfPoints=unlist(lapply(str_split(nPbs,','),function(x)sum(as.numeric(x)))),
            DataMin=min,
            DataMax=max,
            RiskGrade=LawaBand,
            Module=siteType)
recDataITE$Module[recDataITE$Module=="Site"] <- "River"
recDataITE$Module[recDataITE$Module=="LakeSite"] <- "Lake"
recDataITE$Module[recDataITE$Module=="Beach"] <- "Coastal"
write.csv(recDataITE,paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                            "/ITERecData",format(Sys.time(),'%Y-%b-%d'),".csv"),row.names = F)  

file.copy(from=paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                      "/ITERecData",format(Sys.time(),'%Y-%b-%d'),".csv"),
  to = "c:/users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021/Can I Swim here/Analysis/",overwrite=T)


#Audit differences

CISHsums = dir(path="h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/",pattern="^CISHsite",recursive=T,full.names=T,ignore.case=T)

aCISH = read.csv(CISHsums[7],stringsAsFactors = F)

aCISH%>%filter(grepl('manuherikia',siteName,ignore.case=T))

lastCISH = read.csv(tail(CISHsums,1),stringsAsFactors = F)%>%arrange(siteName)
prevCISH = read.csv(head(tail(CISHsums,2),1),stringsAsFactors = F)%>%arrange(siteName)

bandCheck = merge(lastCISH%>%select(LawaSiteID,SiteID,siteName,SiteID,region,property,lastband=LawaBand),
                  prevCISH%>%select(LawaSiteID,SiteID,siteName,SiteID,region,property,prevband=LawaBand))%>%
  filter(lastband!=prevband)


bandCheck%>%filter(region=="Hawke's Bay region")































#7/12/2020
#Hi Eric

# Here is hopefully a quick job for Monday, if you can please.
# 
# Would it be possible to regenerate the below figure, but only use samples from the last swim season? (last week of Oct 2019 through to the end of Mar 2021).
# 
# Needs to go be ready to go out with the LAWA media release on first thing Tuesday 8th Dec.  It’s short notice, so if this isn’t doable, please let us know, and we will revise what we send out.
# 
# Thanking you kindly
# 
# Have a lovely weekend.
# 
# Cheers
# Abi


AbisPivotData = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data//LAWA Recreational water quality monitoring dataset_Nov2021_pivot draft.xlsx',sheet= "Weekly monitoring dataset")

AbisPivotData <- AbisPivotData %>% filter(property!='Cyanobacteria')%>%droplevels()
AbisPivotData <- AbisPivotData %>% filter(dateCollected>lubridate::dmy('22-10-2019')&dateCollected<=lubridate::dmy('31-3-2020'))

AbisPivotData$Grade = factor(AbisPivotData$`Swim guidelines test result description`,
                             levels=c("Suitable for swimming","Caution advised","Unsuitable for swimming","Not available"))

RiverGrades=AbisPivotData%>%
  dplyr::filter(siteType=='River',Grade!="Not available")%>%droplevels%>%
  dplyr::select(Grade)%>%table%>%as.data.frame
LakeGrades=AbisPivotData%>%
  dplyr::filter(siteType=='Lake',Grade!="Not available")%>%droplevels%>%
  dplyr::select(Grade)%>%table%>%as.data.frame
CoastalGrades=AbisPivotData%>%
  dplyr::filter(siteType=='Coastal',Grade!="Not available")%>%droplevels%>%
  dplyr::select(Grade)%>%table%>%as.data.frame

library(showtext)
if(!'source'%in%font_families()){
  sysfonts::font_add_google("Source Sans Pro",family='source')
}


tiff('H:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/OneYearSummary.tif',
     width=15,height=12,res=300,compression='lzw',type='cairo',units='in')
showtext::showtext_begin()
layout(matrix(c(1,1,1,1,1,1,1,1,1,
                2,2,2,3,3,3,4,4,4,
                2,2,2,3,3,3,4,4,4,
                5,5,5,6,6,6,7,7,7),nrow=4,byrow=T))
par(family='source',mar=c(0,0,0,0),bg='white')
par(xpd=NA)
plot(0,0,bty='n',type='n',xlab='',ylab='',xaxt='n',yaxt='n',xlim=c(0,10),ylim=c(0,10))
text(5,6,"New Zealand swim spot water quality summary",cex=20)
text(5,3,expression('Recreational water quality 2019 - 2021'^'*'),cex=15)
pie(RiverGrades$Freq,labels=NA,col=c('#85bb5b','#ffa827','#e85129'),border='white',radius=0.9)
title(main = 'River',line=-4,cex.main=12)
points(0,0,pch=16,cex=50,col='white')
pie(LakeGrades$Freq,labels=NA,col=c('#85bb5b','#ffa827','#e85129'),border='white',radius=0.9)
title(main = 'Lake',line=-4,cex.main=12)
points(0,0,pch=16,cex=50,col='white')
pie(CoastalGrades$Freq,labels=NA,col=c('#85bb5b','#ffa827','#e85129'),border='white',radius=0.9)
title(main = 'Coastal',line=-4,cex.main=12)
points(0,0,pch=16,cex=50,col='white')

plot(0,0,bty='n',type='n',xlab='',ylab='',xaxt='n',yaxt='n',xlim=c(0,10),ylim=c(0,10))
text(1,7+3,paste0(round(RiverGrades$Freq[1]/sum(RiverGrades$Freq)*100,0),'%'),col='#85bb5b',cex=12)
text(1,5+3,paste0(round(RiverGrades$Freq[2]/sum(RiverGrades$Freq)*100,0),'%'),col='#ffa827',cex=12)
text(1,3+3,paste0(round(RiverGrades$Freq[3]/sum(RiverGrades$Freq)*100,0),'%'),col='#e85129',cex=12)
text(2,7+3,"Suitable for swimming",cex=8,pos = 4)
text(2,5+3,"Caution advised",cex=8,pos = 4)
text(2,3+3,"Unsuitable for swimming",cex=8,pos = 4)
text(1,1+3,paste0("Summary from ",sum(RiverGrades$Freq)," samples."),cex=6,pos=4)
plot(0,0+3,bty='n',type='n',xlab='',ylab='',xaxt='n',yaxt='n',xlim=c(0,10),ylim=c(0,10))
text(1,7+3,paste0(round(LakeGrades$Freq[1]/sum(LakeGrades$Freq)*100,0),'%'),col='#85bb5b',cex=12)
text(1,5+3,paste0(round(LakeGrades$Freq[2]/sum(LakeGrades$Freq)*100,0),'%'),col='#ffa827',cex=12)
text(1,3+3,paste0(round(LakeGrades$Freq[3]/sum(LakeGrades$Freq)*100,0),'%'),col='#e85129',cex=12)
text(2,7+3,"Suitable for swimming",cex=8,pos = 4)
text(2,5+3,"Caution advised",cex=8,pos = 4)
text(2,3+3,"Unsuitable for swimming",cex=8,pos = 4)
text(1,1+3,paste0("Summary from ",sum(LakeGrades$Freq)," samples."),cex=6,pos=4)
text(5,2,expression(""^'*'*"Faecal indicator bacterial test results (excludes predicted data) supplied to LAWA. Data were collected over"),cex=8)
text(5,1,"the recreational bathing season (last week Oct – end of Mar) during 2019 – 2021  from regularly monitored swim sites.",cex=8)
plot(0,0+3,bty='n',type='n',xlab='',ylab='',xaxt='n',yaxt='n',xlim=c(0,10),ylim=c(0,10))
text(1,7+3,paste0(round(CoastalGrades$Freq[1]/sum(CoastalGrades$Freq)*100,0),'%'),col='#85bb5b',cex=12)
text(1,5+3,paste0(round(CoastalGrades$Freq[2]/sum(CoastalGrades$Freq)*100,0),'%'),col='#ffa827',cex=12)
text(1,3+3,paste0(round(CoastalGrades$Freq[3]/sum(CoastalGrades$Freq)*100,0),'%'),col='#e85129',cex=12)
text(2,7+3,"Suitable for swimming",cex=8,pos = 4)
text(2,5+3,"Caution advised",cex=8,pos = 4)
text(2,3+3,"Unsuitable for swimming",cex=8,pos = 4)
text(1,1+3,paste0("Summary from ",sum(CoastalGrades$Freq)," samples."),cex=6,pos=4)
showtext_end()
if(names(dev.cur())=='tiff')dev.off()
