#July 2021
#This file relies on a list of SSM sites provided by Effect, but then iterates across that list, trying to pull the actual data from the council servers direct.

rm(list=ls())
library(tidyverse)
library(sysfonts)
library(googleVis)
library(xml2)
library(parallel)
library(doParallel)
source('H:/ericg/16666LAWA/LAWA2021/Scripts/LAWAFunctions.R')
dir.create(paste0("H:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/",format(Sys.Date(),"%Y-%m-%d")),showWarnings = F,recursive = T)

checkXMLFile <- function(regionName,siteName,propertyName){
  fname=paste0('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/',regionName,'/',siteName,'/',propertyName,'.xml')
  fnameNew=paste0('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/',regionName,'/',siteName,'/',propertyName,'NEW.xml')
  
  if(file.exists(fname)){
    xmlfileOld <- read_xml(fname)
    if('wml2'%in%names(xml_ns(xmlfileOld))){
      mvalsOld <- xml_find_all(xmlfileOld,'//wml2:value')
    }else{
      pvalsOld=xml_find_all(xmlfileOld,'//Parameter')
    }
  }
  
  xmlfile<-read_xml(fnameNew)
  if('ows'%in%names(xml_ns(xmlfile)) && length(xml_find_all(xmlfile,'ows:Exception'))>0){
    #Dont wanna keep exception files
    file.remove(fnameNew)
    return(1);#next
  }
  #Got Data
  if('wml2'%in%names(xml_ns(xmlfile))){
    mvals <- xml_find_all(xmlfile,'//wml2:value')
    if(!file.exists(fname)){
      file.rename(from = fnameNew,to = fname)
    }else{
      if(!exists('mvalsOld')){
        file.remove(fname)
        file.rename(from = fnameNew,to = fname)
      }
    }
    if(exists('mvalsOld')&&length(mvals)>length(mvalsOld)){
      cat('+')
      file.remove(fname)
      file.rename(from = fnameNew,to = fname)
    }else{
      if(file.exists(fnameNew)){
      file.remove(fnameNew)
        }
    }
    if(length(mvals)>0){
      return(2);#hoorah break
    }else{
      #Dont wanna keep empty files
      file.remove(fname)
    }
    return(1);#next
  }else{
    if(propertyName=="wq.sample"){
      pvals=xml_find_all(xmlfile,'//Parameter')
      
      if(!file.exists(fname)){
        file.rename(from = fnameNew,to = fname)
      }else{
        if(!exists('pvalsOld')){
          file.remove(fname)
          file.rename(from = fnameNew,to = fname)
        }
      }
      if(exists('pvalsOld')&&length(pvals)>length(pvalsOld)){
        cat('+')
        file.remove(fname)
        file.rename(from = fnameNew,to = fname)
      }else{
        if(file.exists(fnameNew)){
          file.remove(fnameNew)
          }
      }
      
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
        
        if(file.exists(fname)&file.exists(fnameNew)){
          if(file.info(fnameNew)$size > file.info(fname)$size){
            file.remove(fname)
            file.rename(from=fnameNew,to=fname)
          }else{
            file.remove(fnameNew)
          }
        }else{
          if(file.exists(fnameNew)){
            file.rename(from=fnameNew,to=fname)
          }
        }
        
        if(file.info(fname)$size>2000){
          file.rename(from=fname,to=fname)
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
  fname=paste0('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/',regionName,'/',siteName,'/',propertyName,'.xml')
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
    if(propertyName=="wq.sample"){
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

# lmsl=read_csv("H:/ericg/16666LAWA/LAWA2021/Masterlist of sites in LAWA Umbraco as at 1 June 2021.csv")

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


ssm = readxl::read_xlsx(tail(dir(path='h:/ericg/16666LAWA/LAWA2021/CanISwimHere/MetaData/',
                                 pattern='SwimSiteMonitoringResults.*.xlsx',
                                 recursive = T,full.names = T),1),
                        sheet=1)%>%as.data.frame%>%unique
ssm$TimeseriesUrl[ssm$Region=="Bay of Plenty region"] <- gsub(pattern = '@',
                                                              replacement = '&featureOfInterest=',
                                                              x =ssm$TimeseriesUrl[ssm$Region=="Bay of Plenty region"])
ssm$callID =  NA
ssm$callID[!is.na(ssm$TimeseriesUrl)] <- c(unlist(sapply(X = ssm[!is.na(ssm$TimeseriesUrl),]%>%
                                                           select(TimeseriesUrl),
                                                         FUN = function(x){
                                                           unlist(strsplit(x,split='&'))
                                                         })))%>%
  grep('featureofinterest',x = .,ignore.case=T,value=T)%>%
  gsub('featureofinterest=','',x=.,ignore.case = T)%>%
  sapply(.,URLdecode)%>%trimws

# if(Sys.Date()=="2021-08-06"){
length(grep('nrc.govt.nz/SOESwimmingCoastal.hts',ssm$TimeseriesUrl))
length(grep('nrc.govt.nz/SOESwimmingFW.hts',ssm$TimeseriesUrl))
ssm$TimeseriesUrl <- gsub('nrc.govt.nz/SOESwimmingCoastal.hts','nrc.govt.nz/SOESwimming.hts',ssm$TimeseriesUrl)
ssm$TimeseriesUrl <- gsub('nrc.govt.nz/SOESwimmingFW.hts','nrc.govt.nz/SOESwimming.hts',ssm$TimeseriesUrl)
# }



RegionTable=data.frame(ssm=unique(ssm$Region),
                       wfs=c("bay of plenty","canterbury","gisborne","hawkes bay",
                             "horizons","marlborough","nelson","northland",
                             "otago","southland","taranaki","tasman",
                             "waikato","wellington","west coast"))


#Download data to XML files ####
agencyURLtable=data.frame(region="",server="",property='',feature='')
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
foreach(SSMregion = unique(ssm$Region),.combine=rbind,.errorhandling="stop",.inorder=F)%dopar%{
  # for(SSMregion in unique(ssm$Region)){
  regionName=make.names(word(SSMregion,1,1))
  write.csv(ssm$Region,file = paste0("D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/",regionName,"/inHere",make.names(Sys.time()),".txt"))

  cat('\n\n\n',SSMregion,'\n')
  
  
  #Find agency properties from the SSM file ####
  observedProperty <- sapply(X = ssm%>%filter(Region==SSMregion,TimeseriesUrl!='')%>%
                               select(TimeseriesUrl),
                             FUN = function(x){gsub('(.+)http.+','\\1',x=x)%>%
                                 strsplit(split='&')%>%unlist%>%
                                 grep('observedproperty|offering',x = .,ignore.case=T,value=T)%>%
                                 gsub('observedproperty=|offering=','',x=.,ignore.case = T)})%>%
    sapply(.,URLdecode)%>%unname#%>%tolower
  if(regionName=="Bay"){
    observedProperty = gsub(pattern = 'i_Rec',replacement = "i.Rec",x = observedProperty)
    observedProperty = sapply(observedProperty,function(s)strTo(s,c='\\@'))
  }
  propertyType = ssm%>%
    filter(Region==SSMregion & TimeseriesUrl!='')%>%
    select(Property)%>%
    unlist%>%
    unname
  stopifnot(length(observedProperty)==length(propertyType))
  agProps=unique(data.frame(observedProperty,propertyType,stringsAsFactors=F))
  agProps=rbind(agProps,c('WQ sample','WQ sample'))
  rm(observedProperty,propertyType)

    
  #Extract sites from the timeSeriesURLs delivered in the SwimSiteMonitoring file ####
  agSites <- c(unlist(sapply(X = ssm%>%
                               filter(Region==SSMregion)%>%
                               select(TimeseriesUrl),
                             FUN = function(x)unlist(strsplit(x,split='&')))))%>%
    grep('featureofinterest',x = .,ignore.case=T,value=T)%>%
    gsub('featureofinterest=','',x=.,ignore.case = T)%>%
    sapply(.,URLdecode)%>%trimws%>%unique
  
  
  #Find agency format keys.  There may be none ####
  (agFormats <- sort(unique(c(unlist(sapply(X = ssm$TimeseriesUrl[ssm$Region==SSMregion],
                                            FUN = function(x)unlist(strsplit(x,split='&')))))))%>%
      grep('format',x = .,ignore.case=T,value=T))%>%cat
  
  
  
  #Find agency URLs ####
    agURLs <-     sapply(X = ssm$TimeseriesUrl[ssm$Region==SSMregion],
                       FUN = function(x)unlist(strsplit(x,split='&')))%>%
    unlist%>%
    grep('http',x = .,ignore.case=T,value=T)%>%
    table%>%
    sort(decr=T)%>%names
  
  agURLs = unique(gsub(pattern = 'Service','service',agURLs))
  agURLs%>%paste(collapse='\t')%>%cat
  if(any(!grepl('^http',agURLs))){  #This will also allow https
    agURLs = agURLs[grepl('^http',agURLs)]
  }
  
  if(regionName=="Bay"){
    agURLs = paste0(agURLs,'&version=2.0.0')
    "http://sos.boprc.govt.nz/service?"
    "service=SOS&version=2.0.0&request=GetObservation&"
    "offering="
    "@"
    "&temporalfilter=om:phenomenonTime,P15Y/2021-01-01"
    if(!'JL348334'%in%agSites){
      agSites = sort(c(agSites,'JL348334'))
    }
  }
  if(regionName=="Manawatu.Whanganui"){
    agURLs = gsub(pattern = 'hilltopserver',replacement = 'maps',x = agURLs)
  }
  if(regionName=='Canterbury'){
    # agURLs = 'http://wateruse.ecan.govt.nz/WQRecLawa.hts?Service=Hilltop'  3/9/21 not needed
  }
  if(regionName=="West"){
    agURLs = gsub(pattern = 'data.wcrc.govt.nz:9083',replacement = 'hilltop.wcrc.govt.nz',x = agURLs)
  }
  if(regionName=="Waikato"){
    agURLs = paste0(agURLs,'&service=SOS')
  }
  if(regionName=="Waikato"){
    agURLs = gsub(pattern="datasource=0$",replacement = "datasource=0&service=SOS&version=2.0.0&procedure=CBACTO.Sample.Results.P",agURLs)
  }
  if(regionName=="Hawke's"){
    agURLs = gsub(pattern="EMAR.hts",replacement = "Recreational_WQ.hts",agURLs)%>%unique
  }
  if(regionName=='Taranaki'){
    # https://extranet.trc.govt.nz/getdata/LAWA_rec_WQ.hts?Service=Hilltop&Request=GetData&Site=SEA901033&Measurement=ECOL&From=1/11/2015&To=1/4/2016
  }
  
  #Load each site/property combo ####
  # by trying each known URL for that council.  Already-existing files will not be reloaded
  cat('\n',regionName,'\n')
  cat(length(agSites)*dim(agProps)[1]*length(agURLs),'\n')
  for(agSite in seq_along(agSites)){
    siteName=make.names(agSites[agSite])
      if(!dir.exists(paste0('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/',regionName,'/',siteName))){
        dir.create(paste0('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/',regionName,'/',siteName),recursive = T)
      }
    for(ap in seq_along(agProps$observedProperty)){
      propertyName=make.names(tolower(agProps$observedProperty[ap]))
      fname=paste0('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/',regionName,'/',siteName,'/',propertyName,'.xml')
      fnameNew=paste0('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/',regionName,'/',siteName,'/',propertyName,'NEW.xml')
      # if(propertyName!="wq.sample"&file.exists(fname)&&file.info(fname)$size>1000){
      #   next
      # }else{
        for(ur in seq_along(agURLs)){
          if(propertyName=="wq.sample"|SSMregion%in%c('Taranaki region')){
            urlToTry = paste0(gsub('SOS','Hilltop',agURLs[ur]),
                              '&request=GetData',
                              '&Site=',agSites[agSite],
                              '&Measurement=',agProps$observedProperty[ap],
                              '&From=1/6/2010&To=1/7/2021')
          }else{
            urlToTry = paste0(agURLs[ur],
                              '&request=GetObservation',
                              '&observedProperty=',agProps$observedProperty[ap],
                              '&featureOfInterest=',agSites[agSite],
                              '&temporalfilter=om:phenomenonTime,P10Y/2021-06-01')
            if(SSMregion%in%c("Bay of Plenty region")){
              urlToTry = gsub(pattern = 'observedProperty',replacement = 'offering',x = urlToTry,ignore.case = T)
              urlToTry = gsub(pattern = '&FeatureOfInterest=',replacement = '@',x = urlToTry,ignore.case = T)
            }
          }
            
            if(length(agFormats)==1){
              urlToTry = paste0(urlToTry,'&',agFormats[1])
            }
            urlToTry=URLencode(urlToTry)
            dlTry=try(curl::curl_fetch_disk(urlToTry,path = fnameNew),silent=T)
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
                break #got a data from this URL, dont try other URLs if there are any
              }
            }
            rm(response)
          }
          rm(ur)
        # }
      }
      rm(ap)
    }
    rm(agSite)
    write.csv(ssm$Region,file = paste0("D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/",regionName,"/outHere",make.names(Sys.time()),".txt"))
    
    rm(regionName)
    return(NULL)
  }
  stopCluster(workers)
  Sys.time()-startTime  
  #30mins 2/7/2021
  #5mins 14/7/2021
  rm(startTime)
  rm(workers)
  
  #Find region size difference
for(SSMregion in unique(ssm$Region)){
  regionName=make.names(word(SSMregion,1,1))
  cat(regionName,': ')
  oldDirSize = sum(file.info(list.files(path = paste0("D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache - Copy/",
                                                      regionName),
                                        pattern = '.xml',
                                        full.names = T,recursive = T))$size)
  newDirSize = sum(file.info(list.files(path = paste0("D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/",
                                                      regionName),
                                        pattern = '.xml',
                                        full.names = T,recursive = T))$size)
  cat((oldDirSize - newDirSize)/1000)
  cat('\n')
  }
  
  #Audit a specific region for which site size difference.
  SSMregion=unique(ssm$Region)[13]
  regionName=make.names(word(SSMregion,1,1))
  #Extract sites from the timeSeriesURLs delivered in the SwimSiteMonitoring file ####
  agSites <- c(unlist(sapply(X = ssm%>%
                               filter(Region==SSMregion)%>%
                               select(TimeseriesUrl),
                             FUN = function(x)unlist(strsplit(x,split='&')))))%>%
    grep('featureofinterest',x = .,ignore.case=T,value=T)%>%
    gsub('featureofinterest=','',x=.,ignore.case = T)%>%
    sapply(.,URLdecode)%>%trimws%>%unique
  
  
  
  for(agSite in agSites){
    siteName=make.names(agSite)
    cat(agSite,': ')
    oldDirSize = sum(file.info(list.files(path = paste0("D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache - Copy/",
                                                        regionName,'/siteName'),
                                          pattern = '.xml',
                                          full.names = T,recursive = T))$size)
    newDirSize = sum(file.info(list.files(path = paste0("D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/",
                                                        regionName,'/siteName'),
                                          pattern = '.xml',
                                          full.names = T,recursive = T))$size)
    cat((oldDirSize - newDirSize)/1000)
   cat('\n') 
  }
  
  if(0){
    #Investigate the yield of metadata files ####
    loadedFiles = dir('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/')
    grep(pattern = 'sample',x = loadedFiles,ignore.case = F,value = T)->WQSf
    sapply(make.names(word(unique(ssm$Region),1,1)),
           FUN=function(s){
             sum(grepl(pattern = paste0('^',s),
                       x = loadedFiles,ignore.case = T))
           })->Dfcount
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
     #Find agency Sites from the  SSM
    agSites <- c(unlist(sapply(X = ssm%>%filter(Region==SSMregion)%>%select(TimeseriesUrl),
                               FUN = function(x)unlist(strsplit(x,split='&')))))%>%
      grep('featureofinterest',x = .,ignore.case=T,value=T)%>%
      gsub('featureofinterest=','',x=.,ignore.case = T)%>%
      sapply(.,URLdecode)%>%trimws%>%unique
    #Find agency properties from the SSM file
    observedProperty <- sapply(X = ssm%>%filter(Region==SSMregion,TimeseriesUrl!='')%>%select(TimeseriesUrl),
                               FUN = function(x){gsub('(.+)http.+','\\1',x=x)%>%
                                   strsplit(split='&')%>%unlist%>%
                                   grep('observedproperty|offering',x = .,ignore.case=T,value=T)%>%
                                   gsub('observedproperty=|offering=','',x=.,ignore.case = T)})%>%
      sapply(.,URLdecode)%>%unname%>%tolower
    if(regionName=="Bay"){
      observedProperty = gsub(pattern = 'i_rec',replacement = "i.rec",x = observedProperty)
      observedProperty = sapply(observedProperty,function(s)strTo(s,c='\\@'))
      if(!'JL348334'%in%agSites){
        agSites = sort(c(agSites,'JL348334'))
      }
    }
    propertyType = ssm%>%
      filter(Region==SSMregion&TimeseriesUrl!='')%>%
      select(Property)%>%
      unlist%>%
      unname
    stopifnot(length(observedProperty)==length(propertyType))
    agProps=unique(data.frame(observedProperty,propertyType,stringsAsFactors=F))
    agProps=rbind(agProps,c('WQ sample','WQ sample'))
    rm(observedProperty,propertyType)
    # agProps=structure(list(observedProperty = "WQ sample", propertyType = "WQ sample"), row.names = 4L, class = "data.frame")
    
   
    
    #Step through sites and properties, loading XML files to memory for subsequent combination
    for(agSite in seq_along(agSites)){
      cat('.')
      siteName=make.names(agSites[agSite])
      for(ap in seq_along(agProps$observedProperty)){
        property=agProps$propertyType[ap]
        propertyName=make.names(tolower(agProps$observedProperty[ap]))
        fname=paste0('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/',regionName,'/',siteName,'/',propertyName,'.xml')
        if(file.exists(fname)&
           file.info(fname)$size>2000){
          readXMLFile(regionName,siteName,propertyName,property) #returns 1 or 2, and builds 'dataToCombine' and 'metaToCombine' lists
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
  
  Sys.time()-startTime  
  #38 s 2/7/2021
  #41s  14/7/21
  #48s 29/7/21
  #2.0mins 13/8/2021
  #3.7 mins 3/9/21  (wqdata in background)
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
      if(!'dateCollected'%in%names(recMetaData) & 'SampleDate'%in%names(recMetaData)){
        recMetaData$dateCollected = as.POSIXct(lubridate::ymd_hms(recMetaData$SampleDate,tz = 'Pacific/Auckland'))
      }
      recMetaData <- recMetaData%>%filter(dateCollected>Sys.time()-lubridate::years(7))
      recMetaData <- recMetaData%>%filter(!apply(recMetaData[,-c(1,2,3,4)],1,FUN=function(r)all(is.na(r))))
      
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
  BayMD = readxl::read_xlsx("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/BOPRC Rec Repeats List 2021 - LAWA v2.xlsx",sheet=1)%>%
    filter(Parameter%in%c("E coli","Enterococci"))%>%mutate(Parameter=ifelse(Parameter=="E coli","E-coli",Parameter))
  lubridate::tz(BayMD$Time)<-'Pacific/Auckland' #to specify what time zone the time was in
  BayMD$dateCollected = BayMD$Time - lubridate::hours(12)
  BayMD$regionName="Bay"
  BayMD <-BayMD%>%dplyr::rename(property=Parameter,siteName=LocationName)
  
  BayD = left_join(x=BayD,y=BayMD%>%select(-siteName,-Unit,-LawaID,-Value),
                   by=c("siteName"="Site","property","dateCollected"))%>%
    filter(is.na(Qualifiers))%>% #This excludes "Recreational repeats", leaving only routine samples
    select(-Qualifiers,-Time,Quality,Approval)
  rm(BayMD)
  tz(BayD$dateCollected) <- ""
  
  
  if(exists('CanterburyMD')){
    CanterburyMD <- CanterburyMD%>%select(-Rain,-`Rain Previously`,-starts_with('Number of'),
                                          -`Birds on Beach and/or Water`,-`Wave height (m)`,
                                          -starts_with('Foam '),-`Dead Marine Life`,-Seaweed,
                                          -`Site is dry`,-`Site is Dry`,-starts_with('High '),
                                          -starts_with('Visual'),-`Water Colour Marine`,
                                          -`Water Clarity`,-`Water Colour`,-`Sent to Lab`,
                                          -`Wind Speed Average (m/s)`,-`Wind Direction`,
                                          -`Wind Strength`,-`Bed is Visible`,-starts_with('YSI'),
                                          -`Meter Number`,-`Cloud Cover`,-`Auto Archived`,
                                          -Archived,-`Cost Code`)
  }
  if(exists('SouthlandMD')){
    SouthlandMD <- SouthlandMD%>%select(-Weather,-Tide,-`Digital Photo`,-`Lake Conditions`,-`Wind Speed`,
                                        -`Entered By`,-`Checked By`,-`Method of Collection`,-Odour,
                                        -FieldFiltered,-starts_with('Isotope'),-`Checked Time`,-`Entered Time`,
                                        -`Meter Number`,-`Black Disc Size`,-Clarity,-`Wind Direction`,
                                        -`Flow Value`,-`Water Level`,-`Field Technician`,-`Collected By`,
                                        -Archived,-`Cost Code`)
  }
  if(exists('OtagoMD')){
    OtagoMD <- OtagoMD%>%select(-`Source of Water`,-Observer,-`Field Technician`,-Odour,-Weather,-Colour,
                                -Clarity,-`Wind Speed`,-`Wind Direction`,-`Flow Rate`,-Tide,-`Tide Direction`,
                                -`Grid Reference`,-`Collection Method`,-`Samples from`,-Archived,-`Cost Code`)
  }
  if(exists('NorthlandMD')){
    NorthlandMD <- NorthlandMD%>%select(-`Field Technician`,-`Run Number`,-`Received at Lab`,-`Wind Strength`,
                                        -`Wind Direction`,-`Meter number`,-Rainfall,-`Weather Affected`,
                                        -`Tidal State`,-`Vehicles on beach`,-`Cloud Cover`,-`Cloud cover`,
                                        -Tide,-Weather,-`Photo taken`,-`Cyanobacteria warning sign`,-Archived,-`Cost Centre`)
  }
  if(exists('Manawatu.WhanganuiMD')){
    Manawatu.WhanganuiMD <- Manawatu.WhanganuiMD%>%select(-`Source Type`,-`Input By`,-`Sampling point`,
                                                          -Fieldsheet,-`Sampling Method`,-`Observed Colour`,
                                                          -SampledBy,-Weather,-MeterID,-`Observed Clarity`,
                                                          -`Observed Velocity`,#-`Observed Flow`,
                                                          -`Compliance Prosecution`,-`Data Audited`,
                                                          #-`Cyanobacteria Detached`,-`Cyanobacteria Exposed`,-`Cyanobacterial Cover`,
                                                          -Archived,-`Cost Code`)
  }
  if(exists('WellingtonMD')){
    WellingtonMD <- WellingtonMD%>%select(-`Detached Cyanobacteria Mats`,-`Rubbish amount`,-Rainfall,-Weather,
                                          -`Sewage Overflow`,-`Wind Direction`,-`Wind strength`,-`GWRC Programme`,
                                          -`Seaweed %`,-Tide,-`Tidal height`)
  }
  if(exists('MarlboroughMD')){
    MarlboroughMD <- MarlboroughMD%>%select(-`Climate Field Meter Used`,-Weather,-Water,-`Tide Flow`,-`Sea State`,
                                            -`Field Technician`,-`Cloud Cover`,-`Meter Used`,-`Auto Archived`,
                                            -Archived,-`Cost Code`)
  }
  if(exists('Hawke.sMD')){
    Hawke.sMD <- Hawke.sMD%>%select(-`Verified`,-`VerifiedBy`,-`VerifiedDate`,-`Sampling Issue`,-Observer)
  }
  if(exists('GisborneMD')){
    GisborneMD <- GisborneMD%>%select(-`Sample ID`,-Archived,-Lab,-EnteredBy,-`Field Technician`,-`Auto Archived`,
                                      -Tide,-`Received at Lab`,-`Lab Batch ID`,-`Lab Batch Id`,-`Lab Sample ID`,
                                      -QAAgency,-CollectionMethod,-QAMethod,-`Cost Code`,-`Low Tide`,-`High Tide`,
                                      -`Thermomter Number`)
  }
  if(exists('NelsonMD')){
    NelsonMD <- NelsonMD%>%select(-Archived)
  }
  if(exists("TaranakiMD")){
    TaranakiMD$SampleDate = as.character(TaranakiMD$SampleDate)
  }
  
  #Then combine all region-specifics to a nationwide overall 
  MDlist <- sapply(ls(pattern='MD$'),get,simplify = FALSE)
  MDlist[sapply(MDlist,is.null)] <- NULL
  sapply(MDlist,names)
  recMetaData=Reduce(function(x,y) dplyr::full_join(x,y%>%distinct),MDlist)
  print(dim(recMetaData))
  
  #88765 75    5/8/21
  #114281 86   13/8/21
  #114338 86  20/8
  #114310
  #114355 86    3/9/21
  #114354      10/9/21
  #114343      13/9/21
  #114279 86   15/9/21   BoP deliverd more
  #114251
  #114525 83   16/9/21 GWRC had more
  
  save(recMetaData,file = paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/",format(Sys.Date(),'%Y-%m-%d'),
                                 "/RecMetaData",format(Sys.time(),'%Y-%b-%d'),".Rdata"))
  rm(list=ls(pattern='MD$'))
  rm(MDlist)
  
  Dlist <- sapply(ls(pattern='[^M]D$'),FUN=function(s)get(s),simplify=FALSE)
  Dlist[sapply(Dlist,is.null)] <- NULL
  sapply(Dlist,names)
  recData=Reduce(function(x,y) dplyr::full_join(x,y%>%distinct),Dlist)
  print(dim(recData))
  
  # 76686 13    5/8/21
  # 81971 13    6/8/21
  # 81900 13   13/8/21
  # 82080 13   20/8/21
  # 82306
  # 82305 13  3/9/21
  # 82302     10/9/21
  # 85280 13  13/9/21
  # 86801 13  13/9/21
  # 86950 13  14/9/21
  # 87579 12  15/9/21 BoP filtered away more
  # 86872 13  15/9/21  BoP timezone
  # 87006 15  16/9/21 GWRC had more
  
  save(recData,file = paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/",format(Sys.Date(),'%Y-%m-%d'),
                             "/RecData",format(Sys.time(),'%Y-%b-%d'),".Rdata"))
  rm(list=ls(pattern='D$'))
  rm(Dlist)
  
  
  
  