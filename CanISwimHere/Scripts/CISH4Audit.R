library(tidyverse)
checkXMLFile <- function(regionName,siteName,propertyName){
  xmlfile<-read_xml(paste0('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/',regionName,siteName,propertyName,'.xml'))
  if('ows'%in%names(xml_ns(xmlfile)) && length(xml_find_all(xmlfile,'ows:Exception'))>0){
    #Dont wanna keep exception files
    file.remove(paste0('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/',regionName,siteName,propertyName,'.xml'))
    return(1);#next
  }
  #Got Data
  if('wml2'%in%names(xml_ns(xmlfile))){
    mvals <- xml_find_all(xmlfile,'//wml2:value')
    if(length(mvals)>0){
      return(2);#hoorah break
    }else{
      #Dont wanna keep empty files
      file.remove(paste0('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/',regionName,siteName,propertyName,'.xml'))
    }
    return(1);#next
  }else{
    if(propertyName=="WQ.sample"){
      pvals=xml_find_all(xmlfile,'//Parameter')
      if(length(pvals)>0){
        return(2);
      }else{
        file.remove(paste0('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/',regionName,siteName,propertyName,'.xml'))
        return(1);
      }
      rm(pvals)
    }else{
      #Remove non WML files
      file.remove(paste0('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/',regionName,siteName,propertyName,'.xml'))
      return(1);#next
    }
  }
}
readXMLFile <- function(regionName,siteName,propertyName,property){
  xmlfile<-read_xml(paste0('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/',regionName,siteName,propertyName,'.xml'))
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
    file.remove(paste0('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/',regionName,siteName,propertyName,'.xml'))
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
                         # LawaSiteID=trimws(ssm$LawaId[siteNum]),
                         siteName=siteName,
                         # siteType=trimws(ssm$SiteType[siteNum]),
                         propertyName=propertyName,
                         property=property,
                         dateCollected=mT,
                         val=faceVals,
                         lCens=lCens,
                         rCens=rCens)%>%
        plyr::mutate(week=lubridate::isoweek(mT),
                     month=lubridate::month(mT),
                     year=lubridate::isoyear(mT),
                     YW=paste0(year,week))
      eval(parse(text=paste0(make.names(paste0(regionName,siteName,propertyName)),"<<-siteDat")))
      dataToCombine <<- c(dataToCombine,make.names(paste0(regionName,siteName,propertyName)))
      rm(siteDat)
      return(2)#        break
    }else{
      #Dont wanna keep empty files
      file.remove(paste0('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/',regionName,siteName,propertyName,'.xml'))
    }
    return(1);#next
  }else{
    #Check is it WQSample
    if(propertyName=="WQ.sample"){
      xmlfile=xmlParse(paste0('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/',regionName,siteName,propertyName,'.xml'))
      xmltop<-xmlRoot(xmlfile)
      m<-xmltop[['Measurement']]
      if(!is.null(m)){
        dtV <- xpathApply(m,"//T",xmlValue)
        dtV <- unlist(dtV)
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
      file.remove(paste0('D:/LAWA/LAWA2021/CanISwimHere/Data/DataCache/',regionName,siteName,propertyName,'.xml'))
      return(1);#next
    }
  }
}

ssm = readxl::read_xlsx(tail(dir(path='h:/ericg/16666LAWA/LAWA2021/CanISwimHere/MetaData/',
                                 pattern='SwimSiteMonitoringResults.*.xlsx',
                                 recursive = T,full.names = T),1),
                        sheet=1)
ssm$callID =  NA
ssm$callID[!is.na(ssm$TimeseriesUrl)] <- c(unlist(sapply(X = ssm%>%select(TimeseriesUrl),
                                                         FUN = function(x)unlist(strsplit(x,split='&')))))%>%
  grep('featureofinterest',x = .,ignore.case=T,value=T)%>%
  gsub('featureofinterest=','',x=.,ignore.case = T)%>%
  sapply(.,URLdecode)%>%trimws

ssm$LawaSiteID=tolower(ssm$LawaId)



load(tail(dir(path="h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/",
              pattern="RecData2021",recursive = T,full.names = T,ignore.case=T),1),verbose = T)  

recData$LawaSiteID=ssm$LawaSiteID[match(make.names(tolower(recData$siteName)),make.names(tolower(ssm$callID)))]

table(ssm$LawaSiteID%in%recData$LawaSiteID)

sort(unique(ssm$LawaSiteID[!ssm$LawaSiteID%in%recData$LawaSiteID]))
sort(unique(ssm$SiteName[!ssm$LawaSiteID%in%recData$LawaSiteID]))
sort(unique(ssm$Region[!ssm$LawaSiteID%in%recData$LawaSiteID]))

knitr::kable(table(ssm$Region[!ssm$LawaSiteID%in%recData$LawaSiteID]),format='rst')
knitr::kable(table(ssm$Region[ssm$LawaSiteID%in%recData$LawaSiteID]),format='rst')



RegionTable=data.frame(ssm=unique(ssm$Region),
                       wfs=c("bay of plenty","canterbury","gisborne","hawkes bay",
                             "horizons","marlborough","nelson","northland",
                             "otago","southland","taranaki","tasman",
                             "waikato","wellington","west coast"))



#Load all previous CISH datasets, and merge
CISHdata = dir(path="h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/",
               pattern="RecData2021",recursive = T,full.names = T,ignore.case=T)
collectCISH=NULL
for (renminbi in 1:length(CISHdata)){
  load(CISHdata[renminbi],verbose=T)
  # cat(str(recData),'\n')
  if('lawaSiteID'%in%names(recData)){
    recData <- recData%>%dplyr::rename(LawaSiteID=lawaSiteID)
  }
  # recData$LawaSiteID = tolower(recData$LawaSiteID)
  collectCISH = merge(collectCISH,recData,all.y=T)
  cat(dim(collectCISH),'\n')
}
rm(recData,CISHdata,renminbi)


#Compare the number of sites retrieved via SSM with that via WFS enquiry.
WFSsiteTable = read.csv(tail(dir(path="H:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data",pattern='SiteTable',
                                 recursive=T,full.names=T,ignore.case=T),1),stringsAsFactors = F)
length(unique(ssm$LawaId))              #654
length(unique(collectCISH$LawaSiteID))  #587
length(unique(WFSsiteTable$LawaSiteID)) #577

length(unique(c(unique(tolower(ssm$LawaId)),unique(tolower(WFSsiteTable$LawaSiteID)))))              #739


sort(unique((unlist(sapply(X = ssm$TimeseriesUrl,FUN = function(x)unlist(strsplit(x,split='&')))))))
sort(unique(tolower(unlist(sapply(X = ssm$TimeseriesUrl,FUN = function(x)unlist(strsplit(x,split='&')))))))
"http://sos.boprc.govt.nz/service?service=sos"                              #bop          
"http://data.ecan.govt.nz/wqreclawa.hts?service=sos"                        #ecan          
"http://odp.es.govt.nz/coastal.hts?service=sos"                             #es     1     
"http://odp.es.govt.nz/cyanoalert.hts?service=sos"                          #es     2     
"http://hilltop.gdc.govt.nz/data.hts?service=sos"                           #gdc          
"http://hilltop.gw.govt.nz/cwq_web.hts?service=sos"                         #gw     1     
"http://hilltop.gw.govt.nz/data.hts?service=sos"                            #gw     2     
"https://data.hbrc.govt.nz/envirodata/recreational_wq.hts?service=sos"      #hbrc          
"http://hilltopserver.horizons.govt.nz/cr_provisional.hts?service=sos"      #hrc    1      
"http://tsdata.horizons.govt.nz/cr_provisional.hts?service=sos"             #hrc    2      
"https://tsdata.horizons.govt.nz/cr_provisional.hts?service=sos"            #hrc    3
"http://hydro.marlborough.govt.nz/lawa_rb.hts?service=sos"                  #mdc          
"http://envdata.nelson.govt.nz/lawa.hts?service=sos"                        #ncc          
"http://hilltop.nrc.govt.nz/soeswimmingcoastal.hts?service=sos"             #nrc    1      
"http://hilltop.nrc.govt.nz/soeswimmingfw.hts?service=sos"                  #nrc    2      
"http://gisdata.orc.govt.nz/hilltop/orcrec.hts?service=sos"                 #orc          
"https://extranet.trc.govt.nz/getdata/lawa_benthic_wq.hts?service=sos"      #trc    1      
"https://extranet.trc.govt.nz/getdata/lawa_rec_wq.hts?service=sos"          #trc    2      
"http://envdata.tasman.govt.nz/waterquality.hts?service=sos"                #tdc          
"http://envdata.waikatoregion.govt.nz:8080/kiwis/kiwis?datasource=0"        #wrc          
"http://data.wcrc.govt.nz:9083/contactrecreation.hts?service=sos"           #wcrc          

#2020 archive
"http://ec2-52-6-196-14.compute-1.amazonaws.com/sos-bop/service?service=sos"         #BOPRC 
"http://data.ecan.govt.nz/wqreclawa.hts?service=sos"                                 #ECAN
"http://wateruse.ecan.govt.nz/bathing.hts?service=sos"                               #ECAN
"http://odp.es.govt.nz/coastal.hts?service=sos"                                       #ES
"http://odp.es.govt.nz/cyanoalert.hts?service=sos"                                    #ES
"http://hilltop.gdc.govt.nz/data.hts?service=sos"                                     #GDC
"http://hilltop.gw.govt.nz/cwq_web.hts?service=sos"                                   #GW
"http://hilltop.gw.govt.nz/data.hts?service=sos"                                      #GW
"https://data.hbrc.govt.nz/envirodata/recreational_wq.hts?service=sos"                #HBRC
"https://data.hbrc.govt.nz/envirodata/emar.hts?service=sos"                           #HBRC
"http://hilltopserver.horizons.govt.nz/cr_provisional.hts?service=sos"                #HRC
"http://hydro.marlborough.govt.nz/boo.hts?service=sos"                                #MDC
"http://hydro.marlborough.govt.nz/data.hts?service=sos"                               #MDC
"http://envdata.nelson.govt.nz/lawa.hts?service=sos"                                  #NCC
"http://hilltop.nrc.govt.nz/soeswimmingcoastal.hts?service=sos"                       #NRC
"http://hilltop.nrc.govt.nz/soeswimmingfw.hts?service=sos"                            #NRC
"http://gisdata.orc.govt.nz/hilltop/orcrec.hts?service=sos"                           #ORC
"http://envdata.tasman.govt.nz/waterquality.hts?service=sos"                          #TDC
"https://extranet.trc.govt.nz/getdata/lawa_benthic_wq.hts?service=sos"                #TRC
"https://extranet.trc.govt.nz/getdata/lawa_rec_wq.hts?service=sos"                    #TRC
"http://data.wcrc.govt.nz:9083/contactrecreation.hts?service=sos"                     #WCRC
"http://envdata.waikatoregion.govt.nz:8080/kiwis/kiwis?datasource=0"                  #WRC

table(ssm$Region)

# Bay of Plenty region         Canterbury region           Gisborne region        Hawke's Bay region 
#                        91                       142                        33                        37 
# Manawatū-Whanganui region        Marlborough region             Nelson region          Northland region 
#                       148                        17                        15                        68 
#              Otago region          Southland region           Taranaki region             Tasman region 
#                        27                        28                        39                         7 
#            Waikato region         Wellington region         West Coast region 
#                        43                       109                        18 







for(SSMregion in unique(ssm$Region)){
  #Find agency Sites
  agSites <- c(unlist(sapply(X = ssm$TimeseriesUrl[ssm$Region==SSMregion],
                             FUN = function(x)unlist(strsplit(x,split='&')))))%>%
    grep('featureofinterest',x = .,ignore.case=T,value=T)%>%
    gsub('featureofinterest=','',x=.,ignore.case = T)%>%
    sapply(.,URLdecode)%>%trimws%>%tolower%>%unique
  siteTableSites = WFSsiteTable$CouncilSiteID[WFSsiteTable$Region==RegionTable$wfs[RegionTable$ssm==SSMregion]]%>%
    trimws%>%tolower%>%unique%>%sort
  cat('\n\n',SSMregion,'\n')
  #Find agency URLs
  agURLs <- unlist(sapply(X = ssm$TimeseriesUrl[ssm$Region==SSMregion],
                          FUN = function(x)unlist(strsplit(x,split='&'))))%>%
    grep('http',x = .,ignore.case=T,value=T)%>%tolower%>%unique%>%sort
  cat("URL(s) specified on umbraco:\n")
  cat(paste(agURLs,collapse='\n'))
  cat('\n\n')
  cat("Sites in SSM but not on WFS:\n")
  cat(paste(agSites[!agSites%in%siteTableSites],collapse=';\t'),'\n')
  cat("\nSites on WFS but not in SSM:\n")
  cat(paste(siteTableSites[!siteTableSites%in%agSites],collapse=';\t'),'\n')
}  






