rm(list = ls())
library(tidyverse)
library(parallel)
library(doParallel)
setwd("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates")
source("H:/ericg/16666LAWA/LAWA2021/scripts/lawaFunctions.R")
source('K:/R_functions/nzmg2WGS.r')
source('K:/R_functions/nztm2WGS.r')

dir.create(paste0("H:/ericg/16666LAWA/LAWA2021/Macroinvertebrates/Data/",format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)


urls          <- read.csv("H:/ericg/16666LAWA/LAWA2021/Metadata/CouncilWFS.csv",stringsAsFactors=FALSE)

# Config for data extract from WFS
#The first on the list prioritises the ID to be used and match every else to it.
WFSvars <- c("CouncilSiteID","SiteID","LawaSiteID",
             "NZReach","NZSegment","SWQAltitude","SWQLanduse",
             "Region","Agency","Catchment")
method=rep('curl',length(urls$URL))
method[8]='wininet'

if(exists('siteTable')){
  rm(siteTable)
}
#For each council, find the "emar:Macro" sites that are switched on, and get their details
startTime=Sys.time()
workers <- makeCluster(6)
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
  if(urls$Agency[h]=="NCC"){
    return(NULL)
  }
  
  xmldata <- try(ldWFSlist(urlIn = urls$URL[h],agency=urls$Agency[h],dataLocation = urls$Source[h],method=method[h],case.fix = TRUE))
  
  if(is.null(xmldata)||'try-error'%in%attr(xmldata,'class')){#||grepl(pattern = '^501|error',
    #x = xmlValue(getNodeSet(xmldata,'/')[[1]]),
    #ignore.case=T)){
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
  } else {
    ### Determine the values used in the [emar:Macro] element
    cat("\n",urls$Agency[h],"\n---------------------------\n",urls$URL[h],"\n",sep="")
    
    # Determine number of records in a wfs with module before attempting to extract all the necessary columns needed
    macroNodes=unname(which(sapply(xmldata,FUN=function(li){
      'Macro'%in%names(li)&&li$Macro%in%c("Yes","yes","YES","Y","y","TRUE","true","T","t","True")})))
    macroData=lapply(macroNodes,FUN=function(li){
      liout <- unlist(xmldata[[li]][WFSvars])
    })
    if(length(macroNodes)==0){
      cat(urls$Agency[h],"has no records for <emar:Macro>\n")
    } else {
      if(!(all(lengths(macroData)==length(WFSvars)))){
        missingWFSvars=which(lengths(macroData)<length(WFSvars))
        for(mv in missingWFSvars){
          missingVarNames = WFSvars[!WFSvars%in%names(macroData[[mv]])]
          for(mvn in missingVarNames){
            eval(parse(text=paste0("macroData[[mv]]=c(macroData[[mv]],",mvn,"=NA)")))
          }
        }
      }
      macroData <- bind_rows(macroData)
      macroData <- as.data.frame(macroData,stringsAsFactors=FALSE)
      #Do lat longs separately from other WQParams because they are value pairs and need separating
      macroPoints=t(sapply(macroNodes,FUN=function(li){
        liout <- unlist(xmldata[[li]]$Shape$Point$pos)
        if(is.null(liout)){liout <- unlist(xmldata[[li]]$SHAPE$Point$pos)}
        as.numeric(unlist(strsplit(liout,' ')))
      }))
      macroPoints=as.data.frame(macroPoints,stringsAsFactors=F)
      names(macroPoints)=c("Lat","Long")
      macroData <- cbind(macroData,macroPoints)
      macroData$accessDate=format(Sys.Date(),"%d-%b-%Y")
      macroData$Agency=urls$Agency[h]
      rm(xmldata)
      return(macroData)
    }
  }
}->siteTable
stopCluster(workers)
rm(workers)

Sys.time()-startTime  #11s (2020)  8mins (2021)

siteTable$SiteID=as.character(siteTable$SiteID)
siteTable$LawaSiteID=as.character(siteTable$LawaSiteID)
siteTable$Region=tolower(as.character(siteTable$Region))
siteTable$Agency=tolower(as.character(siteTable$Agency))

#Once the NIWA load script has run, we can add teh NIWAsiteTable tot eh SiteTable
#The NIWA load script made some stuff up about site locations.
NIWAMacroSites = read.csv('H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Metadata/NIWASiteTable.csv',stringsAsFactors = F)
NIWAMacroSites$LawaSiteID[NIWAMacroSites$LawaSiteID=='ECAN-10028'] <- 'NRWQN-00054'
NIWAMacroSites <- NIWAMacroSites%>%transmute(CouncilSiteID=sitename,SiteID=lawaid,LawaSiteID=LawaSiteID,
                                             NZReach=nzreach,Region=NA,
                                             Agency='niwa',SWQAltitude='',SWQLanduse='',Catchment='river',
                                             Lat=nzmg2wgs(East = nzmge,North = nzmgn)[,1],
                                             Long=nzmg2wgs(East = nzmge,North = nzmgn)[,2],
                                             accessDate=format(Sys.Date(),'%d-%b-%Y'))
NIWAMacroSites$NZSegment=NA
siteTable = rbind(siteTable,NIWAMacroSites%>%select(names(siteTable)))



table(siteTable$Agency)
siteTable$Agency[siteTable$Agency%in%c('arc','auckland council','auckland')] <- 'ac'
siteTable$Agency[tolower(siteTable$Agency)%in%c("christchurch", "environment canterbury")] <- 'ecan'
table(siteTable$Agency)

table(siteTable$Region)
siteTable$Region = tolower(siteTable$Region)
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

## Swapping coordinate values where necessary
plot(siteTable$Long,siteTable$Lat,col=as.numeric(factor(siteTable$Agency)))

#A few of those auckland sites had the worng coordinates as of Sept 2020
# if(abs(siteTable$Long[which(siteTable$LawaSiteID=='lawa-102734')]-176.5804)<0.0001){
#   siteTable$Long[which(siteTable$LawaSiteID=='lawa-102734')] <- 174.7273
#   siteTable$Lat[which(siteTable$LawaSiteID=='lawa-102734')] <- (-36.8972)
# }
# 
# if(abs(siteTable$Long[which(siteTable$LawaSiteID=='lawa-102744')]-174.5198)<0.0001){
#   siteTable$Long[which(siteTable$LawaSiteID=='lawa-102744')] <- 174.8418
#   siteTable$Lat[which(siteTable$LawaSiteID=='lawa-102744')] <- (-37.1811)
# }




toSwitch=which(siteTable$Long<0 & siteTable$Lat>0)
if(length(toSwitch)>0){
  unique(siteTable$Agency[toSwitch])
  newLon=siteTable$Lat[toSwitch]
  siteTable$Lat[toSwitch] <- siteTable$Long[toSwitch]
  siteTable$Long[toSwitch]=newLon
  rm(newLon)
}
rm(toSwitch)

plot(siteTable$Long,siteTable$Lat,col=as.numeric(factor(siteTable$Region)))




table(siteTable$Agency)

siteTable=unique(siteTable)

plot(siteTable$Long,siteTable$Lat,col=as.numeric(factor(siteTable$Agency)))

these=which(siteTable$Long<(160))
if(length(these)>0){
  siteTable$Long[these] -> store
  siteTable$Long[these] <- siteTable$Lat[these]
  siteTable$Lat[these] <-  -store
  rm(store)
}
rm(these)

plot(siteTable$Long,siteTable$Lat,col=as.numeric(factor(siteTable$Agency)))
these=which(siteTable$Long<(0))
if(length(these)>0){
  siteTable$Long[these] -> store
  siteTable$Long[these] <- -siteTable$Lat[these]
  siteTable$Lat[these] <-  store
  plot(siteTable$Long,siteTable$Lat,col=as.numeric(factor(siteTable$Agency)))
  rm(store)
}
rm(these)


plot(siteTable$Long,siteTable$Lat,col=as.numeric(factor(siteTable$Agency)))
points(siteTable$Long,siteTable$Lat,pch=16,cex=0.2)
table(siteTable$Agency)

tolower(urls$Agency)[!tolower(urls$Agency)%in%tolower(siteTable$Agency)]




#Pull in land use from LCDB
source('K:/R_functions/nzmg2WGS.r')
source('K:/R_functions/nztm2WGS.r')
fwgr <- read_csv('d:/RiverData/FWGroupings.txt')
load("D:/RiverData/REC2_LCDB4.RData",verbose=T) #REC2_LCDB4
rec=read_csv("d:/RiverData/RECnz.txt")
rec$easting=fwgr$easting[match(rec$NZREACH,fwgr$NZREACH)]
rec$northing=fwgr$northing[match(rec$NZREACH,fwgr$NZREACH)]
rm(fwgr)
latLong=nzmg2wgs(East = rec$easting,North = rec$northing)
rec$Long=latLong[,2]
rec$Lat=latLong[,1]
rm(latLong)
rec2=read_csv('D:/RiverData/River_Environment_Classification_(REC2)_New_Zealand.csv')

latLong=nztm2wgs(ce = rec2$upcoordX,cn = rec2$upcoordY)
rec2$Long=latLong[,2]
rec2$Lat=latLong[,1]
rm(latLong)

siteTable$NZReach=as.numeric(siteTable$NZReach)
siteTable$NZSegment=as.numeric(siteTable$NZSegment)
siteTable$Agency[which(is.na(siteTable$NZReach)|siteTable$NZReach<min(rec$NZREACH)|siteTable$NZReach>max(rec$NZREACH))]
siteTable$NZReach[siteTable$NZReach<min(rec$NZREACH)|siteTable$NZReach>max(rec$NZREACH)] <- NA
table(siteTable$Agency[which(is.na(siteTable$NZSegment)|siteTable$NZSegment<min(rec2$nzsegment)|siteTable$NZSegment>max(rec2$nzsegment))])
siteTable$NZSegment[siteTable$NZSegment<min(rec2$nzsegment)|siteTable$NZSegment>max(rec2$nzsegment)] <- NA
siteTable$Landcover=NA #rec
siteTable$Altitude=NA  #rec2

st=1
for(st in st:dim(siteTable)[1]){
  if(is.na(siteTable$NZReach[st])){
    dists=sqrt((siteTable$Long[st]-rec$Long)^2+(siteTable$Lat[st]-rec$Lat)^2)
    mindist=which.min(dists)
    siteTable$NZReach[st] = rec$NZREACH[mindist]
    rm(dists,mindist)
  }
  recMatch = which(rec$NZREACH==siteTable$NZReach[st])
  if(length(recMatch)==0){
    cat(st,'No REC reachmatch\t',siteTable$NZReach[st],'\t\n')
    dists=sqrt((siteTable$Long[st]-rec$Long)^2+(siteTable$Lat[st]-rec$Lat)^2)
    recMatch=which.min(dists)
  }
  #siteTable$Order[st]=paste(unique(rec$ORDER_[recMatch]),collapse='&')
  siteTable$Landcover[st]=paste(unique(rec$LANDCOVER[recMatch]),collapse='&')
  
  rec2match = which(rec2$nzsegment == siteTable$NZSegment[st]) 
  if(length(rec2match)==0){
    dists=sqrt((siteTable$Long[st]-rec2$Long)^2+(siteTable$Lat[st]-rec2$Lat)^2)
    rec2match=which.min(dists)
    cat(st,'No REC2 reachmatch\t',siteTable$Agency[st],'\t',siteTable$NZSegment[st],'\t',
        min(dists,na.rm=T),rec2$nzsegment[rec2match],'\n')
  }
  #  siteTable$StreamOrder[st]=paste(unique(rec2$StreamOrde[rec2match]),collapse='&')
  siteTable$Altitude[st]=mean(rec2$upElev[rec2match],na.rm=T)
  rm(rec2match)
}

write.csv(siteTable,'H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Metadata/SiteTableRawLandUse.csv',row.names=F)
# siteTable = read.csv('H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Metadata/SiteTableRawLandUse.csv',stringsAsFactors = F)

#Categorise landcover and altitude
siteTable$SWQLanduse=tolower(as.character(siteTable$SWQLanduse))
siteTable$SWQLanduse[tolower(siteTable$SWQLanduse)%in%c("unstated","")] <- NA
siteTable$SWQLanduse[tolower(siteTable$SWQLanduse)%in%c("reference","forest","forestry","native","exotic","natural")] <- "forest"

table(siteTable$SWQLanduse,siteTable$Landcover,useNA = 'a')

siteTable$Landcover=tolower(as.character(siteTable$Landcover))
siteTable$rawRecLandcover = siteTable$Landcover
siteTable$Landcover[siteTable$Landcover%in%c('if','ef','s','t','w','b')] <- 'Forest'
siteTable$Landcover[siteTable$Landcover%in%c('p','m')] <- 'Rural'
siteTable$Landcover[siteTable$Landcover%in%c('u')] <- 'Urban'

table(siteTable$SWQLanduse,siteTable$Landcover,useNA = 'a')


siteTable$SWQAltitude[siteTable$SWQAltitude==""] <- "Unstated"
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

siteTable$Agency=tolower(siteTable$Agency)


agencies=c('ac','boprc','ecan','es','gdc','gwrc','hbrc','hrc','mdc','ncc','nrc','orc','tdc','trc','wcrc','wrc')
if(!all(agencies%in%unique(siteTable$Agency))){  #actually if you need to pull sites in from an old WFS sesh, like if one of them doesn respond
  oldsiteTable = read.csv("H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data/2020-09-09/SiteTable_Macro09Sep20.csv",stringsAsFactors = F)
  missingCouncils = agencies[!agencies%in%unique(siteTable$Agency)]
  oldsiteTable=oldsiteTable%>%filter(Agency%in%missingCouncils)
  if(dim(oldsiteTable)[1]>0){
    if(!'NZSegment'%in%names(oldsiteTable)){oldsiteTable$NZSegment=NA}
    siteTable = rbind(siteTable,oldsiteTable%>%select(names(siteTable)))
  }
  rm(oldsiteTable)
}


# siteTable$CouncilSiteID = gsub(pattern = 'Hedgehope Confluence$',replacement = "Hedgehope Confluence ",x = siteTable$CouncilSiteID)

siteTable$LawaSiteID <- gsub(pattern = ' *- *',replacement = '-',x = siteTable$LawaSiteID)


write.csv(x = siteTable,file = paste0("H:/ericg/16666LAWA/LAWA2021/Macroinvertebrates/data/",
                                      format(Sys.Date(),'%Y-%m-%d'),
                                      "/SiteTable_Macro",format(Sys.Date(),'%d%b%y'),".csv"),row.names = F)

#Get numbers of sites per agency ####
AgencyRep=table(factor(tolower(siteTable$Agency),levels=c("ac", "boprc", "ecan", "es", "gdc", "gwrc", "hbrc","hrc", "mdc", 
                                                          "ncc","niwa", "nrc", "orc", "tdc", "trc", "wcrc", "wrc")))
AgencyRep=data.frame(agency=names(AgencyRep),count=as.numeric(AgencyRep))
names(AgencyRep)[2]=format(Sys.Date(),"%d%b%y")
MacroWFSsiteFiles=dir(path = "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/",
                      pattern = 'SiteTable_Macro',
                      recursive = T,full.names = T)
for(wsf in MacroWFSsiteFiles){
  stin=read.csv(wsf,stringsAsFactors=F)
  agencyRep=table(factor(tolower(stin$Agency),levels=c("ac", "boprc", "ecan", "es", "gdc", "gwrc", "hbrc","hrc", "mdc", 
                                                       "ncc","niwa", "nrc", "orc", "tdc", "trc", "wcrc", "wrc")))
  AgencyRep = cbind(AgencyRep,as.numeric(agencyRep))
  colnames(AgencyRep)[dim(AgencyRep)[2]] = strTo(strFrom(wsf,'_Macro'),'.csv')
  rm(agencyRep)
}
AgencyRep=AgencyRep[,-2]

rm(MacroWFSsiteFiles)
write.csv(AgencyRep,'h:/ericg/16666LAWA/LAWA2021/Metadata/AgencyRepMacroWFS.csv',row.names=F)

#      agency 02Jul21 07Jul21 08Jul21 16Jul21 23Jul
# 1      ac      60      60      60  60         61
# 2   boprc     126     126     126  126       126
# 3    ecan     162     162     163  163                263
# 4      es      87      87      87  87         87
# 5     gdc      80      80      80  80           80
# 6    gwrc      53      53      53  53            53
# 7    hbrc      88      87      83  86         85
# 8     hrc       0      87      87  87           87
# 9     mdc      31      31      31  31          31
# 10    ncc      26       0      26  26          26
# 11   niwa      77      77      77  77          77
# 12    nrc      32      32      32  32          32
# 13    orc      31      31      31  31           31
# 14    tdc      24      24      24  24           24
# 15    trc      60      60      60  60           60
# 16   wcrc      34      34      34  34        34 
# 17    wrc      75      75      75  75          75
