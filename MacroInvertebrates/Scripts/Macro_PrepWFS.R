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
  if(urls$Agency[h]%in%c("GDC","NCC")){
    # return(NULL)
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

Sys.time()-startTime  #11s (2020)  24secs (2021)

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



table(siteTable$Agency,useNA = 'a')

table(siteTable$Region,useNA = 'a')
siteTable$Region[siteTable$Agency=='boprc'] <- 'bay of plenty'
siteTable$Region[siteTable$Region=='gdc'|siteTable$Agency=='gdc'] <- 'gisborne'
siteTable$Region[siteTable$Region=='gwrc'|siteTable$Agency=='gwrc'] <- 'wellington'
siteTable$Region[siteTable$Region=='horizons'] <- 'manawat\u16b-whanganui'
siteTable$Region[siteTable$Region=='mdc'|siteTable$Agency=='mdc'] <- 'marlborough'
siteTable$Region[siteTable$Region=='ncc'|siteTable$Agency=='ncc'] <- 'nelson'
siteTable$Region[siteTable$Region=='nrc'|siteTable$Agency=='nrc'] <- 'northland'
siteTable$Region[siteTable$Region=='orc'|siteTable$Agency=='orc'] <- 'otago'
siteTable$Region[siteTable$Region=='tdc'|siteTable$Agency=='tdc'] <- 'tasman'
siteTable$Region[siteTable$Region=='trc'|siteTable$Agency=='trc'] <- 'taranaki'
siteTable$Region[siteTable$Region=='wcrc'|siteTable$Agency=='wcrc'] <- 'west coast'
siteTable$Region[siteTable$Region=='manawatu-whanganui'] <- 'manawat\u16b-whanganui'
table(siteTable$Region,useNA = 'a')

## Swapping coordinate values where necessary
plot(siteTable$Long,siteTable$Lat,col=as.numeric(factor(siteTable$Agency)))


tolower(urls$Agency)[!tolower(urls$Agency)%in%tolower(siteTable$Agency)]



#Pull in land use from LCDB


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
  oldsiteTable = read.csv("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/2021-08-13/SiteTable_Macro13Aug21.csv",stringsAsFactors = F)
  missingCouncils = agencies[!agencies%in%unique(siteTable$Agency)]
  oldsiteTable=oldsiteTable%>%filter(Agency%in%missingCouncils)
  if(dim(oldsiteTable)[1]>0){
    if(!'NZSegment'%in%names(oldsiteTable)){oldsiteTable$NZSegment=NA}
    siteTable = bind_rows(siteTable,oldsiteTable%>%select(matches(names(siteTable))))
  }
  rm(oldsiteTable)
}

#Flick ACs SiteID and CouncilSiteID around
storeACsiteIDs = siteTable$SiteID[siteTable$Agency=='ac']
siteTable$SiteID[siteTable$Agency=='ac'] <- siteTable$CouncilSiteID[siteTable$Agency=='ac']
siteTable$CouncilSiteID[siteTable$Agency=='ac'] <- storeACsiteIDs
  
siteTable$LawaSiteID <- gsub(pattern = ' *- *',replacement = '-',x = siteTable$LawaSiteID)
siteTable$LawaSiteID <- tolower(siteTable$LawaSiteID)

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

#  agency 02Jul21 07Jul21 08Jul21 16Jul21 23Jul21 30Jul21 03Aug21 04Aug21 05Aug21 09Aug21 11Aug21 12Aug21 13  20Aug21
#ac       60      60      60      60      61      61      61      61      61      61      62      62      62      62
#boprc   126     126     126     126     126     126     126     126     126     126     126     132     132     132
#ecan    162     162     163     163     163     163     163     163     163     163     163     163     163     163
#es       87      87      87      87      87      87      87      87      87      87      87      87      87      87
#gdc      80      80      80      80      80      80      80      80      80      80      80      80      80      80
#gwrc     53      53      53      53      53      53      53      53      53      53      53      53      53      53
#hbrc     88      87      83      86      85      85      85      85      85      85      85      85      85      85
#hrc       0      87      87      87      87      87      87      87      87      87      87      87      87      87
#mdc      31      31      31      31      31      31      31      31      31      31      31      31      31      31
#ncc      26       0      26      26      26      26      26      26      26      26      26      26      26      26
#niwa     77      77      77      77      77      77      77      77      77      77      77      77      77      77
#nrc      32      32      32      32      32      32      32      32      32      32      32      33      33      33
#orc      31      31      31      31      31      31      31      31      31      31      31      31      31      31
#tdc      24      24      24      24      24      24      24      24      24      24      24      24      24      24
#trc      60      60      60      60      60      60      60      60      60      60      60      60      60      60
#wcrc     34      34      34      34      34      34      34      34      34      34      34      35      35      35
#wrc      75      75      75      75      75      75      75      75      75      75      75      75      75      75

#agency 20Aug21   08Sep21
#ac         62        62
#boprc     132       132
#ecan      163       163
#es         87        87
#gdc        80        80
#gwrc       53        55
#hbrc       85        85
#hrc        87        87
#mdc        31        31
#ncc        26        26
#niwa       77        57
#nrc        33        33
#orc        31        31
#tdc        24        24
#trc        60        58
#wcrc       35        35
#wrc        75        75