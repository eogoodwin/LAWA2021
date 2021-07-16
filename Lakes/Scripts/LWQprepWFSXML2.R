rm(list = ls())
library(tidyverse)
library(parallel)
library(doParallel)
setwd('h:/ericg/16666LAWA/LAWA2021/Lakes')
source("h:/ericg/16666LAWA/LAWA2021/scripts/lawaFunctions.R")

dir.create(paste0("H:/ericg/16666LAWA/LAWA2021/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)


urls          <- read.csv("H:/ericg/16666LAWA/LAWA2021/Metadata/CouncilWFS.csv",stringsAsFactors=FALSE)

# Config for data extract from WFS
vars <- c("CouncilSiteID","SiteID","LawaSiteID",
          "LFENZID","LType","GeomorphicLType",
          "Region","Agency","Catchment")

if(exists('siteTable')){
  rm(siteTable)
}
#For each council, find the "emar:LWQuality" sites that are switched on, and get their details
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
  if(urls$Agency[h]%in%c('GDC','MDC','NCC','TDC')){
    return(NULL)
  }
  
  xmldata <- try(ldWFSlist(urlIn = urls$URL[h],agency=urls$Agency[h],dataLocation = urls$Source[h],case.fix = TRUE))
  if(is.null(xmldata)||'try-error'%in%attr(xmldata,'class')){#||grepl(pattern = '^501|error',
    # x=xmlValue(getNodeSet(xmldata,'/')[[1]]),
    # ignore.case=T)){
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
    ### Determine the values used in the [emar:SWQuality] element
    cat("\n",urls$Agency[h],"\n---------------------------\n",urls$URL[h],"\n",sep="")
    
    # Determine number of records in a wfs with module before attempting to extract all the necessary columns needed
    lakeNodes=unname(which(sapply(xmldata,FUN=function(li){
      'LWQuality'%in%names(li)&&li$LWQuality%in%c("Yes","yes","YES","Y","y","TRUE","true","T","t","True")})))
    lakeData=lapply(lakeNodes,FUN=function(li){
      liout <- unlist(xmldata[[li]][vars])
      })
    if(length(lakeNodes)==0){
      cat(urls$Agency[h],"has no records for <emar:LWQuality>\n")
    } else {
    if(!(all(lengths(lakeData)==length(vars)))){
      missingvars=which(lengths(lakeData)<length(vars))
      for(mv in missingvars){
        missingVarNames = vars[!vars%in%names(lakeData[[mv]])]
        for(mvn in missingVarNames){
          eval(parse(text=paste0("lakeData[[mv]]=c(lakeData[[mv]],",mvn,"=NA)")))
        }
      }
    }
      lakeData <- bind_rows(lakeData)
      lakeData <- as.data.frame(lakeData,stringsAsFactors=FALSE)
      #Do lat longs separately from other WQParams because they are value pairs and need separating
      lakePoints=t(sapply(lakeNodes,FUN=function(li){
        liout <- unlist(xmldata[[li]]$Shape$Point$pos)
        if(is.null(liout)){liout <- unlist(xmldata[[li]]$SHAPE$Point$pos)}
        as.numeric(unlist(strsplit(liout,' ')))
      }))
      lakePoints=as.data.frame(lakePoints,stringsAsFactors=F)
      names(lakePoints)=c("Lat","Long")
      lakeData <- cbind(lakeData,lakePoints)
      lakeData$accessDate=format(Sys.Date(),"%d-%b-%Y")
      lakeData$Agency=urls$Agency[h]
     rm(xmldata)
      return(lakeData)
    }
  }
}->siteTable
stopCluster(workers)
rm(workers)

Sys.time()-startTime  #27 minutes with NCC, 14 seconds without.

siteTable$SiteID=as.character(siteTable$SiteID)
siteTable$LawaSiteID=as.character(siteTable$LawaSiteID)
siteTable$Region=tolower(as.character(siteTable$Region))
siteTable$Agency=tolower(as.character(siteTable$Agency))



table(siteTable$region)
siteTable$Region[siteTable$Region=='alexandra'] <- 'otago'
siteTable$Region[siteTable$Region=='dunedin'] <- 'otago'
siteTable$Region[siteTable$Region=='tekapo'] <- 'otago'
siteTable$Region[siteTable$Region=='orc'] <- 'otago'
siteTable$Region[siteTable$Region=='christchurch'] <- 'canterbury'
siteTable$Region[siteTable$Region=='gdc'] <- 'gisborne'
siteTable$Region[siteTable$Region=='greymouth'] <- 'west coast'
siteTable$Region[siteTable$Region=='wcrc'] <- 'west coast'
siteTable$Region[siteTable$Region=='hamilton'] <- 'waikato'
siteTable$Region[siteTable$Region=='turangi'] <- 'waikato'
siteTable$Region[siteTable$Region=='wrc'] <- 'waikato'
siteTable$Region[siteTable$Region=='havelock nth'] <- 'hawkes bay'
siteTable$Region[siteTable$Region=='hbrc'] <- 'hawkes bay'
siteTable$Region[siteTable$Region=='ncc'] <- 'nelson'
siteTable$Region[siteTable$Region=='rotorua'] <- 'bay of plenty'
siteTable$Region[siteTable$Region%in%c('wanganui','horizons','hrc')] <- 'manawat\u16b-whanganui'
siteTable$Region[siteTable$Region=='gwrc'] <- 'wellington'
siteTable$Region[siteTable$Region=='whangarei'] <- 'northland'
siteTable$Region[siteTable$Region=='nrc'] <- 'northland'
siteTable$Region[siteTable$Region=='mdc'] <- 'marlborough'
siteTable$Region[siteTable$Region=='tdc'] <- 'tasman'
siteTable$Region[siteTable$Region=='trc'] <- 'taranaki'
siteTable$Region[siteTable$Region=='gwrc'] <- 'wellington'
table(siteTable$Region)

################################################################################################



siteTable$Agency=tolower(siteTable$Agency)
siteTable$Agency[siteTable$Agency=='arc'] <- 'ac'
siteTable$Agency[siteTable$Agency=='auckland council'] <- 'ac'
siteTable$Agency[siteTable$Agency=='christchurch'] <- 'ecan'
siteTable$Agency[siteTable$Agency=='environment canterbury'] <- 'ecan'
table(siteTable$Agency)

par(mfrow=c(1,1))
plot(siteTable$Long,siteTable$Lat,col=as.numeric(factor(siteTable$Agency)),asp=1)
points(siteTable$Long,siteTable$Lat,pch=16,cex=0.2)

#Find which agencies have no lakes yet
tolower(urls$Agency)[!tolower(urls$Agency)%in%tolower(siteTable$Agency)]




siteTable$LFENZID[which(siteTable$CouncilSiteID=="SQ36148")] <- (-16)
# siteTable$LFENZID[grepl('brunner',siteTable$CouncilSiteID,ignore.case=T)&siteTable$Agency=='wcrc'] <- 38974
siteTable$LFENZID[grepl('haupiri',siteTable$CouncilSiteID,ignore.case=T)&siteTable$Agency=='wcrc'] <- 39225


FENZlake = read.csv("D:/RiverData/FENZLake.txt",stringsAsFactors = F)[,c(2,3,13,14,15,17,26,27,32,33,34,35,36,38)]
cbind(siteTable$Agency[siteTable$GeomorphicLType==""],
      siteTable$CouncilSiteID[siteTable$GeomorphicLType==""],
      FENZlake$GeomorphicType[match(siteTable$LFENZID[siteTable$GeomorphicLType==""],FENZlake$LID)])

siteTable$GeomorphicLType[which(is.na(siteTable$GeomorphicLType)|siteTable$GeomorphicLType=="")] <- 
  FENZlake$GeomorphicType[match(siteTable$LFENZID[which(is.na(siteTable$GeomorphicLType)|siteTable$GeomorphicLType=="")],FENZlake$LID)]


siteTable$LType[which(siteTable$LType%in%c("polymictc","artificall"))] <- "polymictic"
siteTable$LType[which(siteTable$LType%in%c("monomictic"))] <- "stratified"
siteTable$LType=pseudo.titlecase(tolower(siteTable$LType))
siteTable$GeomorphicLType=pseudo.titlecase(tolower(siteTable$GeomorphicLType))

table(siteTable$Agency,siteTable$LType,useNA = 'a')
table(siteTable$Agency,siteTable$GeomorphicLType,useNA='a')

if(!all(c('ac','boprc','ecan','es','gwrc','hbrc','hrc','nrc','orc','trc','wcrc','wrc')%in%unique(siteTable$Agency))){  #actually if you need to pull sites in from an old WFS sesh, like if one of them doesn respond
  oldsiteTable = read.csv("H:/ericg/16666LAWA/LAWA2020/Lakes/Data/2020-07-31/SiteTable_Lakes31Jul20.csv",stringsAsFactors = F)
  missingCouncils = c('ac','boprc','ecan','es','gwrc','hbrc','hrc','nrc','orc','trc','wcrc','wrc')[!c('ac','boprc','ecan','es','gwrc','hbrc','hrc','nrc','orc','trc','wcrc','wrc')%in%unique(siteTable$Agency)]
  oldsiteTable=oldsiteTable%>%filter(Agency%in%missingCouncils)
  if(dim(oldsiteTable)[1]>0){
    siteTable = rbind(siteTable,oldsiteTable)
  }
  rm(oldsiteTable)
}


## Output for next script
write.csv(x = siteTable,file = paste0("h:/ericg/16666LAWA/LAWA2021/Lakes/Data/",
                                      format(Sys.Date(),'%Y-%m-%d'),
                                      "/SiteTable_Lakes",format(Sys.Date(),'%d%b%y'),".csv"),row.names=F)

AgencyRep=table(factor(tolower(siteTable$Agency),levels=c("ac", "boprc", "ecan", "es", "gdc", "gwrc", "hbrc","hrc", "mdc", 
                                                "ncc", "nrc", "orc", "tdc", "trc", "wcrc", "wrc")))
LakeWFSsiteFiles=dir(path = "H:/ericg/16666LAWA/LAWA2021/Lakes/Data/",
                     pattern = 'SiteTable_Lakes',
                     recursive = T,full.names = T)
for(wsf in LakeWFSsiteFiles){
  stin=read.csv(wsf,stringsAsFactors=F)
  agencyRep=table(factor(tolower(stin$Agency),levels=c("ac", "boprc", "ecan", "es", "gdc", "gwrc", "hbrc","hrc", "mdc", 
                                                       "ncc", "nrc", "orc", "tdc", "trc", "wcrc", "wrc")))
  AgencyRep = cbind(AgencyRep,as.numeric(agencyRep))
  colnames(AgencyRep)[dim(AgencyRep)[2]] = strTo(strFrom(wsf,'_Lakes'),'.csv')
  rm(agencyRep)
}
AgencyRep=AgencyRep[,-1]
colnames(AgencyRep)[dim(AgencyRep)[2]]=format(Sys.Date(),'%d%b%y')
rm(LakeWFSsiteFiles)
write.csv(AgencyRep,'h:/ericg/16666LAWA/LAWA2021/Metadata/AgencyRepLakeWFS.csv')

#       02Jul21 07Jul21 08Jul21 16Jul21
# ac          4       4       4  4
# boprc      13      13      13  13
# ecan       43      43      44  44
# es         18      18      18  18
# gdc         0       0       0 0
# gwrc        5       5       5  5
# hbrc        7       7       7  7
# hrc        15      15      15  15
# mdc         0       0       0 0
# ncc         0       0       0 0
# nrc        25      25      25  25
# orc        14      14      14  14
# tdc         0       0       0 0
# trc         3       3       9  9
# wcrc        3       3       3  3
# wrc        12      12      12  12


# st14apr=read.csv('h:/ericg/16666LAWA/LAWA2021/Lakes/Data/2020-04-14/SiteTable_Lakes14Apr20.csv',stringsAsFactors = F)
