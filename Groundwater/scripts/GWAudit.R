rm(list=ls())
library(XML)
library(tidyverse)
library(parallel)
library(doParallel)
source('H:/ericg/16666LAWA/LAWA2021/Scripts/LAWAFunctions.R')
dir.create(paste0("H:/ericg/16666LAWA/LAWA2021/Groundwater//Audit/",format(Sys.Date(),"%Y-%m-%d")))
StartYear5 <- lubridate::isoyear(Sys.Date())-5  #2014
EndYear <-lubridate::isoyear(Sys.Date())-1    #2018
periodYrs=5

GWdata=read.csv(tail(dir(path='h:/ericg/16666LAWA/LAWA2021/Groundwater/Data/',pattern='GWdata.csv',full.names = T,recursive =T) ,1),stringsAsFactors = F)
GWdata$Agency=factor(GWdata$Source,labels=c('ac','boprc','ecan','gdc','hbrc','hrc','mdc',
                                            'nrc','orc','es','trc','tdc','wrc','gwrc','wcrc'))

#Per agency audit site/measurement start, stop, n and range ####
workers <- makeCluster(5)
registerDoParallel(workers)
clusterCall(cl = workers,fun = function(){
    library(tidyverse)
  library(lubridate)
})
# for(agency in c("ac", "boprc", "ecan", "gdc", "hbrc", "hrc", "mdc","nrc", "orc", "es", "trc", "tdc", "wrc", "gwrc", "wcrc")){
foreach(agency = c("ac", "boprc", "ecan", "gdc", "hbrc", "hrc", "mdc",
                   "nrc", "orc", "es", "trc", "tdc", "wrc", "gwrc", "wcrc"),
        .combine=rbind,.errorhandling="stop")%dopar%{
          agencyGWdata=GWdata%>%filter(Agency==agency)%>%
            dplyr::filter(lubridate::year(myDate)>=(EndYear-periodYrs+1)&lubridate::year(myDate)<=EndYear)
          if(!is.null(agencyGWdata)&&dim(agencyGWdata)[1]>0){
            nvar=length(uvars <- unique(agencyGWdata$Measurement))
            nsite=length(usites <- unique(agencyGWdata$LawaSiteID))
            siteTerm="LawaSiteID"
            agencyDeets=as.data.frame(matrix(nrow=nvar*nsite,ncol=8))
            names(agencyDeets)=c("Site",'Bore',"Var","StartDate","EndDate","nMeas","MinVal","MaxVal") #'Aquifer',
            r=1
            for(ns in 1:nsite){
              boreID=GWdata$Bore_ID[match(tolower(usites[ns]),tolower(GWdata$LawaSiteID))]
              if(is.null(boreID)||is.na(boreID)){
                boreID=GWdata$Site_ID[match(tolower(usites[ns]),tolower(GWdata$LawaSiteID))]
              }
              # aquiferID=GWdata$Aquifer_Object_ID[match(tolower(usites[ns]),tolower(GWdata$LawaSiteID))]
              # if(is.null(aquiferID)||is.na(aquiferID)){
              #   aquiferID=GWdata$Aquifer_Name[match(tolower(usites[ns]),tolower(GWdata$LawaSiteID))]
              # }
              # if(is.null(aquiferID)||is.na(aquiferID)){
              #   aquiferID = 'NA'
              # }
              for(nv in 1:nvar){
                these=which(agencyGWdata[,siteTerm]==usites[ns]&agencyGWdata$Measurement==uvars[nv])
                agencyDeets[r,]=c(usites[ns],                               #Site
                                  boreID,                                   #Bore
                                  # aquiferID,                                #Aquifer
                                  uvars[nv],                                #Var
                                  as.character(min(ymd(agencyGWdata$myDate[these]))),  #StartDate
                                  as.character(max(ymd(agencyGWdata$myDate[these]))),  #EndDate
                                  length(these),                            #nMeas
                                  min(agencyGWdata$Value[these],na.rm=T),            #minval
                                  max(agencyGWdata$Value[these],na.rm=T))            #maxval
                r=r+1
              }
            }
            write.csv(agencyDeets,paste0("h:/ericg/16666LAWA/LAWA2021/Groundwater/Audit/",format(Sys.Date(),"%Y-%m-%d"),
                                         "/",agency,format(Sys.Date(),'%d%b%y'),"audit.csv"),row.names = F)
          }
          rm(agencyGWdata)
          return(NULL)
        }
stopCluster(workers)
remove(workers)



#Audit plots to allow comparison between agencies - check units consistency etc  ####
wqds <- GWdata%>%dplyr::group_by(LawaSiteID,Measurement,myDate)%>%
  dplyr::summarise(.groups='keep',
                   Agency=first(Agency),
            median=quantile(Value,probs=0.5,type=5,na.rm=T))%>%
  pivot_wider(names_from=Measurement,values_from=median)%>%as.data.frame
# wqd=doBy::summaryBy(data=GWdata,
#                     formula=Value~LawaSiteID+Measurement+myDate,
#                     id=~Agency,
#                     FUN=median)
# wqds=spread(wqd,Measurement,Value.median)

params=names(wqds)
for(param in 4:length(params)){
  png(filename = paste0("h:/ericg/16666LAWA/LAWA2021/Groundwater/Audit/",
                        format(Sys.Date(),"%Y-%m-%d"),"/",
                        gsub(pattern = '\\.',replacement='',make.names(names(wqds)[param])),
                        format(Sys.Date(),'%d%b%y'),".png"),width = 12,height=9,units='in',res=300,type='cairo')
  toplot=which(!is.na(wqds[,param]) & wqds[,param]>0)
    plot(as.factor(wqds$Agency[toplot]),wqds[toplot,param],
         ylab=names(wqds)[param],main=names(wqds)[param],log='y')
  if(names(dev.cur())=='png'){dev.off()}
}



#And the ubercool html summary audit report doncuments per council!
for(agency in c("ac", "boprc", "ecan","es", "gdc",  "gwrc","hbrc", "hrc", "mdc",
                   "nrc", "orc",  "trc", "tdc", "wrc", "wcrc")){
          if(length(dir(path = paste0("H:/ericg/16666LAWA/LAWA2021/Groundwater/Audit/",format(Sys.Date(),"%Y-%m-%d")),
                pattern = paste0(agency,".*audit\\.csv"),
                recursive = T,full.names = T,ignore.case = T))>0){
            rmarkdown::render('H:/ericg/16666LAWA/LAWA2021/GroundWater/Scripts/GWAuditDocument.Rmd',
                              params=list(agency=agency),
                              # intermediates_dir=paste0("H:/ericg/16666LAWA/LAWA2021/GroundWater/Audit/",format(Sys.Date(),"%Y-%m-%d"),'/',agency),
                              output_dir = paste0("H:/ericg/16666LAWA/LAWA2021/GroundWater/Audit/",format(Sys.Date(),"%Y-%m-%d")),
                              output_file = paste0(toupper(agency),"Audit",format(Sys.Date(),'%d%b%y'),".html"),
                              envir = new.env(),clean=TRUE)
            file.copy(from = paste0("H:/ericg/16666LAWA/LAWA2021/GroundWater/Audit/",format(Sys.Date(),"%Y-%m-%d"),"/",toupper(agency),"Audit",format(Sys.Date(),'%d%b%y'),".html"),
              to = "C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021/Groundwater Quality/Audits")
          }
}


