# ------------------------------
# BATCH LOADER FOR COUNCIL DATA
# ------------------------------
rm(list=ls())
dir.create(path = paste0("H:/ericg/16666LAWA/LAWA2021/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)

#This is find out what they had last year?
# ldl=readxl::read_xlsx("h:/ericg/16666LAWA/LAWA2021/Lakes/Data/LakeDownloadData.xlsx",sheet = "Lakes Data")
# by(data = ldl[,7:14],INDICES = ldl$rcid,FUN = function(x)apply(x,2,FUN=function(x)any(!is.na(x))))
# rm(ldl)
## ----------------------------------------------------------------------------,
## Import Lake data to the "1.Imported" folder for 2018

## import destination will be in folder with todays date (created above)
# importDestination <- paste("H:/ericg/16666LAWA/LAWA2021/Lakes/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",sep="")
source('h:/ericg/16666LAWA/LAWA2021/scripts/LAWAFunctions.R')
siteTable=loadLatestSiteTableLakes()
siteTable$Agency=tolower(siteTable$Agency)


scriptsToRun = c("H:/ericg/16666LAWA/LAWA2021/Lakes/Scripts/loadES.R",
                 # "H:/ericg/16666LAWA/LAWA2021/Lakes/Scripts/loadGDC.R", # no lakes
                 "H:/ericg/16666LAWA/LAWA2021/Lakes/Scripts/loadGW.R",
                 "H:/ericg/16666LAWA/LAWA2021/Lakes/Scripts/loadHBRC.R",
                 "H:/ericg/16666LAWA/LAWA2021/Lakes/Scripts/loadHRC.R",
                 # "H:/ericg/16666LAWA/LAWA2021/Lakes/Scripts/loadMDC.R",  #  no lakes
                 # "H:/ericg/16666LAWA/LAWA2021/Lakes/Scripts/loadNCC.R", # no lakes
                 "H:/ericg/16666LAWA/LAWA2021/Lakes/Scripts/loadORC.R",
                 # "H:/ericg/16666LAWA/LAWA2021/Lakes/Scripts/loadTDC.R",  #no lakes
                 "H:/ericg/16666LAWA/LAWA2021/Lakes/Scripts/loadTRC.R")
source("H:/ericg/16666LAWA/LAWA2021/Lakes/Scripts/loadAC.R")
source("H:/ericg/16666LAWA/LAWA2021/Lakes/Scripts/loadBOP_list.R")
source("H:/ericg/16666LAWA/LAWA2021/Lakes/Scripts/loadECAN_list.R")
source("H:/ericg/16666LAWA/LAWA2021/Lakes/Scripts/loadNRC_list.R")
source("H:/ericg/16666LAWA/LAWA2021/Lakes/Scripts/loadWCRC_list.R")
source("H:/ericg/16666LAWA/LAWA2021/Lakes/Scripts/loadWRC.R")

xmlAgencies = c('es','gwrc','hbrc','hrc','orc','trc')
csvAgencies = c('ac','boprc','ecan','nrc','wcrc','wrc')


workers <- makeCluster(4)
startTime=Sys.time()
registerDoParallel(workers)
clusterCall(workers,function(){
  source('H:/ericg/16666LAWA/LAWA2021/scripts/LAWAFunctions.R',echo=F)
})
foreach(i = 1:length(scriptsToRun),.errorhandling = "stop")%dopar%{
  source(scriptsToRun[i])
  return(NULL)
}
stopCluster(workers)
rm(workers)
cat("Done load\n")
Sys.time()-startTime #2.9mins
 


for(ag in xmlAgencies){
  checkXMLageLakes(agency = ag)
  # checkCSVageLakes(ag)
}
for(ag in c(xmlAgencies,csvAgencies)){
  # checkXMLageLakes(agency = ag)
  checkCSVageLakes(ag)
}



##############################################################################
#                                 XML to CSV
##############################################################################
lawaset=c("TN","NH4N","TP","CHLA","pH","Secchi","ECOLI","CYANOTOT","CYANOTOX")


for(agency in xmlAgencies){
   suppressWarnings({rm(forcsv)})
  forcsv=xml2csvLake(agency=agency,maxHistory = 40,quiet=F)
  cat(length(unique(forcsv$Measurement)),paste(unique(forcsv$Measurement),collapse=', '),'\n')
  if(is.null(forcsv)){next}
  
  if(agency=='nrc'){
    forcsv$Value[forcsv$Measurement=="Chlorophyll a"] = as.numeric(forcsv$Value[forcsv$Measurement=="Chlorophyll a"])*1000 #g/m3 to mg/m3
  }
  
  #In SWQ this is done by the transfers table file
  forcsv$Measurement[grepl(pattern = 'Transparency|Secchi|Clarity|Black',x = forcsv$Measurement,ignore.case = T)] <- "Secchi"
  forcsv$Measurement[grepl(pattern = 'loroph|CHL|hloro',x = forcsv$Measurement,ignore.case = T)] <- "CHLA"
  forcsv$Measurement[grepl(pattern = 'coli|ecol',x = forcsv$Measurement,ignore.case = T)] <- "ECOLI"
  forcsv$Measurement[grepl(pattern = 'phosphorus|phosphorous|TP|P _Tot|Tot P',x = forcsv$Measurement,ignore.case = T)] <- "TP"
  forcsv$Measurement[grepl(pattern = 'Ammonia|NH4',x = forcsv$Measurement,ignore.case = T)] <- "NH4N"
  forcsv$Measurement[grepl(pattern = 'TN..HRC.|total nitrogen|totalnitrogen|Nitrogen..Total.|N _Tot|Tot N',
                            x = forcsv$Measurement,ignore.case = T)] <- "TN" #Note, might be nitrate nitrogen
  forcsv$Measurement[grepl(pattern = 'field ph|ph \\(field\\)|ph \\(lab\\)|pH_Lab|pH \\(pH|pH \\(Disc',
                           x = forcsv$Measurement,ignore.case = T)] <- "pH"
  forcsv$Measurement[grepl(pattern = '.*tox.*',
                           x = forcsv$Measurement,ignore.case = T)] <- "CYANOTOX"
  forcsv$Measurement[grepl(pattern = '.*Cyanobacteria BioVolume.*',
                           x = forcsv$Measurement,ignore.case = T)] <- "CYANOTOT"
  
  cat(length(unique(forcsv$Measurement)),paste(unique(forcsv$Measurement),collapse='\t'),'\n')
  cat(agency,'\tmissing\t',lawaset[!lawaset%in%unique(forcsv$Measurement)],'\n') #Missing Measurements
  excess=unique(forcsv$Measurement)[!unique(forcsv$Measurement)%in%lawaset] #Surplus Measurements
  if(length(excess)>0){
    browser()
    forcsv=forcsv[-which(forcsv$Measurement%in%excess),]
  }
  rm(excess)
  prenacount=sum(is.na(forcsv$Value))
  forcsv$Value=as.numeric(forcsv$Value)
  stopifnot(sum(is.na(forcsv$Value))==prenacount)
  rm(prenacount)
  write.csv(forcsv,
            file=paste0( 'H:/ericg/16666LAWA/LAWA2021/Lakes/Data/',format(Sys.Date(),"%Y-%m-%d"),'/',agency,'.csv'),
            row.names=F)
  suppressWarnings({rm(forcsv)})
}

##############################################################################
#                                 COMBO
##############################################################################
#Build the combo ####
library(parallel)
library(doParallel)
lawaset=c("TN","NH4N","TP","CHLA","pH","Secchi","ECOLI","CYANOTOT","CYANOTOX")
siteTable=loadLatestSiteTableLakes()
rownames(siteTable)=NULL
suppressWarnings(rm(lakeData,"ac","boprc","ecan","es","gwrc","hbrc","hrc","nrc","orc","trc","wcrc","wrc" ))
agencies= sort(unique(siteTable$Agency))
workers=makeCluster(4)
registerDoParallel(workers)
startTime=Sys.time()
foreach(agency =1:length(agencies),.combine = rbind,.errorhandling = 'stop')%dopar%{
  # for(agency in 1:length(agencies)){
  mfl=loadLatestCSVLake(agencies[agency],maxHistory = 30)
  if(!is.null(mfl)&&dim(mfl)[1]>0){
    if(agencies[agency]=='boprc'){
      mfl$CouncilSiteID[which(tolower(mfl$CouncilSiteID)=='fi680541')]="fi680541_int"
    }
    if(sum(!tolower(mfl$CouncilSiteID)%in%tolower(siteTable$CouncilSiteID))>0){
      cat('\t',sum(!unique(tolower(mfl$CouncilSiteID))%in%tolower(siteTable$CouncilSiteID)),'CouncilSiteIDs not in site table\n')
      cat('\t',sum(!unique(tolower(mfl$LawaSiteID))%in%tolower(siteTable$LawaSiteID)),'LawaSiteIDs not in site table\n')
    }
    mfl$agency=agencies[agency]
    # ,'nrc'    because NRC has two CHLA measureents, and only one of them needs converting, I'll have to do it prriot to transfering names
    if(agencies[agency] %in% c('es','wrc')){ #es mg/L  arc mg/L  nrc g/m3             wanted in mg/m3
      mfl$Value[mfl$Measurement=="CHLA"]=mfl$Value[mfl$Measurement=="CHLA"]*1000
    }
    
    targetSites=tolower(siteTable$CouncilSiteID[siteTable$Agency==agencies[agency]])
    targetCombos=apply(expand.grid(targetSites,tolower(lawaset)),MARGIN = 1,FUN=paste,collapse='')
    currentSiteMeasCombos=unique(paste0(tolower(mfl$CouncilSiteID),tolower(mfl$Measurement)))
    missingCombos=targetCombos[!targetCombos%in%currentSiteMeasCombos]
    #BackFill
    if(length(missingCombos)>0){
      agencyFiles = rev(dir(path = "H:/ericg/16666LAWA/LAWA2021/Lakes/Data/",
                            pattern = paste0('^',agencies[agency],'.csv'),
                            full.names = T,recursive = T,ignore.case = T))[-1]
      for(af in seq_along(agencyFiles)){
        agencyCSV = read.csv(agencyFiles[af],stringsAsFactors=F)
        if(agencies[agency]=='boprc'){
          agencyCSV$CouncilSiteID[which(tolower(agencyCSV$CouncilSiteID)=='fi680541')]="fi680541_int"
        }
        if("Measurement"%in%names(agencyCSV)){
          agencyCSVsiteMeasCombo=paste0(tolower(agencyCSV$CouncilSiteID),tolower(agencyCSV$Measurement))
          if(any(missingCombos%in%unique(agencyCSVsiteMeasCombo))){
      # browser()
            these=agencyCSVsiteMeasCombo%in%missingCombos
            agencyCSV=agencyCSV[these,]
            agencyCSV$agency=agencies[agency]
            if('QC'%in%names(mfl)&!'QC'%in%names(agencyCSV)){
              agencyCSV$QC <- ''
            }
            if(any(is.na(agencyCSV$centype)|agencyCSV$centype=="F")){
              agencyCSV$centype[is.na(agencyCSV$centype)|agencyCSV$centype=="F"] <- FALSE
            }
            mfl=rbind(mfl,agencyCSV[,names(mfl)])
            rm(agencyCSV,these)
            currentSiteMeasCombos=unique(paste0(tolower(mfl$CouncilSiteID),tolower(mfl$Measurement)))
            missingCombos=targetCombos[!targetCombos%in%currentSiteMeasCombos]
            if(length(missingCombos)==0){
              break
            }            
          }
          rm(agencyCSVsiteMeasCombo)
        }
        suppressWarnings(rm(agencyCSV))
        
      }
    }
    rm(missingCombos,targetCombos,currentSiteMeasCombos,targetSites)
    
    if(agencies[agency]=='niwa'){
      mfl$Value[mfl$Measurement%in%c("DRP","NH4","TN","TP")]=mfl$Value[mfl$Measurement%in%c("DRP","NH4","TN","TP")]/1000
    }
    
  }
  # eval(parse(text=paste0(agencies[agency],'mfl=mfl')));lakeData=rbind(lakeData,mfl)}
  return(mfl)
}->lakeData 
stopCluster(workers)
rm(workers)
Sys.time()-startTime #0.3s
#23Jun 70297
#25Jun 77483
#9July 79582
#24/7/2020 80692
#7/8/20 80660
#14/8/20 71344
#218/20 71731
# 14/9/2020 72954
# 21/9/2020 72983
# 23/9/2020 72983
# 2/7/2021 60039
# 7/7 2021 60484
# 12/7/21  63224
# 16/7/2021 69909
# 30/7/2021 70855
# 5/8/21   79980
# 13/8/21  80083
# 20/8/21  81622
# 25/8/21 80972 8.5 s 
# 26/8/21 81622
# 27/8/21 81743 ES added cyano 09:11am

#September 2020 TRC had a bunch of lake sites with the same LAWAID. Simplify those.
lakeData <- lakeData%>%filter(!CouncilSiteID%in%c('lrt00h450','lrt00h300'))
lakeData <- lakeData%>%filter(!(CouncilSiteID%in%c('lrt00s450','lrt00s300')&Measurement=='pH'))
lakeData$CouncilSiteID[grepl(pattern = 'lrt00.300',lakeData$CouncilSiteID)] = 'lrt00x300'
lakeData$CouncilSiteID[grepl(pattern = 'lrt00.450',lakeData$CouncilSiteID)] = 'lrt00x450'

if('lrt00h300'%in%tolower(siteTable$CouncilSiteID)){
  siteTableRep=siteTable%>%filter(!grepl('lrt00.300',CouncilSiteID,T))
  lrt300rep = siteTable%>%filter(grepl('lrt00.300',CouncilSiteID,T))%>%
    mutate(CouncilSiteID='lrt00x300',Lat=mean(Lat),Long=mean(Long))
  siteTableRep=siteTableRep%>%filter(!grepl('lrt00.450',CouncilSiteID,T))
  lrt450rep = siteTable%>%filter(grepl('lrt00.450',CouncilSiteID,T))%>%
    mutate(CouncilSiteID='lrt00x450',Lat=mean(Lat),Long=mean(Long))
  siteTableRep=rbind(siteTableRep,lrt300rep[1,],lrt450rep[1,])
  write.csv(x = siteTableRep,file = paste0("h:/ericg/16666LAWA/LAWA2021/Lakes/Data/",
                                           format(Sys.Date(),'%Y-%m-%d'),
                                           "/SiteTable_Lakes",format(Sys.Date(),'%d%b%y'),".csv"),row.names=F)
  siteTable=siteTableRep
  rm(siteTableRep,lrt450rep,lrt300rep)
}
#81093


write.csv(lakeData,paste0('h:/ericg/16666LAWA/LAWA2021/Lakes/Data/',format(Sys.Date(),"%Y-%m-%d"),'/LakesCombined.csv'),row.names = F)
# lakeData=read.csv(tail(dir(path='h:/ericg/16666LAWA/LAWA2021/Lakes/Data/',pattern='LakesCombined',recursive=T,full.names = T,ignore.case = T),1),stringsAsFactors=F)
lakeData$LawaSiteID = siteTable$LawaSiteID[match(tolower(lakeData$CouncilSiteID),tolower(siteTable$CouncilSiteID))]
lakeData$LawaSiteID[which(is.na(lakeData$LawaSiteID))]=siteTable$LawaSiteID[match(tolower(lakeData$CouncilSiteID[is.na(lakeData$LawaSiteID)]),
                                                                     tolower(siteTable$SiteID))]

lakeData%>%group_by(LawaSiteID)%>%
  dplyr::summarise(agCount=length(unique(agency)),
                   ags=paste(unique(agency),collapse=' '),
                   cid=paste(unique(CouncilSiteID),collapse=', '))%>%
  ungroup%>%
  filter(agCount>1)%>%dplyr::select(-agCount)

lakeData <- lakeData%>%drop_na(LawaSiteID)


# lakeData$LawaSiteID[tolower(lakeData$CouncilSiteID) =="omanuka lagoon (composite)"] = siteTable$LawaSiteID[tolower(siteTable$CouncilSiteID) =="omanuka lagoon (composite)"]

lakeData$CouncilSiteID=tolower(lakeData$CouncilSiteID)
siteTable$CouncilSiteID=tolower(siteTable$CouncilSiteID)
lakesWithMetadata=merge(lakeData,siteTable,by="CouncilSiteID",all.x=T,all.y=F)

missingCouncilSiteIDs=unique(lakesWithMetadata$CouncilSiteID.x[is.na(lakesWithMetadata$LawaSiteID)])
missingCouncilSiteIDs%in%siteTable$CouncilSiteID
missingCouncilSiteIDs%in%siteTable$SiteID
missingCouncilSiteIDs%in%siteTable$LawaSiteID

# lakesWithMetadata$CouncilSiteID.x[is.na(lakesWithMetadata$CouncilSiteID.x)] <- 
#   lakesWithMetadata$CouncilSiteID.y[is.na(lakesWithMetadata$CouncilSiteID.x)]


lakesWithMetadata <- lakesWithMetadata%>%
  dplyr::select(-LawaSiteID.y,-agency,-Method)%>%
  dplyr::rename(LawaSiteID=LawaSiteID.x)%>%
  dplyr::select(LawaSiteID,CouncilSiteID,SiteID,Agency,Region,everything())


table(lakesWithMetadata$Agency,lakesWithMetadata$GeomorphicLType)
table(factor(lakesWithMetadata$Agency,levels=c('ac','boprc','ecan','es','gdc','gwrc','hbrc','hrc','mdc','ncc','nrc','orc','tdc','trc','wcrc','wrc')))

# ac   boprc  ecan    es   gdc  gwrc  hbrc   hrc   mdc   ncc   nrc   orc   tdc   trc  wcrc   wrc 
# 3327 12061 18946  10520    0  2164  4070  2276     0     0  8957  7601     0   859  1771  8541  27/8 9:11am
# 3327 12061 18946  10399    0  2164  4070  2276     0     0  8957  7601     0   859  1771  8541
# 3327 12061 18946  10399    0  2164  4070  2276     0     0  8957  7601     0   859  1771  8541 
# 3327 11053 18946  10399    0  2164  4070  2276     0     0  8957  7601     0   859  1772  8009 
# 3327 11040 18946  10403    0  2164  4070  2276     0     0  8957  7601     0   859  1678  8009 
# 3327 11040 18946  10403    0  1997  4070  2276     0     0     0  7601     0   859  1678  8008 
# 2685 11040 18913  10403    0  1997  4070  2128     0     0     0  7601     0   859  1678  8008 
# 2685 11040 24118  10403       1997  3946  2129     0     0     0  7601     0   859  1678  8008
#    0 11040 16883  9844     0  1997     0  1479     0     0     0  7601     0   859  1678  8008 
# 6580 22513 15939  9194     0  1997  3514  1794     0     0  8927  6829     0  1461  1563  7186 
# 3050 9704  15939  9194     0  1997  3513  1794     0     0  8927  7023     0  1461  1556  7186
# 3050 9742  15939  9194     0  1997  3513  1794     0     0  8927  7372     0  1461  1556  7186 
# 3050 10965 15939  9194     0  1997  3513  1794   0     0    8927  7401     0   831  1556  7186
#    0 11040 17077  9565     0  1997     0  2009     0     0     0  7601     0   859  1678  8008 

suppressWarnings(try(dir.create(paste0('h:/ericg/16666LAWA/LAWA2021/Lakes/Data/',format(Sys.Date(),"%Y-%m-%d")))))
write.csv(lakesWithMetadata,paste0('h:/ericg/16666LAWA/LAWA2021/Lakes/Data/',format(Sys.Date(),"%Y-%m-%d"),'/LakesWithMetadata.csv'),row.names = F)
save(lakesWithMetadata,file = paste0('h:/ericg/16666LAWA/LAWA2021/Lakes/Data/',format(Sys.Date(),"%Y-%m-%d"),'/LakesWithMetadata.rData'))


table(lakeData$agency,lakeData$Measurement)/table(lakeData$agency,lakeData$Measurement)
#       CHLA CYANOTOT CYANOTOX ECOLI NH4N  pH Secchi  TN  TP
# ac       1        1        1     1    1   1      1   1   1
# boprc    1                            1   1      1   1   1
# ecan     1        1        1     1    1   1      1   1   1
# es       1        1              1    1   1      1   1   1
# gwrc     1                       1    1   1      1   1   1
# hbrc     1                       1    1   1      1   1   1
# hrc      1                       1    1   1      1   1   1
# nrc      1                       1    1   1      1   1   1
# orc      1                       1    1   1      1   1   1
# trc      1                       1    1   1      1   1   1
# wcrc     1                       1    1   1      1   1   1
# wrc      1        1              1    1   1      1   1   1

