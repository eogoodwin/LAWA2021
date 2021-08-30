rm(list=ls())
library(parallel)
library(doParallel)
source('H:/ericg/16666LAWA/LAWA2021/Scripts/LAWAFunctions.R')
siteTable=loadLatestSiteTableMacro(maxHistory = 20)

dir.create(paste0("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/",
                  format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)





scriptsToRun = c(
  "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadAC.R", #1
  "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadNIWA.R",
  "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadGDC.R",              #5
  "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadGWRC.R",
   "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadHBRC.R",  #this is hte one
  "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadMDC.R",        #10
  "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadORC.R",
  "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadTDC.R",
  "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadTRC.R",         #15
  "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadNCC.R")
source("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadBOP_list.R")
source("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadECAN_list.R")
source( "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadES_list.R")
# source("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadHBRC_list.R") this one doesnt work yet
source("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadHRC.R")
source("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadNRC_list.R")
# source("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadWCRC_list.R")
source("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadWRC.R")

workers <- makeCluster(4)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(readr)
  source('H:/ericg/16666LAWA/LAWA2021/scripts/LAWAFunctions.R')
})
startTime=Sys.time()
foreach(i = 1:length(scriptsToRun),.errorhandling = "stop")%dopar%{
  (source(scriptsToRun[i]))
  return(NULL)
}
stopCluster(workers)
rm(workers)
Sys.time()-startTime
cat("Done load\n")
#17 minutes


xmlAgencies=c("gdc","gwrc","hbrc","mdc","ncc","orc","tdc","trc")
csvAgencies=c('ac','boprc','ecan','es','hbrc','hrc','nrc','wcrc','wrc')
for(agency in xmlAgencies){
  checkXMLageMacro(agency)
}
for(agency in csvAgencies){
   checkCSVageMacros(agency)
}



#XML 2 CSV for MACROS ####
lawaset=c("TaxaRichness","MCI","PercentageEPTTaxa","QMCI","ASPM")

startTime=Sys.time()
for(agency in xmlAgencies){
# foreach(agency = 1:length(agencies))
  forcsv=xml2csvMacro(agency,maxHistory = 60,quiet=T)
  if(is.null(forcsv))next
  cat(agency,'\t',paste0(unique(forcsv$Measurement),collapse=', '),'\n')
  
  forcsv$measName = forcsv$Measurement
  forcsv$Measurement[grepl(pattern = 'Taxa',x = forcsv$Measurement,ignore.case = T)&
                     !grepl('EPT',forcsv$Measurement,ignore.case = F)] <- "TaxaRichness"
  forcsv$Measurement[grepl(pattern='QMCI',x=forcsv$Measurement,ignore.case=T)] <- 'QMCI'
  forcsv$Measurement[grepl(pattern='ASPM',x=forcsv$Measurement,ignore.case=T)] <- 'ASPM'
  forcsv$Measurement[grepl(pattern = 'Reported MCI|^MCI|ate( community)* ind',
                           x = forcsv$Measurement,ignore.case = T)] <- "MCI"
  forcsv$Measurement[grepl(pattern = 'EPT',x = forcsv$Measurement,ignore.case = T)] <- "PercentageEPTTaxa"
  forcsv$Measurement[grepl(pattern = 'Rich',x = forcsv$Measurement,ignore.case = T)] <- "TaxaRichness"
  excess=unique(forcsv$Measurement)[!unique(forcsv$Measurement)%in%lawaset]
  if(length(excess)>0){
browser()
        forcsv=forcsv[-which(forcsv$Measurement%in%excess),]
  }
  rm(excess)
  write.csv(forcsv,
            file=paste0( 'H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/',format(Sys.Date(),"%Y-%m-%d"),'/',agency,'.csv'),
            row.names=F)
  rm(forcsv)
}
Sys.time()-startTime  #12 secs

for(agency in xmlAgencies){
  checkCSVageMacros(agency)
}


##############################################################################
#                                 *****
##############################################################################
#Build the combo ####
startTime=Sys.time()
if(exists('macroData')){rm(macroData)}
siteTable=loadLatestSiteTableMacro()
rownames(siteTable)=NULL
lawaset=c("TaxaRichness","MCI","PercentageEPTTaxa","QMCI","ASPM")
suppressWarnings(rm(macrodata,"ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc"))
agencies= c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc")


workers=makeCluster(6)
clusterCall(workers,function(){
  library(tidyverse)
})
registerDoParallel(workers)
foreach(agencyi =1:length(agencies),.combine = bind_rows,.errorhandling = 'stop')%dopar%{
  # for(agencyi in 1:length(agencies)){
  mfl=loadLatestCSVmacro(agencies[agencyi],maxHistory = 30)
  if(agencies[agencyi]=='boprc'){
    mfl=unique(mfl)
  }
  if(!is.null(mfl)&&dim(mfl)[1]>0){
    if(sum(!tolower(mfl$CouncilSiteID)%in%tolower(siteTable$CouncilSiteID))>0){
      cat('\t',sum(!unique(tolower(mfl$CouncilSiteID))%in%tolower(siteTable$CouncilSiteID)),'CouncilSiteIDs not in site table\n')
      cat('\t',sum(!unique(tolower(mfl$LawaSiteID))%in%tolower(siteTable$LawaSiteID)),'LawaSiteIDs not in site table\n')
    }
    
    #Refill dataset from previous pulls, if gaps
    if(!agencies[agencyi]%in%c("hrc","ac",'nrc')){
    targetSites=tolower(siteTable$CouncilSiteID[siteTable$Agency==agencies[agencyi]])
    targetCombos=apply(expand.grid(targetSites,tolower(lawaset)),MARGIN = 1,FUN=paste,collapse='')
    currentSiteMeasCombos=unique(paste0(tolower(mfl$CouncilSiteID),tolower(mfl$Measurement)))
    missingCombos=targetCombos[!targetCombos%in%currentSiteMeasCombos]
    
    if(length(missingCombos)>0){
      agencyFiles = rev(dir(path = "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/",
                            pattern = paste0('^',agencies[agencyi],'.csv'),
                            full.names = T,recursive = T,ignore.case = T))[-1]
      for(af in seq_along(agencyFiles)){
        agencyCSV = read.csv(agencyFiles[af],stringsAsFactors=F)
        if("Measurement"%in%names(agencyCSV)){
          agencyCSVsiteMeasCombo=paste0(tolower(agencyCSV$CouncilSiteID),tolower(agencyCSV$Measurement))
          if(any(missingCombos%in%unique(agencyCSVsiteMeasCombo))){
            # browser()
            these=agencyCSVsiteMeasCombo%in%missingCombos
            agencyCSV=agencyCSV[these,]
            mfl=bind_rows(mfl,agencyCSV%>%select(matches(names(mfl))))
            rm(agencyCSV,these)
            currentSiteMeasCombos=unique(paste0(tolower(mfl$CouncilSiteID),tolower(mfl$Measurement)))
            missingCombos=targetCombos[!targetCombos%in%currentSiteMeasCombos]
            if(length(missingCombos)==0){
              break
            }            
          }
        }
        suppressWarnings(rm(agencyCSV))
      }
    }
    rm(missingCombos,targetCombos,currentSiteMeasCombos,targetSites)
    }
    if(agencies[agencyi]%in%c('nrc','wrc')){mfl$CouncilSiteID=as.character(mfl$CouncilSiteID)}
    
    mfl$Agency=agencies[agencyi]
   
    if(agencies[agencyi]=='es'){
      toCut=which(mfl$CouncilSiteID=="mataura river 200m d/s mataura bridge"&mfl$Date=="21-Feb-2017")
      if(length(toCut)>0){
        mfl=mfl[-toCut,]
      }
      rm(toCut)
    }
    if(agencies[agencyi]=='tdc'){
      funnyTDCsites=!tolower(mfl$CouncilSiteID)%in%tolower(siteTable$CouncilSiteID)
      cat(length(funnyTDCsites),'\n')
      if(length(funnyTDCsites)>0){
        mfl$CouncilSiteID[funnyTDCsites] <-
          siteTable$CouncilSiteID[match(tolower(mfl$CouncilSiteID[funnyTDCsites]),tolower(siteTable$SiteID))]
      }
      rm(funnyTDCsites)
    }
  }
  mfl$CouncilSiteID=as.character(mfl$CouncilSiteID)
  # eval(parse(text=paste0(agencies[agencyi],'mfl=mfl')))
  # macroDdata=rbind(macroData,mfl)}
  
  return(mfl)
}->macroData 
stopCluster(workers)
rm(workers)
Sys.time()-startTime #2.03
#2/7/2021 38338
#7/7/2021 51643
#8/7/21   53375
#15/7/21  56792
#16/7/21  57235
#23/7/21 57375
#30/7/21 59582
#5/8/21 55508
#5/8/21 75637
#9/8/2021 77239
#13/8/21 66189 #4.3 s
#19/8/21 66223
#20/8/21 66341
#25/8/21 66223




missingLSID=which(is.na(macroData$LawaSiteID))
macroData$LawaSiteID[missingLSID] = 
  siteTable$LawaSiteID[match(tolower(macroData$CouncilSiteID[missingLSID]),tolower(siteTable$CouncilSiteID))]
table(is.na(macroData$LawaSiteID),macroData$Agency)

missingLSID=which(is.na(macroData$LawaSiteID))
macroData$LawaSiteID[missingLSID] = 
  siteTable$LawaSiteID[match(tolower(macroData$CouncilSiteID[missingLSID]),
                             tolower(siteTable$SiteID))]
rm(missingLSID)

table(is.na(macroData$LawaSiteID),macroData$Agency)
macroData%>%filter(is.na(LawaSiteID))%>%select(CouncilSiteID,Agency)%>%distinct

macroData <- macroData%>%filter(!is.na(LawaSiteID))


#This one with rounding is a good way to assign samples to a sampling season.  
#November/December etc gets rounded forward to the following year
#But remember there's that pigdog bug problem with lubridate::isoyear so be careful

macroData$sYear = lubridate::year(lubridate::round_date(lubridate::dmy(macroData$Date),unit = 'year'))

macroData$cYear = lubridate::year(lubridate::dmy(macroData$Date))


write.csv(macroData,paste0('h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/',format(Sys.Date(),"%Y-%m-%d"),'/MacrosCombined.csv'),row.names = F)
# macroData=read.csv(tail(dir(path='h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/',pattern='MacrosCombined.csv',recursive = T,full.names = T),1),stringsAsFactors = F)



#Guided by the below
macroData$CouncilSiteID[macroData$Agency=='niwa'] <- 
  siteTable$CouncilSiteID[match(macroData$CouncilSiteID[macroData$Agency=='niwa'],
                                siteTable$SiteID)]

table(unique(tolower(macroData$CouncilSiteID))%in%tolower(siteTable$CouncilSiteID))
unique(macroData$Agency[!tolower(macroData$CouncilSiteID)%in%tolower(siteTable$CouncilSiteID)])
unique(macroData$CouncilSiteID[!tolower(macroData$CouncilSiteID)%in%tolower(siteTable$CouncilSiteID)])



macroData=merge(macroData%>%mutate(LawaSiteID = tolower(LawaSiteID)),
                siteTable%>%mutate(LawaSiteID=tolower(LawaSiteID)),
                by=c("CouncilSiteID","Agency"),all.x=T,all.y=F)%>%
  dplyr::select("LawaSiteID.x","CouncilSiteID","SiteID","Region","Agency","Date","cYear","sYear","Measurement","Value",'CollMeth','ProcMeth',"Lat","Long","Landcover","Altitude")%>%
  dplyr::rename(LawaSiteID=LawaSiteID.x)%>%distinct  #Drop macro,and sitnamelc
macroData$Agency=tolower(macroData$Agency)
macroData$Region=tolower(macroData$Region)

#66096


agencies= c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc")
table(factor(macroData$Agency,levels=agencies),useNA='a')%>%addmargins()

# ac   boprc  ecan    es   gdc  gwrc  hbrc   hrc   mdc   ncc  niwa   nrc   orc   tdc   trc  wcrc   wrc
# 3420  3963 12810  5560  1083  1814  3580  4437  1465  1105  5464  2133  1102   651  7802  3560  6029     0 65553
# 3420  3963 12810  5135  1083  1814  3580  4437  1465  1105  5464  2676  1102   651  7802  3560  6029 66096
# 3420  3963 12810  5135  1083  1691  3580  4437  1465  1105  5464  2676  1102   651  7799  3577  6014 65972 19/8/21 delt duplicate sitenames
# 3465  3963 12810  5135  1083  1691  3581  4437  1465  1105  5464  2676  1102   678  7799  3577  6014 66045 13/8/21 
# 3420  3945 12810  5135  1083  1691  3581  4437  1465  1105  5464  2606  1102   678  7793  3561  6014 65890 
# 3420  3688 12810  5135  1083  1691  3580  4437  1465  1105  5464  2606  1102   678  7793  2586  6014 80203  9/8/21
# 3420  3688 12810  5135  1083  1691  3580  2835  1465  1105  5464  2606  1102   678  7793  2586  6014 
# 3420  3762 12810  5135  1083  1691  3580  2835  1465  1105  5464  2606  1102   678  7793  2586  3621  5/8/21
# 2736  3762 17180  5135  1083  1568  3580  4462  1465  1105  5464  1701  1102   678  7787  2586  3441
write.csv(macroData,paste0('h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/',format(Sys.Date(),"%Y-%m-%d"),
                       '/MacrosWithMetadata.csv'),row.names = F)


table(macroData$Agency,macroData$Measurement)/table(macroData$Agency,macroData$Measurement)
#       ASPM MCI PercentageEPTTaxa QMCI TaxaRichness
# ac       1   1                 1    1            1
# boprc        1                 1                 1
# ecan     1   1                 1    1            1
# es       1   1                 1    1            1
# gdc          1                 1                 1
# gwrc         1                 1                 1
# hbrc     1   1                 1    1            1
# hrc      1   1                 1    1            1
# mdc          1                 1    1            1
# ncc          1                 1                 1
# niwa         1                 1                 1
# nrc      1   1                 1    1            1
# orc          1                 1                 1
# tdc          1                 1                 1
# trc          1                 1                 1
# wcrc         1                 1    1            1
# wrc      1   1                 1    1            1