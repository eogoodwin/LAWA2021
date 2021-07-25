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
  "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadBOP.R", #slow - next to xml2
  # "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadECAN.R",
  "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadES.R",
  "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadGDC.R",              #5
  "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadGWRC.R",
  "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadHBRC.R",
  "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadHRC.R",
  "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadMDC.R",
  "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadNCC.R",        #10
  "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadNRC.R",
  "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadORC.R",
  "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadTDC.R",
  "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadTRC.R",         #15
  "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadWCRC.R",
  "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Scripts/loadWRC.R")
agencies = c('ac','niwa','boprc','es',
             'gdc','gwrc','hbrc','hrc','mdc',
             'ncc','nrc','orc','tdc','trc',
             'wcrc','wrc')
workers <- makeCluster(6)
registerDoParallel(workers)
clusterCall(workers,function(){
  source('H:/ericg/16666LAWA/LAWA2021/scripts/LAWAFunctions.R')
})
startTime=Sys.time()
foreach(i = 1:length(scriptsToRun),.errorhandling = "stop")%dopar%{
  if(agencies[i]=='ncc'){return(NULL)}
  if(agencies[i]%in%tolower(siteTable$Agency)) try(source(scriptsToRun[i]))
  rm(con)
  gc()
  return(NULL)
}
stopCluster(workers)
rm(workers)
Sys.time()-startTime
cat("Done load\n")
#17 minutes


for(agency in agencies){
  checkXMLageMacro(agency)
  checkCSVageMacros(agency)
}


#XML 2 CSV for MACROS ####
lawaset=c("TaxaRichness","MCI","PercentageEPTTaxa","QMCI","ASPM")
agencies=c("ac","boprc","es","gdc","gwrc","hbrc","hrc","mdc","ncc","nrc","orc","tdc","trc","wcrc","wrc")
startTime=Sys.time()
for(agency in c("ac","boprc","es","gdc","gwrc","hbrc","hrc","mdc","ncc","nrc","orc","tdc","trc","wcrc","wrc")){
# foreach(agency = 1:length(agencies))
  forcsv=xml2csvMacro(agency,maxHistory = 60,quiet=T)
  if(is.null(forcsv))next
  cat(agency,'\t',paste0(unique(forcsv$Measurement),collapse=', '),'\n')
  
  #Drop sqMCI
  if('sqmci'%in%unique(tolower(forcsv$Measurement))){
    forcsv=forcsv[-which(forcsv$Measurement%in%c('SQMCI')),]
    cat(agency,'\t',paste0(unique(forcsv$Measurement),collapse=', '),'\n')
  }
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
Sys.time()-startTime 




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
agencies= c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","nrc","orc","tdc","trc","wcrc","wrc")
library(parallel)
library(doParallel)
workers=makeCluster(4)
clusterCall(workers,function(){
  library(tidyverse)
})
registerDoParallel(workers)
foreach(agency =1:length(agencies),.combine = bind_rows,.errorhandling = 'stop')%dopar%{
  mfl=loadLatestCSVmacro(agencies[agency],maxHistory = 30)
  if(agencies[agency]=='boprc'){
    mfl=unique(mfl)
  }
  if(!is.null(mfl)&&dim(mfl)[1]>0){
    if(sum(!tolower(mfl$CouncilSiteID)%in%tolower(siteTable$CouncilSiteID))>0){
      cat('\t',sum(!unique(tolower(mfl$CouncilSiteID))%in%tolower(siteTable$CouncilSiteID)),'CouncilSiteIDs not in site table\n')
      cat('\t',sum(!unique(tolower(mfl$LawaSiteID))%in%tolower(siteTable$LawaSiteID)),'LawaSiteIDs not in site table\n')
    }
    
    
    #Refill dataset from previous pulls, if gaps
    targetSites=tolower(siteTable$CouncilSiteID[siteTable$Agency==agencies[agency]])
    targetCombos=apply(expand.grid(targetSites,tolower(lawaset)),MARGIN = 1,FUN=paste,collapse='')
    currentSiteMeasCombos=unique(paste0(tolower(mfl$CouncilSiteID),tolower(mfl$Measurement)))
    missingCombos=targetCombos[!targetCombos%in%currentSiteMeasCombos]
    
    if(length(missingCombos)>0){
      agencyFiles = rev(dir(path = "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/",
                            pattern = paste0('^',agencies[agency],'.csv'),
                            full.names = T,recursive = T,ignore.case = T))[-1]
      for(af in seq_along(agencyFiles)){
        agencyCSV = read.csv(agencyFiles[af],stringsAsFactors=F)
        if("Measurement"%in%names(agencyCSV)){
          agencyCSVsiteMeasCombo=paste0(tolower(agencyCSV$CouncilSiteID),tolower(agencyCSV$Measurement))
          if(any(missingCombos%in%unique(agencyCSVsiteMeasCombo))){
            browser()
            these=agencyCSVsiteMeasCombo%in%missingCombos
            agencyCSV=agencyCSV[these,]
            mfl=rbind(mfl,agencyCSV[,names(mfl)])
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
    
    mfl$Agency=agencies[agency]
    if(agencies[agency]=='ac'){
      #Auckland
      # mfl$CouncilSiteID=trimws(mfl$CouncilSiteID)
      # store=mfl$CouncilSiteID
      mfl$CouncilSiteID=trimws(mfl$SiteID)
      # mfl$SiteID=store
      # rm(store)
      mfl <- mfl%>%select(-LAWASiteID,-SiteID)
      sort(unique(tolower(mfl$CouncilSiteID))[unique(tolower(mfl$CouncilSiteID))%in%tolower(siteTable$CouncilSiteID)])
      # mfl$CouncilSiteID[tolower(mfl$CouncilSiteID)=="avondale @ thuja"] <- "Avondale @ Thuja Pl"
      # mfl$Date = format(lubridate::ymd(mfl$Date),'%d-%b-%Y')
    }
    if(agencies[agency]=='es'){
      toCut=which(mfl$CouncilSiteID=="mataura river 200m d/s mataura bridge"&mfl$Date=="21-Feb-2017")
      if(length(toCut)>0){
        mfl=mfl[-toCut,]
      }
      rm(toCut)
    }
    if(agencies[agency]=='tdc'){
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


macroData$LawaSiteID = siteTable$LawaSiteID[match(tolower(macroData$CouncilSiteID),tolower(siteTable$CouncilSiteID))]
table(is.na(macroData$LawaSiteID),macroData$Agency)
macroData$LawaSiteID[which(is.na(macroData$LawaSiteID))] = 
  siteTable$LawaSiteID[match(tolower(macroData$CouncilSiteID[which(is.na(macroData$LawaSiteID))]),
                             tolower(siteTable$SiteID))]
table(is.na(macroData$LawaSiteID),macroData$Agency)


#Add CSV-delivered datasets
niwamac = loadLatestCSVmacro(agency='niwa')
niwamac$LawaSiteID=tolower(niwamac$LawaSiteID)
niwamac$measName=niwamac$Measurement
# niwamac$Measurement[niwamac$Measurement=="ntaxa"] <- "TaxaRichness"
macroData=bind_rows(macroData,niwamac)
#50875 24/9/20
#43804 2/7/21
#59623 7/7/21
#58838 8/7/21
#62258 15/7/21
#62701 16/7/21
#62841 23/7/21
 
rm(niwamac)

#This one with rounding is a good way to assign samples to a sampling season.  
#November/December etc gets rounded forward to the following year
#But remember there's that pigdog bug problem with lubridate::isoyear so be careful

macroData$sYear = lubridate::year(lubridate::round_date(lubridate::dmy(macroData$Date),unit = 'year'))

macroData$cYear = lubridate::year(lubridate::dmy(macroData$Date))
# macroData$sYear = macroData$cYear
# macroData$sYear[which(lubridate::month(lubridate::dmy(macroData$Date))>6)]=macroData$sYear[which(lubridate::month(lubridate::dmy(macroData$Date))>6)]+1

#Audit the sites that are under NIWA and under an agency
macroData%>%group_by(tolower(LawaSiteID))%>%
  dplyr::summarise(agCount=length(unique(Agency)),
                   ags=paste(unique(Agency),collapse=' '),
                   cid=paste(unique(CouncilSiteID),collapse=', '))%>%
  ungroup%>%
  filter(agCount>1)%>%dplyr::select(-agCount)%>%arrange(`tolower(LawaSiteID)`)%>%as.data.frame->dupData

macroData%>%group_by(tolower(LawaSiteID))%>%
  dplyr::summarise(agCount=length(unique(Agency)),
                   ags=paste(unique(Agency),collapse=' '),
                   cid=paste(unique(CouncilSiteID),collapse=', '))%>%
  ungroup%>%
  filter(agCount>1)%>%dplyr::select(`tolower(LawaSiteID)`)%>%unlist%>%unname->dupSites



macroData$LawaSiteID[macroData$LawaSiteID%in%dupSites&macroData$Agency=='niwa'] <- 
  paste0(macroData$LawaSiteID[macroData$LawaSiteID%in%dupSites&macroData$Agency=='niwa'],"_NIWA")


#Let's check this.
# ebop223=macroData%>%filter(grepl("ebop-00223",LawaSiteID,T))
# plot(dmy(ebop223$Date),ebop223$Value,pch=as.numeric(factor(ebop223$Measurement)),col=as.numeric(factor(ebop223$Agency)))

table(macroData$Agency,macroData$Measurement)/table(macroData$Agency,macroData$Measurement)


write.csv(macroData,paste0('h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/',format(Sys.Date(),"%Y-%m-%d"),'/MacrosCombined.csv'),row.names = F)
# macroData=read.csv(tail(dir(path='h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/',pattern='MacrosCombined.csv',recursive = T,full.names = T),1),stringsAsFactors = F)

#62841


# #audit presence of CouncilSiteIDs in sitetable
# table(unique(tolower(macroData$CouncilSiteID))%in%unique(tolower(c(siteTable$SiteID,siteTable$CouncilSiteID))))
# unique((macroData$CouncilSiteID))[!unique(tolower(macroData$CouncilSiteID))%in%unique(tolower(c(siteTable$SiteID,siteTable$CouncilSiteID)))]->unmatched
# 
# # based on CouncilSiteID
# table(unique(macroData$CouncilSiteIDlc)%in%siteTable$CouncilSiteIDlc)
# unique(macroData$agency[!macroData$CouncilSiteIDlc%in%siteTable$CouncilSiteIDlc])
# (unique(macroData$CouncilSiteID[!macroData$CouncilSiteIDlc%in%siteTable$CouncilSiteIDlc])->unmatched)
# table(unique(macroData[,c(1,2)])$agency[unique(macroData[,c(1,2)])$CouncilSiteID%in%unmatched])
# unique(macroData[,c(1,2)])[unique(macroData[,c(1,2)])$CouncilSiteID%in%unmatched,]
# rm(unmatched)
# table(unique(macroData$CouncilSiteIDlc)%in%siteTable$CouncilSiteIDlc)

# ac boprc  ecan    es   gdc  gwrc  hbrc   hrc   mdc   ncc  niwa   nrc   orc   tdc   trc  wcrc   wrc 
# 1878 38535  8949  2997  1083  1568  2559  2646  1159  1105  5466  1734  1096     0  7712  2655  3393 

macroData=merge(macroData,siteTable,by=c("LawaSiteID","Agency"),all.x=T,all.y=F)%>%
  dplyr::select("LawaSiteID","CouncilSiteID.x","SiteID","Region","Agency","Date","cYear","sYear","Measurement","Value","Lat","Long","Landcover","Altitude")%>%
  dplyr::rename(CouncilSiteID=CouncilSiteID.x)%>%distinct  #Drop macro,and sitnamelc
macroData$Agency=tolower(macroData$Agency)
macroData$Region=tolower(macroData$Region)



agencies= c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc")
table(factor(macroData$Agency,levels=agencies))
table(macroData$Agency)
# ac   boprc  ecan    es   gdc  gwrc  hbrc   hrc   mdc   ncc  niwa   nrc   orc   tdc   trc  wcrc   wrc 
# 2736 3762  17180  2997  1083  1568  3580  4702  1099  1105  5464  1701  1102   726  7787  2586  3441
write.csv(macroData,paste0('h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/',format(Sys.Date(),"%Y-%m-%d"),
                       '/MacrosWithMetadata.csv'),row.names = F)

table(macroData$Agency,macroData$Measurement)/table(macroData$Agency,macroData$Measurement)

#Some sites are under the purview of both a local agency and a NIWA
# macroData%>%group_by(LawaSiteID=gsub(pattern = '_NIWA','',LawaSiteID))%>%
#   summarise(nA=length(unique(Agency)),
#             nCSI=length(unique(CouncilSiteID)),
#             nSI=length(unique(SiteID)))%>%
#   ungroup%>%
#   filter(nA>1|nCSI>1|nSI>1)%>%
#   dplyr::select(LawaSiteID)%>%
#   drop_na->dupSites

# 
# macroData%>%filter(gsub(pattern = '_NIWA','',LawaSiteID)%in%dupSites)%>%select(LawaSiteID,SiteID,CouncilSiteID,Agency)%>%distinct
# 
# macroData%>%filter(gsub(pattern = '_NIWA','',LawaSiteID)%in%dupSites)->dupData
# dupData$Date=lubridate::dmy(dupData$Date)
# dupData <- dupData%>%filter(Date>dmy('1-12-2004'))
# windows()
# par(mfrow=c(5,4))
# for(sss in dupSites){
#   with(dupData%>%filter(grepl(sss,LawaSiteID)&Measurement=="MCI"),
#        plot(Date,Value,pch=as.numeric(factor(Agency)),main=sss,yli=c(0,200),ylab='MCI'))
#   with(dupData%>%filter(grepl(sss,LawaSiteID)&Measurement=="MCI"),
#        legend('bottomleft',unique(Agency),pch=1:2))
# }
# par(mfrow=c(4,5))
# for(sss in dupSites){
#   with(dupData%>%filter(grepl(sss,LawaSiteID)&Measurement=="TaxaRichness"),
#        plot(Date,Value,pch=as.numeric(factor(Agency)),main=sss,yli=c(3,37),log='y',ylab='TaxaRichness'))
#   with(dupData%>%filter(grepl(sss,LawaSiteID)&Measurement=="MCI"),
#        legend('topleft',unique(Agency),pch=1:2))
# }
# par(mfrow=c(4,5))
# for(sss in dupSites){
#   with(dupData%>%filter(grepl(sss,LawaSiteID)&Measurement=="PercentageEPTTaxa"),
#        plot(Date,Value,pch=as.numeric(factor(Agency)),main=sss,yli=c(0,100),ylab='PercentEPTTaxa'))
#   with(dupData%>%filter(grepl(sss,LawaSiteID)&Measurement=="MCI"),
#        legend('bottomleft',unique(Agency),pch=1:2))
# }
