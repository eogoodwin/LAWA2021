rm(list=ls())
gc()
library(tidyverse)
library(data.table)
rollMed <- function(bp,n=5){
  if(length(bp)<n) return(bp)
  an=function(n,len)c(0,0,3,4,5,rep(n,len-n))
  nn=an(n,length(bp))  
  zoo::rollapply(bp,width=nn,median,type=5,probs=0.5,align="right",adaptive=T)
}
source("h:/ericg/16666LAWA/LAWA2021/scripts/lawa_state_functions.R")
source("h:/ericg/16666LAWA/LAWA2021/scripts/LAWAFunctions.R")

startYear15 <- lubridate::year(Sys.Date())-15  #2005  #This convention requires >=
startYear10 <- lubridate::year(Sys.Date())-10  #2010
startYear5 <-  lubridate::year(Sys.Date())-5  #2015
EndYear <- lubridate::year(Sys.Date())-1    #2019     #This convention requires <=

firstYear=2005-5
yr <- c(as.character(firstYear:EndYear),paste0(as.character(firstYear:(EndYear-4)),'to',as.character((firstYear+4):EndYear)))
rollyrs=which(grepl('to',yr))
nonrollyrs=which(!grepl('to',yr))
reps <- length(yr)

macroSiteTable=loadLatestSiteTableMacro()


dir.create(paste0("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)

#Load the latest made (might need to check it works nicely across month folders) 
if(!exists('macroData')){
  macroDataFileName=tail(dir(path = "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data",pattern = "MacrosWithMetadata.csv",recursive = T,full.names = T),1)
  cat(as.character(file.info(macroDataFileName)$mtime))
  macroData=read_csv(macroDataFileName)
  rm(macroDataFileName)
  # macroData$Date[macroData$Date=="(blank)"]=paste0("01-Jan-",macroData$Year[macroData$Date=="(blank)"])
  macroData$Date=format(dmy(macroData$Date),'%d-%b-%Y')
  macroData$LawaSiteID=tolower(macroData$LawaSiteID)
}

macroData$NZReach = macroSiteTable$NZReach[match(gsub(pattern = '_niwa',replacement = '',x = tolower(macroData$LawaSiteID)),
                                                 tolower(macroSiteTable$LawaSiteID))]


#Output the raw data for ITE
#Output for ITE
write.csv(macroData%>%
            filter(Measurement%in%c("MCI","TaxaRichness","PercentageEPTTaxa","QMCI","ASPM"))%>%
            transmute(LAWAID=gsub('_NIWA','',LawaSiteID,ignore.case = T),
                      SiteName = CouncilSiteID,
                      Agency=Agency,
                      CollectionDate=format(lubridate::dmy(Date),"%Y-%m-%d"),
                      Metric=Measurement,
                      Value=Value)%>%
            group_by(LAWAID,SiteName,Agency,CollectionDate,Metric)%>%
            dplyr::summarise(Value=median(Value,na.rm=T,type=5))%>%ungroup,
          file = paste0("h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                        "/ITEMacroHistoricData",format(Sys.time(),'%d%b%Y'),".csv"),row.names=F)


macroData$month=lubridate::month(lubridate::dmy(macroData$Date))

# macroData$isoYear=lubridate::isoyear(lubridate::dmy(macroData$Date))

macroData=macroData[which(macroData$sYear>=firstYear & macroData$sYear<=EndYear),]

#63163 to 59417 for firstYear==2000 16/7/2021
macroData$Date=lubridate::dmy(macroData$Date)

lawaMacroData = macroData%>%group_by(LawaSiteID,
                                     sYear,
                                     Measurement)%>%
  dplyr::summarise(.groups='keep',
                   count=n(),
                   Value= quantile(Value,type=5,prob=0.5,na.rm=T),
                   SiteID=unique(SiteID),
                   Agency=unique(Agency),
                   LandcoverGroup=unique(Landcover),
                   AltitudeGroup=unique(Altitude),
                   date=mean(Date))%>%ungroup

lawaMacroData%>%
  dplyr::filter(sYear>=startYear5)%>%
  save(file=paste("h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/",format(Sys.Date(),"%Y-%m-%d"),
                  "/lawaMacroData",startYear5,"-",EndYear,".RData",sep=""))
# load(file=tail(dir(path = "h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis",pattern = "lawaMacroData",recursive = T,full.names = T),1),verbose = T)

lawaMacroState5yr <- lawaMacroData%>%
  filter(!is.na(LawaSiteID))%>%
  filter(sYear>=startYear5)%>%
  dplyr::group_by(LawaSiteID,Measurement)%>%
  dplyr::summarise(.groups='keep',
                   Median=quantile(Value,prob=0.5,type=5,na.rm=T),n=n())%>%ungroup

#How many 5-yr medians are based on 3 or more years?
sum(lawaMacroState5yr$n>=3)
sum(lawaMacroState5yr$n>=3)/dim(lawaMacroState5yr)[1]
#0.96  3584 out of 3735
#Drop the ones that are based on fewer than 3 years
lawaMacroState5yr <- lawaMacroState5yr%>%filter(n>=3)
#3735 to 3584

write.csv(lawaMacroState5yr%>%transmute(LAWAID=LawaSiteID,
                                        Parameter=Measurement,
                                        Median=Median),
          file=paste0('h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/',
                                        format(Sys.Date(),"%Y-%m-%d"),
                                        '/ITEMacroState',format(Sys.time(),'%d%b%Y'),'.csv'),row.names = F)
# lawaMacroState5yr = read.csv(tail(dir(path='h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/',pattern='MACRO_STATE_ForITE|MacroState',recursive = T,full.names = T),1),stringsAsFactors = F)



# rolling 5 year median for MCI
rollingMCI <- lawaMacroData%>%filter(Measurement=='MCI')%>%select(LawaSiteID,sYear,Value,date)
setDT(rollingMCI)     
setkey(rollingMCI,LawaSiteID,sYear)
rollingMCI[,rollMCI:=rollMed(Value,5),by=LawaSiteID]

rollingMCI <- rollingMCI%>%filter(sYear>=2005)
#13918 to 12347
write.csv(rollingMCI,paste0('h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/',
                            format(Sys.Date(),"%Y-%m-%d"),
                            '/MacroRollingMCI',format(Sys.time(),'%d%b%Y'),'.csv'),row.names = F)

# rolling 5 year median for QMCI
rollingQMCI <- lawaMacroData%>%filter(Measurement=='QMCI')%>%select(LawaSiteID,sYear,Value,date)
setDT(rollingQMCI)     
setkey(rollingQMCI,LawaSiteID,sYear)
rollingQMCI[,rollQMCI:=rollMed(Value,5),by=LawaSiteID]

rollingQMCI <- rollingQMCI%>%filter(sYear>=2005)
#4303 to 3766
write.csv(rollingQMCI,paste0('h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/',
                            format(Sys.Date(),"%Y-%m-%d"),
                            '/MacroRollingQMCI',format(Sys.time(),'%d%b%Y'),'.csv'),row.names = F)


# rolling 5 year median for ASPM
rollingASPM <- lawaMacroData%>%filter(Measurement=='ASPM')%>%select(LawaSiteID,sYear,Value,date)
setDT(rollingASPM)     
setkey(rollingASPM,LawaSiteID,sYear)
rollingASPM[,rollASPM:=rollMed(Value,5),by=LawaSiteID]

rollingASPM <- rollingASPM%>%filter(sYear>=2005)
#4210 to 3672
write.csv(rollingASPM,paste0('h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/',
                             format(Sys.Date(),"%Y-%m-%d"),
                             '/MacroRollingASPM',format(Sys.time(),'%d%b%Y'),'.csv'),row.names = F)




cat('Stop here!')
stop()


  #NPSFM is 90,110,130

lawaMacroState5yr$MCIband=(cut(lawaMacroState5yr$Median,breaks = c(0,80,100,120,200)))
lawaMacroState5yr$quartile=(cut(lawaMacroState5yr$Median,breaks = quantile(lawaMacroState5yr$Median,probs=c(0,0.25,0.5,0.75,1))))
plot(jitter(lawaMacroState5yr$MCIband),jitter(lawaMacroState5yr$quartile))
table(lawaMacroState5yr$MCIband,lawaMacroState5yr$quartile)
