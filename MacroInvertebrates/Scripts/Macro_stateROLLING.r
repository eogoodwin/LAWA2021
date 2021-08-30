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

#Load the latest made 
if(!exists('macroData')){
  macroDataFileName=tail(dir(path = "H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data",pattern = "MacrosWithMetadata.csv",recursive = T,full.names = T),1)
  cat(as.character(file.info(macroDataFileName)$mtime))
  macroData=read_csv(macroDataFileName,guess_max = 50000)
  rm(macroDataFileName)
  # macroData$Date[macroData$Date=="(blank)"]=paste0("01-Jan-",macroData$Year[macroData$Date=="(blank)"])
  macroData$Date=format(dmy(macroData$Date),'%d-%b-%Y')
  macroData$LawaSiteID=tolower(macroData$LawaSiteID)
}

macroData$NZReach = macroSiteTable$NZReach[match(gsub('_niwa','',x = tolower(macroData$LawaSiteID)),
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
            dplyr::summarise(.groups='keep',Value=median(Value,na.rm=T,type=5))%>%ungroup,
          file = paste0("h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                        "/ITEMacroHistoricData",format(Sys.time(),'%d%b%Y'),".csv"),row.names=F)


macroData$month=lubridate::month(lubridate::dmy(macroData$Date))

macroData=macroData[which(macroData$sYear>=firstYear & macroData$sYear<=EndYear),]

#66096 to 62166 for firstYear==2000 20/8/21

macroData$Date=lubridate::dmy(macroData$Date)

lawaMacroData = macroData%>%
  group_by(LawaSiteID,#=gsub('_niwa','',LawaSiteID),
           sYear,
           Measurement)%>%
  dplyr::summarise(.groups='keep',
                   count=n(),
                   Value= quantile(Value,type=5,prob=0.5,na.rm=T),
                   SiteID=unique(SiteID,na.rm=T),
                   Agency=paste(unique(Agency),collapse=''),
                   LandcoverGroup=unique(Landcover,na.rm=T),
                   AltitudeGroup=unique(Altitude,na.rm=T),
                   date=mean(Date))%>%ungroup
table(lawaMacroData$count,useNA='a')

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

cumsum(rev(table(lawaMacroState5yr$n)))

#How many 5-yr medians are based on 4 or more years?
sum(lawaMacroState5yr$n>=4)
sum(lawaMacroState5yr$n>=4)/dim(lawaMacroState5yr)[1]
#0.89  4050 out of 4541
#Drop the ones that are based on fewer than 3 years
lawaMacroState5yr <- lawaMacroState5yr%>%filter(n>=4)
#4541 to 4050

write.csv(lawaMacroState5yr%>%transmute(LAWAID=LawaSiteID,
                                        Parameter=Measurement,
                                        Median=Median),
          file=paste0('h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/',
                                        format(Sys.Date(),"%Y-%m-%d"),
                                        '/ITEMacroState',format(Sys.time(),'%d%b%Y'),'.csv'),row.names = F)
# lawaMacroState5yr = read.csv(tail(dir(path='h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/',pattern='MACRO_STATE_ForITE|MacroState',recursive = T,full.names = T),1),stringsAsFactors = F)



# rolling 5 year median for MCI ####
rollingMCI <- lawaMacroData%>%filter(Measurement=='MCI')%>%select(LawaSiteID,sYear,Value,date)
setDT(rollingMCI)     
setkey(rollingMCI,LawaSiteID,sYear)
rollingMCI[,rollMCI:=rollMed(Value,5),by=LawaSiteID]

rollingMCI <- rollingMCI%>%filter(sYear>=2005)
#14290 to 13244
write.csv(rollingMCI,paste0('h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/',
                            format(Sys.Date(),"%Y-%m-%d"),
                            '/MacroRollingMCI',format(Sys.time(),'%d%b%Y'),'.csv'),row.names = F)

# rolling 5 year median for QMCI
rollingQMCI <- lawaMacroData%>%filter(Measurement=='QMCI')%>%select(LawaSiteID,sYear,Value,date)
setDT(rollingQMCI)     
setkey(rollingQMCI,LawaSiteID,sYear)
rollingQMCI[,rollQMCI:=rollMed(Value,5),by=LawaSiteID]

rollingQMCI <- rollingQMCI%>%filter(sYear>=2005)
#7928 to 7650
write.csv(rollingQMCI,paste0('h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/',
                            format(Sys.Date(),"%Y-%m-%d"),
                            '/MacroRollingQMCI',format(Sys.time(),'%d%b%Y'),'.csv'),row.names = F)


# rolling 5 year median for ASPM
rollingASPM <- lawaMacroData%>%filter(Measurement=='ASPM')%>%select(LawaSiteID,sYear,Value,date)
setDT(rollingASPM)     
setkey(rollingASPM,LawaSiteID,sYear)
rollingASPM[,rollASPM:=rollMed(Value,5),by=LawaSiteID]

rollingASPM <- rollingASPM%>%filter(sYear>=2005)
#6918 to 6717
write.csv(rollingASPM,paste0('h:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Analysis/',
                             format(Sys.Date(),"%Y-%m-%d"),
                             '/MacroRollingASPM',format(Sys.time(),'%d%b%Y'),'.csv'),row.names = F)


