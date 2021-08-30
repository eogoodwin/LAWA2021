## Load libraries ------------------------------------------------

require(dplyr)   ### dply library to manipulate table joins on dataframes
# require(XML)     ### XML library to write hilltop XML
# require(RCurl)
setwd("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/")


agency='ac'

acmacros = read_csv("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/2021 LAWA refresh_AC macro data_13Aug.csv")

acMacros <- acmacros%>%
  dplyr::select(CouncilSiteID,SiteID,Date=SampleDate,
                MCI,QMCI,ASPM=APSM,TaxaRichness,
                PercentageEPTTaxa="%EPTRichness",
                "Quality Code",CollMeth=CollectionMethod,ProcMeth=ProcessingMethod)%>%
  tidyr::gather(key="Measurement",value="Value",c("MCI",QMCI,ASPM,TaxaRichness,PercentageEPTTaxa))

acMacros$Agency='ac'
acMacros$Date=format(lubridate::dmy(acMacros$Date),'%d-%b-%y')
acMacros$CouncilSiteID=as.character(acMacros$CouncilSiteID)

#Auckland
acMacros$CouncilSiteID=trimws(acMacros$CouncilSiteID)
store=acMacros$CouncilSiteID
acMacros$CouncilSiteID=trimws(acMacros$SiteID)
acMacros$SiteID=store
rm(store)
acMacros <- acMacros%>%select(-SiteID)
sort(unique(tolower(acMacros$CouncilSiteID))[unique(tolower(acMacros$CouncilSiteID))%in%tolower(siteTable$CouncilSiteID)])
  
  
  acMacros$CouncilSiteID[acMacros$CouncilSiteID=="Meola Creek @ Motions Rd"] <- "Meola Ck @ Motions Rd"

dir.create(paste0("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/",
                  format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)

write.csv(acMacros,
          file=paste0('H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/',
                       format(Sys.Date(),"%Y-%m-%d"),'/ac.csv'),
          row.names=F)


 