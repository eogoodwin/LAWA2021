## Load libraries ------------------------------------------------

require(dplyr)   ### dply library to manipulate table joins on dataframes
# require(XML)     ### XML library to write hilltop XML
# require(RCurl)
setwd("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/")


agency='ac'

acmacros = read_csv("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/2021 LAWA refresh_AC macro data.csv")

acMacros <- acmacros%>%
  dplyr::select(CouncilSiteID,LAWASiteID,SiteID,Date=SampleDate,
                MCI,ASPM=APSM,TaxaRichness,
                "%EPTRichness","Quality Code")%>%
    tidyr::gather(key="Measurement",value="Value",c("MCI",ASPM,TaxaRichness,"%EPTRichness"))
acMacros$Agency='ac'
acMacros$Date=format(lubridate::dmy(acMacros$Date),'%d-%b-%y')

dir.create(paste0("H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/",
                  format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)

write.csv(acMacros,
          file=paste0('H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data/',
                       format(Sys.Date(),"%Y-%m-%d"),'/',agency,'.csv'),
          row.names=F)

 cat("AC MACROS DELIVERED BY SPREADSHEET")

 