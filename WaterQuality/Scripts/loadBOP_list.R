require(xml2)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)
library(parallel)
library(doParallel)


setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")
agency='boprc'

Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName)%>%unname%>%unlist
Measurements <- gsub('COMMA',',',Measurements)
# Measurements=c(Measurements,'WQ Sample')

siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])

workers = makeCluster(3)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)  
  library(dplyr)
  library(tidyr)
})

options(timeout=5)
if(exists("Data"))rm(Data)
boprcSWQ=NULL
suppressMessages({
  for(i in 1:length(sites)){
    cat('\n',sites[i],i,'out of ',length(sites))
    suppressWarnings({rm(siteDat)})
    foreach(j = 1:length(Measurements),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
      url <- paste0("http://sos.boprc.govt.nz/service?",
                    "service=SOS&version=2.0.0&request=GetObservation&",
                    "offering=",Measurements[j],
                    "@",sites[i],
                    "&temporalfilter=om:phenomenonTime,P15Y/2021-01-01")
      
      url <- URLencode(url)
      
      destFile=paste0("D:/LAWA/2021/tmp",gsub(' ','',Measurements[j]),"WQboprc.xml")
      
      dl=try(download.file(url,destfile=destFile,method='curl',quiet=T,cacheOK = F),silent = T)
      if(!'try-error'%in%attr(dl,'class')){
        Data=xml2::read_xml(destFile)
        Data = xml2::as_list(Data)[[1]]
        if(length(Data)>0){
          Data=Data$observationData$OM_Observation$result$MeasurementTimeseries
          if('defaultPointMetadata'%in%names(Data)){
            metaData = Data$defaultPointMetadata
            uom=attr(metaData$DefaultTVPMeasurementMetadata$uom,'code')
          }else{
            uom='unfound'
          }
          Data=do.call(rbind, unname(sapply(Data,FUN=function(listItem){
            if("MeasurementTVP"%in%names(listItem)){
              if(length(listItem$MeasurementTVP$value)>0){
                retVal=data.frame(time=unlist(listItem$MeasurementTVP$time),
                                  value=unlist(listItem$MeasurementTVP$value),
                                  Censored=F)
              }else{
                retVal = data.frame(time=unlist(listItem$MeasurementTVP$time),
                                    value=unlist(listItem$MeasurementTVP$metadata$TVPMeasurementMetadata$qualifier$Quantity$value),
                                    Censored=T)
              }
              return(retVal)
            }
          })))
          if(!is.null(Data)){
            Data$measurement=Measurements[j]
            Data$Units=uom
          }
        }
      }else{Data=NULL}
      file.remove(destFile)
      rm(destFile)
      return(Data)
    }->siteDat
    
    if(!is.null(siteDat)){
      rownames(siteDat) <- NULL
      siteDat$CouncilSiteID = sites[i]
      boprcSWQ=bind_rows(boprcSWQ,siteDat)
    }
    rm(siteDat)
  }
})
stopCluster(workers)
rm(workers)



  save(boprcSWQ,file = 'boprcSWQraw.rData')
  # load('boprcSWQraw.rData')
   agency='boprc'
  
   
   
   
   
  Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
    filter(Agency==agency)%>%select(CallName)%>%unname%>%unlist
  Measurements <- gsub('COMMA',',',Measurements)
  

boprcSWQ <- boprcSWQ%>%select(CouncilSiteID,Date=time,Value=value,Measurement=measurement,Units)



boprcSWQb=data.frame(CouncilSiteID=boprcSWQ$CouncilSiteID,
                   Date=as.character(format(lubridate::ymd_hms(boprcSWQ$Date),'%d-%b-%y')),
                   Value=boprcSWQ$Value,
                   Measurement=boprcSWQ$Measurement,
                   Units=boprcSWQ$Units,
                   Censored=grepl(pattern = '<|>',x = boprcSWQ$Value),
                   CenType=F,
                   QC=NA)
boprcSWQb$CenType[grep('<',boprcSWQb$Value)] <- 'Left'
boprcSWQb$CenType[grep('>',boprcSWQb$Value)] <- 'Right'

boprcSWQb$Value = readr::parse_number(boprcSWQb$Value)


translate=read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName,LAWAName)

table(boprcSWQb$Measurement,useNA = 'a')
boprcSWQb$Measurement <- as.character(factor(boprcSWQb$Measurement,
                                           levels = translate$CallName,
                                           labels = translate$LAWAName))
table(boprcSWQb$Measurement,useNA='a')



#Censor the data there that came in.
# Datasource or Parameter Type	Measurement or Timeseries Name	Units	AQUARIUS Parameter	              Detection Limit
# Total Oxidised Nitrogen	      Total Oxidised Nitrogen	        g/m3	Nitrite Nitrate _as N__LabResult	0.001
# Total Nitrogen	              Total Nitrogen	                g/m3	N _Tot__LabResult	                0.01
# Ammoniacal Nitrogen	          Ammoniacal Nitrogen	            g/m3	Ammoniacal N_LabResult	          0.002
# Dissolved Reactive Phosphorus	DRP	                            g/m3	DRP_LabResult	                    0.001
# Total Phosphorus	            TP	                            g/m3	P _Tot__LabResult	                0.001
# Turbidity	                    Turbidity	                      NTU	  Turbidity, Nephelom_LabResult	    0.1
# pH	                          pH  	                      pH units	pH_LabResult	                    0.2
# Visual Clarity	              BDISC	                            m	  Water Clarity_LabResult	          0.01
# Escherichia coli	            Ecoli	                      /100 ml	  E coli_LabResult	                1
cenTON = which(boprcSWQb$Measurement=="TON"&boprcSWQb$Value<0.001)
cenTN =  which(boprcSWQb$Measurement=="TN"&boprcSWQb$Value<0.01)
cenNH4 = which(boprcSWQb$Measurement=="NH4"&boprcSWQb$Value<0.002)
cenDRP = which(boprcSWQb$Measurement=="DRP"&boprcSWQb$Value<0.001)
cenTP =  which(boprcSWQb$Measurement=="TP"&boprcSWQb$Value<0.001)
cenTURB =which(boprcSWQb$Measurement=="TURB"&boprcSWQb$Value<0.1)
cenPH =  which(boprcSWQb$Measurement=="PH"&boprcSWQb$Value<0.2)
cenCLAR =which(boprcSWQb$Measurement=="BDISC"&boprcSWQb$Value<0.01)
cenECOLI=which(boprcSWQb$Measurement=="ECOLI"&boprcSWQb$Value<1)
if(length(cenTON)>0){
  boprcSWQb$Censored[cenTON] <- TRUE
  boprcSWQb$CenType[cenTON] <- "Left"
  boprcSWQb$Value[cenTON] <- 0.001
}
if(length(cenTN)>0){
  boprcSWQb$Censored[cenTN] <- TRUE
  boprcSWQb$CenType[cenTN] <- "Left"
  boprcSWQb$Value[cenTN] <- 0.01
}
if(length(cenNH4)>0){
  boprcSWQb$Censored[cenNH4] <- TRUE
  boprcSWQb$CenType[cenNH4] <- "Left"
  boprcSWQb$Value[cenNH4] <- 0.002
}
if(length(cenDRP)>0){
  boprcSWQb$Censored[cenDRP] <- TRUE
  boprcSWQb$CenType[cenDRP] <- "Left"
  boprcSWQb$Value[cenDRP] <- 0.001
}
if(length(cenTP)>0){
  boprcSWQb$Censored[cenTP] <- TRUE
  boprcSWQb$CenType[cenTP] <- "Left"
  boprcSWQb$Value[cenTP] <- 0.001
}
if(length(cenTURB)>0){
  boprcSWQb$Censored[cenTURB] <- TRUE
  boprcSWQb$CenType[cenTURB] <- "Left"
  boprcSWQb$Value[cenTURB] <- 0.1
}
if(length(cenPH)>0){
  boprcSWQb$Censored[cenPH] <- TRUE
  boprcSWQb$CenType[cenPH] <- "Left"
  boprcSWQb$Value[cenTON] <- 0.2
}
if(length(cenCLAR)>0){
  boprcSWQb$Censored[cenCLAR] <- TRUE
  boprcSWQb$CenType[cenCLAR] <- "Left"
  boprcSWQb$Value[cenCLAR] <- 0.01
}
if(length(cenECOLI)>0){
  boprcSWQb$Censored[cenECOLI] <- TRUE
  boprcSWQb$CenType[cenECOLI] <- "Left"
  boprcSWQb$Value[cenECOLI] <- 1
}
rm(cenTON,cenTN,cenNH4,cenDRP,cenTP,cenTURB,cenPH,cenCLAR,cenECOLI)

# boprcSWQb <- merge(boprcSWQb,siteTable,by='CouncilSiteID')
boprcSWQb <- unique(boprcSWQb)


write.csv(boprcSWQb,file = paste0("D:/LAWA/2021/boprc.csv"),row.names = F)
file.copy(from=paste0("D:/LAWA/2021/boprc.csv"),
          to = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/boprc.csv"),
          overwrite = T)
rm(boprcSWQ,boprcSWQb)
