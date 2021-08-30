require(xml2)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)
library(parallel)
library(doParallel)


setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")
agency='boprc'


translate <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%filter(Agency==agency)
# translate$CallName <- gsub('COMMA','.',translate$CallName)
translate$retName = c("Ammoniacal Nitrogen","Dissolved Reactive Phosphorus","E. coli (Escherichia coli)",
                      "Nitrogen Total  (Water)","Nitrite and Nitrate as N","Nitrite and Nitrate as N",
                      "pH","Phosphorus (Total)","Turbidity, water, unfiltered, broad band light source (400-680 nm), detection angle 90 +-30 degrees to incident light, nephelometric turbidity units (NTU)",
                      "Water Clarity (secchi or black disc)")
  

siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])


i=1

for(i in i:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  dir.create(paste0('D:/LAWA/2021/BOPRC/',make.names(sites[i])),showWarnings = F,recursive = T)
  for(j in 1:length(translate$CallName)){
    url <- paste0("http://sos.boprc.govt.nz/service?",
                  "service=SOS&version=2.0.0&request=GetObservation&",
                  "offering=",URLencode(translate$CallName[j],reserved = T),
                  "@",sites[i],
                  "&temporalfilter=om:phenomenonTime,P15Y/2021-01-01")
    
    url <- URLencode(url)
    
    destFile=paste0("D:/LAWA/2021/BOPRC","/",make.names(sites[i]),'/',make.names(translate$CallName[j]),".xml")
    if(!file.exists(destFile)|file.info(destFile)$size<3500){
      dl=download.file(url,destfile=destFile,method='wininet',quiet=T)
      if(!'try-error'%in%attr(dl,'class')){
        if(exists('Data'))rm(Data)
        Data=xml2::read_xml(destFile)
        Data = xml2::as_list(Data)[[1]]
        if(length(Data)>0&&names(Data)[1]!='Exception'){
          cat('.')
          RetProperty=attr(Data$observationData$OM_Observation$observedProperty,'title')
          RetCID = attr(Data$observationData$OM_Observation$featureOfInterest,'href')
          while(RetProperty!=translate$retName[j]|RetCID!=sites[i]){
            file.rename(from = destFile,to = paste0(make.names(RetProperty),destFile))
            dl=download.file(url,destfile=destFile,method='libcurl',quiet=T)
            Data=xml2::read_xml(destFile)
            Data = xml2::as_list(Data)[[1]]
            if(length(Data)>0&&names(Data)[1]!='Exception'){
              RetProperty=attr(Data$observationData$OM_Observation$observedProperty,'title')
              RetCID = attr(Data$observationData$OM_Observation$featureOfInterest,'href')
            }
          }
        }
      }
    }
  }
}
    




#Check site and measurement returned
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  for(j in 1:10){
    destFile=paste0("D:/LAWA/2021/BOPRC","/",
                    gsub('\\.','',make.names(sites[i])),'/',
                    make.names(translate$CallName[j]),".xml")
    if(file.exists(destFile)&file.info(destFile)$size>2000){
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
      if(length(Data)>0&&names(Data)[1]!='Error'){
        cat('.')
        RetProperty=attr(Data$observationData$OM_Observation$observedProperty,'title')
        RetCID = attr(Data$observationData$OM_Observation$featureOfInterest,'href')
        
        if(RetProperty!=translate$retName[j]|RetCID!=sites[i]){
          browser()
        }
      }
    }
  }
}


      

workers = makeCluster(8)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  library(dplyr)
  library(tidyr)
})
      

boprcSWQ=NULL
foreach(i = 1:length(sites),.combine = bind_rows,.errorhandling = 'stop',.inorder = FALSE)%dopar%{
  cat('\n',sites[i],i,'out of ',length(sites))
  siteDat=NULL
  for(j in 1:length(translate$CallName)){
    
    destFile=paste0("D:/LAWA/2021/BOPRC","/",make.names(sites[i]),'/',make.names(translate$CallName[j]),".xml")
    if(file.exists(destFile)&file.info(destFile)$size>1000){
      if(exists("Data"))rm(Data)
      Data=xml2::read_xml(destFile)
      Data = xml2::as_list(Data)[[1]]
      if(length(Data)>0){
        RetProperty=
          attr(Data$observationData$OM_Observation$observedProperty,'title')
        RetCID = attr(Data$observationData$OM_Observation$featureOfInterest,'href')
        
        if(RetProperty!=translate$retName[j]|RetCID!=sites[i]){
          file.remove(destFile)
          next
        }
        
        Data=Data$observationData$OM_Observation$result$MeasurementTimeseries
        if('defaultPointMetadata'%in%names(Data)){
          metaData = Data$defaultPointMetadata
          uom=attr(metaData$DefaultTVPMeasurementMetadata$uom,'code')
          rm(metaData)
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
          Data$measurement=translate$CallName[j]
          Data$retProp=RetProperty
          Data$CouncilSiteID = sites[i]
          Data$RetCID=strFrom(s = RetCID,c="stations/")
          Data$Units=uom
        }
      }
      rm(destFile)
    }else{
      Data=NULL
    }
    siteDat=bind_rows(siteDat,Data)
  }
  return(siteDat)
}->boprcSWQ
stopCluster(workers)
rm(workers)

      


save(boprcSWQ,file = 'boprcSWQraw.rData')
# load('boprcSWQraw.rData')
agency='boprc'


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
