## Load libraries ------------------------------------------------
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(XML)     ### XML library to write hilltop XML
# require(RCurl)
setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")


agency='ac'
Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName)%>%unname%>%unlist
Measurements=c(Measurements,'WQ Sample')

siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])

setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")

if(exists('Data'))rm(Data)

for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of',length(sites),'\t')
  for(j in 1:length(Measurements)){
    url <- paste0("http://aklc.hydrotel.co.nz:8080/KiWIS/KiWIS?datasource=3",
                  "&Procedure=Sample.Results.LAWA&service=SOS&version=2.0.0&request=GetObservation",
                  "&observedProperty=",Measurements[j],
                  "&featureOfInterest=",sites[i],
                  "&temporalfilter=om:phenomenonTime,P25Y/2021-01-01")
    url <- URLencode(url)
    url <- gsub(pattern = '\\+',replacement = '%2B',x = url)
    xmlfile <- ldWQ(url,agency,QC=T)
    
    if(xpathApply(xmlRoot(xmlfile),path="count(//wml2:point)",xmlValue)>0){
      cat(Measurements[j],'\t')
      #STEP 1: Load wml2:DefaultTVPMetadata to a vector
      xattrs_qualifier         <- xpathSApply(xmlfile, "//wml2:DefaultTVPMeasurementMetadata/wml2:qualifier/@xlink:title")
      if(is.null(xattrs_qualifier)){
        xattrs_qualifier <- ""
      }
      xattrs_uom               <- xpathSApply(xmlfile, "//wml2:DefaultTVPMeasurementMetadata/wml2:uom/@code")
      xattrs_interpolationType <- xpathSApply(xmlfile, "//wml2:DefaultTVPMeasurementMetadata/wml2:interpolationType/@xlink:href")
      xattrs_default           <- c(xattrs_qualifier,xattrs_uom,xattrs_interpolationType)
      names(xattrs_default)    <- c("qualifier","uom","interpolationType")
      rm(xattrs_qualifier,xattrs_uom,xattrs_interpolationType)
      
      #STEP 2: Get wml2:MeasurementTVP metadata values
      xattrs_qualifier         <- xpathSApply(xmlfile, "//wml2:TVPMeasurementMetadata/wml2:qualifier/@xlink:title")
      #If xattrs_qualifier is empty, it means there are no additional qualifiers in this timeseries.
      #Test for Null, and create an empty dataframe as a consequence 
      if(is.null(xattrs_qualifier)){
        df_xattrs <- data.frame(time=character(),qualifier=character())
      } else{
        xattrs_time              <- sapply(getNodeSet(doc=xmlfile, "//wml2:TVPMeasurementMetadata/wml2:qualifier/@xlink:title/../../../../wml2:time"), xmlValue)
        #Store measurementTVPs in dataframe to join back into data after it is all retrieved
        df_xattrs <- as.data.frame(xattrs_time,stringsAsFactors = FALSE)
        names(df_xattrs) <- c("time")
        df_xattrs$qualifier <- xattrs_qualifier
        rm(xattrs_time,xattrs_qualifier)
      }    
      #Step3
      #Create vector of times
      time <- sapply(getNodeSet(doc=xmlfile, "//wml2:time"), xmlValue)
      #Create vector of  values
      value <- sapply(getNodeSet(doc=xmlfile, "//wml2:value"), xmlValue)
      # df <- data.frame(time=time,value=value, stringsAsFactors = FALSE)
      df=data.frame(CouncilSiteID=sites[i],
                    Measurement=Measurements[j],
                    time=time,
                    value=value,
                    Units=xattrs_default[2])
      rm(time, value)
      
      # merge in additional qualifiers, if present, from df_xattrs
      if(nrow(df_xattrs)!=0) {
        df <- merge(df,df_xattrs,by="time",all=TRUE)
        df$qualifier[is.na(df$qualifier)] <- xattrs_default[1]
      } else {
        df$qualifier<-xattrs_default[1]
      }
      
      # Remove default metadata attributes for current timeseries
      rm(xattrs_default, df_xattrs)
      
      if(!exists("Data")){
        Data <- df
      } else{
        Data <- rbind.data.frame(Data, df)
      }
      rm(df)
    }
    rm(xmlfile)
  }
}
qualifiers_added <- unique(Data$qualifier)

Data$time=as.character(Data$time)
Data$CouncilSiteID=as.character(Data$CouncilSiteID)
Data$Measurement=as.character(Data$Measurement)
Data$value=as.character(Data$value)
Data$Units=as.character(Data$Units)



if(0){
  unique(Data$qualifier)
  sapply(as.numeric(unique(Data$qualifier)),FUN=function(x)bitwAnd(x,255))
  sapply(as.numeric(unique(Data$qualifier)),FUN=function(x)bitwAnd(x,511))
  sapply(as.numeric(unique(Data$qualifier)),FUN=function(x)bitwAnd(x,1023))
  sapply(as.numeric(unique(Data$qualifier)),FUN=function(x)bitwAnd(x,2047))
  sapply(as.numeric(unique(Data$qualifier)),FUN=function(x)bitwAnd(x,4095))
  sapply(as.numeric(unique(Data$qualifier)),FUN=function(x)bitwAnd(x,8191))
  sapply(as.numeric(unique(Data$qualifier)),FUN=function(x)bitwAnd(x,16383))
  sapply(as.numeric(unique(Data$qualifier)),FUN=function(x)bitwAnd(x,32767))
  sapply(as.numeric(unique(Data$qualifier)),FUN=function(x)bitwAnd(x,32767))-sapply(as.numeric(unique(Data$qualifier)),FUN=function(x)bitwAnd(x,255))
  
  sapply(c(10, 16394, 43, 151, 30, 8222, 42, 16426, 16427),FUN=function(x)paste(sapply(strsplit(paste(rev(intToBits(x))),""),`[[`,2),collapse=""))
  unique(sapply(sapply(c(10, 16394, 43, 151, 30, 8222, 42, 16426, 16427),
                       FUN=function(x)bitwAnd(x,sum(2^(13:14)))),
                FUN=function(x)paste(sapply(strsplit(paste(rev(intToBits(x))),""),`[[`,2),collapse="")))
}

Data = Data%>%filter(!bitwAnd(as.numeric(qualifier),255)%in%c(42,151))
#54799 to 54617



acSWQ=data.frame(CouncilSiteID=Data$CouncilSiteID,
                 Date=format(lubridate::ymd_hms(Data$time)),
                 Value=Data$value,
                 # Method=NA,
                 Measurement=Data$Measurement,
                 Units=Data$Units,
                 Censored=Data$qualifier,
                 centype=F,
                 QC=NA)

acSWQ$centype=sapply(as.numeric(acSWQ$Censored),FUN=function(cenCode){
  retVal=""
  if(is.na(cenCode)){return(retVal)}
  if(bitwAnd(2^13,cenCode)==2^13){
    retVal=paste0(retVal,'>')
  }
  if(bitwAnd(2^14,cenCode)==2^14){
    retVal=paste0(retVal,'<')
  }
  return(retVal)
})

acSWQ$Censored=FALSE
acSWQ$Censored[acSWQ$centype!=""] <- TRUE

acSWQ$centype=as.character(factor(acSWQ$centype,
                                  levels = c('<','','>'),
                                  labels=c('Left','FALSE','Right')))

transfers=read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",
                     sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(-Agency)

table(acSWQ$Measurement)
acSWQ$Measurement = factor(acSWQ$Measurement,levels=transfers$CallName,labels = transfers$LAWAName)
table(acSWQ$Measurement)

acSWQ = merge(acSWQ,siteTable,by='CouncilSiteID')

# By this point, we have all the data downloaded from the council, in a data frame called Data.
write.csv(acSWQ,file = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/ac.csv"),row.names = F)

# 
# con <- xmlOutputDOM("Hilltop")
# con$addTag("Agency", agency)
# 
# max<-nrow(Data)
# 
# i<-1
# #for each site
# while(i<=max){
#   s<-Data$Site[i]
#   # store first counter going into while loop to use later in writing out sample values
#   start<-i
#   
#   cat(i,Data$Site[i],"\n")   ### Monitoring progress as code runs
#   
#   while(Data$Site[i]==s){
#     #for each measurement
#     con$addTag("Measurement",  attrs=c(SiteName=Data$Site[i]), close=FALSE)  #Until 21/6/19 this was CouncilSiteID=Data$Site, which yeah it should be, but, consistency with other agencies
#     con$addTag("DataSource",  attrs=c(Name=Data$Measurement[i],NumItems="2"), close=FALSE)
#     con$addTag("TSType", "StdSeries")
#     con$addTag("DataType", "WQData")
#     con$addTag("Interpolation", "Discrete")
#     con$addTag("ItemInfo", attrs=c(ItemNumber="1"),close=FALSE)
#     con$addTag("ItemName", Data$Measurement[i])
#     con$addTag("ItemFormat", "F")
#     con$addTag("Divisor", "1")
#     con$addTag("Units", Data$Units[i])
#     con$addTag("Format", "#.###")
#     con$closeTag() # ItemInfo
#     con$closeTag() # DataSource
#     
#     # for the TVP and associated measurement water quality parameters
#     con$addTag("Data", attrs=c(DateFormat="Calendar", NumItems="2"),close=FALSE)
#     d<- Data$Measurement[i]
#     
#     cat("       - ",Data$Measurement[i],"\n")   ### Monitoring progress as code runs
#     
#     while(Data$Measurement[i]==d & Data$Site[i]==s){
#       # for each tvp
#       # Handle Greater than symbols
#       
#       # Handle Less than symbols  
#       if(!is.na(Data$qualifier[i])){    # this will need to be expanded to deal with range of qualifiers
#         # if(grepl("8202",Data$qualifier[i])){                   ## GREATER THAN VALUES
#           if(bitwAnd(2^13,as.numeric(Data$qualifier[i]))==2^13){
#           con$addTag("E",close=FALSE)
#           con$addTag("T",Data$time[i])
#           con$addTag("I1", Data$value[i])
#           con$addTag("I2", "$ND\t>\t")
#           con$closeTag() # E
#         # } else if(grepl("16394",Data$qualifier[i])){           ## LESS THAN VALUES
#         }else if (bitwAnd(2^14,as.numeric(Data$qualifier[i]))==2^14){
#           con$addTag("E",close=FALSE)
#           con$addTag("T",Data$time[i])
#           con$addTag("I1", Data$value[i])
#           con$addTag("I2", "$ND\t<\t")
#           con$closeTag() # E 
#         } else {                                               ## UNCENSORED VALUES
#           con$addTag("E",close=FALSE)
#           con$addTag("T",Data$time[i])
#           con$addTag("I1", Data$value[i])
#           con$addTag("I2", "\t")
#           con$closeTag() # E
#         }
#         # Write all other result values  
#       } else {                                                 ## UNCENSORED VALUES
#         con$addTag("E",close=FALSE)
#         con$addTag("T",Data$time[i])
#         con$addTag("I1", Data$value[i])
#         con$addTag("I2", "\t")
#         con$closeTag() # E
#         
#       }
#       
#       i<-i+1 # incrementing overall for loop counter
#       if(i>max){break}
#     }
#     # next
#     con$closeTag() # Data
#     con$closeTag() # Measurement
#     
#     if(i>max){break}
#     # Next 
#   }
#   # store last counter going out of while loop to use later in writing out sample values
#   end<-i-1
#   
#   # Adding WQ Sample Datasource to finish off this Site
#   # along with Sample parameters
#   con$addTag("Measurement",  attrs=c(SiteName=Data$Site[start]), close=FALSE)#Until 21/6/19 this was CouncilSiteID=Data$Site, which yeah it should be, but, consistency with other agencies
#   con$addTag("DataSource",  attrs=c(Name="WQ Sample", NumItems="1"), close=FALSE)
#   con$addTag("TSType", "StdSeries")
#   con$addTag("DataType", "WQSample")
#   con$addTag("Interpolation", "Discrete")
#   con$addTag("ItemInfo", attrs=c(ItemNumber="1"),close=FALSE)
#   con$addTag("ItemName", "WQ Sample")
#   con$addTag("ItemFormat", "S")
#   con$addTag("Divisor", "1")
#   con$addTag("Units")
#   con$addTag("Format", "$$$")
#   con$closeTag() # ItemInfo
#   con$closeTag() # DataSource
#   
#   # for the TVP and associated measurement water quality parameters
#   con$addTag("Data", attrs=c(DateFormat="Calendar", NumItems="1"),close=FALSE)
#   # for each tvp
#   ## LOAD SAMPLE PARAMETERS
#   ## SampleID, ProjectName, SourceType, SamplingMethod and mowsecs
#   sample<-Data[start:end,3]
#   sample<-unique(sample)
#   sample<-sample[order(sample)]
#   ## THIS NEEDS SOME WORK.....
#   for(a in 1:length(sample)){ 
#     con$addTag("E",close=FALSE)
#     con$addTag("T",sample[a])
#     #put metadata in here when it arrives
#     con$addTag("I1", "")
#     con$closeTag() # E
#   }
#   con$closeTag() # Data
#   con$closeTag() # Measurement    
# }
# 
# # saveXML(con$value(), file = paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/MetaData/",format(Sys.Date(),'%Y-%m-%d'),"/",agency,"SWQ.xml"))
# saveXML(con$value(), paste0("D:/LAWA/2021/",agency,"SWQ.xml"))
# file.copy(from=paste0("D:/LAWA/2021/",agency,"SWQ.xml"),
#           to=paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"SWQ.xml"))
