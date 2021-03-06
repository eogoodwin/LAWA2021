## Load libraries ------------------------------------------------
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(XML)     ### XML library to write hilltop XML
# require(RCurl)

setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")

agency='es'
tab="\t"
Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%dplyr::select(CallName)%>%unname%>%unlist
# Measurements=c(Measurements,'WQ Sample')

Measurements[1] <- "Black%20Disc%20(Field)%20[Clarity%20(Black%20Disc,%20Field)]"#"Clarity (Black Disc, Field)"

siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])

# sites = gsub(pattern = 'Wairaki River at',replacement = "Wairaki River ds",x = sites)
# sites = gsub(pattern = 'Dipton Rd',replacement = "Dipton Road",x = sites)
# sites = gsub(pattern = 'Makarewa Confluence',replacement = "Makarewa Confl",x = sites)

suppressWarnings({rm(Data)})
for(i in 1:length(sites)){
  cat(sites[i],i,'out of',length(sites),'\n')
  for(j in 1:length(Measurements)){
    if(Measurements[j]=="E-Coli <CFU>"){
      url <- paste0("http://odp.es.govt.nz/WQ.hts?service=Hilltop&request=GetData",
                    "&Site=",sites[i],
                    "&Measurement=E-Coli <CFU>",
                    "&From=2004-01-01",
                    "&To=2021-01-01")
      url <- URLencode(url)
      xmlfile <- ldWQ(url,agency) 
      
      urlWQ <- paste0("http://odp.es.govt.nz/WQ.hts?service=Hilltop&request=GetData",
                      "&Site=",sites[i],
                      "&Measurement=WQ Sample",
                      "&From=2004-01-01",
                      "&To=2021-01-01")
      urlWQ <- URLencode(urlWQ)
      xmlfileWQ <- ldWQ(urlWQ,agency)  
      if(!is.null(xmlfile)){
        timeWQ <- sapply(getNodeSet(doc=xmlfileWQ, "//E/Parameter[@Value='SOE River Water Quality']/../T"), xmlValue)
        df2 <- data.frame(time=timeWQ)
        if(nrow(df2)!=0){
          cat(Measurements[j],'\t')
          time <- sapply(getNodeSet(doc=xmlfile, "//T"), xmlValue)
          value <- sapply(getNodeSet(doc=xmlfile, "//Value"), xmlValue)
          u <- sapply(getNodeSet(doc=xmlfile, "//Units"), xmlValue)
          #Add in bit here to get the measurements I2 Info
          df4 <- data.frame(Site=sites[i],Measurement=Measurements[j],time, value, Units=u,stringsAsFactors=FALSE)
          df4 <- merge(df2,df4, by= "time")
          df4 <- na.omit(df4)
          df4=df4%>%dplyr::select(Site,Measurement,time,value,Units)
          if(!exists("Data")){
            Data <- df4
          } else{
            Data <- rbind.data.frame(Data, df4)
          }
        }
      }
      #add an else if for WQ sample to pull in data to add as a column in data frame, 
    }else{
      url <- paste0("http://odp.es.govt.nz/WQ.hts?service=Hilltop&request=GetData",
                    "&Site=",sites[i],
                    "&Measurement=",Measurements[j],
                    "&From=2006-01-01",
                    "&To=2021-01-01")
      url <- URLencode(url)
      xmlfile <- ldWQ(url,agency,method='wininet')
      if(!is.null(xmlfile)){
        DataType <- sapply(getNodeSet(doc=xmlfile, "//DataType"), xmlValue)
        # print(DataType)
        if(length(DataType)==0){next}
        cat(Measurements[j],'\t')
        if (DataType == "WQData"){
          time <- sapply(getNodeSet(doc=xmlfile, "//T"), xmlValue)
          value <- sapply(getNodeSet(doc=xmlfile, "//Value"), xmlValue)
          u <- sapply(getNodeSet(doc=xmlfile, "//Units"), xmlValue)
          #Add in bit here to get the I2 info
          df <- data.frame(Site=sites[i],Measurement=Measurements[j],time, value,Units=u,stringsAsFactors = FALSE)
          if(!exists("Data")){
            Data <- df
          } else{
            Data <- rbind.data.frame(Data, df)
          }
        }else{
          next
        }
      } 
    }
  }
}


Data$Measurement[Data$Measurement=="Black%20Disc%20(Field)%20[Clarity%20(Black%20Disc,%20Field)]"] <- "Clarity (Black Disc Field)"

#Save it here as csv!

con <- xmlOutputDOM("Hilltop")
con$addTag("Agency", "ES")

max<-nrow(Data)

i<-1
#for each site
while(i<=max){
  s<-Data$Site[i]
  # store first counter going into while loop to use later in writing out sample values
  start<-i
  
  cat(i,Data$Site[i],"\n")   ### Monitoring progress as code runs
  
  while(Data$Site[i]==s){
    #for each measurement
    #cat(datatbl$CouncilSiteID[i],"\n")
    con$addTag("Measurement",  attrs=c(CouncilSiteID=Data$Site[i]), close=FALSE)
    con$addTag("DataSource",  attrs=c(Name=Data$Measurement[i],NumItems="2"), close=FALSE)
    con$addTag("TSType", "StdSeries")
    con$addTag("DataType", "WQData")
    con$addTag("Interpolation", "Discrete")
    con$addTag("ItemInfo", attrs=c(ItemNumber="1"),close=FALSE)
    con$addTag("ItemName", Data$Measurement[i])
    con$addTag("ItemFormat", "F")
    con$addTag("Divisor", "1")
    con$addTag("Units", Data$Units[i])
    #con$addTag("Units", "Joking")
    con$addTag("Format", "#.###")
    con$closeTag() # ItemInfo
    con$closeTag() # DataSource
    #saveXML(con$value(), file="out.xml")
    
    # for the TVP and associated measurement water quality parameters
    con$addTag("Data", attrs=c(DateFormat="Calendar", NumItems="2"),close=FALSE)
    d<- Data$Measurement[i]
    e<-Data$Site[i]
    
    cat("       - ",Data$Measurement[i],"\n")   ### Monitoring progress as code runs
    
    while(Data$Measurement[i]==d & Data$Site[i]==s){
      # for each tvp
      con$addTag("E",close=FALSE)
      con$addTag("T",Data$time[i])
      
      
      #Check for < or > or *
      ## Handle Greater than symbol
      if(grepl(pattern = "^\\>",x =  Data$value[i],perl = TRUE)){
        elemValue <- substr(Data$value[i],2,nchar(Data$value[i]))
        item2 <- paste("$ND",tab,">",tab,sep="")
        
        # Handle Less than symbols
      } else if(grepl(pattern = "^\\<",x =  Data$value[i],perl = TRUE)){
        elemValue <- substr(Data$value[i],2,nchar(Data$value[i]))
        item2 <- paste("$ND",tab,"<",tab,sep="")
        
        # Handle Asterixes
      } else if(grepl(pattern = "^\\*",x =  Data$value[i],perl = TRUE)){
        elemValue <- gsub(pattern = "^\\*", replacement = "", x = Data$value[i])
        item2 <- paste("$ND",tab,"*",tab,sep="")
      } else{
        elemValue <- Data$value[i]
        item2 <- ""
      }
      
      con$addTag("I1", elemValue)
      if(exists("item2")){
        item2 <- paste(item2,"Units", tab, Data$Units[i], tab, sep="")
      } else {
        item2 <- paste("Units", tab, Data$Units[i], tab, sep="")
      }
      con$addTag("I2", item2)
      rm(item2)
      
      con$closeTag() # E
      i<-i+1 # incrementing overall for loop counter
      if(i>max){break}
    }
    
    # next
    con$closeTag() # Data
    con$closeTag() # Measurement
    
    if(i>max){break}
    # Next
  }
  # store last counter going out of while loop to use later in writing out sample values
  end<-i-1
  
  # Adding WQ Sample Datasource to finish off this Site
  # along with Sample parameters
  con$addTag("Measurement",  attrs=c(CouncilSiteID=Data$Site[start]), close=FALSE)
  con$addTag("DataSource",  attrs=c(Name="WQ Sample", NumItems="1"), close=FALSE)
  con$addTag("TSType", "StdSeries")
  con$addTag("DataType", "WQSample")
  con$addTag("Interpolation", "Discrete")
  con$addTag("ItemInfo", attrs=c(ItemNumber="1"),close=FALSE)
  con$addTag("ItemName", "WQ Sample")
  con$addTag("ItemFormat", "S")
  con$addTag("Divisor", "1")
  con$addTag("Units")
  con$addTag("Format", "$$$")
  con$closeTag() # ItemInfo
  con$closeTag() # DataSource
  
  # for the TVP and associated measurement water quality parameters
  con$addTag("Data", attrs=c(DateFormat="Calendar", NumItems="1"),close=FALSE)
  # for each tvp
  ## LOAD SAMPLE PARAMETERS
  ## SampleID, ProjectName, SourceType, SamplingMethod and mowsecs
  sample<-Data[start:end,3]
  sample<-unique(sample)
  sample<-sample[order(sample)]
  ## THIS NEEDS SOME WORK.....
  for(a in 1:length(sample)){
    con$addTag("E",close=FALSE)
    con$addTag("T",sample[a])
    #put metadata in here when it arrives
    con$addTag("I1", "")
    con$closeTag() # E
  }
  
  con$closeTag() # Data
  con$closeTag() # Measurement
  
  
}

# saveXML(con$value(), paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"SWQ.xml"))
saveXML(con$value(), paste0("D:/LAWA/2021/",agency,"SWQ.xml"))
file.copy(from=paste0("D:/LAWA/2021/",agency,"SWQ.xml"),
          to=paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"SWQ.xml"),
          overwrite=T)
