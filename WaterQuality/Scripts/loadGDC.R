## Load libraries ------------------------------------------------
require(XML)     ### XML library to write hilltop XML
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(RCurl)

setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")

agency='gdc'
Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName)%>%unname%>%unlist
Measurements=c(Measurements,"WQ Sample")

siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])

con <- xmlOutputDOM("Hilltop")
con$addTag("Agency", agency)

for(i in 1:length(sites)){
  cat(sites[i],i,'out of',length(sites),'\n')
  for(j in 1:length(Measurements)){
    url <- paste0("http://hilltop.gdc.govt.nz/data.hts?service=Hilltop&request=GetData",
                 "&Site=",sites[i],
                 "&Measurement=",Measurements[j],
                 "&From=2004-01-01",
                 "&To=2021-01-01")
    url <- URLencode(url)
    xmlback <- ldWQ(url,agency,method='wininet',QC=T)
    if(!is.null(xmlback)){
      if(is.list(xmlback)){
        dataxml=xmlback$dataxml
        qcxml=xmlback$qcxml
        browser()
      }else{
        dataxml=xmlback
      }
      rm(xmlback)
      xmltop<-xmlRoot(dataxml)
      m<-xmltop[['Measurement']]
      # Create new node to replace existing <Data /> node in m
      DataNode <- newXMLNode("Data",attrs=c(DateFormat="Calendar",NumItems="2"))
      if(Measurements[j]=="WQ Sample"){
        ## Make new E node
        # Get Time values
        ans <- xpathSApply(m,"//T",xmlValue)
        for(k in 1:length(ans)){
          # Identifying and iterating through Parameter Elements of "m"
          p <- m[["Data"]][[k]] 
          # i'th E Element inside <Data></Data> tags
          c <- length(xmlSApply(p, xmlSize)) # number of children for i'th E Element inside <Data></Data> tags
          if(c>1){
            # Adding the new "E" node
            addChildren(DataNode, newXMLNode(name = "E",parent = DataNode))
            # Adding the new "T" node
            addChildren(DataNode[[xmlSize(DataNode)]], newXMLNode(name = "T",ans[k]))
            for(n in 2:c){   # Starting at '2' and '1' is the T element for time
              #added in if statement to pass over if any date&times with no metadata attached
              metaName  <- as.character(xmlToList(p[[n]])[1])   ## Getting the name attribute
              metaValue <- as.character(xmlToList(p[[n]])[2])   ## Getting the value attribute
              if(n==2){      # Starting at '2' and '1' is the T element for time
                item1 <- paste(metaName,metaValue,sep="\t")     ## Concatenating all pairs separated by tabs. Ack.
              } else {
                item1 <- paste(item1,metaName,metaValue,sep="\t")
              }
            }
            # Adding the Item1 node
            addChildren(DataNode[[xmlSize(DataNode)]], newXMLNode(name = "I1",item1))
          }
        }
      } else {
        ## Make new E node
        # Get Time values
        ansTime <- xpathSApply(m,"//T",xmlValue)
        ansValue <- xpathSApply(m,"//Value",xmlValue)
        
        # loop through TVP nodes
        for(N in 1:xmlSize(m[['Data']])){  ## Number of Time series values
          # loop through all Children - T, Value, Parameters ..    
          addChildren(DataNode, newXMLNode(name = "E",parent = DataNode))
          addChildren(DataNode[[xmlSize(DataNode)]], newXMLNode(name = "T",ansTime[N]))
          
          #Check for < or > or *
          ## Hand Greater than symbol
          if(grepl(pattern = "^\\>",x =  ansValue[N],perl = TRUE)){
            ansValue[N] <- substr(ansValue[N],2,nchar(ansValue[N]))
            item2 <- "$ND\t>\t"
            # Handle Less than symbols  
          } else if(grepl(pattern = "^\\<",x =  ansValue[N],perl = TRUE)){
            ansValue[N] <- substr(ansValue[N],2,nchar(ansValue[N]))
            item2 <- "$ND\t<\t"
            
            # Handle Asterisks  
          } else if(grepl(pattern = "^\\*",x =  ansValue[N],perl = TRUE)){
            ansValue[N] <- gsub(pattern = "^\\*", replacement = "", x = ansValue[N])
            item2 <- "$ND\t*\t"
          } else{
            item2 <- ""
          }
          addChildren(DataNode[[xmlSize(DataNode)]], newXMLNode(name = "I1",ansValue[N]))
          
          if(xmlSize(m[['Data']][[N]])>2){    ### Has attributes to add to Item 2
            for(n in 3:xmlSize(m[['Data']][[N]])){      
              #Getting attributes and building string to put in Item 2
              attrs <- xmlAttrs(m[['Data']][[N]][[n]])  
              item2 <- paste(item2,attrs[1],"\t",attrs[2],"\t",sep="")
            }
          }
          addChildren(DataNode[[xmlSize(DataNode)]], newXMLNode(name = "I2",item2))
        } 
      }
      oldNode <- m[['Data']]
      newNode <- DataNode
      replaceNodes(oldNode, newNode)
      con$addNode(m) 
    }
  }
}

# saveXML(con$value(), paste0("h:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"SWQ.xml",sep=""))
saveXML(con$value(), paste0("D:/LAWA/2021/",agency,"SWQ.xml"))
file.copy(from=paste0("D:/LAWA/2021/",agency,"SWQ.xml"),
          to=paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"SWQ.xml"),overwrite = T)

