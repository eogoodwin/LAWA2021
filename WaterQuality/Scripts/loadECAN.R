## Load libraries ------------------------------------------------
require(dplyr)   ### dply library to manipulate table joins on dataframes
require(xml2)     ### XML library to write hilltop XML
require(RCurl)

agency='ecan'
setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality")
Measurements <- read.table("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/Transfers_plain_english_view.txt",sep=',',header=T,stringsAsFactors = F)%>%
  filter(Agency==agency)%>%select(CallName)%>%unname%>%unlist
Measurements=c(Measurements,'WQ Sample')

siteTable=loadLatestSiteTableRiver()
sites = unique(siteTable$CouncilSiteID[siteTable$Agency==agency])


if(exists("Data"))rm(Data)
ecanSWQ=NULL
for(i in 1:length(sites)){
  cat('\n',sites[i],i,'out of ',length(sites))
  siteDat=NULL
  for(j in 1:length(Measurements)){
    url <- paste0("http://wateruse.ecan.govt.nz/wqlawa.hts?service=Hilltop&Agency=LAWA&request=GetData",
                  "&Site=",sites[i],
                  "&Measurement=",Measurements[j],
                  "&From=2004-01-01",
                  "&To=2021-01-01")
    url <- URLencode(url)
    
    
    dl=try(download.file(url,destfile="D:/LAWA/2021/tmpWQecan.xml",method='curl',quiet=T),silent = T)
    if(dl>0){
      cat('.')
      Data=xml2::read_xml("D:/LAWA/2021/tmpWQecan.xml")
      Data = xml2::as_list(Data)[[1]]
      dataSource = unlist(Data$Measurement$DataSource)
      Data = Data$Measurement$Data
      if(length(Data)>0){
        cat('.')
        Data=lapply(seq_along(Data),function(listIndex){
          liout <- unlist(Data[[listIndex]][c("T","Value")])
          liout=data.frame(Name=names(liout),Value=unname(liout))
          if(length(Data[[listIndex]])>2){
            attrs=sapply(seq_along(Data[[listIndex]])[-c(1,2)],
                         FUN=function(inListIndex){
                           as.data.frame(attributes(Data[[listIndex]][inListIndex]$Parameter))
                         })
            if(class(attrs)=="list"&&any(lengths(attrs)==0)){
              attrs[[which(lengths(attrs)==0)]] <- NULL
              liout=rbind(liout,bind_rows(attrs))
            }else{
              attrs=t(attrs)
              liout=rbind(liout,attrs)
            }
          }
          return(liout)
        })
        
        allNames = unique(unlist(t(sapply(Data,function(li)li$Name))))
        
        eval(parse(text=paste0("Datab=data.frame(`",paste(allNames,collapse='`=NA,`'),"`=NA)")))
        for(dd in seq_along(Data)){
          newrow=Data[[dd]]
          newrow=pivot_wider(newrow,names_from = 'Name',values_from = 'Value')%>%as.data.frame
          newrow=apply(newrow,2,unlist)%>%as.list%>%data.frame
          Datab <- merge(Datab,newrow,all=T)
          rm(newrow)
        }
        rm(dd)
        
        mtRows=which(apply(Datab,1,FUN=function(r)all(is.na(r))))
        if(length(mtRows)>0){Datab=Datab[-mtRows,]}
        Data=Datab
        rm(Datab,mtRows)
        
        Data$Units=dataSource[grep('unit',names(dataSource),ignore.case=T)]
        Data$Measurement=Measurements[j]
        siteDat=bind_rows(siteDat,Data)
      }
      rm(Data)
    }
  }
  if(!is.null(siteDat)){
    siteDat$CouncilSiteID = sites[i]
    ecanSWQ=bind_rows(ecanSWQ,siteDat)
  }
  rm(siteDat)
}




        xmlfile <- ldWQ(url,agency,QC=T)
    if(!is.null(xmlfile)){
      xmltop<-xmlRoot(xmlfile)
      m<-xmltop[['Measurement']]
      if(!is.null(m)){
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
      } else {          cat(Measurements[j],'\t')
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
              item2 <- paste("$ND",tab,">",tab,sep="")
              
              # Handle Less than symbols  
            } else if(grepl(pattern = "^\\<",x =  ansValue[N],perl = TRUE)){
              ansValue[N] <- substr(ansValue[N],2,nchar(ansValue[N]))
              item2 <- paste("$ND",tab,"<",tab,sep="")
              
              # Handle Asterixes  
            } else if(grepl(pattern = "^\\*",x =  ansValue[N],perl = TRUE)){
              ansValue[N] <- gsub(pattern = "^\\*", replacement = "", x = ansValue[N])
              item2 <- paste("$ND",tab,"*",tab,sep="")
            } else{
              item2 <- ""
            }
            addChildren(DataNode[[xmlSize(DataNode)]], newXMLNode(name = "I1",ansValue[N]))
            
            if(xmlSize(m[['Data']][[N]])>2){    ### Has attributes to add to Item 2
              for(n in 3:xmlSize(m[['Data']][[N]])){      
                #Getting attributes and building string to put in Item 2
                attrs <- xmlAttrs(m[['Data']][[N]][[n]])  
                
                item2 <- paste(item2,attrs[1],tab,attrs[2],tab,sep="")
                
              }
            }
            
            addChildren(DataNode[[xmlSize(DataNode)]], newXMLNode(name = "I2",item2))
            
          } 
        }
        #saveXML(DataNode)
        
        oldNode <- m[['Data']]
        newNode <- DataNode
        replaceNodes(oldNode, newNode)
        con$addNode(m) 
      }  
    }
  }
}
# saveXML(con$value(), paste0("h:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"SWQ.xml"))
saveXML(con$value(), paste0("D:/LAWA/2021/",agency,"SWQ.xml"))
file.copy(from=paste0("D:/LAWA/2021/",agency,"SWQ.xml"),
          to=paste0("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data/",format(Sys.Date(),"%Y-%m-%d"),"/",agency,"SWQ.xml"),
          overwrite = T)

