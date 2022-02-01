
rm(list=ls())
library(tidyverse)
library(areaplot)
library(lubridate)
# library(doBy)
library(showtext)
library(rgdal)
if(!'source'%in%font_families()){
  font_add(family = 'source',regular = "h:/ericg/16666LAWA/LAWA2021/SourceFont/SourceSansPro-Regular.ttf")
  # font_add_google("Source Sans Pro",family='source')
}
labelAreas <- function(areaTable,textcex=0.75,invert=F){
  colPos=apply(areaTable,1,sum)
  colPos=colPos/sum(colPos)
  colPos=cumsum(colPos)
  colPos=apply(cbind(c(0,colPos[1:(length(colPos)-1)]),colPos),1,mean)
  colPos=colPos*par('usr')[2]
  for(ll in 1:dim(areaTable)[1]){
    rowPos=areaTable[ll,]
    if(invert){
      rowPos = rev(rowPos/sum(rowPos))
    }else{
      rowPos = rowPos/sum(rowPos)
    }
    rowPos = cumsum(rowPos)
    rowPos=apply(cbind(c(0,rowPos[1:(length(rowPos)-1)]),rowPos),1,mean)
    if(invert){
      text(colPos[ll],rowPos[rev(areaTable[ll,])>0],rev(areaTable[ll,areaTable[ll,]>0]),cex=textcex)
    }else{
      text(colPos[ll],rowPos[areaTable[ll,]>0],areaTable[ll,areaTable[ll,]>0],cex=textcex)
    }
  }
}
setwd("H:/ericg/16666LAWA/LAWA2021/Groundwater//")
source("h:/ericg/16666LAWA/LAWA2021/Scripts/LAWAFunctions.R")

dir.create(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/GWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/"),showWarnings = F,recursive=T)
dir.create(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/GWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/"),showWarnings = F,recursive=T)


LAWAPalette=c("#95d3db","#74c0c1","#20a7ad","#007197",
              "#e85129","#ff8400","#ffa827","#85bb5b","#bdbcbc")
OLPalette=c('#cccccc','#f2f2f2')
plot(1:9,1:9,col=LAWAPalette,cex=5,pch=16)
NOFPal4 = LAWAPalette[c(5,7,8,3)]
NOFPal5 = LAWAPalette[c(5,6,7,8,3)]
AtoE = c("A","B","C","D","E","NA")
AtoD = c("A","B","C","D","NA")



GWPal3 = rev(c("#cbb9ff","#9a01ba","#5c0071"))
GWPal4 = rev(c("#cbb9ff","#c880ff","#9a01ba","#5c0071"))



EndYear <- 2020#year(Sys.Date())-1
periodYrs=15
firstYear=EndYear-periodYrs+1
yr <-   paste0(as.character(firstYear:(EndYear-4)),'to',as.character((firstYear+4):EndYear))




rolling5 <- function(siteChemSet,quantProb=0.5){ 
  sapply(yr,FUN=function(dt){
    startYear = as.numeric(strTo(s = dt,c = 'to'))
    stopYear = as.numeric(strFrom(s= dt,c = 'to'))
    inTime = siteChemSet$Year>=startYear & siteChemSet$Year<=stopYear
    nAvailable=sum(inTime)
    nQuarters=length(unique(siteChemSet$YearQuarter[inTime]))
    round(quantile(siteChemSet$Value[inTime],prob=quantProb,type=5,na.rm=T,names=F),4)
  })
}


GWdata = read_csv(tail(dir(path = 'h:/ericg/16666LAWA/LAWA2021/Groundwater/Data/',
                           pattern = 'GWdata.csv',recursive=T,full.names=T),1),guess_max = 5000)


#For national picutre history summary
startTime=Sys.time()
GWNationalPicture <- GWdata%>%
  filter(Measurement%in%c("Nitrate nitrogen","Chloride",
                          "Dissolved reactive phosphorus",
                          "Electrical conductivity/salinity",
                          "E.coli","Ammoniacal nitrogen"))%>%
  filter(`Result-raw`!='*')%>%         #This excludes non-results discussed by Carl Hanson by email April 2020
  dplyr::filter(lubridate::year(myDate)>=(EndYear-periodYrs+1)&lubridate::year(myDate)<=EndYear)%>%  
  select(-'Result-raw',-'Result-metadata',-'Variable')%>%
  dplyr::group_by(siteMeas,monYear)%>%  #Month and year
  dplyr::summarise(.groups='keep',
                   LawaSiteID=unique(LawaSiteID),
                   Measurement=unique(Measurement),
                   Year=unique(Year),
                   Qtr=unique(Qtr),
                   Month=unique(Month),
                   Value=median(Value,na.rm=T),
                   Date=first(Date,1),
                   myDate=first(myDate,1),
                   LcenLim = suppressWarnings({max(`Result-edited`[`Result-prefix`=='<'],na.rm=T)}),
                   RcenLim = suppressWarnings({min(`Result-edited`[`Result-prefix`=='>'],na.rm=T)}),
                   CenType = ifelse(Value<LcenLim,'lt',ifelse(Value>RcenLim,'gt',NA)))%>%
  ungroup%>%distinct
Sys.time()-startTime #36 seconds  3 minutes feb2022
GWNationalPicture$CenBin = 0
GWNationalPicture$CenBin[GWNationalPicture$CenType=='lt'] = 1
GWNationalPicture$CenBin[GWNationalPicture$CenType=='gt'] = 2
freqs <- split(x=GWNationalPicture,
               f=GWNationalPicture$siteMeas)%>%purrr::map(~freqCheck(.))%>%unlist
GWNationalPicture$Frequency=freqs[GWNationalPicture$siteMeas]  
rm(freqs)




# GWNationalPicture$p5y = NA
# GWNationalPicture$p5y[GWNationalPicture$Year>=2010] = unlist(sapply(as.character(GWNationalPicture$Year),
                                                                     # function(s)unlist(grep(paste0('to',s),yr,value = T))))
GWNatPicMedians <- 
  expand.grid(Measurement=unique(GWNationalPicture$Measurement),
              p5y=paste0('to',2011:2020),
              LawaSiteID=unique(GWNationalPicture$LawaSiteID),
              stringsAsFactors = F)
GWNatPicMedians$year5=readr::parse_number(GWNatPicMedians$p5y)
GWNatPicMedians$year1=GWNatPicMedians$year5-4
GWNatPicMedians$median=NA
GWNatPicMedians$MAD=NA
GWNatPicMedians$count=NA
GWNatPicMedians$minPerYear=NA
GWNatPicMedians$nYear=NA
GWNatPicMedians$nQuart=NA
GWNatPicMedians$nMonth=NA
GWNatPicMedians$censoredCount=NA
GWNatPicMedians$CenType=NA
GWNatPicMedians$Frequency=NA
curSite='teppo'
startTime=Sys.time()
gwnpm=1
for(gwnpm in gwnpm:dim(GWNatPicMedians)[1]){
  if(gwnpm%%100==0){cat(gwnpm,'\t')}
  if(GWNatPicMedians$LawaSiteID[gwnpm]!=curSite){
    curSite=GWNatPicMedians$LawaSiteID[gwnpm]
    rightSite = which(GWNationalPicture$LawaSiteID==GWNatPicMedians$LawaSiteID[gwnpm])
  }
  these=rightSite[which(GWNationalPicture$Measurement[rightSite]==GWNatPicMedians$Measurement[gwnpm]&
                          GWNationalPicture$Year[rightSite]>=GWNatPicMedians$year1[gwnpm]&
                          GWNationalPicture$Year[rightSite]<=GWNatPicMedians$year5[gwnpm]&
                          !is.na(GWNationalPicture$Value[rightSite]))]
  if(length(these)>0){
    GWNatPicMedians$median[gwnpm]=quantile(GWNationalPicture$Value[these],probs=0.5,type=5,na.rm=T)
    GWNatPicMedians$MAD[gwnpm]=quantile(abs(GWNationalPicture$Value[these]-GWNatPicMedians$median[gwnpm]),probs=0.5,type=0.5,na.rm=T)
    GWNatPicMedians$count[gwnpm] = length(these)
    GWNatPicMedians$minPerYear[gwnpm] = min(as.numeric(table(factor(as.character(GWNationalPicture$Year[these]),                                                                    levels=as.character(firstYear:EndYear)))))
    GWNatPicMedians$nYear[gwnpm] = length(unique(GWNationalPicture$Year[these]))
    GWNatPicMedians$nQuart[gwnpm] = length(unique(GWNationalPicture$Qtr[these]))
    GWNatPicMedians$nMonth[gwnpm] = length(unique(GWNationalPicture$Month[these]))
    GWNatPicMedians$censoredCount[gwnpm] = sum(GWNationalPicture$Value[these]<=(0.5*GWNationalPicture$LcenLim[these]) | GWNationalPicture$Value[these]>=(1.1*GWNationalPicture$RcenLim[these]))
    GWNatPicMedians$CenType[gwnpm] = Mode(GWNationalPicture$CenBin[these])
    GWNatPicMedians$Frequency[gwnpm]=unique(GWNationalPicture$Frequency[these])  
  }
}
GWNatPicMedians$censoredPropn = GWNatPicMedians$censoredCount/GWNatPicMedians$count
Sys.time()-startTime #1.7s  52s feb2022
#

GWNatPicMedians$CenType = as.character(GWNatPicMedians$CenType)
GWNatPicMedians$CenType[GWNatPicMedians$CenType==1] <- '<'
GWNatPicMedians$CenType[GWNatPicMedians$CenType==2] <- '>'
GWNatPicMedians$censMedian = GWNatPicMedians$median

toCut = which(apply(GWNatPicMedians[,6:12],1,function(r)all(is.na(r))))
GWNatPicMedians <- GWNatPicMedians[-toCut,]
rm(toCut)

GWNatPicMedians$censMedian[GWNatPicMedians$censoredPropn>=0.5 & GWNatPicMedians$CenType=='<'] <- 
  paste0('<',2*GWNatPicMedians$median[GWNatPicMedians$censoredPropn>=0.5 & GWNatPicMedians$CenType=='<'])
GWNatPicMedians$censMedian[GWNatPicMedians$censoredPropn>=0.5 & GWNatPicMedians$CenType=='>'] <- 
  paste0('>',GWNatPicMedians$median[GWNatPicMedians$censoredPropn>=0.5 & GWNatPicMedians$CenType=='>']/1.1)

GWNatPicMedians$EcoliDetectAtAll=NA
GWNatPicMedians$EcoliDetectAtAll[which(GWNatPicMedians$Measurement=="E.coli")] <- "1"  #Detect
GWNatPicMedians$EcoliDetectAtAll[which(GWNatPicMedians$Measurement=="E.coli"&
                                         (GWNatPicMedians$censoredPropn==1|
                                            GWNatPicMedians$median==0|
                                            is.na(GWNatPicMedians$median)))] <- "2"  #Non-detect
GWNatPicMedians$meas = factor(GWNatPicMedians$Measurement,
                              levels=c("Ammoniacal nitrogen","Chloride","Dissolved reactive phosphorus",
                                       "E.coli","Electrical conductivity/salinity","Nitrate nitrogen"),
                              labels=c("NH4","Cl","DRP","ECOLI","NaCl","NO3"))

GWNatPicMedians$Source = GWdata$Source[match(GWNatPicMedians$LawaSiteID,GWdata$LawaSiteID)]

GWNatPicMedians$StateVal = GWNatPicMedians$median
GWNatPicMedians$StateVal[GWNatPicMedians$Measurement=="E.coli"] <- GWNatPicMedians$EcoliDetectAtAll[GWNatPicMedians$Measurement=="E.coli"]

dir.create(path = paste0('h:/ericg/16666LAWA/LAWA2021/Groundwater/Analysis/',
                                format(Sys.Date(),"%Y-%m-%d")),recursive = T)
write.csv(GWNatPicMedians,file = paste0('h:/ericg/16666LAWA/LAWA2021/Groundwater/Analysis/',
                                        format(Sys.Date(),"%Y-%m-%d"),
                                        '/GWNatPicState',format(Sys.time(),"%d%b%Y"),'.csv'),row.names = F)











nzmap <- readOGR('S:/New_S/Basemaps_Vector/NZ_Coastline/WGS_84/coast_wgs84.shp')
as.hexmode(c(242,242,242))
#prep data ####
dir.create(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/GWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/"),showWarnings = F,recursive=T)
dir.create(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/GWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/"),showWarnings = F,recursive=T)


siteTab = read_csv("h:/ericg/16666LAWA/LAWA2021/Groundwater/Metadata/SiteTable.csv")

length(unique(c(tolower(siteTab$LAWA_ID)))) #960


# GWNatPicMedians=read.csv(tail(dir(path = "h:/ericg/16666LAWA/LAWA2021/Groundwater/Analysis/",
#                                   pattern="GWNatPic",
#                                   recursive=T,full.names = T),1),stringsAsFactors = F)
# GWNatPicMedians <- GWNatPicMedians%>%filter(!Year%in%c('2004to2008','2005to2009','2006to2010'))  #Just, you see this way we're left with a single decade
# GWNatPicMedians$SWQAltitude = pseudo.titlecase(tolower(GWNatPicMedians$SWQAltitude))
# GWNatPicMedians$rawRecLandcover = riverSiteTable$rawRecLandcover[match(tolower(GWNatPicMedians$LawaSiteID),tolower(riverSiteTable$LawaSiteID))]
# GWNatPicMedians$rawRecLandcover = factor(GWNatPicMedians$rawRecLandcover,
#                                          levels=c("if","w","t","s","b","ef", "p", "u"))
# GWNatPicMedians$gRecLC=GWNatPicMedians$rawRecLandcover
# GWNatPicMedians$gRecLC <- factor(GWNatPicMedians$gRecLC,
#                                  levels=c("if","w","t","s","b",
#                                           "ef",
#                                           "p",
#                                           "u"),
#                                  labels=c(rep("Native vegetation",5),
#                                           "Plantation forest","Pasture","Urban"))



#The level order is from bottom to top on a spineplot.  
#Barplots need them the other way round, so need to reorder them, where used.


GWBands <- GWNatPicMedians%>%pivot_wider(id_cols = c(LawaSiteID,p5y),
                                         names_from = 'meas',values_from = median)

GWBands$ClBand = factor(Hmisc::cut2(GWBands$Cl,cuts = c(-Inf,125,250,Inf)),
                        levels=c("[-Inf, 125)", "[ 125, 250)", "[ 250, Inf]"),
                        labels=c("A","B","C"))
GWBands$NaClBand = factor(Hmisc::cut2(GWBands$NaCl,cuts = c(-Inf,500,1000,Inf)),
                        levels=c("[-Inf, 500)", "[ 500,1000)", "[1000, Inf]"),
                        labels=c("A","B","C"))
GWBands$NO3Band = factor(Hmisc::cut2(GWBands$NO3,cuts = c(-Inf,1,5.65,11.3,Inf)),
                        levels=c("[ -Inf, 1.00)", "[ 1.00, 5.65)", "[ 5.65,11.30)", "[11.30,  Inf]"),
                        labels=c("A","B","C","D"))
GWBands <- merge(x=GWBands,y=GWNatPicMedians%>%select(LawaSiteID,p5y,EcoliDetectAtAll)%>%drop_na(EcoliDetectAtAll),all.x=T)
GWBands <- GWBands%>%rename(EcoliBand = EcoliDetectAtAll)
GWBands$EcoliBand <- factor(GWBands$EcoliBand,levels=c(1,2),labels=c("B","A")) #2 is nondetect
GWBands$DRPBand = factor(Hmisc::cut2(GWBands$DRP,cuts = c(-Inf,0.01,0.025,Inf)),
                           levels=c("[ -Inf,0.010)", "[0.010,0.025)", "[0.025,  Inf]"),
                           labels=c("A","B","C"))

GWBands$Long = siteTab$Longitude[match(GWBands$LawaSiteID,siteTab$LAWA_ID)]
GWBands$Lat = siteTab$Latitude[match(GWBands$LawaSiteID,siteTab$LAWA_ID)]


GWNatPicMedians$Long=siteTab$Longitude[match(tolower(GWNatPicMedians$LawaSiteID),tolower(siteTab$LAWA_ID))]
GWNatPicMedians$Lat=siteTab$Latitude[match(tolower(GWNatPicMedians$LawaSiteID),tolower(siteTab$LAWA_ID))]
GWlatest = droplevels(GWNatPicMedians%>%filter(p5y=="to2020"))






