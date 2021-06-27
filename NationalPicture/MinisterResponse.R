#NOFPlots #### 
rm(list=ls())
library(tidyverse)
library(areaplot)
library(lubridate)
library(doBy)
library(showtext)

setwd("H:/ericg/16666LAWA/LAWA2020/WaterQuality/")
source("h:/ericg/16666LAWA/LAWA2020/Scripts/LAWAFunctions.R")


dir.create(paste0("h:/ericg/16666LAWA/LAWA2020/NationalPicture/",format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)
riverSiteTable=loadLatestSiteTableRiver()
macroSiteTable=loadLatestSiteTableMacro()


#A few of those auckland sites had the worng coordinates as of Sept 2020
if(abs(macroSiteTable$Long[which(macroSiteTable$LawaSiteID=='lawa-102734')]-176.5804)<0.0001){
  macroSiteTable$Long[which(macroSiteTable$LawaSiteID=='lawa-102734')] <- 174.7273
  macroSiteTable$Lat[which(macroSiteTable$LawaSiteID=='lawa-102734')] <- (-36.8972)
}

if(abs(macroSiteTable$Long[which(macroSiteTable$LawaSiteID=='lawa-102744')]-174.5198)<0.0001){
  macroSiteTable$Long[which(macroSiteTable$LawaSiteID=='lawa-102744')] <- 174.8418
  macroSiteTable$Lat[which(macroSiteTable$LawaSiteID=='lawa-102744')] <- (-37.1811)
}


these = which(is.na(macroSiteTable$Region))

for(ms in seq_along(these)){
  dists = sqrt((macroSiteTable$Long[these[ms]]-riverSiteTable$Long)^2+(macroSiteTable$Lat[these[ms]]-riverSiteTable$Lat)^2)
  cat(min(dists)*1000,'\t')
  points(riverSiteTable$Long[which.min(dists)],riverSiteTable$Lat[which.min(dists)],pch=16,col='blue',cex=0.5)
  macroSiteTable$Region[these[ms]] <- riverSiteTable$Region[which.min(dists)]
  macroSiteTable$LawaSiteID[these[ms]] <- riverSiteTable$LawaSiteID[which.min(dists)]
}




#prep data ####
# wqdataFileName=tail(dir(path = "H:/ericg/16666LAWA/LAWA2020/WaterQuality/Data",pattern = "AllCouncils.csv",recursive = T,full.names = T),1)
# cat(wqdataFileName)
# wqdata=read_csv(wqdataFileName,guess_max = 100000)%>%as.data.frame
# rm(wqdataFileName)
# macroData=read_csv(tail(dir(path="H:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Data",pattern="MacrosWithMetadata",
# recursive = T,full.names = T),1))
# macroData$LawaSiteID = tolower(macroData$LawaSiteID)


NOFSummaryTable=read.csv(tail(dir(path = "h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",pattern="NOFSummaryTable_Rolling",
                                  recursive=T,full.names = T),1),stringsAsFactors = F)
NOFSummaryTable <- NOFSummaryTable%>%filter(Year!='2005to2009')  #Just, you see this way we're left with a single decade
NOFSummaryTable$SWQAltitude = pseudo.titlecase(tolower(NOFSummaryTable$SWQAltitude))
NOFSummaryTable$rawRecLandcover = riverSiteTable$rawRecLandcover[match(tolower(NOFSummaryTable$LawaSiteID),tolower(riverSiteTable$LawaSiteID))]
NOFSummaryTable$rawRecLandcover = factor(NOFSummaryTable$rawRecLandcover,levels=c("if","w","t","s","b","ef", "p", "u"))
NOFSummaryTable$gRecLC=NOFSummaryTable$rawRecLandcover
NOFSummaryTable$gRecLC <- factor(NOFSummaryTable$gRecLC,levels=c("if","w","t","s","b",
                                                                 "ef",
                                                                 "p",
                                                                 "u"),
                                 labels=c("Native","Native","Native","Native","Native",
                                          "Exotic forest","Pasture","Urban"))

NOFSummaryTable$EcoliSummaryband=factor(NOFSummaryTable$EcoliSummaryband,levels=c("E","D", "C", "B", "A", "NA"),labels=c("E","D", "C", "B", "A", "NA"))
NOFSummaryTable$EcoliSummaryband[is.na(NOFSummaryTable$EcoliSummaryband)] <- "NA"
NOFSummaryTable$Ammonia_Toxicity_Band=factor(NOFSummaryTable$Ammonia_Toxicity_Band,levels=c("D", "C", "B", "A", "NA"),labels=c("D", "C", "B", "A", "NA"))
NOFSummaryTable$Ammonia_Toxicity_Band[is.na(NOFSummaryTable$Ammonia_Toxicity_Band)] <- "NA"
NOFSummaryTable$DRP_Summary_Band=factor(NOFSummaryTable$DRP_Summary_Band,levels=c("D", "C", "B", "A", "NA"),labels=c("D", "C", "B", "A", "NA"))
NOFSummaryTable$DRP_Summary_Band[is.na(NOFSummaryTable$DRP_Summary_Band)] <- "NA"

NOFSummaryTable$Long=riverSiteTable$Long[match(tolower(NOFSummaryTable$LawaSiteID),tolower(riverSiteTable$LawaSiteID))]
NOFSummaryTable$Lat=riverSiteTable$Lat[match(tolower(NOFSummaryTable$LawaSiteID),tolower(riverSiteTable$LawaSiteID))]
NOFlatest = droplevels(NOFSummaryTable%>%filter(Year=="2015to2019"))


MCINOF = read.csv(tail(dir(path = "h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/",pattern="MacroRollingMCI",
                           recursive = T,full.names = T),1),stringsAsFactors = F)
MCINOF$Region=macroSiteTable$Region[match(gsub('_niwa','',tolower(MCINOF$LawaSiteID)),tolower(macroSiteTable$LawaSiteID))]

MCINOF$Region[MCINOF$LawaSiteID=='ebop-00049'] <- 'bay of plenty'
MCINOF$Region[MCINOF$LawaSiteID=='ecan-10004'] <- 'canterbury'
MCINOF$Region[MCINOF$LawaSiteID=='nrc-10008'] <- 'northland'
MCINOF$Region[MCINOF$LawaSiteID=='lawa-102739'] <- 'auckland'
MCINOF$Region[MCINOF$LawaSiteID=='nrwqn-00006'] <- 'canterbury'
MCINOF$Region[MCINOF$LawaSiteID=='nrwqn-00013'] <- 'hawkes bay'
MCINOF$Region[MCINOF$LawaSiteID=='nrwqn-00029'] <- 'otago'
MCINOF$Region[MCINOF$LawaSiteID=='nrwqn-00007'] <- 'canterbury'
MCINOF$Region[MCINOF$LawaSiteID=='nrwqn-00009'] <- 'canterbury'
MCINOF$Region[MCINOF$LawaSiteID=='nrwqn-00015'] <- 'hawkes bay'
MCINOF$Region[MCINOF$LawaSiteID=='nrwqn-00042'] <- 'waikato'

MCINOF$band = cut(MCINOF$rollMCI,breaks = c(0,90,110,130,200),labels = c("D","C","B","A"),right = F,ordered_result = T)
table(MCINOF$band)
MCINOF$band=factor(as.character(MCINOF$band),levels=c("D", "C", "B", "A", "NA"),labels=c("D", "C", "B", "A", "NA"))
MCINOF$band[is.na(MCINOF$band)] <- "NA"
table(MCINOF$band)

MCINOF$Landcover = macroSiteTable$Landcover[match(gsub('_NIWA','',MCINOF$LawaSiteID,ignore.case = T),macroSiteTable$LawaSiteID)]
MCINOF$SWQLanduse = pseudo.titlecase(macroSiteTable$SWQLanduse[match(gsub('_NIWA','',MCINOF$LawaSiteID,ignore.case = T),macroSiteTable$LawaSiteID)])
MCINOF$AltitudeCl = macroSiteTable$AltitudeCl[match(gsub('_NIWA','',MCINOF$LawaSiteID,ignore.case = T),macroSiteTable$LawaSiteID)]
MCINOF$SWQAltitude = pseudo.titlecase(macroSiteTable$SWQAltitude[match(gsub('_NIWA','',MCINOF$LawaSiteID,ignore.case = T),macroSiteTable$LawaSiteID)])
MCINOF$rawRecLandcover = macroSiteTable$rawRecLandcover[match(gsub('_niwa','',tolower(MCINOF$LawaSiteID)),
                                                              tolower(macroSiteTable$LawaSiteID))]
MCINOF$rawRecLandcover[is.na(MCINOF$rawRecLandcover)] = riverSiteTable$rawRecLandcover[match(tolower(MCINOF$LawaSiteID[is.na(MCINOF$rawRecLandcover)]),
                                                                                             tolower(riverSiteTable$LawaSiteID))]
MCINOF$rawRecLandcover = factor(MCINOF$rawRecLandcover,levels=c("if","w","t","s","b","ef", "p","u"))
MCINOF$gRecLC = MCINOF$rawRecLandcover

MCINOF$gRecLC <- factor(MCINOF$gRecLC,levels=c("if","w","t","s", "b","ef", "p", "u"),
                        labels=c("Native","Native","Native","Native","Native",
                                 "Exotic forest","Pasture","Urban"))
# MCINOF$gRecLC[is.na(MCINOF$gRecLC)] <- 'Pasture'
MCINOF$Long =macroSiteTable$Long[match(tolower(MCINOF$LawaSiteID),tolower(macroSiteTable$LawaSiteID))]
MCINOF$Lat =macroSiteTable$Lat[match(tolower(MCINOF$LawaSiteID),tolower(macroSiteTable$LawaSiteID))]




# font_add_google("Comic Neue",family='comic')
font_add_google("Source Sans Pro",family='source')
labelAreas <- function(areaTable){
  colPos=apply(areaTable,1,sum)
  colPos=colPos/sum(colPos)
  colPos=cumsum(colPos)
  colPos=apply(cbind(c(0,colPos[1:(length(colPos)-1)]),colPos),1,mean)
  colPos=colPos*par('usr')[2]
  for(ll in 1:dim(areaTable)[1]){
    rowPos=areaTable[ll,]
    rowPos = rowPos/sum(rowPos)
    rowPos = cumsum(rowPos)
    rowPos=apply(cbind(c(0,rowPos[1:(length(rowPos)-1)]),rowPos),1,mean)
    text(colPos[ll],rowPos,areaTable[ll,],cex=0.75)
  }
}
library(rgdal)
nzmap <- readOGR('S:/New_S/Basemaps_Vector/NZ_Coastline/WGS_84/coast_wgs84.shp')
as.hexmode(c(242,242,242))
LAWAPalette=c("#95d3db","#74c0c1","#20a7ad","#007197",
              "#e85129","#ff8400","#ffa827","#85bb5b","#bdbcbc")
OLPalette=c('#cccccc','#f2f2f2')
BWPalette=grey.colors(9)
plot(1:9,1:9,col=LAWAPalette,cex=5,pch=16)
xlabpos=(1:10)*0.12-0.07



#LANDUSE TRENDS

#Landuse trend plots ####
for(GLCR in levels(NOFSummaryTable$gRecLC)){
  tiff(paste0("h:/ericg/16666LAWA/LAWA2020/NationalPicture/",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrend",pseudo.titlecase(GLCR),".tif"),
       width = 10,height=8,units='in',res=600,compression='lzw',type='cairo')
  showtext_begin()
  layout(matrix(c(1,1,3,4,4,6,
                  1,1,2,4,4,5,
                  1,1,2,4,4,5,
                  7,7,9,10,10,12,
                  7,7,8,10,10,11,
                  7,7,8,10,10,11),nrow=6,byrow=T))
  DRPNOFs = NOFSummaryTable%>%drop_na(DRP_Summary_Band)%>%filter(DRP_Summary_Band!="NA"&gRecLC==GLCR)%>%
    select(-starts_with(c('Ecoli','Ammonia'),ignore.case = T))%>%
    group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY>=7)%>%ungroup%>%select(-nY)
  nSites=length(unique(DRPNOFs$LawaSiteID))
  if(nSites>0){
    par(col.main=BWPalette[2],col.axis=BWPalette[3],col.lab=BWPalette[3],mar=c(3,2,2,0.5),mgp=c(1.75,0.5,0),
        cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5,family='comic')
    iT=plot(factor(strFrom(DRPNOFs$Year,'to')),(DRPNOFs$DRP_Summary_Band),ylab='',xlab='',
            col=BWPalette[c(5,7,8,3)],tol.ylab=0,main="DRP Summary",
            axes=F,border = NA)
    axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=BWPalette[3],tck=-0.025)
    axis(1,at=xlabpos,labels = c(as.character(2010:2019)),col=BWPalette[3],cex.axis=1.25)
    mtext(side = 3,paste0("Showing the ",nSites," almost-complete sites."),line = 0.5,col=BWPalette[3],cex=2.5)
    par(mar=c(1,0,0,0))
    plot(nzmap,col=BWPalette[1],border = NA)
    with(DRPNOFs%>%filter(Year=='2015to2019'),
         points(Long,Lat,pch=16,cex=0.3,col=BWPalette[c(5,7,8,3)][as.numeric(DRP_Summary_Band)])
    )
    legend(166.4,-35.4,legend = c('A','B','C','D'),pch=16,col=BWPalette[c(3,8,7,5)],pt.cex=0.5,y.intersp = 0.25,bty='n')
    plot.new()
    plot.window(xlim=c(0,10),ylim=c(0,10))
    par(xpd=T)
    plotrix::addtable2plot(0,-2,table = iT,display.colnames = T,display.rownames = T,vlines=T,xpad=1,ypad=0.5,cex=1)
    par(xpd=F)
    showtext_end()
  }else{    
    plot.new()
    plot.new()
    plot.new()
  }
  rm(DRPNOFs,nSites,iT)
  
  # NOFTrendMCI  
  showtext_begin()
  MCINOFs = MCINOF%>%filter(sYear>=2010)%>%group_by(LawaSiteID)%>%filter(band!='NA'&gRecLC==GLCR)%>%drop_na(band)%>%
    dplyr::mutate(nY=length(unique(sYear)))%>%filter(nY>=7)%>%ungroup%>%select(-nY)
  nSites=length(unique(MCINOFs$LawaSiteID))
  if(nSites>0){
    par(col.main=BWPalette[2],col.axis=BWPalette[3],col.lab=BWPalette[3],mar=c(3,2,2,0.5),mgp=c(1.75,0.5,0),
        cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
    iT=plot(factor(MCINOFs$sYear),MCINOFs$band,ylab='',xlab='',
            col=BWPalette[c(5,7,8,3)],tol.ylab=0,main="MCI",
            axes=F,border = NA)
    axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=BWPalette[3],tck=-0.025)
    axis(1,at=xlabpos,labels = c(as.character(2010:2019)),col=BWPalette[3],cex.axis=1.25)
    mtext(side = 3,paste0("Showing the ",nSites," almost-complete sites."),line = 0.5,col=BWPalette[3],cex=2.5)
    par(mar=c(1,0,0,0))
    plot(nzmap,col=BWPalette[1],border = NA)
    with(MCINOFs%>%filter(sYear==2019),
         points(Long,Lat,pch=16,cex=0.3,col=BWPalette[c(5,7,8,3)][as.numeric(band)])
    )
    legend(166.4,-35.4,legend = c('A','B','C','D'),pch=16,col=BWPalette[c(3,8,7,5)],pt.cex=0.5,y.intersp = 0.25,bty='n')
    plot.new()
    plot.window(xlim=c(0,10),ylim=c(0,10))
    par(xpd=T)
    plotrix::addtable2plot(0,-2,table = iT,display.colnames = T,display.rownames = T,vlines=T,xpad=1,ypad=0.5,cex=1)
    par(xpd=F)
    showtext_end()
  }else{
    plot.new()
    plot.new()
    plot.new()
  }
  rm(MCINOFs,nSites,iT)
  # NOFTrendAmmoniacalTox  
  showtext_begin()
  ammonNOFs = NOFSummaryTable%>%filter(Ammonia_Toxicity_Band!="NA"&gRecLC==GLCR)%>%drop_na(Ammonia_Toxicity_Band)%>%
    select(-starts_with(c('ecoli','nitrate'),ignore.case=T))%>%
    group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY>=7)%>%ungroup%>%select(-nY)
  nSites=length(unique(ammonNOFs$LawaSiteID))
  if(nSites>0){
    par(col.main=BWPalette[2],col.axis=BWPalette[3],col.lab=BWPalette[3],mar=c(3,2,2,0.5),mgp=c(1.75,0.5,0),
        cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
    iT=plot(factor(strFrom(ammonNOFs$Year,'to')),(ammonNOFs$Ammonia_Toxicity_Band),ylab='',xlab='',
            col=BWPalette[c(5,7,8,3)],tol.ylab=0,main="Ammonia toxicity",
            axes=F,border = NA)
    axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=BWPalette[3],tck=-0.025)
    axis(1,at=xlabpos,labels = c(as.character(2010:2019)),col=BWPalette[3],cex.axis=1.25)
    mtext(side = 3,paste0("Showing the ",nSites," almost-complete sites."),line = 0.5,col=BWPalette[3],cex=2.5)
    par(mar=c(1,0,0,0))
    plot(nzmap,col=BWPalette[1],border = NA)
    with(ammonNOFs%>%filter(Year=='2015to2019'),
         points(Long,Lat,pch=16,cex=0.3,col=BWPalette[c(5,7,8,3)][as.numeric(Ammonia_Toxicity_Band)])
    )
    legend(166.4,-35.4,legend = c('A','B','C','D'),pch=16,col=BWPalette[c(3,8,7,5)],pt.cex=0.5,y.intersp = 0.25,bty='n')
    plot.new()
    plot.window(xlim=c(0,10),ylim=c(0,10))
    par(xpd=T)
    plotrix::addtable2plot(0,-2,table = iT,display.colnames = T,display.rownames = T,vlines=T,xpad=1,ypad=0.5,cex=1)
    par(xpd=F)
    showtext_end()
  }else{    
    plot.new()
    plot.new()
    plot.new()
  }
  rm(ammonNOFs,nSites,iT)
  
  # NOFTrendEcoliSummary 
  showtext_begin()
  ecoliNOFs = NOFSummaryTable%>%filter(EcoliSummaryband!="NA"&gRecLC==GLCR)%>%drop_na(EcoliSummaryband)%>%
    select(-starts_with(c('ammon','nitrate'),ignore.case=T))%>%
    group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY>=7)%>%ungroup%>%select(-nY)
  nSites=length(unique(ecoliNOFs$LawaSiteID))
  if(nSites>0){
    par(col.main=BWPalette[2],col.axis=BWPalette[3],col.lab=BWPalette[3],mar=c(3,2,2,0.5),mgp=c(1.75,0.5,0),
        cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
    iT=plot(factor(strFrom(ecoliNOFs$Year,'to')),(ecoliNOFs$EcoliSummaryband),ylab='',xlab='',
            col=BWPalette[c(5,6,7,8,3)],tol.ylab=0,main=expression(italic(E.~coli)~Summary),
            axes=F,border = NA)
    axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=BWPalette[3],tck=-0.025)
    axis(1,at=xlabpos,labels = c(as.character(2010:2019)),col=BWPalette[3],cex.axis=1.25)
    mtext(side = 3,paste0("Showing the ",nSites," almost-complete sites."),line = 0.5,col=BWPalette[3],cex=2.5)
    par(mar=c(1,0,0,0))
    plot(nzmap,col=BWPalette[1],border = NA)
    with(ecoliNOFs%>%filter(Year=='2015to2019'),
         points(Long,Lat,pch=16,cex=0.3,col=BWPalette[c(5,6,7,8,3)][as.numeric(EcoliSummaryband)])
    )
    legend(166.4,-35.4,legend = c('A','B','C','D','E'),pch=16,col=BWPalette[c(3,8,7,6,5)],pt.cex=0.5,y.intersp = 0.25,bty='n')
    plot.new()
    plot.window(xlim=c(0,10),ylim=c(0,10))
    par(xpd=T)
    plotrix::addtable2plot(0,-2,table = iT,display.colnames = T,display.rownames = T,vlines=T,xpad=1,ypad=0.5,cex=1)
    par(xpd=F)
    showtext_end()
  }else{
    plot.new()
    plot.new()
    plot.new()
  }
  rm(ecoliNOFs,nSites,iT)
  if(names(dev.cur())=='tiff'){dev.off()}
}

rm(GLCR)

for(indic in c("DRP_Summary_Band","Ammonia_Toxicity_Band","EcoliSummaryband","MCIBand")){
  if(indic!="EcoliSummaryband"){
    relPal = BWPalette[c(5,7,8,3)]
  }else{
    relPal = BWPalette[c(5,6,7,8,3)]
  }
  tiff(paste0("h:/ericg/16666LAWA/LAWA2020/NationalPicture/",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrend",pseudo.titlecase(indic),"ByLU.tif"),
       width = 10,height=8,units='in',res=600,compression='lzw',type='cairo')
  showtext_begin()
  layout(matrix(c(1,1,3,4,4,6,
                  1,1,2,4,4,5,
                  1,1,2,4,4,5,
                  7,7,9,10,10,12,
                  7,7,8,10,10,11,
                  7,7,8,10,10,11),nrow=6,byrow=T))
  for(GLCR in  levels(NOFSummaryTable$gRecLC)){
  if(indic!='MCIBand'){
    NOFresults = NOFSummaryTable%>%filter(!!sym(indic)!="NA")%>%filter(gRecLC==GLCR)%>%droplevels%>%
      dplyr::group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY>=7)%>%ungroup%>%select(-nY)%>%
      dplyr::rename(band=!!indic)%>%select(LawaSiteID,band,gRecLC,Year,Long,Lat)
  }else{
    NOFresults = MCINOF%>%filter(sYear>=2010)%>%group_by(LawaSiteID)%>%filter(band!='NA')%>%filter(gRecLC==GLCR)%>%drop_na(band)%>%droplevels%>%
      dplyr::mutate(nY=length(unique(sYear)))%>%filter(nY>=7)%>%ungroup%>%select(-nY)%>%dplyr::rename(Year=sYear)
  }
  nSites=length(unique(NOFresults$LawaSiteID))
  if(nSites>0){
    par(col.main=BWPalette[2],col.axis=BWPalette[3],col.lab=BWPalette[3],mar=c(3,2,2,0.5),mgp=c(1.75,0.5,0),
        cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5,family='comic')
    iT=plot(factor(strFrom(NOFresults$Year,'to')),(NOFresults$band),ylab='',xlab='',
            col=relPal,tol.ylab=0,main=GLCR,
            axes=F,border = NA)
    axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=BWPalette[3],tck=-0.025)
    axis(1,at=xlabpos,labels = c(as.character(2010:2019)),col=BWPalette[3],cex.axis=1.25)
    mtext(side = 3,paste0("Showing the ",nSites," almost-complete sites."),line = 0.5,col=BWPalette[3],cex=2.5)
    par(mar=c(1,0,0,0))
    plot(nzmap,col=BWPalette[7],border = NA)
    with(NOFresults%>%filter(Year%in%c('2015to2019','2019')),
         points(Long,Lat,pch=16,cex=0.3,col=relPal[as.numeric(band)])
    )

    legend(166.4,-35.4,legend = rev(levels(NOFresults$band)),pch=16,col=rev(relPal),pt.cex=0.5,y.intersp = 0.25,bty='n')
    plot.new()
    plot.window(xlim=c(0,10),ylim=c(0,10))
    par(xpd=T)
    plotrix::addtable2plot(0,-2,table = iT,display.colnames = T,display.rownames = T,vlines=T,xpad=1,ypad=0.5,cex=1)
    par(xpd=F)
    showtext_end()
  }else{    
    plot.new()
    plot.new()
    plot.new()
  }
  rm(NOFresults,nSites,iT)
  }
  if(names(dev.cur())=='tiff'){dev.off()}
}












# Now trend results! ####

riverSiteTable=loadLatestSiteTableRiver()
macroSiteTable=loadLatestSiteTableMacro()


#A few of those auckland sites had the worng coordinates as of Sept 2020
if(abs(macroSiteTable$Long[which(macroSiteTable$LawaSiteID=='lawa-102734')]-176.5804)<0.0001){
  macroSiteTable$Long[which(macroSiteTable$LawaSiteID=='lawa-102734')] <- 174.7273
  macroSiteTable$Lat[which(macroSiteTable$LawaSiteID=='lawa-102734')] <- (-36.8972)
}

if(abs(macroSiteTable$Long[which(macroSiteTable$LawaSiteID=='lawa-102744')]-174.5198)<0.0001){
  macroSiteTable$Long[which(macroSiteTable$LawaSiteID=='lawa-102744')] <- 174.8418
  macroSiteTable$Lat[which(macroSiteTable$LawaSiteID=='lawa-102744')] <- (-37.1811)
}


these = which(is.na(macroSiteTable$Region))

for(ms in seq_along(these)){
  dists = sqrt((macroSiteTable$Long[these[ms]]-riverSiteTable$Long)^2+(macroSiteTable$Lat[these[ms]]-riverSiteTable$Lat)^2)
  cat(min(dists)*1000,'\t')
  points(riverSiteTable$Long[which.min(dists)],riverSiteTable$Lat[which.min(dists)],pch=16,col='blue',cex=0.5)
  macroSiteTable$Region[these[ms]] <- riverSiteTable$Region[which.min(dists)]
  macroSiteTable$LawaSiteID[these[ms]] <- riverSiteTable$LawaSiteID[which.min(dists)]
}


table(unique(gsub('_niwa','',tolower(combTrend$LawaSiteID)))%in%c(tolower(riverSiteTable$LawaSiteID),tolower(macroSiteTable$LawaSiteID)))

unique(gsub('_niwa','',tolower(combTrend$LawaSiteID)))[!unique(gsub('_niwa','',tolower(combTrend$LawaSiteID)))%in%c(tolower(riverSiteTable$LawaSiteID),tolower(macroSiteTable$LawaSiteID))]




  #Load trend results ####
  EndYear <- lubridate::year(Sys.Date())-1
  startYear5 <- EndYear - 5+1
  startYear10 <- EndYear - 10+1
  startYear15 <- EndYear - 15+1
  load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",
                pattern = "Trend15Year.rData",full.names = T,recursive = T),1),verbose =T)
  load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",
                pattern = "Trend10Year.rData",full.names = T,recursive = T),1),verbose = T)
  load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",
                pattern = "Trend5Year.rData",full.names = T,recursive = T),1),verbose = T)
  
  combTrend=read.csv(tail(dir("h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis","RiverWQ_Trend",	recursive = T,full.names = T,ignore.case=T),1),stringsAsFactors = F)


  combTrend$ConfCat=factor(combTrend$ConfCat,levels=c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))
  
  combTrend$gRecLC = riverSiteTable$rawRecLandcover[match(gsub('_niwa','',tolower(combTrend$LawaSiteID),ignore.case = T),
                                                          tolower(riverSiteTable$LawaSiteID))]
  combTrend$gRecLC[is.na(combTrend$gRecLC)] = macroSiteTable$rawRecLandcover[match(gsub('_niwa','',tolower(combTrend$LawaSiteID[is.na(combTrend$gRecLC)]),ignore.case = T),
                                                          tolower(macroSiteTable$LawaSiteID))]
  combTrend$gRecLC <- factor(combTrend$gRecLC,levels=c("if","w","t","s","b",
                                                                   "ef",
                                                                   "p",
                                                                   "u"),
                                   labels=c("Native","Native","Native","Native","Native",
                                            "Exotic forest","Pasture","Urban"))
  
  combTrend$Measurement = factor(combTrend$Measurement,levels=c("MCI","ECOLI","NH4","DRP","TN","TON","TP","TURB","BDISC"))

                                   # labels=c("MCI",expression(italic(E.~coli)),expression(NH[4]),"DRP","TN","TON","TP","TURB","BDISC"))
  
  # 52 52 92  35345c
  # 12 148 162 0c94a2
  # 211 211 211 d3d3d3
  # 174 78 80 ae4e50
  # 112 46 56 702e38
  MfEPalette = c("#35345c","#0c94a2","#d3d3d3","#ae4e50","#702e38")
  trendPalette=c("#E85129","#ff8200","#bdbdbd","#c0df16","#85bb58")
 
  
  

  
  
 with(combTrend%>%filter(period==10,
                         Measurement%in%c("MCI","NH4","DRP","ECOLI"),
                         !is.na(gRecLC))%>%droplevels,
      plotable <<- table(gRecLC,ConfCat))
 nperlanduse = apply(plotable,1,sum,na.rm=T)
 nresults = sum(sum(plotable))
  plotable <- apply(plotable,1,FUN=function(x)rev(x/sum(x,na.rm=T)*100))
# colnames(plotable) = paste0(colnames(plotable),'\n(',nperlanduse[colnames(plotable)],' sites)')
  
  
  tiff(paste0("h:/ericg/16666LAWA/LAWA2020/NationalPicture/"
              ,format(Sys.Date(),"%Y-%m-%d"),"/Trend10yrByLU.tif"),
       width = 8,height=8,units='in',res=600,compression='lzw',type='cairo')
  showtext_begin()
  par(mar=c(8,4,4,10),cex.lab=5,cex.axis=5,cex.main=7,family='source',mgp=c(2,1,0))
  barplot(plotable,horiz=F,col=trendPalette,border = NA,names.arg=rep('',4),
          main='',axes=F)
  abline(h=c(0.1,seq(20,80,by=20),99.9),lty=2,col='gray75',lwd=2)
  barplot(plotable,horiz=F,col=trendPalette,border = NA,add=T,
          main="LAWA National River Water Quality 10-Year Trends by Land Cover Class (2010-2019)",
          las=1,yaxt='n',names.arg=c(expression(atop(Native,'(633 sites)')),
                                     expression(atop(Exotic~forest,'(87 sites)')),
                                     expression(atop(Pasture,'(1898 sites)')),
                                     expression(atop(Urban,'(228 sites)'))))
  # mtext(side = 3,text = paste0(nresults,' total results'),cex=5)
  axis(side=2,at=seq(0,100,by=20),labels=paste(seq(0,100,by=20),'%'),las=2)
  par(xpd=T)
  legend(4.25,96,rev(rownames(plotable)),col=rev(trendPalette),pch=15,bty='n',cex=6,pt.cex=3,xjust=0,
         y.intersp=0.25,x.intersp = 0.25)
  showtext_end()
  if(names(dev.cur())=='tiff'){dev.off()}
  
    
  
  
  
  with(combTrend%>%filter(period==10,
                          Measurement%in%c("MCI","NH4","DRP","ECOLI"),
                          !is.na(gRecLC))%>%droplevels,
       plotable <<- table(Measurement,ConfCat))
  nperMeasure = apply(plotable,1,sum,na.rm=T)
  nresults = sum(sum(plotable))
  plotable <- apply(plotable,1,FUN=function(x)rev(x/sum(x,na.rm=T)*100))
  # colnames(plotable) = paste0(colnames(plotable),'\n(',nperMeasure[colnames(plotable)],' sites)')
  
  tiff(paste0("h:/ericg/16666LAWA/LAWA2020/NationalPicture/"
              ,format(Sys.Date(),"%Y-%m-%d"),"/Trend10yr4Indic.tif"),
       width = 8,height=8,units='in',res=600,compression='lzw',type='cairo')
  showtext_begin()
  par(mar=c(8,4,4,10),cex.lab=5,cex.axis=5,cex.main=7,family='source',mgp=c(2,1,0))
  barplot(plotable,horiz=F,col=trendPalette,border = NA,names.arg=rep('',4),
          main="",axes=F)
  abline(h=c(0.1,seq(20,80,by=20),99.9),lty=2,col='gray75',lwd=2)
  barplot(plotable,horiz=F,col=trendPalette,las=2,border = NA,add=T,
          main=expression(LAWA~National~River~Water~Quality~'10-Year'~Trends~'(2010-2019)'),
          las=1,yaxt='n',names.arg=c(expression(atop(MCI,'(758 sites)')),
                                     expression(atop(italic(E.~coli),'(741 sites)')),
                                     expression(atop(NH[4],'(646 sites)')),
                                     expression(atop(DRP,'(701 sites)'))))
  # mtext(side = 3,text = paste0(nresults,' total results'),cex=5)
  axis(side=2,at=seq(0,100,by=20),labels=paste(seq(0,100,by=20),'%'),las=2)
  par(xpd=T)
  legend(4.25,96,rev(rownames(plotable)),col=rev(trendPalette),pch=15,bty='n',cex=6,pt.cex=3,xjust=0,
         y.intersp=0.25,x.intersp = 0.25)
  showtext_end()
  if(names(dev.cur())=='tiff'){dev.off()}
  
  
  
  for(indic in c("MCI","ECOLI","NH4","DRP")){
    if(indic=="MCI"){indiclabel=as.expression(bquote(atop(LAWA~National~River~Water~Quality~'10-Year'~Trends~by~Land~Cover~Class~'(2010-2019)',
                                                          Macroinvertebrate~Community~Index~'(MCI)')))}
    if(indic=="ECOLI"){indiclabel=as.expression(bquote(atop(LAWA~National~River~Water~Quality~'10-Year'~Trends~by~Land~Cover~Class~'(2010-2019)',
                                                            italic(E.~coli))))}
    if(indic=="NH4"){indiclabel=as.expression(bquote(atop(LAWA~National~River~Water~Quality~'10-Year'~Trends~by~Land~Cover~Class~'(2010-2019)',
                                                          Ammoniacal~Nitrogen)))}
    if(indic=="DRP"){indiclabel=as.expression(bquote(atop(LAWA~National~River~Water~Quality~'10-Year'~Trends~by~Land~Cover~Class~'(2010-2019)',
                                                          Dissolved~Reactive~Phosphorus~'(DRP)')))}

      tiff(paste0("h:/ericg/16666LAWA/LAWA2020/NationalPicture/"
                ,format(Sys.Date(),"%Y-%m-%d"),"/Trend",pseudo.titlecase(indic),"ByLU.tif"),
         width = 8,height=8,units='in',res=600,compression='lzw',type='cairo')
    with(combTrend%>%filter(period==10&Measurement==indic),plotable <<- table(gRecLC,ConfCat))
    nperlanduse = apply(plotable,1,sum,na.rm=T)
    nresults = sum(sum(plotable,na.rm=T))
    plotable <- apply(plotable,1,FUN=function(x)rev(x/sum(x,na.rm=T)*100))
    # colnames(plotable) = paste0(colnames(plotable),', ',nperlanduse[colnames(plotable)])
  
      showtext_begin()
    par(cex.lab=5,cex.axis=5,cex.main=7,family='source',mgp=c(2,1,0),mar=c(3,4,4,10))
    barplot(plotable,horiz=F,col=trendPalette,las=2,border = NA,
            main='',yaxt='n',ylab='',names.arg=rep('',4))
    abline(h=c(0.1,seq(20,80,by=20),99.9),lty=2,col='gray75',lwd=2)
    barplot(plotable,horiz=F,col=trendPalette,las=1,border = NA,add=T,
            main=indiclabel,
            yaxt='n',ylab='',
            names.arg=c(as.expression(bquote(atop(.(colnames(plotable)[1]),'('*.(unname(nperlanduse[colnames(plotable)[1]]))~'sites)'))),
                        as.expression(bquote(atop(.(colnames(plotable)[2]),'('*.(unname(nperlanduse[colnames(plotable)[2]]))~'sites)'))),
                        as.expression(bquote(atop(.(colnames(plotable)[3]),'('*.(unname(nperlanduse[colnames(plotable)[3]]))~'sites)'))),
                        as.expression(bquote(atop(.(colnames(plotable)[4]),'('*.(unname(nperlanduse[colnames(plotable)[4]]))~'sites)')))))
            # mtext(side = 3,text = nresults,cex=5)
    axis(side=2,at=seq(0,100,by=20),labels=paste(seq(0,100,by=20),'%'),las=2)
    par(xpd=T)
    legend(4.25,96,rev(rownames(plotable)),col=rev(trendPalette),pch=15,cex=6,pt.cex=3,xjust=0,
           y.intersp = 0.25,bty = 'n',x.intersp=0.25)
    showtext_end()
    if(names(dev.cur())=='tiff'){dev.off()}
  }    
  
  

  #Plot ten year trends with NOF and state ####
  
  TrendsForPlotting = combTrend%>%
    dplyr::filter(period==10&(frequency=='monthly'|Measurement=='MCI'))
  TrendsForPlotting$TrendScore[TrendsForPlotting$TrendScore==-99] <- NA
  table(TrendsForPlotting$Measurement[which(is.na(TrendsForPlotting$TrendScore))]) ->naTab
  #    DRP ECOLI   NH4    TN   TON    TP  TURB   MCI 
  #     29    16    96     9    26     8     5   331 
  #Drop the NAs
  TrendsForPlotting = TrendsForPlotting%>%tidyr::drop_na(TrendScore)
  #4636 to 4148
  #Starts as DRP   NH4   TP    TON   TURB  ECOLI TN    BDISC  MCI
  #labelled  DRP   NH4   TP    TON   TURB  ECOLI TN    CLAR   MCI
  #Reorder 2 CLAR TURB   TN    TON   NH4   TP    DRP   ECOLI  MCI
  TrendsForPlotting$Measurement <- factor(TrendsForPlotting$Measurement,
                                          levels=c("BDISC","TURB","TN","TON","NH4","TP","DRP","ECOLI","MCI")) 
  
  
  nfp=NOFSummaryTable%>%
    filter(Year=="2015to2019")%>%
    select(LawaSiteID,Nitrate_Toxicity_Band,Ammonia_Toxicity_Band,EcoliSummaryband)%>%
    left_join(MCINOF%>%filter(sYear==2019)%>%select(LawaSiteID,MCI_band=band))%>%
    dplyr::rename(TON=Nitrate_Toxicity_Band,
                  NH4=Ammonia_Toxicity_Band,
                  ECOLI=EcoliSummaryband,
                  MCI=MCI_band)%>%
    pivot_longer(cols = TON:MCI,names_to = "Measurement",values_to = "NOFband")
  
  tfp=TrendsForPlotting%>%
    filter(Measurement%in%c('MCI','NH4','ECOLI','TON'))%>%
    select(LawaSiteID,Measurement,ConfCat,p)%>%
    mutate(ConfCat=factor(ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading"))))
  
  ntfp=merge(nfp,tfp,by = c("LawaSiteID","Measurement"))
  
  ntfp$Region = riverSiteTable$Region[match(ntfp$LawaSiteID,riverSiteTable$LawaSiteID)]
  
  ntfp <- ntfp%>%filter(NOFband!="NA")
  
  #nfp and #tfp merge to ntfp
  #nfp TON 854  NH4 868  ECOLI 1008  MCI 543
  #tfp TON 456  NH4 454  ECOLI  411  MCI 761
  #ntfpTON 456  NH4 421  ECOLI  396  MCI 430
  #
  #
  #Trends10yrMonthly and RiverNOF merge to tfp10m
  #RiverNOF TON 854 NH4 868 ECOLI 1008
  #T10yrM TON 456  NH4 454  ECOLI 411  MCI 761
  #tfp10m TON 456  NH4 421  ECOLI 396  MCI 761
  
  
  
  #TrendAndNOF plots ####
  # tiff(paste0("h:/ericg/16666LAWA/LAWA2020/NationalPicture/",format(Sys.Date(),"%Y-%m-%d"),"/TrendAndNOF_4.tif"),width = 12,height=12,units='in',res=300,compression='lzw',type='cairo')
  par(mfrow=c(2,2))
  by(data = ntfp,INDICES = ntfp$Measurement,FUN = function(x){
    x <- x%>%drop_na(NOFband,ConfCat)
    plot(factor(x$NOFband),x$ConfCat,col=c("#dd1111FF","#cc7766FF","#AAAAAAFF","#55bb66FF","#008800FF"),
         main=paste0(unique(x$Measurement),'\nn = ',dim(x)[1]),off=0,xlab="NOF Band",ylab="Trend Category")->invisibleTable
    labelAreas(invisibleTable)})
  if(names(dev.cur())=='tiff'){dev.off()}
  
  # tiff(paste0("h:/ericg/16666LAWA/LAWA2020/NationalPicture/",format(Sys.Date(),"%Y-%m-%d"),"/TrendAndNOF_1.tif"),width = 12,height=12,units='in',res=300,compression='lzw',type='cairo')
  par(mfrow=c(1,1))
  plot(factor(ntfp$NOFband),factor(ntfp$ConfCat),col=c("#dd1111FF","#cc7766FF","#AAAAAAFF","#55bb66FF","#008800FF"),
       main="Ecoli, MCI, NH4 and TON combined",off=0,xlab="NOF Band",ylab="Trend Category")->invisibleTable
  labelAreas(invisibleTable)
  if(names(dev.cur())=='tiff'){dev.off()}
  
  
  #Load state info ####
  lawaMacroState5yr = read.csv(tail(dir(path='h:/ericg/16666LAWA/LAWA2020/MacroInvertebrates/Analysis/',pattern='MACRO_STATE_ForITE|MacroState',recursive = T,full.names = T),1),stringsAsFactors = F)
  lawaMacroState5yr = lawaMacroState5yr%>%filter(Parameter=="MCI")%>%dplyr::rename(LawaSiteID=LAWAID,Measurement=Parameter,Q50=Median)
  
  sa <- read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2020/WaterQuality/Analysis/",pattern=paste0("^sa",startYear5,"-",EndYear,".csv"),recursive=T,full.names=T,ignore.case=T),1),stringsAsFactors = F)
  sa <- sa%>%filter(Scope=="Site")%>%select(LawaSiteID,Measurement,Q50)
  sa <- rbind(sa,lawaMacroState5yr)
  tfp <- merge(tfp,sa)
  rm(sa,lawaMacroState5yr)
  
  
  
  #Make the trend summary plot ####
  par(mfrow=c(1,1))
  colMPs=-0.5+(1:9)*1.2
  tb <- plot(factor(TrendsForPlotting$Measurement),
             factor(TrendsForPlotting$TrendScore),
             col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"), #"#dddddd",
             main="Ten year trends")
  tbp <- apply(X = tb,MARGIN = 1,FUN = function(x)x/sum(x))
  mbp <- apply(tbp,MARGIN = 2,FUN=cumsum)
  mbp <- rbind(rep(0,9),mbp)
  mbp = (mbp[-1,]+mbp[-6,])/2
  
  # TenYearMonthlyTrendsClean.tif ####
  # tiff(paste0("h:/ericg/16666LAWA/LAWA2020/NationalPicture/",format(Sys.Date(),"%Y-%m-%d"),"/TenYearMonthlyTrendsClean.tif"),width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
  par(mfrow=c(1,1),mar=c(5,10,6,2))
  barplot(tbp,main="Ten year monthly trends",las=2,
          col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),yaxt='n',xaxt='n') #"#dddddd",
  axis(side = 1,at=colMPs,labels = c('Clarity','Turbidity','TN','TON',expression(NH[4]),'TP',
                                     'DRP',expression(italic(E.~coli)),'MCI'),las=2)
  axis(side = 2,at = seq(0,1,le=11),labels = paste0(100*seq(0,1,le=11),'%'),las=2,lty = 1,lwd = 0,lwd.ticks = 0)
  axis(side = 2,at = seq(0,1,le=11),labels = NA,las=2,lty = 1,lwd = 0,lwd.ticks = 1,line=-1)
  par(xpd=NA)
  for(cc in 1:9){
    text(colMPs[cc],1.025,sum(tb[cc,]))
  }
  if(names(dev.cur())=='tiff'){dev.off()}
  
  # TenYearMonthlyTrendsNumbres.tif ####
  # tiff(paste0("h:/ericg/16666LAWA/LAWA2020/NationalPicture/",format(Sys.Date(),"%Y-%m-%d"),"/TenYearMonthlyTrendsNumbres.tif"),width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
  par(mfrow=c(1,1),mar=c(5,10,6,2))
  barplot(tbp,main="Ten year monthly trends",las=2,
          col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),yaxt='n',xaxt='n') #"#dddddd",
  axis(side = 1,at=colMPs,labels = c('Clarity','Turbidity','TN','TON',expression(NH[4]),'TP',
                                     'DRP',expression(italic(E.~coli)),'MCI'),las=2)
  axis(side = 2,at = seq(0,1,le=11),labels = paste0(100*seq(0,1,le=11),'%'),las=2,lty = 1,lwd = 0,lwd.ticks = 0)
  axis(side = 2,at = seq(0,1,le=11),labels = NA,las=2,lty = 1,lwd = 0,lwd.ticks = 1,line=-1)
  par(xpd=NA)
  for(cc in 1:9){
    text(rep(colMPs[cc],6),mbp[,cc],tb[cc,])
    text(colMPs[cc],1.025,sum(tb[cc,]))
  }
  if(names(dev.cur())=='tiff'){dev.off()}
  
  # TenYearMonthlyTrendsPercentage.tif ####
  # tiff(paste0("h:/ericg/16666LAWA/LAWA2020/NationalPicture/",format(Sys.Date(),"%Y-%m-%d"),"/TenYearMonthlyTrendsPercentage.tif"),width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
  par(mfrow=c(1,1),mar=c(5,10,6,2))
  barplot(tbp,main="Ten year monthly trends",las=2,
          col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),yaxt='n',xaxt='n') #"#dddddd",
  axis(side = 1,at=colMPs,labels = c('Clarity','Turbidity','TN','TON',expression(NH[4]),'TP',
                                     'DRP',expression(italic(E.~coli)),'MCI'),las=2)
  axis(side = 2,at = seq(0,1,le=11),labels = paste0(100*seq(0,1,le=11),'%'),las=2,lty = 0)
  axis(side = 2,at = seq(0,1,le=11),labels = NA,las=2,lty = 1,lwd = 1,lwd.ticks = 1,line=0,tcl=1,tck=0.02)
  par(xpd=NA)
  for(cc in 1:9){
    text(rep(colMPs[cc],6),mbp[,cc],paste0(round(tb[cc,]/sum(tb[cc,])*100,0),'%'),cex=0.75)
    text(colMPs[cc],1.025,sum(tb[cc,]))
  }
  if(names(dev.cur())=='tiff'){dev.off()}
  
  
  
  #Plot fifteen year trends ####
  
  TrendsForPlotting = combTrend%>%
    dplyr::filter(period==15)#%>%
  # dplyr::select(LawaSiteID,Measurement,Region,TrendScore)
  TrendsForPlotting$TrendScore[TrendsForPlotting$TrendScore==-99] <- NA
  
  table(TrendsForPlotting$Measurement[which(is.na(TrendsForPlotting$TrendScore))]) ->naTab
  # BDISC   DRP ECOLI   NH4    TN   TON    TP  TURB   MCI 
  #   678   532   483   595   631   645   599   503   544
  #Drop the  NAs
  TrendsForPlotting = TrendsForPlotting%>%drop_na(TrendScore)
  #8771 to 3561
  #Starts as DRP   NH4   TP    TON   TURB  ECOLI TN    BDISC  MCI
  #labelled  DRP   NH4   TP    TON   TURB  ECOLI TN    CLAR   MCI
  #Reorder 2 CLAR TURB   TN    TON   NH4   TP    DRP   ECOLI  MCI
  TrendsForPlotting$Measurement <- factor(TrendsForPlotting$Measurement,
                                          levels=c("BDISC","TURB","TN","TON","NH4","TP","DRP","ECOLI","MCI")) #,"MCI"
  
  #Make the trend summary plot
  par(mfrow=c(1,1))
  colMPs=-0.5+(1:9)*1.2
  tb <- plot(factor(TrendsForPlotting$Measurement),
             factor(TrendsForPlotting$TrendScore),
             col=c("#dd1111FF","#ee4411FF","#aaaaaaFF","#11cc11FF","#008800FF"), #"#dddddd",
             main="Ten year trends")
  tbp <- apply(X = tb,MARGIN = 1,FUN = function(x)x/sum(x))
  mbp <- apply(tbp,MARGIN = 2,FUN=cumsum)
  mbp <- rbind(rep(0,9),mbp)
  mbp = (mbp[-1,]+mbp[-6,])/2
  
  #FifteenYearTrendsClean.tif ####
  # tiff(paste0("h:/ericg/16666LAWA/LAWA2020/NationalPicture/",format(Sys.Date(),"%Y-%m-%d"),"/FifteenYearTrendsClean.tif"),width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
  par(mfrow=c(1,1),mar=c(5,10,6,2))
  barplot(tbp,main="Fifteen year trends",las=2,
          col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),yaxt='n',xaxt='n') #"#dddddd",
  axis(side = 1,at=colMPs,labels = c('Clarity','Turbidity','TN','TON',expression(NH[4]),'TP',
                                     'DRP',expression(italic(E.~coli)),'MCI'),las=2)
  axis(side = 2,at = seq(0,1,le=11),labels = paste0(100*seq(0,1,le=11),'%'),las=2,lty = 1,lwd = 0,lwd.ticks = 0)
  axis(side = 2,at = seq(0,1,le=11),labels = NA,las=2,lty = 1,lwd = 0,lwd.ticks = 1,line=-1)
  par(xpd=NA)
  for(cc in 1:9){
    text(rep(colMPs[cc],6),mbp[,cc],tb[cc,])
    text(colMPs[cc],1.025,sum(tb[cc,]))
  }
  if(names(dev.cur())=='tiff'){dev.off()}
  
  #FifteenYearTrendsNumbres.tif ####
  # tiff(paste0("h:/ericg/16666LAWA/LAWA2020/NationalPicture/",format(Sys.Date(),"%Y-%m-%d"),"/FifteenYearTrendsNumbres.tif"),width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
  par(mfrow=c(1,1),mar=c(5,10,6,2))
  barplot(tbp,main="Fifteen year trends",las=2,
          col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),yaxt='n',xaxt='n') #"#dddddd",
  axis(side = 1,at=colMPs,labels = c('Clarity','Turbidity','TN','TON',expression(NH[4]),'TP',
                                     'DRP',expression(italic(E.~coli)),'MCI'),las=2)
  axis(side = 2,at = seq(0,1,le=11),labels = paste0(100*seq(0,1,le=11),'%'),las=2,lty = 1,lwd = 0,lwd.ticks = 0)
  axis(side = 2,at = seq(0,1,le=11),labels = NA,las=2,lty = 1,lwd = 0,lwd.ticks = 1,line=-1)
  par(xpd=NA)
  for(cc in 1:9){
    text(rep(colMPs[cc],6),mbp[,cc],tb[cc,])
    text(colMPs[cc],1.025,sum(tb[cc,]))
  }
  if(names(dev.cur())=='tiff'){dev.off()}
  
  #FifteenYearTrendsPercentage.tif ####
  # tiff(paste0("h:/ericg/16666LAWA/LAWA2020/NationalPicture/",format(Sys.Date(),"%Y-%m-%d"),"/FifteenYearTrendsPercentage.tif"),width = 8,height=8,units='in',res=300,compression='lzw',type='cairo')
  par(mfrow=c(1,1),mar=c(5,10,6,2))
  barplot(tbp,main="Fifteen year trends",las=2,
          col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),yaxt='n',xaxt='n') #"#dddddd",
  axis(side = 1,at=colMPs,labels = c('Clarity','Turbidity','TN','TON',expression(NH[4]),'TP',
                                     'DRP',expression(italic(E.~coli)),'MCI'),las=2)
  axis(side = 2,at = seq(0,1,le=11),labels = paste0(100*seq(0,1,le=11),'%'),las=2,lty = 0)
  axis(side = 2,at = seq(0,1,le=11),labels = NA,las=2,lty = 1,lwd = 1,lwd.ticks = 1,line=0,tcl=1,tck=0.02)
  par(xpd=NA)
  for(cc in 1:9){
    text(rep(colMPs[cc],6),mbp[,cc],paste0(round(tb[cc,]/sum(tb[cc,])*100,0),'%'),cex=0.75)
    text(colMPs[cc],1.025,sum(tb[cc,]))
  }
  if(names(dev.cur())=='tiff'){dev.off()}
  
  
  
  
  
  
  par(mfrow=c(3,1))
  t5 <- plot(factor(trendTable5$Measurement),trendTable5$ConfCat,col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),main="5 Year monthly")
  t10 <- plot(factor(trendTable10$Measurement),trendTable10$ConfCat,col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),main="10 Year monthly")
  t15 <- plot(factor(trendTable15$Measurement),trendTable15$ConfCat,col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),main="15 Year monthly")
  # write.csv(t5,paste0("h:/ericg/16666LAWA/LAWA2020/NationalPicture/",format(Sys.Date(),"%Y-%m-%d"),"/Trend5Year.csv"))
  # write.csv(t10,paste0("h:/ericg/16666LAWA/LAWA2020/NationalPicture/",format(Sys.Date(),"%Y-%m-%d"),"/Trend10Year.csv"))
  # write.csv(t15,paste0("h:/ericg/16666LAWA/LAWA2020/NationalPicture/",format(Sys.Date(),"%Y-%m-%d"),"/Trend10YearQuarterly.csv"))
  t5p <- apply(X = t5,MARGIN = 1,FUN = function(x)x/sum(x))
  t10p <- apply(X = t10,MARGIN = 1,FUN = function(x)x/sum(x))
  t15p <- apply(X = t15,MARGIN = 1,FUN = function(x)x/sum(x))
  
  m5p <- apply(t5p,MARGIN = 2,FUN=cumsum)
  m5p <- rbind(rep(0,8),m5p)
  m5p = (m5p[-1,]+m5p[-6,])/2
  
  m10p <- apply(t10p,MARGIN = 2,FUN=cumsum)
  m10p <- rbind(rep(0,8),m10p)
  m10p = (m10p[-1,]+m10p[-6,])/2
  
  m15p <- apply(t15p,MARGIN = 2,FUN=cumsum)
  m15p <- rbind(rep(0,8),m15p)
  m15p = (m15p[-1,]+m15p[-6,])/2
  
  colMPs=-0.5+(1:8)*1.2
  
  # tiff(paste0("h:/ericg/16666LAWA/LAWA2020/NationalPicture/",format(Sys.Date(),"%Y-%m-%d"),"/TrendByPeriod.tif"),width = 10,height=15,units='in',res=350,compression='lzw',type='cairo')
  par(mfrow=c(3,1),mar=c(5,10,4,2))
  barplot(t5p,main="5 Year",las=2,
          col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),yaxt='n')
  axis(side = 2,at = m5p[,1],labels = colnames(t5),las=2,lty = 0)
  for(cc in 1:8){
    text(rep(colMPs[cc],5),m5p[,cc],paste0(t5[cc,],'\n(',round(t5p[,cc]*100,0),'%)'))
  }
  barplot(t10p,main="10 Year",las=2,
          col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),yaxt='n')
  axis(side = 2,at = m10p[,1],labels = colnames(t10),las=2,lty = 0)
  for(cc in 1:8){
    text(rep(colMPs[cc],5),m10p[,cc],paste0(t10[cc,],'\n(',round(t10p[,cc]*100,0),'%)'))
  }
  barplot(t15p,main="15 Year",las=2,
          col=c("#dd1111FF","#cc7766FF","#aaaaaaFF","#55bb66FF","#008800FF"),yaxt='n')
  axis(side = 2,at = m15p[,1],labels = colnames(t15),las=2,lty = 0)
  for(cc in 1:8){
    text(rep(colMPs[cc],5),m15p[,cc],paste0(t15[cc,],'\n(',round(t15p[,cc]*100,0),'%)'))
  }
  if(names(dev.cur())=='tiff'){dev.off()}
  par(mfrow=c(1,1))
  
  
  Sys.time()-startTime
  
  
  
  
  
  
  
  #nfp and #tfp merge to ntfp
  #nfp TON 854  NH4 868  ECOLI 1008  MCI 543
  #tfp TON 456  NH4 454  ECOLI  411  MCI 761
  #ntfpTON 456  NH4 439  ECOLI  396  MCI 455
  #
  #
  #Trends10yrMonthly and RiverNOF merge to tfp10m
  #RiverNOF TON 854 NH4 868 ECOLI 1008
  #T10yrM TON 456  NH4 454  ECOLI 411  MCI 761
  #tfp10m TON 456  NH4 421  ECOLI 396  MCI 761
  
  