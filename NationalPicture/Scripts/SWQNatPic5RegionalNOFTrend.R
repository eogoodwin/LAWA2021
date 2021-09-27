
#Regional trend plots ####
xlabpos=(1:10)*0.12-0.07

for(region in unique(NOFSummaryTable$Region)){
  #prep fig file ####
  tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrend",pseudo.titlecase(region),".tif"),
       width = 15,height=12,units='in',res=600,compression='lzw',type='cairo')
  showtext_begin()
  layout(matrix(c(01,01,03,04,04,06,13,13,15,
                  01,01,02,04,04,05,13,13,14,
                  01,01,02,04,04,05,13,13,14,
                  07,07,09,10,10,12,16,16,18,
                  07,07,08,10,10,11,16,16,17,
                  07,07,08,10,10,11,16,16,17,
                  19,19,21,22,22,24,25,25,27,
                  19,19,20,22,22,23,25,25,26,
                  19,19,20,22,22,23,25,25,26),nrow=9,byrow=T))
  
  showtext_begin()
  # DIN ####
  DINNOFs = NOFSummaryTable%>%drop_na(DIN_Summary_Band)%>%
    filter(!DIN_Summary_Band%in%c('','NA')&Region==region)%>%
    group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%
    filter(nY==10)%>%ungroup%>%select(-nY)
  nSites=length(unique(DINNOFs$LawaSiteID))
  if(nSites>0){
    par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(3,2,2,0.5),mgp=c(1.75,0.5,0),
        cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
    iT=plot(factor(strFrom(DINNOFs$Year,'to')),
            factor(DINNOFs$DIN_Summary_Band,levels=c("A","B","C","D")),ylab='',xlab='',
            col=NOFPal4,tol.ylab=0,main="DIN Summary",
            axes=F,border = NA)
    axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
    axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
    mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
    par(mar=c(1,0,0,0))
    plot(nzmap,col=LAWAPalette[1],border = NA,xlim=range(DINNOFs$Long,na.rm=T),ylim=range(DINNOFs$Lat,na.rm=T))
    with(DINNOFs%>%filter(Year=='2016to2020'),
         points(Long,Lat,pch=16,cex=0.5,col=LAWAPalette[c(3,8,7,5)][as.numeric(DIN_Summary_Band)])
    )
    plot.new()
    plot.window(xlim=c(0,10),ylim=c(0,10))
    par(xpd=T)
    plotrix::addtable2plot(0,-2,table = iT,display.colnames = T,display.rownames = T,vlines=T,xpad=1,ypad=0.5,cex=1.25)
    par(xpd=F)
    showtext_end()
  }else{plot.new();plot.new();plot.new()}
  rm(DINNOFs,nSites,iT)
  
  
  # NO3 ####
  NO3NOFs = NOFSummaryTable%>%drop_na(Nitrate_Toxicity_Band)%>%
    filter(!Nitrate_Toxicity_Band%in%c('','NA')&Region==region)%>%
    group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%
    filter(nY==10)%>%ungroup%>%select(-nY)
  nSites=length(unique(NO3NOFs$LawaSiteID))
  if(nSites>0){
    par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(3,2,2,0.5),mgp=c(1.75,0.5,0),
        cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
    iT=plot(factor(strFrom(NO3NOFs$Year,'to')),
            factor(NO3NOFs$Nitrate_Toxicity_Band,levels=c("A","B","C","D")),ylab='',xlab='',
            col=NOFPal4,tol.ylab=0,main="Nitrate toxicity",
            axes=F,border = NA)
    axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
    axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
    mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
    par(mar=c(1,0,0,0))
    plot(nzmap,col=LAWAPalette[1],border = NA,xlim=range(NO3NOFs$Long,na.rm=T),ylim=range(NO3NOFs$Lat,na.rm=T))
    with(NO3NOFs%>%filter(Year=='2016to2020'),
         points(Long,Lat,pch=16,cex=0.5,col=LAWAPalette[c(3,8,7,5)][as.numeric(Nitrate_Toxicity_Band)])
    )
    plot.new()
    plot.window(xlim=c(0,10),ylim=c(0,10))
    par(xpd=T)
    plotrix::addtable2plot(0,-2,table = iT,display.colnames = T,display.rownames = T,vlines=T,xpad=1,ypad=0.5,cex=1.25)
    par(xpd=F)
    showtext_end()
  }else{plot.new();plot.new();plot.new()}
  rm(NO3NOFs,nSites,iT)
  
  # DRP ####
  DRPNOFs = NOFSummaryTable%>%drop_na(DRP_Summary_Band)%>%
    filter(!DRP_Summary_Band%in%c('','NA')&Region==region)%>%
    group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%
    filter(nY==10)%>%ungroup%>%select(-nY)
  nSites=length(unique(DRPNOFs$LawaSiteID))
  if(nSites>0){
    par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(3,2,2,0.5),mgp=c(1.75,0.5,0),
        cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
    iT=plot(factor(strFrom(DRPNOFs$Year,'to')),
            factor(DRPNOFs$DRP_Summary_Band,levels=c("A","B","C","D")),ylab='',xlab='',
            col=NOFPal4,tol.ylab=0,main="DRP Summary",
            axes=F,border = NA)
    axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
    axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
    mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
    par(mar=c(1,0,0,0))
    plot(nzmap,col=LAWAPalette[1],border = NA,xlim=range(DRPNOFs$Long,na.rm=T),ylim=range(DRPNOFs$Lat,na.rm=T))
    with(DRPNOFs%>%filter(Year=='2016to2020'),
         points(Long,Lat,pch=16,cex=0.5,col=LAWAPalette[c(3,8,7,5)][as.numeric(DRP_Summary_Band)])
    )
    plot.new()
    plot.window(xlim=c(0,10),ylim=c(0,10))
    par(xpd=T)
    plotrix::addtable2plot(0,-2,table = iT,display.colnames = T,display.rownames = T,vlines=T,xpad=1,ypad=0.5,cex=1.25)
    par(xpd=F)
    showtext_end()
  }else{plot.new();plot.new();plot.new()}
  rm(DRPNOFs,nSites,iT)
  
  # Ecoli #### 
  showtext_begin()
  ecoliNOFs = NOFSummaryTable%>%drop_na(EcoliSummaryband)%>%
    filter(!EcoliSummaryband%in%c('','NA')&Region==region)%>%
    group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%
    filter(nY==10)%>%ungroup%>%select(-nY)
  nSites=length(unique(ecoliNOFs$LawaSiteID))
  if(nSites>0){
    par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(3,2,2,0.5),mgp=c(1.75,0.5,0),
        cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
    iT=plot(factor(strFrom(ecoliNOFs$Year,'to')),
            factor(ecoliNOFs$EcoliSummaryband,levels=c("A","B","C","D","E")),ylab='',xlab='',
            col=LAWAPalette[c(5,6,7,8,3)],tol.ylab=0,main=expression(italic(E.~coli)~Summary),
            axes=F,border = NA)
    axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
    axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
    mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
    par(mar=c(1,0,0,0))
    plot(nzmap,col=LAWAPalette[1],border = NA,xlim=range(ecoliNOFs$Long,na.rm=T),ylim=range(ecoliNOFs$Lat,na.rm=T))
    with(ecoliNOFs%>%filter(Year=='2016to2020'),
         points(Long,Lat,pch=16,cex=0.5,col=NOFPal5[as.numeric(EcoliSummaryband)])
    )
    plot.new()
    plot.window(xlim=c(0,10),ylim=c(0,10))
    par(xpd=T)
    plotrix::addtable2plot(0,-2,table = iT,display.colnames = T,display.rownames = T,vlines=T,xpad=1,ypad=0.5,cex=1.25)
    par(xpd=F)
    showtext_end()
  }else{plot.new();plot.new();plot.new()}
  rm(ecoliNOFs,nSites,iT)
  
  # SED ####
  SEDNOFs = NOFSummaryTable%>%drop_na(SusSedBand)%>%
    filter(!SusSedBand%in%c('','NA')&Region==region)%>%
    group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%
    filter(nY==10)%>%ungroup%>%select(-nY)
  nSites=length(unique(SEDNOFs$LawaSiteID))
  if(nSites>0){
    par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(3,2,2,0.5),mgp=c(1.75,0.5,0),
        cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
    iT=plot(factor(strFrom(SEDNOFs$Year,'to')),
            factor(SEDNOFs$SusSedBand,levels=c("A","B","C","D")),ylab='',xlab='',
            col=NOFPal4,tol.ylab=0,main="Suspended Sediment",
            axes=F,border = NA)
    axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
    axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
    mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
    par(mar=c(1,0,0,0))
    plot(nzmap,col=LAWAPalette[1],border = NA,xlim=range(SEDNOFs$Long,na.rm=T),ylim=range(SEDNOFs$Lat,na.rm=T))
    with(SEDNOFs%>%filter(Year=='2016to2020'),
         points(Long,Lat,pch=16,cex=0.5,col=LAWAPalette[c(3,8,7,5)][as.numeric(SusSedBand)])
    )
    plot.new()
    plot.window(xlim=c(0,10),ylim=c(0,10))
    par(xpd=T)
    plotrix::addtable2plot(0,-2,table = iT,display.colnames = T,display.rownames = T,vlines=T,xpad=1,ypad=0.5,cex=1.25)
    par(xpd=F)
    showtext_end()
  }else{plot.new();plot.new();plot.new()}
  rm(SEDNOFs,nSites,iT)
  
  # AmmoniacalTox  ####
  ammonNOFs = NOFSummaryTable%>%drop_na(Ammonia_Toxicity_Band)%>%
    filter(!Ammonia_Toxicity_Band%in%c('','NA')&Region==region)%>%
    group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%
    filter(nY==10)%>%ungroup%>%select(-nY)
  nSites=length(unique(ammonNOFs$LawaSiteID))
  if(nSites>0){
    par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(3,2,2,0.5),mgp=c(1.75,0.5,0),
        cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
    iT=plot(factor(strFrom(ammonNOFs$Year,'to')),
            factor(ammonNOFs$Ammonia_Toxicity_Band,levels=c("A","B","C","D")),ylab='',xlab='',
            col=NOFPal4,tol.ylab=0,main="Ammonia toxicity",
            axes=F,border = NA)
    axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
    axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
    mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
    par(mar=c(1,0,0,0))
    plot(nzmap,col=LAWAPalette[1],border = NA,xlim=range(ammonNOFs$Long,na.rm=T),ylim=range(ammonNOFs$Lat,na.rm=T))
    with(ammonNOFs%>%filter(Year=='2016to2020'),
         points(Long,Lat,pch=16,cex=0.5,col=LAWAPalette[c(3,8,7,5)][as.numeric(Ammonia_Toxicity_Band)])
    )
    plot.new()
    plot.window(xlim=c(0,10),ylim=c(0,10))
    par(xpd=T)
    plotrix::addtable2plot(0,-2,table = iT,display.colnames = T,display.rownames = T,vlines=T,xpad=1,ypad=0.5,cex=1.25)
    par(xpd=F)
    showtext_end()
  }else{plot.new();plot.new();plot.new()}
  rm(ammonNOFs,nSites,iT)
  
  # MCI  ####
  showtext_begin()
  MCINOFs = MCINOF%>%drop_na(MCIband)%>%
    filter(!MCIband%in%c('','NA')&Region==region)%>%
    filter(sYear>=2011)%>%
    group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(sYear)))%>%
    filter(nY==10)%>%ungroup%>%select(-nY)
  nSites=length(unique(MCINOFs$LawaSiteID))
  if(nSites>0& !all(is.na(MCINOFs$Long))){
    par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(3,2,2,0.5),mgp=c(1.75,0.5,0),
        cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
    iT=plot(factor(MCINOFs$sYear),
            factor(MCINOFs$MCIband,levels=c("A","B","C","D")),ylab='',xlab='',
            col=NOFPal4,tol.ylab=0,main="MCI",
            axes=F,border = NA)
    axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
    axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
    mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
    par(mar=c(1,0,0,0))
    plot(nzmap,col=LAWAPalette[1],border = NA,xlim=range(MCINOFs$Long,na.rm=T),ylim=range(MCINOFs$Lat,na.rm=T))
    with(MCINOFs%>%filter(sYear==2020),
         points(Long,Lat,pch=16,cex=0.5,col=LAWAPalette[c(3,8,7,5)][as.numeric(MCIband)])
    )
    plot.new()
    plot.window(xlim=c(0,10),ylim=c(0,10))
    par(xpd=T)
    plotrix::addtable2plot(0,-2,table = iT,display.colnames = T,
                           display.rownames = T,vlines=T,xpad=1,ypad=0.5,cex=1.25)
    par(xpd=F)
    showtext_end()
  }else{plot.new();plot.new();plot.new()}
  rm(MCINOFs,nSites,iT)
  #Close plot ####
  if(names(dev.cur())=='tiff'){dev.off()}
}

#Copy fioles to sharepoint ####
file.copy(from = dir(path=tail(dir(path="h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",
                                   pattern='^2021',full.names = T,recursive=T,include.dirs = T),1),
                     pattern = "tif",full.names = T,recursive = T),
          to="c:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/SWQNationalPicture/Plots/",
          overwrite = T,copy.date = T)



# #Copy fioles to sharepoint ####
file.copy(from = dir(path=tail(dir(path="h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",
                                   pattern='^2021',full.names = T),1),
                     pattern = "csv$",full.names = T,recursive=T),
          to="c:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/SWQNationalPicture/DataFiles/",
          overwrite = T,copy.date = T)


#Spacer ####







#Best + Worst trend plots ####
if(0){
  source("h:/ericg/16666LAWA/LWPTrends_v2101/LWPTrends_v2101.R")
  wqdataFileName=tail(dir(path = "H:/ericg/16666LAWA/LAWA2021/WaterQuality/Data",pattern = "AllCouncils.csv",recursive = T,full.names = T),1)
  cat(wqdataFileName)
  wqdata=read_csv(wqdataFileName,guess_max = 100000)%>%as.data.frame
  rm(wqdataFileName)
  macroData=read_csv(tail(dir(path="H:/ericg/16666LAWA/LAWA2021/MacroInvertebrates/Data",pattern="MacrosWithMetadata",
                              recursive = T,full.names = T),1))
  macroData$LawaSiteID = tolower(macroData$LawaSiteID)
  
  wqdata$myDate <- as.Date(lubridate::dmy(as.character(wqdata$Date)),"%d-%b-%Y")
  wqdata$myDate[wqdata$myDate<as.Date('2000-01-01')] <- as.Date(lubridate::dmy(as.character(wqdata$Date[wqdata$myDate<as.Date('2000-01-01')])),
                                                                "%d-%b-%y")
  wqdata <- GetMoreDateInfo(wqdata)
  wqdata$monYear = format(wqdata$myDate,"%b-%Y")
  wqdata$Season=wqdata$Month
  wqdata$Year = lubridate::year(lubridate::dmy(wqdata$Date))
  
  combTrend=read_csv(tail(dir("h:/ericg/16666LAWA/LAWA2021/WaterQuality/Analysis/","RiverWQ_Trend",	recursive = T,full.names = T,ignore.case=T),1),guess_max=2000)
  EndYear <- lubridate::year(Sys.Date())-1
  startYear5 <- EndYear - 5+1
  startYear10 <- EndYear - 10+1
  startYear15 <- EndYear - 15+1
  
  SeasonString=sort(unique(wqdata$Season))
  savePlott=T
  usites=unique(combTrend$LawaSiteID)
  uMeasures=unique(combTrend$Measurement)
  for(uparam in seq_along(uMeasures)){
    if(uMeasures[uparam]=='MCI'){
      subwq = macroData[macroData$Measurement=="MCI",]%>%as.data.frame
      subTrend=combTrend[which((combTrend$Measurement==uMeasures[uparam])),]
      subwq$Season=subwq$sYear
      subwq$Censored=FALSE
      subwq$CenType='not'
        subwq$myDate = as.Date(lubridate::dmy(as.character(subwq$Date)),'%d-%b-%Y')
      subwq <- GetMoreDateInfo(subwq)
    }else{
      subwq=wqdata[wqdata$Measurement==uMeasures[uparam],]
      subTrend=combTrend[which((combTrend$Measurement==uMeasures[uparam]&combTrend$frequency=='monthly')),]
    }
    worstDeg <- which.max(subTrend$Cd) 
    bestImp <- which.min(subTrend$Cd)
    leastKnown <- which.min(abs(subTrend$Cd-0.5))
    if(savePlott){
      tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",
                  format(Sys.Date(),"%Y-%m-%d"),"/Plots/BestWorst",uMeasures[uparam],".tif"),
           width = 12,height=8,units='in',res=300,compression='lzw',type='cairo')
    }else{
      windows()
    }
    par(mfrow=c(3,1),mar=c(2,4,1,2),family='source',cex.lab=4,cex.axis=4,cex.main=4)
    if(uMeasures[uparam]=='MCI'){
      logax=''
      theseDeg <- which(subwq$LawaSiteID==subTrend$LawaSiteID[worstDeg] )
      theseInd <- which(subwq$LawaSiteID==subTrend$LawaSiteID[leastKnown])
      theseImp <- which(subwq$LawaSiteID==subTrend$LawaSiteID[bestImp] )
    }else{
      logax='y'
      theseDeg <- which(subwq$LawaSiteID==subTrend$LawaSiteID[worstDeg] & subwq$Year>=startYear10)
      theseInd <- which(subwq$LawaSiteID==subTrend$LawaSiteID[leastKnown] & subwq$Year>=startYear10)
      theseImp <- which(subwq$LawaSiteID==subTrend$LawaSiteID[bestImp] & subwq$Year>=startYear10)
    }
    if(length(theseDeg)>0){
      st <- SeasonalityTest(x = subwq[theseDeg,],
                            main=uMeasures[uparam],ValuesToUse = 'Value',do.plot =F)
      if(!is.na(st$pvalue)&&st$pvalue<0.05){
        SeasonalSenSlope(HiCensor=T,x = subwq[theseDeg,],ValuesToUse = 'Value',ValuesToUseforMedian = 'Value',doPlot = T,
                         mymain = subTrend$LawaSiteID[worstDeg])
      }else{
        SenSlope(HiCensor=T,x = subwq[theseDeg,],ValuesToUse = 'Value',ValuesToUseforMedian = 'Value',
                 doPlot = T,mymain = subTrend$LawaSiteID[worstDeg])
      }
    }
    if(length(theseInd)>0){
      st <- SeasonalityTest(x = subwq[theseInd,],main=uMeasures[uparam],ValuesToUse = 'Value',do.plot =F)
      if(!is.na(st$pvalue)&&st$pvalue<0.05){
        SeasonalSenSlope(HiCensor=T,x = subwq[theseInd,],ValuesToUse = 'Value',ValuesToUseforMedian = 'Value',
                         doPlot = T,mymain = subTrend$LawaSiteID[leastKnown])
      }else{
        SenSlope(HiCensor=T,x = subwq[theseInd,],ValuesToUse = 'Value',ValuesToUseforMedian = 'Value',
                 doPlot = T,mymain = subTrend$LawaSiteID[leastKnown])
      }
    }
    if(length(theseImp)>0){
      st <- SeasonalityTest(x = subwq[theseImp,],main=uMeasures[uparam],ValuesToUse = 'Value',do.plot =F)
      if(!is.na(st$pvalue)&&st$pvalue<0.05){
        if(is.na(SeasonalSenSlope(HiCensor=T,x = subwq[theseImp,],ValuesToUse = 'Value',ValuesToUseforMedian = 'Value',
                                  doPlot = T,mymain = subTrend$LawaSiteID[bestImp])$Sen_Probability)){
          SenSlope(HiCensor=T,x = subwq[theseImp,],ValuesToUse = 'Value',ValuesToUseforMedian = 'Value',
                   doPlot = T,mymain = subTrend$LawaSiteID[bestImp])
        }
      }else{
        SenSlope(HiCensor=T,x = subwq[theseImp,],ValuesToUse = 'Value',ValuesToUseforMedian = 'Value',
                 doPlot = T,mymain = subTrend$LawaSiteID[bestImp])
      }
    }
    if(names(dev.cur())=='tiff'){dev.off()}
    rm(theseDeg,theseImp,theseInd)
    rm(worstDeg,bestImp,leastKnown)
  }
  rm(uparam,uMeasures)
}



