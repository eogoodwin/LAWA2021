copyThrough=F
xlabpos=(1:10)*0.12-0.07

stdise=function(x){
  (x-min(x))/max(x-min(x))
}

# TLITrend  ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/Plots/TLITrend.tif"),
     width = 12,height=8,units='in',res=300,compression='lzw',type='cairo')
showtext_auto()
TLINOFs = TLI%>%filter(TLIBand!="NA")%>%drop_na(TLIBand)%>%
  filter(TLIYear>=2011)%>%
  select(-ends_with("type"))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(TLIYear)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSites=length(unique(TLINOFs$LawaSiteID))
layout(matrix(c(1,1,1,2,2,
                1,1,1,2,2,
                1,1,1,2,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,2),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5,xpd=F)
iT=plot(factor(TLINOFs$TLIYear),TLINOFs$TLIBand,ylevels=c("A","B","C","D","E"),
        main='',ylab='',xlab='',col=NOFPal5,tol.ylab=0,
        axes=F,border = NA)
title(main="TLI scores of monitored lakes over time (2011 - 2020)",
      col=LAWAPalette[3])
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = levels(factor(strFrom(TLINOFs$TLIYear,'to'))),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSites," continuously-monitored lakes."),line = 0.5,col=LAWAPalette[3],cex=3)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
points(TLINOFs$Long,TLINOFs$Lat,pch=16,cex=0.25,col='black')
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSites,"sites"))

write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/DataFiles/TLITrend.csv"),row.names=T)
rm(TLINOFs,nSites,iT)



tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/Plots/TLITrendwCounts.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
TLINOFs = TLI%>%filter(TLIBand!="NA")%>%drop_na(TLIBand)%>%
  filter(TLIYear>=2011)%>%
  select(-ends_with("type"))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(TLIYear)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSites=length(unique(TLINOFs$LawaSiteID))

layout(matrix(c(1,1,1,2,2,
                1,1,1,2,2,
                1,1,1,2,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,2),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5,xpd=F)
iT=plot(factor(TLINOFs$TLIYear),TLINOFs$TLIBand,ylevels=c("A","B","C","D","E"),
        ylab='',xlab='',
        col=NOFPal5,tol.ylab=0,main="",
        axes=F,border = NA)
title(main="TLI scores of monitored lakes over time (2011 - 2020)",
      col=LAWAPalette[3])
labelAreas(iT,invert = T)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = levels(factor(strFrom(TLINOFs$TLIYear,'to'))),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSites," continuously-monitored lakes."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
points(TLINOFs$Long,TLINOFs$Lat,pch=16,cex=0.25,col='black')
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
rm(TLINOFs,nSites,iT)



# NOFTrendTNSum ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendTNSum.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
TNNOFs = NOFSummaryTable%>%drop_na(NitrogenMed_Band)%>%filter(NitrogenMed_Band!="NA")%>%
  select(-starts_with(c('Ecoli','Ammonia','Clarity','ChlA','Phos','TLI','Cyano'),ignore.case = T))%>%
  select(-ends_with(c('Note','Type'),ignore.case=T))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSites=length(unique(TNNOFs$LawaSiteID))
layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(strFrom(TNNOFs$Year,'to')),factor(TNNOFs$NitrogenMed_Band),ylab='',xlab='',
        col=NOFPal4,tol.ylab=0,main="TN Summary",
        axes=F,border = NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = levels(factor(strFrom(TNNOFs$Year,'to'))),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(TNNOFs%>%filter(Year=='2015to2019'),
     points(Long,Lat,pch=16,cex=0.25,col='black')
)
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}

iT=rbind(iT,c("Total","of",nSites,"sites"))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/DataFiles/NOFTrendTN.csv"),row.names=T)
rm(TNNOFs,nSites,iT)



# NOFTrendTPSum ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendTPSum.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
TPNOFs = NOFSummaryTable%>%drop_na(PhosphorusMed_Band)%>%filter(PhosphorusMed_Band!="NA")%>%
  select(-starts_with(c('Ecoli','Ammonia',"Nitr","Clar","Chl"),ignore.case = T))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSites=length(unique(TPNOFs$LawaSiteID))

layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(strFrom(TPNOFs$Year,'to')),factor(TPNOFs$PhosphorusMed_Band),ylab='',xlab='',
        col=NOFPal4,tol.ylab=0,main="TP Summary",
        axes=F,border = NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = levels(factor(strFrom(TPNOFs$Year,'to'))),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(TPNOFs%>%filter(Year=='2015to2019'),
     points(Long,Lat,pch=16,cex=0.25,col='black')
)
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSites,"sites"))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/DataFiles/NOFTrendTP.csv"),row.names=T)
rm(TPNOFs,nSites,iT)




# NOFTrendAmmoniacalTox  ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendAmmoniacalTox.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
ammonNOFs = NOFSummaryTable%>%filter(Ammonia_Toxicity_Band!="NA")%>%drop_na(Ammonia_Toxicity_Band)%>%
  select(-SiteID,-CouncilSiteID)%>%
  select(-starts_with(c('nitr','ecoli','nitrate','phos','clar','chla','tli'),ignore.case=T))%>%
  select(-ends_with(c('note','type'),ignore.case=T))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSites=length(unique(ammonNOFs$LawaSiteID))

layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(strFrom(ammonNOFs$Year,'to')),factor(ammonNOFs$Ammonia_Toxicity_Band,levels=AtoD),
        ylevels=c("A","B","C","D"),ylab='',xlab='',
        col=NOFPal4,tol.ylab=0,main="Ammonia toxicity",
        axes=F,border = NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = levels(factor(strFrom(ammonNOFs$Year,'to'))),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
points(ammonNOFs$Long,ammonNOFs$Lat,pch=16,cex=0.25,col='black')
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSites,"sites",""))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/DataFiles/NOFTrendAmmonTox.csv"),row.names=T)
rm(ammonNOFs,nSites,iT)



# NOFTrendClarity  ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendClarity.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
clarNOFs = NOFSummaryTable%>%filter(ClarityMedian_Band!="NA")%>%drop_na(ClarityMedian_Band)%>%
  select(-starts_with(c('ecoli','nitrate','phos','ammon','chla'),ignore.case=T))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSites=length(unique(clarNOFs$LawaSiteID))

layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(clarNOFs$Year),droplevels(clarNOFs$ClarityMedian_Band),ylab='',xlab='',
        col=NOFPal4,tol.ylab=0,main="Clarity",
        axes=F,border = NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = levels(factor(strFrom(clarNOFs$Year,'to'))),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
points(clarNOFs$Long,clarNOFs$Lat,pch=16,cex=0.25,col='black')
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSites,"sites"))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/DataFiles/NOFTrendClarity.csv"),row.names=T)
rm(clarNOFs,nSites,iT)




# NOFTrendChlA  ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendChlA.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
chlNOFs = NOFSummaryTable%>%filter(ChlASummaryBand!="NA")%>%drop_na(ChlASummaryBand)%>%
  select(-starts_with(c('ecoli','nitrate','phos','ammon','Clar'),ignore.case=T))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSites=length(unique(chlNOFs$LawaSiteID))

layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(chlNOFs$Year),droplevels(chlNOFs$ChlASummaryBand),ylab='',xlab='',
        col=NOFPal4,tol.ylab=0,main="Chlorophyll A",
        axes=F,border = NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = levels(factor(strFrom(chlNOFs$Year,'to'))),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
points(chlNOFs$Long,chlNOFs$Lat,pch=16,cex=0.25,col='black')
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSites,"sites"))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/DataFiles/NOFTrendChlA.csv"),row.names=T)
rm(chlNOFs,nSites,iT)


# NOFTrendEcoliSummary ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendEcoliSummary.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
ecoliNOFs = NOFSummaryTable%>%filter(EcoliSummaryBand!="NA")%>%drop_na(EcoliSummaryBand)%>%
  select(-starts_with(c('ammon','nitrate'),ignore.case=T))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY>=7)%>%ungroup%>%select(-nY)
nSites=length(unique(ecoliNOFs$LawaSiteID))

layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=0.8)
iT=plot(factor(strFrom(ecoliNOFs$Year,'to')),ecoliNOFs$EcoliSummaryBand,
        ylevels=c("A","B","C","D","E"),ylab='',xlab='',
        col=NOFPal5,tol.ylab=0,main=expression(italic(E.~coli)~Summary),
        axes=T,border = NA,xaxlabels=levels(factor(strFrom(ecoliNOFs$Year,'to'))),yaxlabels=NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
# axis(1,at=xlabpos,labels = levels(factor(strFrom(ecoliNOFs$Year,'to'))),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSites,"  sites with 7 or more results."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
points(ecoliNOFs$Long,ecoliNOFs$Lat,pch=16,cex=0.25,col='black')
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSites,"sites",""))

write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/DataFiles/NOFTrendEColi.csv"),row.names=T)
rm(ecoliNOFs,nSites,iT)


# 
# TLIb <- TLI%>%filter(TLIYear>2010)%>%
#   group_by(FENZID)%>%dplyr::mutate(n=n())%>%ungroup%>%filter(n==10)%>%select(-n)
# 
# 
# 
# plot(0,0,type='n',xlim=c(2010,2021),ylim=c(0,10),xlab='Year',ylab='TLI',main="TLI change over time\n114 complete lake sites")
# Modes = rep(0,10)
# for(yy in 2011:2020){
#   ydens = TLIb%>%filter(TLIYear==yy)%>%select(TLI)%>%unlist%>%density
#   ydens$y=stdise(ydens$y) 
#   lines(yy+ydens$y,ydens$x)
#   Modes[yy-2010]=ydens$x[which.max(ydens$y)]
# }
# lines(2012:2021,by(data = TLIb$TLI,INDICES = TLIb$TLIYear,FUN=median),col='blue',lwd=2)
# lines(2012:2021,Modes,col='red',lwd=2)
legend('topleft',c("Median","Mode"),col=c('blue','red'),lty=1,lwd=2)

#Copy fioles to sharepoint ####
if(copyThrough){
  sapply(dir(path=paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",
                         format(Sys.Date(),"%Y-%m-%d"),'/Plots/'),
             pattern=".*tif",full.names=T),
         FUN=function(f){
           file.copy(from = f,
                     to = paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021",
                                 " - project team collab/LakesNationalPicture/Plots/"),
                     overwrite=T)})
  sapply(dir(path=paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",
                         format(Sys.Date(),"%Y-%m-%d"),'/DataFiles/'),
             pattern=".*csv",full.names=T),
         FUN=function(f){
           file.copy(from = f,
                     to = paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021",
                                 " - project team collab/LakesNationalPicture/DataFiles/"),
                     overwrite=T)})
  
}