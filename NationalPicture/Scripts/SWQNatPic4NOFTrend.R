

#Changes in NOF with complete sites ####
xlabpos=(1:10)*0.12-0.07
# NOFTrendDINSum ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendDINSum.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
DINNOFs = NOFSummaryTable%>%drop_na(DIN_Summary_Band)%>%filter(!DIN_Summary_Band%in%c("","NA")&!is.na(DIN_Summary_Band))%>%
  select(-starts_with(c('Ecoli','Ammonia'),ignore.case = T))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSites=length(unique(DINNOFs$LawaSiteID))

layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(strFrom(DINNOFs$Year,'to')),factor(DINNOFs$DIN_Summary_Band),ylab='',xlab='',
        col=NOFPal4,tol.ylab=0,main="DIN Summary",
        axes=F,border = NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(DINNOFs%>%filter(Year=='2016to2020'),
     points(Long,Lat,pch=16,cex=0.25,col='black')
)
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSites,"sites"))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFTrendDIN.csv"),row.names=T)
rm(DINNOFs,nSites,iT)







# NOFTrendNO3Tox ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendNO3Sum.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
NO3NOFs = NOFSummaryTable%>%filter(!Nitrate_Toxicity_Band%in%c("","NA"))%>%
  select(-starts_with(c('ammon','ecoli','sussed','DRP'),ignore.case = T))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%
  filter(nY==10)%>%ungroup%>%select(-nY)%>%mutate(haveNO3=T)
nSitesNO3=length(unique(NO3NOFs$LawaSiteID))

nSites=length(unique(NO3NOFs$LawaSiteID))

layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(strFrom(NO3NOFs$Year,'to')),factor(NO3NOFs$Nitrate_Toxicity_Band),ylab='',xlab='',
        col=NOFPal4,tol.ylab=0,main="Nitrate toxicity",
        axes=F,border = NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(NO3NOFs%>%filter(Year=='2016to2020'),
     points(Long,Lat,pch=c(1,16)[as.numeric(haveNO3)+1],cex=0.25,col='black')
)
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSites,"sites"))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFTrendNO3.csv"),row.names=T)
rm(NO3NOFs,nSites,iT)





# NOFTrendNitrateTox ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",
            format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendNitrateTox.tif"),
     width = 14,height=8,units='in',res=300,compression='lzw',type='cairo')
NO3NOFs = NOFSummaryTable%>%filter(!Nitrate_Toxicity_Band%in%c("","NA"))%>%
  select(-starts_with(c('ammon','ecoli','sussed','DRP'),ignore.case = T))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%
  filter(nY==10)%>%ungroup%>%select(-nY)%>%mutate(haveNO3=T)
nSitesNO3=length(unique(NO3NOFs$LawaSiteID))

nSites=length(unique(NO3NOFs$LawaSiteID))

par(mfrow=c(1,2),cex=2,cex.main=1.5,col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3])
plot(factor(strFrom(NO3NOFs$Year,'to')),
     factor(NO3NOFs$Nitrate_Toxicity_Band),ylab='',xlab='',
     col=NOFPal4,tol.ylab=0,main="Nitrate toxicity\n5yr NOF grade",
     axes=F,border=LAWAPalette[4])->iT
axis(2,at=seq(0,1,by=0.2),labels = seq(0,1,by=0.2)*100,las=2,col=LAWAPalette[3])
axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3])
 labelAreas(iT,invert = T)
mtext(side = 1,paste0("Showing only the ",nSites," complete sites."),line = 2,cex=2,col=LAWAPalette[3])
plot(nzmap)
with(NO3NOFs%>%filter(Year=='2016to2020'),
     points(Long,Lat,pch=16,cex=0.25,col=LAWAPalette[1])
)
par(xpd=T)
plotrix::addtable2plot(156,-37,table = iT,display.colnames = T,display.rownames = T,vlines=T,xpad=1,ypad=0.5)
if(names(dev.cur())=='tiff'){dev.off()}
rm(NO3NOFs,nSites,iT)







# NOFTrendDRPSum ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendDRPSum.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
DRPNOFs = NOFSummaryTable%>%drop_na(DRP_Summary_Band)%>%filter(!DRP_Summary_Band%in%c("","NA")&!is.na(DRP_Summary_Band))%>%
  select(-starts_with(c('Ecoli','Ammonia'),ignore.case = T))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSites=length(unique(DRPNOFs$LawaSiteID))

layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(strFrom(DRPNOFs$Year,'to')),factor(DRPNOFs$DRP_Summary_Band),ylab='',xlab='',
        col=NOFPal4,tol.ylab=0,main="DRP Summary",
        axes=F,border = NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(DRPNOFs%>%filter(Year=='2016to2020'),
     points(Long,Lat,pch=16,cex=0.25,col='black')
)
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSites,"sites"))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFTrendDRP.csv"),row.names=T)
rm(DRPNOFs,nSites,iT)





# NOFTrendEcoliSummary ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendEcoliSummary.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
ecoliNOFs = NOFSummaryTable%>%filter(!EcoliSummaryband%in%c('','NA'))%>%drop_na(EcoliSummaryband)%>%
  select(-starts_with(c('ammon','nitrate'),ignore.case=T))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSites=length(unique(ecoliNOFs$LawaSiteID))

layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(strFrom(ecoliNOFs$Year,'to')),
        factor(levels=c("A","B","C","D","E"),ecoliNOFs$EcoliSummaryband),ylab='',xlab='',
        col=LAWAPalette[c(5,6,7,8,3)],tol.ylab=0,main=expression(italic(E.~coli)~Summary),
        axes=F,border = NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
points(ecoliNOFs$Long,ecoliNOFs$Lat,pch=16,cex=0.25,col='black')
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSites,"sites",""))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFTrendEColi.csv"),row.names=T)
rm(ecoliNOFs,nSites,iT)



# NOFTrendSediment  ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendSed.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
sedNOFs = NOFSummaryTable%>%filter(!SusSedBand%in%c('',"NA"))%>%drop_na(SusSedBand)%>%
  select(-starts_with(c('ecoli','nitrate'),ignore.case=T))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSites=length(unique(sedNOFs$LawaSiteID))

layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(strFrom(sedNOFs$Year,'to')),factor(sedNOFs$SusSedBand),ylab='',xlab='',
        col=NOFPal4,tol.ylab=0,main="Suspended sediment",
        axes=F,border = NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
points(sedNOFs$Long,sedNOFs$Lat,pch=16,cex=0.25,col='black')
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSites,"sites"))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFTrendSed.csv"),row.names=T)
rm(sedNOFs,nSites,iT)

# NOFTrendAmmoniacalTox  ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendAmmoniacalTox.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
ammonNOFs = NOFSummaryTable%>%filter(!Ammonia_Toxicity_Band%in%c('','NA'))%>%drop_na(Ammonia_Toxicity_Band)%>%
  select(-starts_with(c('ecoli','nitrate'),ignore.case=T))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSites=length(unique(ammonNOFs$LawaSiteID))

layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(strFrom(ammonNOFs$Year,'to')),factor(ammonNOFs$Ammonia_Toxicity_Band),ylab='',xlab='',
        col=NOFPal4,tol.ylab=0,main="Ammonia toxicity",
        axes=F,border = NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
points(ammonNOFs$Long,ammonNOFs$Lat,pch=16,cex=0.25,col='black')
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSites,"sites"))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFTrendNH4.csv"),row.names=T)
rm(ammonNOFs,nSites,iT)

# NOFTrendMCI  ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendMCI.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
MCINOFs = MCINOF%>%filter(sYear>=2011)%>%group_by(LawaSiteID)%>%filter(MCIband!='NA')%>%drop_na(MCIband)%>%
  dplyr::mutate(nY=length(unique(sYear)))%>%filter(nY==10)%>%ungroup%>%select(-nY)%>%droplevels
nSites=length(unique(MCINOFs$LawaSiteID))

layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(MCINOFs$sYear),factor(MCINOFs$MCIband,levels=c('A','B','C','D')),ylab='',xlab='',
        col=NOFPal4,tol.ylab=0,main="MCI",
        axes=F,border = NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSites," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
points(MCINOFs$Long,MCINOFs$Lat,pch=16,cex=0.25,col='black')
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSites,"sites"))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFTrendMCI.csv"),row.names=T)
rm(MCINOFs,nSites,iT)







#spacer ####


#Copy fioles to sharepoint ####
file.copy(from = dir(path=paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",
                                 format(Sys.Date(),"%Y-%m-%d"),"/Plots/"),
                     pattern = "tif$",full.names = T),
          to="c:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/SWQNationalPicture/Plots/",
          overwrite = T,copy.date = T)


#Spacer ####

