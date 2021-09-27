

#Changes in NOF with complete sites ####
xlabpos=(1:10)*0.12-0.07



# NOFTrendNO3Tox ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/GWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendNO3Sum.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
NO3NOFs = GWBands%>%filter(!NO3Band%in%c("","NA"))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(p5y)))%>%
  filter(nY==10)%>%ungroup%>%select(-nY)%>%mutate(haveNO3=T)
nSitesNO3=length(unique(NO3NOFs$LawaSiteID))

layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(strFrom(NO3NOFs$p5y,'to')),factor(NO3NOFs$NO3Band),ylab='',xlab='',
        col=GWPal4,tol.ylab=0,main="Nitrate",
        axes=F,border = NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSitesNO3," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(NO3NOFs%>%filter(p5y=='to2020'),
     points(Long,Lat,pch=c(1,16)[as.numeric(haveNO3)+1],cex=0.25,col='black')
)
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSitesNO3,"sites"))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/GWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFTrendNO3.csv"),row.names=T)
rm(NOXY,nSitesNO3,iT)




# NOFTrendDRPSum ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/GWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendDRPSum.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
DRPNOFs = GWBands%>%drop_na(DRPBand)%>%filter(!DRPBand%in%c("","NA")&!is.na(DRPBand))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(p5y)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSitesDRP=length(unique(DRPNOFs$LawaSiteID))

layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(strFrom(DRPNOFs$p5y,'to')),factor(DRPNOFs$DRPBand),ylab='',xlab='',
        col=GWPal3,tol.ylab=0,main="DRP Summary",
        axes=F,border = NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSitesDRP," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(DRPNOFs%>%filter(p5y=='to2020'),
     points(Long,Lat,pch=16,cex=0.25,col='black')
)
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSitesDRP,"sites"))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/GWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFTrendDRP.csv"),row.names=T)
rm(DRPNOFs,nSitesDRP,iT)





# NOFTrendEcoliSummary ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/GWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendEcoliSummary.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
ecoliNOFs = GWBands%>%filter(!EcoliBand%in%c('','NA'))%>%drop_na(EcoliBand)%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(p5y)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSitesECOli=length(unique(ecoliNOFs$LawaSiteID))

layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(strFrom(ecoliNOFs$p5y,'to')),
        factor(levels=c("A","B"),ecoliNOFs$EcoliBand),ylab='',xlab='',
        col=GWPal3[c(1,3)],tol.ylab=0,main=expression(italic(E.~coli)~Summary),
        axes=F,border = NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSitesECOli," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(ecoliNOFs%>%filter(p5y=='to2020'),
     points(Long,Lat,pch=16,cex=0.25,col='black')
)
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSitesECOli,"sites",""))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/GWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFTrendEColi.csv"),row.names=T)
rm(ecoliNOFs,nSitesECOli,iT)



# NOFTrendChloride  ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/GWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendCl.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
clNOFs = GWBands%>%filter(!ClBand%in%c('',"NA"))%>%drop_na(ClBand)%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(p5y)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSitesCl=length(unique(clNOFs$LawaSiteID))

layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(strFrom(clNOFs$p5y,'to')),factor(clNOFs$ClBand),ylab='',xlab='',
        col=GWPal3,tol.ylab=0,main="Chloride",
        axes=F,border = NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSitesCl," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(clNOFs%>%filter(p5y=='to2020'),
     points(Long,Lat,pch=16,cex=0.25,col='black')
)
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSitesCl,"sites"))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/GWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFTrendCl.csv"),row.names=T)
rm(clNOFs,nSitesCl,iT)


# NOFTrendConductivity  ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/GWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFTrendConductivity.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
condNOFs = GWBands%>%filter(!NaClBand%in%c('','NA'))%>%drop_na(NaClBand)%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(p5y)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSitesNaCl=length(unique(condNOFs$LawaSiteID))

layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(strFrom(condNOFs$p5y,'to')),factor(condNOFs$NaClBand),ylab='',xlab='',
        col=GWPal3,tol.ylab=0,main="Conductivity",
        axes=F,border = NA)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = c(as.character(2011:2020)),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSitesNaCl," complete sites."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(condNOFs%>%filter(p5y=='to2020'),
     points(Long,Lat,pch=16,cex=0.25,col='black')
)
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSitesNaCl,"sites"))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/GWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFTrendNH4.csv"),row.names=T)
rm(condNOFs,nSitesNaCl,iT)








#spacer ####


#Copy fioles to sharepoint ####
file.copy(from = dir(path=paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/GWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/"),
                     pattern = "tif$",full.names = T),
          to="c:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/GWQNationalPicture/plots/",
          overwrite = T,copy.date = T)


#Spacer ####

