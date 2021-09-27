




#                         NOF results by land cover ####

#NOFDINLC ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFDINLC_map.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),xpd=T,mar=c(5,4,4,2),col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3])
layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
showtext_begin()
latestDIN <- NOFlatest%>%dplyr::filter(!DIN_Summary_Band%in%c("","NA")&!is.na(DIN_Summary_Band))%>%
  tidyr::drop_na(gRecLC)%>%dplyr::group_by(gRecLC,DIN_Summary_Band)%>%
  dplyr::summarise(.groups='keep',nc=n())%>%ungroup%>%arrange(gRecLC)%>%
  pivot_wider(values_from = 'nc',names_from = 'gRecLC')%>%arrange(desc(DIN_Summary_Band))%>%as.matrix
vegTypeCounts = colSums(apply(latestDIN[,-1],2,as.numeric),na.rm = T)
latestbDIN <- apply(latestDIN,2,FUN=function(x)as.numeric(x)/sum(as.numeric(x),na.rm=T))
latestbDIN[is.na(latestbDIN)] <- 0
latestbDIN[,1]=latestDIN[,1]
bp <- barplot(as.matrix(latestbDIN)[,-1],col=NOFPal4,main="DIN Summary",axes=F,
              ylab="",border =NA,cex.lab=4,cex.main=5,cex.names=4)
axis(side = 2,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],cex.axis=4)
mtext(side=1,line = 2,at = bp,text = paste0(vegTypeCounts,' sites'),cex = 2,col = LAWAPalette[3])
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(NOFlatest%>%filter(!DIN_Summary_Band%in%c("","NA")&!is.na(DIN_Summary_Band))%>%tidyr::drop_na(gRecLC),{
  points(Long,Lat,pch=16,cex=0.75,col='black')
  write.csv(cbind(Long,Lat,DIN_Summary_Band),paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFDINmapPts.csv"),row.names=F)
}
)
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
latestbDIN=rbind(latestbDIN,latestDIN,c(sum(apply(latestDIN[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))),apply(latestDIN[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))))
write.csv(latestbDIN,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFDINLC.csv"),row.names=F)
#rm(list=ls(pattern='^test'))




#NOFNO3LC ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFNO3LC_map.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),xpd=T,mar=c(5,4,4,2),col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3])
layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
showtext_begin()
latestNtox <- NOFlatest%>%dplyr::filter(!Nitrate_Toxicity_Band%in%c("","NA"))%>%
 tidyr::drop_na(gRecLC)%>%dplyr::group_by(gRecLC,Nitrate_Toxicity_Band)%>%
  dplyr::summarise(.groups='keep',nc=n())%>%ungroup%>%arrange(gRecLC)%>%
  pivot_wider(values_from = 'nc',names_from = 'gRecLC')%>%arrange(desc(Nitrate_Toxicity_Band))%>%as.matrix
vegTypeCounts = colSums(apply(latestNtox[,-1],2,as.numeric),na.rm = T)
latestbNtox <- apply(latestNtox,2,FUN=function(x)as.numeric(x)/sum(as.numeric(x),na.rm=T))
latestbNtox[is.na(latestbNtox)] <- 0
latestbNtox[,1]=latestNtox[,1]
bp <- barplot(as.matrix(latestbNtox)[,-1],col=NOFPal4,main="Nitrate toxicity",axes=F,
              ylab="",border =NA,cex.lab=4,cex.main=5,cex.names=4)
axis(side = 2,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],cex.axis=4)
mtext(side=1,line = 2,at = bp,text = paste0(vegTypeCounts,' sites'),cex = 2,col = LAWAPalette[3])
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(NOFlatest%>%filter(!Nitrate_Toxicity_Band%in%c("","NA"))%>%tidyr::drop_na(gRecLC),
     {
  points(Long,Lat,pch=16,cex=0.75,col='black')
  write.csv(cbind(Long,Lat,Nitrate_Toxicity_Band),paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFNO3mapPts.csv"),row.names=F)
}
)
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
latestbNtox=rbind(latestbNtox,latestNtox,c(sum(apply(latestNtox[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))),apply(latestNtox[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))))
write.csv(latestbNtox,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFNO3LC.csv"),row.names=F)
#rm(list=ls(pattern='^test'))

#NOFDRPLC ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFDRPLC_map.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),xpd=T,mar=c(5,4,4,2),col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3])
layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
showtext_begin()
latestDRP <- NOFlatest%>%dplyr::filter(!DRP_Summary_Band%in%c("","NA")&!is.na(DRP_Summary_Band))%>%
  tidyr::drop_na(gRecLC)%>%dplyr::group_by(gRecLC,DRP_Summary_Band)%>%
  dplyr::summarise(.groups='keep',nc=n())%>%ungroup%>%arrange(gRecLC)%>%
  pivot_wider(values_from = 'nc',names_from = 'gRecLC')%>%arrange(desc(DRP_Summary_Band))%>%as.matrix
vegTypeCounts = colSums(apply(latestDRP[,-1],2,as.numeric),na.rm = T)
latestbDRP <- apply(latestDRP,2,FUN=function(x)as.numeric(x)/sum(as.numeric(x),na.rm=T))
latestbDRP[is.na(latestbDRP)] <- 0
latestbDRP[,1]=latestDRP[,1]
bp <- barplot(as.matrix(latestbDRP)[,-1],col=NOFPal4,main="DRP Summary",axes=F,
              ylab="",border =NA,cex.lab=4,cex.main=5,cex.names=4)
axis(side = 2,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],cex.axis=4)
mtext(side=1,line = 2,at = bp,text = paste0(vegTypeCounts,' sites'),cex = 2,col = LAWAPalette[3])
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(NOFlatest%>%filter(!DRP_Summary_Band%in%c("","NA")&!is.na(DRP_Summary_Band))%>%tidyr::drop_na(gRecLC),{
  points(Long,Lat,pch=16,cex=0.75,col='black')
  write.csv(cbind(Long,Lat,DRP_Summary_Band),paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFDRPmapPts.csv"),row.names=F)
}
)
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
latestbDRP=rbind(latestbDRP,latestDRP,c(sum(apply(latestDRP[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))),apply(latestDRP[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))))
write.csv(latestbDRP,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFDRPLC.csv"),row.names=F)
#rm(list=ls(pattern='^test'))


#NOFECOLILC ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFEcoliLC_map.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),xpd=T,mar=c(5,4,4,2),col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3])
layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
showtext_auto()

latestECOLI <- NOFlatest%>%dplyr::filter(!EcoliSummaryband%in%c("","NA")&!is.na(EcoliSummaryband))%>%
  tidyr::drop_na(gRecLC)%>%dplyr::group_by(gRecLC,EcoliSummaryband)%>%
  dplyr::summarise(.groups='keep',nc=n())%>%ungroup%>%arrange(gRecLC)%>%
  pivot_wider(values_from = 'nc',names_from = 'gRecLC')%>%arrange(desc(EcoliSummaryband))%>%as.matrix
vegTypeCounts = colSums(apply(latestECOLI[,-1],2,as.numeric),na.rm = T)
latestbECOLI <- apply(latestECOLI,2,FUN=function(x)as.numeric(x)/sum(as.numeric(x),na.rm=T))
latestbECOLI[is.na(latestbECOLI)] <- 0
latestbECOLI[,1]=latestECOLI[,1]
bp <- barplot(as.matrix(latestbECOLI)[,-1],col=NOFPal5,main=expression(italic(E.~coli)),axes=F,
              ylab="",border = NA,cex.lab=4,cex.main=5,cex.names=4)
axis(side = 2,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],cex.axis=4)
mtext(side=1,line = 2,at = bp,text = paste0(vegTypeCounts,' sites'),cex = 2,col = LAWAPalette[3])
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(NOFlatest%>%filter(!EcoliSummaryband%in%c("","NA")&!is.na(EcoliSummaryband))%>%tidyr::drop_na(gRecLC),{
  points(Long,Lat,pch=16,cex=0.75,col='black')
  write.csv(cbind(Long,Lat,EcoliSummaryband),paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFEcolimapPts.csv"),row.names=F)
})
if(names(dev.cur())=='tiff'){dev.off()}
latestbECOLI=rbind(latestbECOLI,latestECOLI,c(sum(apply(latestECOLI[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))),apply(latestECOLI[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))))
write.csv(latestbECOLI,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFEcoliLC.csv"),row.names=F)
#rm(list=ls(pattern='^test'))

#NOFECOLIwo95LC ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFEcoliwo95LC_map.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),xpd=T,mar=c(5,4,4,2),col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3])
layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
showtext_auto()

latestECOLIwo95 <- NOFlatest%>%dplyr::filter(!EcoliSummarybandwo95%in%c("","NA")&!is.na(EcoliSummarybandwo95))%>%
  tidyr::drop_na(gRecLC)%>%dplyr::group_by(gRecLC,EcoliSummarybandwo95)%>%
  dplyr::summarise(.groups='keep',nc=n())%>%ungroup%>%arrange(gRecLC)%>%
  pivot_wider(values_from = 'nc',names_from = 'gRecLC')%>%arrange(desc(EcoliSummarybandwo95))%>%as.matrix
vegTypeCounts = colSums(apply(latestECOLIwo95[,-1],2,as.numeric),na.rm = T)
latestbECOLIwo95 <- apply(latestECOLIwo95,2,FUN=function(x)as.numeric(x)/sum(as.numeric(x),na.rm=T))
latestbECOLIwo95[is.na(latestbECOLIwo95)] <- 0
latestbECOLIwo95[,1]=latestECOLIwo95[,1]
bp <- barplot(as.matrix(latestbECOLIwo95)[,-1],col=NOFPal5,main=expression(italic(E.~coli)~without~95^th~percentile),axes=F,
              ylab="",border = NA,cex.lab=4,cex.main=5,cex.names=4)
axis(side = 2,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],cex.axis=4)
mtext(side=1,line = 2,at = bp,text = paste0(vegTypeCounts,' sites'),cex = 2,col = LAWAPalette[3])
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(NOFlatest%>%filter(!EcoliSummarybandwo95%in%c("","NA")&!is.na(EcoliSummarybandwo95))%>%tidyr::drop_na(gRecLC),{
  points(Long,Lat,pch=16,cex=0.75,col='black')
  write.csv(cbind(Long,Lat,EcoliSummarybandwo95),paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFEcoliwo95mapPts.csv"),row.names=F)
})
if(names(dev.cur())=='tiff'){dev.off()}
latestbECOLIwo95=rbind(latestbECOLIwo95,latestECOLIwo95,c(sum(apply(latestECOLIwo95[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))),apply(latestECOLIwo95[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))))
write.csv(latestbECOLIwo95,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFEcoliwo95LC.csv"),row.names=F)
#rm(list=ls(pattern='^test'))

#NOFSedLC ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFSedLC_map.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),xpd=T,mar=c(5,4,4,2),col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3])
layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
showtext_auto()

latestSED <- NOFlatest%>%dplyr::filter(!SusSedBand%in%c("","NA")&!is.na(SusSedBand))%>%
  tidyr::drop_na(gRecLC)%>%dplyr::group_by(gRecLC,SusSedBand)%>%
  dplyr::summarise(.groups='keep',nc=n())%>%ungroup%>%arrange(gRecLC)%>%
  pivot_wider(values_from = 'nc',names_from = 'gRecLC')%>%arrange(desc(SusSedBand))%>%as.matrix
vegTypeCounts = colSums(apply(latestSED[,-1],2,as.numeric),na.rm = T)
latestbSED <- apply(latestSED,2,FUN=function(x)as.numeric(x)/sum(as.numeric(x),na.rm=T))
latestbSED[is.na(latestbSED)] <- 0
latestbSED[,1]=latestSED[,1]
bp <- barplot(as.matrix(latestbSED)[,-1],col=NOFPal4,main="Suspended sediment",axes=F,
              ylab="",border = NA,cex.lab=4,cex.main=5,cex.names=4)
axis(side = 2,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],cex.axis=4)
mtext(side=1,line = 2,at = bp,text = paste0(vegTypeCounts,' sites'),cex = 2,col = LAWAPalette[3])
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(NOFlatest%>%filter(!SusSedBand%in%c("","NA")&!is.na(SusSedBand))%>%tidyr::drop_na(gRecLC),{
  points(Long,Lat,pch=16,cex=0.75,col='black')
})
if(names(dev.cur())=='tiff'){dev.off()}

with(NOFlatest%>%filter(!SusSedBand%in%c("","NA")&!is.na(SusSedBand))%>%tidyr::drop_na(gRecLC),{
  write.csv(cbind(Long,Lat,SusSedBand),paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFSedmapPts.csv"),row.names=F)
})
latestbSED=rbind(latestbSED,latestSED,c(sum(apply(latestSED[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))),apply(latestSED[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))))
write.csv(latestbSED,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFSedLC.csv"),row.names=F)
#rm(list=ls(pattern='^test'))



#NOFNH4LC ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFNH4LC_map.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),xpd=T,mar=c(5,4,4,2),col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3])
layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
showtext_begin()

latestNH4 <- NOFlatest%>%dplyr::filter(!Ammonia_Toxicity_Band%in%c("","NA")&!is.na(Ammonia_Toxicity_Band))%>%
  tidyr::drop_na(gRecLC)%>%dplyr::group_by(gRecLC,Ammonia_Toxicity_Band)%>%
  dplyr::summarise(.groups='keep',nc=n())%>%ungroup%>%arrange(gRecLC)%>%
  pivot_wider(values_from = 'nc',names_from = 'gRecLC')%>%arrange(desc(Ammonia_Toxicity_Band))%>%as.matrix
vegTypeCounts = colSums(apply(latestNH4[,-1],2,as.numeric),na.rm = T)
latestbNH4 <- apply(latestNH4,2,FUN=function(x)as.numeric(x)/sum(as.numeric(x),na.rm=T))
latestbNH4[is.na(latestbNH4)] <- 0
latestbNH4[,1]=latestNH4[,1]
bp <- barplot(as.matrix(latestbNH4)[,-1],col=NOFPal4,main=expression(NH[4]),axes=F,
              ylab="",border = NA,cex.lab=4,cex.main=5,cex.names=4)
axis(side = 2,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],cex.axis=4)
mtext(side=1,line = 2,at = bp,text = paste0(vegTypeCounts,' sites'),cex = 2,col = LAWAPalette[3])
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(NOFlatest%>%filter(!Ammonia_Toxicity_Band%in%c("","NA")&!is.na(Ammonia_Toxicity_Band))%>%tidyr::drop_na(gRecLC),{
  points(Long,Lat,pch=16,cex=0.75,col='black')
  write.csv(cbind(Long,Lat,Ammonia_Toxicity_Band),paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFNH4mapPts.csv"),row.names=F)
})
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
latestbNH4=rbind(latestbNH4,latestNH4,c(sum(apply(latestNH4[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))),apply(latestNH4[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))))
write.csv(latestbNH4,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFNH4LC.csv"),row.names=F)
#rm(list=ls(pattern='^test'))



#NOFMCILC ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFMCILC_map.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),xpd=T,mar=c(5,4,4,2),col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3])
layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
showtext_auto()

latestMCI <- MCINOF%>%dplyr::filter(sYear=="2020")%>%
  filter(!MCIband%in%c('NA'))%>%
  tidyr::drop_na(gRecLC)%>%dplyr::group_by(gRecLC,MCIband)%>%
  dplyr::summarise(.groups='keep',nc=n())%>%ungroup%>%arrange(gRecLC)%>%
  pivot_wider(values_from = 'nc',names_from = 'gRecLC')%>%arrange(desc(MCIband))%>%as.matrix
vegTypeCounts = colSums(apply(latestMCI[,-1],2,as.numeric),na.rm = T)
latestbMCI <- apply(latestMCI,2,FUN=function(x)as.numeric(x)/sum(as.numeric(x),na.rm=T))
latestbMCI[is.na(latestbMCI)] <- 0
latestbMCI[,1]=latestMCI[,1]
bp <- barplot(as.matrix(latestbMCI)[,-1],col=NOFPal4,main='MCI',axes=F,
              ylab="",border = NA,cex.lab=4,cex.main=5,cex.names=4)
axis(side = 2,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],cex.axis=4)
mtext(side=1,line = 2,at = bp,text = paste0(vegTypeCounts,' sites'),cex = 2,col = LAWAPalette[3])
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(MCINOF%>%filter(sYear=="2020")%>%tidyr::drop_na(gRecLC),{
  points(Long,Lat,pch=16,cex=0.75,col='black')
  write.csv(cbind(Long,Lat,MCIband),paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFMCImapPts.csv"),row.names=F)
})
if(names(dev.cur())=='tiff'){dev.off()}
latestbMCI=rbind(latestbMCI,latestMCI,c(sum(apply(latestMCI[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))),apply(latestMCI[,-1],2,FUN=function(x)sum(as.numeric(x),na.rm=T))))
write.csv(latestbMCI,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",format(Sys.Date(),"%Y-%m-%d"),"/DownloadDataFiles/NOFMCILC.csv"),row.names=F)
# rm(list=ls(pattern='^latestMCI'))

#spacer ####


#A summary figure of the distribution of latest grades for each NOF attribute
latestNtox <- NOFlatest%>%dplyr::filter(!Nitrate_Toxicity_Band%in%c("","NA"))%>%select(Nitrate_Toxicity_Band)

latestDRP <- NOFlatest%>%dplyr::filter(!DRP_Summary_Band%in%c("","NA")&!is.na(DRP_Summary_Band))%>%select(DRP_Summary_Band)

latestECOLI <- NOFlatest%>%dplyr::filter(!EcoliSummaryband%in%c("","NA")&!is.na(EcoliSummaryband))%>%select(EcoliSummaryband)
latestECOLIwo95 <- NOFlatest%>%dplyr::filter(!EcoliSummarybandwo95%in%c("","NA")&!is.na(EcoliSummarybandwo95))%>%select(EcoliSummarybandwo95)

latestSED <- NOFlatest%>%dplyr::filter(!SusSedBand%in%c("","NA")&!is.na(SusSedBand))%>%select(SusSedBand)

latestNH4 <- NOFlatest%>%dplyr::filter(!Ammonia_Toxicity_Band%in%c("","NA")&!is.na(Ammonia_Toxicity_Band))%>%select(Ammonia_Toxicity_Band)

latestMCI <- MCINOF%>%dplyr::filter(sYear=="2020")%>%select(MCIband)

latestNOFgrades <- as.matrix(bind_rows(
  table(droplevels(latestMCI)),
  table(droplevels(latestNH4)),
  table(droplevels(latestNtox)),
  table(droplevels(latestDRP)),
  table(droplevels(latestSED)),
  table(droplevels(latestECOLI))))

sapply(1:6,function(ri){
  latestNOFgrades[ri,is.na(latestNOFgrades[ri,])] <<- 0
  })
row.names(latestNOFgrades) <- c("MCI","NH4tox","Ntox","DRP","Clar","ECOLI")

spineplot(latestNOFgrades,col=NOFPal5)
labelAreas(latestNOFgrades,invert=T)

latestNOFprops = apply(latestNOFgrades,1,function(r)r/sum(r)*100)[5:1,]


write.csv(latestNOFgrades,'c:/users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/SWQNationalPicture/DataFiles/NOFGradesSummary.csv')

tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/",
            format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFGradesSummary.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()

barplot(latestNOFprops,col=NOFPal5,
        main='Latest NPSFM grades',
        ylab='Percentage of graded sites',
        family='source',cex.lab=4,cex.axis=4,cex.main=4,cex=4)
par(xpd=T)
text(c(0.7,1.9,3.1,4.3,5.5,6.7),
     rep(104,5),
     rowSums(latestNOFgrades),cex=3,family='source')
par(xpd=NA)
for(i in 1:6){
  thesens = which(latestNOFgrades[i,]>0)
  rowpos=cumsum(rev(latestNOFprops[6-thesens,i]))
  rowpos=apply(cbind(c(0,rowpos[1:(length(rowpos)-1)]),rowpos),1,mean)
  text(rep(1.2*i-0.5,length(thesens)),
       rowpos,
       rev(latestNOFgrades[i,thesens]),cex=2.5,family='source')
  }
if(names(dev.cur())=='tiff'){dev.off()}

