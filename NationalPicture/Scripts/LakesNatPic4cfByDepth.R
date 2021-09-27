

#Compare TLI by depth  ####
copyThrough=T

#89 shallow and 38 deep

barTLI = cbind(tabulate(bin=factor(TLIlatest$TLIBand,levels=rev(c('A','B','C','D','E'))),5)/sum(!is.na(TLIlatest$TLIBand)),
               tabulate(bin = factor(TLIlatest$TLIBand[TLIlatest$depthBand=="Shallow"],levels=rev(c('A','B','C','D','E'))),5)/
                 sum(!is.na(TLIlatest$TLIBand[TLIlatest$depthBand=="Shallow"])),
               tabulate(bin = factor(TLIlatest$TLIBand[TLIlatest$depthBand=="Deep"],levels=rev(c('A','B','C','D','E'))),5)/
                 sum(!is.na(TLIlatest$TLIBand[TLIlatest$depthBand=="Deep"])))



write.csv(data.frame(barTLI)%>%dplyr::rename("all127"=X1,"shallow89"=X2,"deep38"=X3),
          paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/DataFiles/LakesTLIDepthPropns.csv"),
          row.names=c("Really bad","bad","moderate","great","really great"))



tiff(paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/Plots/LakesTLIbyDepth.tif"),
     width = 12,height=8,units='in',res=300,compression='lzw',type='cairo')
par(mgp=c(4,3,0),#family='source',
    mar=c(5,6,4,2))
layout(matrix(c(1,1,1,2,2,
                1,1,1,2,2,
                1,1,1,2,2),nrow=3,byrow=T))
showtext_auto()
iT=barplot(barTLI,main="",
           col=NOFPal5,axes=F,
           ylab="",border = NA,cex.lab=6,col.axis=LAWAPalette[3],
           cex.names=ifelse(names(dev.cur())=='tiff',6,1))
axis(side = 1,at=iT,labels = c(expression(atop(All~Depths,'(127 sites)')),
                               expression(atop(shallow*' < 20m','(89 sites)')),
                               expression(atop(deep*" > 20m",'(38 sites)'))),
     lty=0,line = 1.25,col.axis=LAWAPalette[3],cex.axis=ifelse(names(dev.cur())=='tiff',6,1))
title(main="Latest TLI scores of monitored lakes (2020) by altitude",col.main=LAWAPalette[3],
      cex.main=ifelse(names(dev.cur())=='tiff',8,1.5))
par(mgp=c(4,1.5,0))
axis(side = 2,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),
     las=2,col.axis=LAWAPalette[3],col=LAWAPalette[3],cex.axis=ifelse(names(dev.cur())=='tiff',6,1))
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border='#CCCCCC',lwd=NULL)
with(TLIlatest%>%filter(TLI!="NA"),points(Long,Lat,col=c('#868686','#000000')[as.numeric(MaxDepth<=shallowDeep)+1],pch=16,cex=1))
# showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
rm(iT)



barTLI = cbind(tabulate(bin = factor(TLIlatest$TLIBand[TLIlatest$depthBand=="Shallow"],
                                     levels=rev(c('A','B','C','D','E'))),5)/
                 sum(!is.na(TLIlatest$TLIBand[TLIlatest$depthBand=="Shallow"])),
               tabulate(bin = factor(TLIlatest$TLIBand[TLIlatest$depthBand=="Deep"],
                                     levels=rev(c('A','B','C','D','E'))),5)/
                 sum(!is.na(TLIlatest$TLIBand[TLIlatest$depthBand=="Deep"])))


tiff(paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",
            format(Sys.Date(),"%Y-%m-%d"),"/Plots/LakesTLIbyDepth2band.tif"),
     width = 12,height=8,units='in',res=300,compression='lzw',type='cairo')
par(mgp=c(4,3,0),#family='source',
    mar=c(5,6,4,2))
layout(matrix(c(1,1,1,2,2,
                1,1,1,2,2,
                1,1,1,2,2),nrow=3,byrow=T))
# showtext_auto()
iT=barplot(barTLI,main="",
           col=NOFPal5,axes=F,
           ylab="",border = NA,cex.lab=6,col.axis=LAWAPalette[3],
           cex.names=ifelse(names(dev.cur())=='tiff',6,1))
axis(side = 1,at=iT,labels = c(expression(atop(shallow*' < 20m','(89 sites)')),
                               expression(atop(deep*" > 20m",'(38 sites)'))),
     lty=0,line = 1.25,col.axis=LAWAPalette[3],cex.axis=ifelse(names(dev.cur())=='tiff',6,1))
title(main="Latest TLI scores of monitored lakes (2020) by altitude",col.main=LAWAPalette[3],
      cex.main=ifelse(names(dev.cur())=='tiff',8,1.5))
par(mgp=c(4,1.5,0))
axis(side = 2,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),
     las=2,col.axis=LAWAPalette[3],col=LAWAPalette[3],cex.axis=ifelse(names(dev.cur())=='tiff',6,1))
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border='#CCCCCC',lwd=NULL)
with(TLIlatest%>%filter(TLI!="NA"),points(Long,Lat,col=c('#868686','#000000')[as.numeric(MaxDepth<=shallowDeep)+1],pch=16,cex=1))
if(names(dev.cur())=='tiff'){dev.off()}
rm(iT)



TLIlatest%>%dplyr::filter(TLI!='NA')%>%
  dplyr::transmute(Long,Lat,uplow=factor((MaxDepth<=shallowDeep),levels=c("TRUE","FALSE"),labels=c("shallow","deep")))%>%
  write.csv(paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/DataFiles/LakeLocnDepth.csv"),row.names=F)


tiff(paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/Plots/LakesTLIbyDepthwCounts.tif"),
     width = 12,height=8,units='in',res=300,compression='lzw',type='cairo')
par(mgp=c(4,3,0),#family='source',
    cex.lab=6,cex.main=8,mar=c(5,6,4,2))
layout(matrix(c(1,1,1,2,2,
                1,1,1,2,2,
                1,1,1,2,2),nrow=3,byrow=T))
showtext_auto()
#spineplot
iT=plot(TLIlatest$depthBand,
        TLIlatest$TLIBand,ylevels=rev(c("E","D","C","B","A")),
        main="Latest TLI scores of monitored lakes (2020) by altitude",
        col=NOFPal5,axes=F,xlab='',
        ylab="",border = NA)
axis(side=1,at=c(0.3,0.8),c(expression(atop(shallow*' < 20m','(89 sites)')),
                            expression(atop(deep*" > 20m",'(38 sites)'))),lty=0,cex.axis =6)
par(mgp=c(4,1.5,0))
axis(side = 2,at = seq(0,1,by=0.2),
     labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],cex.axis=ifelse(names(dev.cur())=='tiff',6,1),line=0)
par(xpd=T)
labelAreas(iT,textcex=6,invert=T)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border='#CCCCCC',lwd=NULL)
with(TLIlatest%>%filter(TLI!="NA"),points(Long,Lat,col=c('#868686','#000000')[as.numeric(MaxDepth<=shallowDeep)+1],pch=16,cex=1))
if(names(dev.cur())=='tiff'){dev.off()}
rm(iT)


if(copyThrough){
  file.copy(from = paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/Plots/LakesTLIbyDepth.tif"),
            to = "C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/LakesNationalPicture/Plots/",
            overwrite=T)
  file.copy(from = paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/Plots/LakesTLIbyDepthwCounts.tif"),
            to = "C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/LakesNationalPicture/Plots/",
            overwrite=T)
}

