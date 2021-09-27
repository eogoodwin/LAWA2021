
#And teh plots created from the data exported abobve ####

# plotLatest=NOFlatest%>%
#   select(LawaSiteID,
#          Long,Lat,
#          Nitrogen=NitrogenMed_Band,
#          Phosphorus=PhosphorusMed_Band,
#          Ammonia=Ammonia_Toxicity_Band,
#          Clarity=ClarityMedian_Band,
#          ChlA=ChlASummaryBand,
#          TLI=TLIBand,
#          Elevation,MaxDepth)
# 
# barNOF=apply(plotLatest%>%select(Nitrogen:ChlA),2,FUN=function(c){
#   barTab=tabulate(bin = factor(c,levels=rev(c('A','B','C','D','E'))),5)
#   print(sum(barTab))
#   barTab = barTab/sum(barTab)
# })
# NOFCounts <- apply(plotLatest,2,function(c)sum(!c%in%c("NA","<NA>")))

#TLI Plot, latest grades ####
# barTLI=apply(TLIlatest%>%select(TLIBand),2,FUN=function(c){
# # barTLI=apply(plotLatest%>%select(TLI),2,FUN=function(c){
#   barTab=tabulate(bin = factor(c,levels=rev(c('A','B','C','D','E'))),5)
#   print(sum(barTab))
#   barTab = barTab/sum(barTab)
# })
# TLICounts <- sum(!is.na(TLIlatest%>%select(TLIBand)%>%unlist))
# TLICounts <- sum(!is.na(plotLatest%>%select(TLI)%>%unlist))

copyThrough=T

tiff(paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFLakeLocations.tif"),
     width = 6,height=7.5,units='in',res=300,compression='lzw',type='cairo')
par(mar=c(1,1,1,1))
plot(nzmap,col=OLPalette[1],border=NA,
     xaxt='n',yaxt='n',xlab='',ylab='')#,xlim=c(166.75,178.25),ylim=c(-47.81645,-33.60308))
points(plotLatest$Long,plotLatest$Lat,asp=1,col='black',pch=16,cex=0.75)
if(names(dev.cur())=='tiff'){dev.off()}


if(copyThrough){
file.copy(from = paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFLakeLocations.tif"),
          to = "C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/LakesNationalPicture/Plots/",
          overwrite=T)
}



tiff(paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFLakes.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),xpd=T,mar=c(5,6,4,2),col.main=LAWAPalette[2],
    col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],#family='source',
    mgp=c(4,2,0))
layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
# showtext_auto()
barplot(as.matrix(barNOF[-1,-TLIcol]),main="LAWA National Lake Water Quality NOF Band Scores", 
        col=NOFPal4,axes=F,
        ylab="",border = NA,cex.lab=4,cex.main=5,cex.names=4,
        names.arg=sapply(c(bquote(atop(.(colnames(barNOF)[1]),
                                              '('*.(unname(NOFCounts[colnames(barNOF)[1]]))~'sites)')),
                    bquote(atop(.(colnames(barNOF)[2]),
                                              '('*.(unname(NOFCounts[colnames(barNOF)[2]]))~'sites)')),
                    bquote(atop(.(colnames(barNOF)[3]),
                                              '('*.(unname(NOFCounts[colnames(barNOF)[3]]))~'sites)')),
                    bquote(atop(.(colnames(barNOF)[4]),
                                              '('*.(unname(NOFCounts[colnames(barNOF)[4]]))~'sites)')),
                    bquote(atop(.(colnames(barNOF)[5]),
                                              '('*.(unname(NOFCounts[colnames(barNOF)[5]]))~'sites)'))),as.expression))
axis(side = 2,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],cex.axis=4)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
plotLatest%>%filter(NOFBand!="TLI")%>%
  select(Long,Lat)%>%points(pch=16,cex=0.75)
if(names(dev.cur())=='tiff'){dev.off()}


if(copyThrough){
  file.copy(from = paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/Plots/NOFLakes.tif"),
          to = "C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/LakesNationalPicture/Plots/",
          overwrite=T)
}

write.csv(barNOF[,TLIcol],
          paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/DataFiles/TLILakesLatest2019Proportions.csv"),
          row.names=c("really poor","poor","average","good","really good"))



tiff(paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/Plots/TLILakes.tif"),
     width = 12,height=8,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),xpd=T,mar=c(5,6,4,2),col.main=LAWAPalette[2],
    col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],#family='source',
    mgp=c(4,2,0))
layout(matrix(c(1,1,1,2,2,
                1,1,1,2,2,
                1,1,1,2,2),nrow=3,byrow=T))
# showtext_begin()
barplot(as.matrix(barNOF[,TLIcol]),main="",
        col=NOFPal5,axes=F,width=0.1125,xlim=c(0,1),space=6,
        ylab="",border = NA,cex.lab=4,cex.main=5,cex.names=4,
        names.arg=c(as.expression(bquote(atop(TLI,'('*.(NOFCounts[TLIcol])~'sites)')))))
par(xpd=T)
title(main = "Latest TLI grades of monitored lakes (2020)",
      adj = 1,cex.main=5,family='source',col=LAWAPalette[3])
par(xpd=F)
axis(side = 2,line = -23,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),
     las=2,col=LAWAPalette[3],cex.axis=4)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
plotLatest%>%filter(NOFBand=="TLI")%>%select(Long,Lat)%>%points(pch=16,cex=0.75)
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}

tiff(paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/Plots/TLILakeswCounts.tif"),
     width = 12,height=8,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),xpd=T,mar=c(5,6,4,2),col.main=LAWAPalette[2],
    col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],#family='source',
    mgp=c(4,2,0))
layout(matrix(c(1,1,1,2,2,
                1,1,1,2,2,
                1,1,1,2,2),nrow=3,byrow=T))
# showtext_begin()
iT <- barplot(as.matrix(barNOF[,TLIcol]),main="",col=NOFPal5,axes=F,width=0.1125,xlim=c(0,1),space=6,
              ylab="",border = NA,
              cex.lab=4,cex.main=5,cex.names=4,
              names.arg=c(as.expression(bquote(atop(TLI,'('*.(NOFCounts[TLIcol])~'sites)')))))
text(iT,
     apply(cbind(c(0,head(cumsum(barNOF[,TLIcol]),length(barNOF[,TLIcol])-1)),cumsum(barNOF[,TLIcol])),1,mean),
     round(barNOF[,TLIcol]*NOFCounts[TLIcol]),cex=4)
par(xpd=T)
title(main = "Latest TLI grades of monitored lakes (2020)",
      adj = 1,cex.main=5,family='source',col=LAWAPalette[3])
par(xpd=F)
axis(side = 2,line = -23,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],cex.axis=4)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
plotLatest%>%filter(NOFBand=="TLI")%>%select(Long,Lat)%>%points(pch=16,cex=0.75)
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}


tiff(paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/Plots/TLILakes_col.tif"),
     width = 12,height=8,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),xpd=T,mar=c(5,6,4,2),col.main=LAWAPalette[2],
    col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],#family='source',
    mgp=c(4,2,0))
layout(matrix(c(1,1,1,2,2,
                1,1,1,2,2,
                1,1,1,2,2),nrow=3,byrow=T))
# showtext_begin()
barplot(as.matrix(barNOF[,TLIcol]),main="",
        col=NOFPal5,axes=F,width=0.1125,xlim=c(0,1),space=6,
        ylab="",border = NA,cex.lab=4,cex.main=5,cex.names=4,
        names.arg=c(as.expression(bquote(atop(TLI,'('*.(NOFCounts[TLIcol])~'sites)')))))
par(xpd=T)
title(main = "Latest TLI grades of monitored lakes (2020)",
      adj = 1,cex.main=5,family='source',col=LAWAPalette[3])
par(xpd=F)
axis(side = 2,line = -23,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],cex.axis=4)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(plotLatest%>%filter(NOFBand=="TLI"),points(Long,Lat,col=(NOFPal5)[NOFGrade],pch=16,cex=1))
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}

if(copyThrough){
  file.copy(from = dir(path = paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",
                                     format(Sys.Date(),"%Y-%m-%d"),"/Plots/"),
                       pattern = "^TLILakes",full.names = T),
            to = "C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/LakesNationalPicture/Plots/",
            overwrite=T)
}




