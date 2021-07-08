#NOFPlots #### 
rm(list=ls())
library(tidyverse)
library(areaplot)
library(lubridate)
library(doBy)
library(showtext)

setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality/")
source("h:/ericg/16666LAWA/LAWA2021/Scripts/LAWAFunctions.R")
source("K:/R_functions/nztm2WGS.r")

dir.create(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)
lakeSiteTable=loadLatestSiteTableLakes()
library(rgdal)
nzmap <- readOGR('S:/New_S/Basemaps_Vector/NZ_Coastline/WGS_84/coast_wgs84.shp')


#Get elevation and distance to sea data for these lakes
FENZlake = read.csv('D:/RiverData/LakeData.txt')
wgs = nztm2wgs(ce = FENZlake$NZTME,cn = FENZlake$NZMTN)
FENZlake$lat=wgs[,1]
FENZlake$long=wgs[,2]
rm(wgs)

lakeSiteTable$Elevation = FENZlake$LakeElevat[match(lakeSiteTable$LFENZID,FENZlake$LID)]
lakeSiteTable$DistToSea = FENZlake$DirectDist[match(lakeSiteTable$LFENZID,FENZlake$LID)]
thisN = which(is.na(lakeSiteTable$Elevation))
dists = sqrt((lakeSiteTable$Long[thisN]-FENZlake$long)^2+(lakeSiteTable$Lat[thisN]-FENZlake$lat)^2)
thatN = which.min(dists)
lakeSiteTable$Elevation[thisN] = FENZlake$LakeElevat[thatN]
lakeSiteTable$DistToSea[thisN] = FENZlake$DirectDist[thatN]
rm(thisN,thatN,FENZlake)

#Represent each lake by its most mid-lake centre representative site
lakeSiteTable%>%
  group_by(LFENZID)%>%
  dplyr::summarise(Agency=unique(Agency),
                   nSite=length(unique(LawaSiteID)),
                   sites=paste0(SiteID,collapse=', '))%>%
  ungroup%>%
  arrange(desc(nSite))%>%
  filter(nSite>1)%>%
  filter(!grepl('cent|mid|open',sites))


#Get a list of siteNames from lakes that ahve multiple sites where one of them is a "mid", a "centre" or an "open water"
lakeTable <- lakeSiteTable%>%
  group_by(LFENZID)%>%
  dplyr::mutate(Agency=unique(Agency),
                   nSite=length(unique(LawaSiteID)),
                   sites=paste0(SiteID,collapse=', '))%>%
  ungroup%>%
  arrange(desc(nSite))%>%
  dplyr::filter(!(nSite>1&grepl('centre|center|mid|open',sites,ignore.case=T)&!grepl('centre|center|mid|open',SiteID,ignore.case=T)))%>%
                               #    These list the oens we dont want to keep
  dplyr::filter(!SiteID%in%c("lake rotorangi l1 (mangamingi)", "lake rotorangi l3 (dam)",# (7506)
                             "1880", "5632",# (34665)
                             "lake rotoiti at okawa bay (integrated)", "lake rotoiti at site 4 (integrated)",# (54730)
                             "lake rotorua at site 2 (integrated)",# (11133)
                             "lake manapouri at stony point top", "lake manapouri near frazers beach top",#(54735)
                             "lake george ne",#(28543)
                             "lake te anau at south fiord top",#(52566)
                             "lake forsyth at catons bay","lake forsyth at birdlings flat"#(47579)
  ))%>%select(-nSite,-sites)



NOFSummaryTable=read.csv(tail(dir(path = "h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/",pattern="NOFLakesOverall",
                                  recursive=T,full.names = T),1),stringsAsFactors = F)
TLI = read.csv(tail(dir(path = "h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/",
                        pattern="ITELakeTLI",recursive=T,full.names=T),1),stringsAsFactors = F)
TLIbyFENZ = read.csv(tail(dir(path = "h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/",
                              pattern="ITELakeTLIBYFENZ",recursive=T,full.names=T),1),stringsAsFactors = F)

NOFSummaryTable <- NOFSummaryTable%>%filter(!Year%in%c('2005to2009'))  #Just, you see this way we're left with a single decade
NOFSummaryTable <- NOFSummaryTable%>%filter(SiteID%in%unique(lakeTable$SiteID)) #And this way, we have only representative sites per lake

NOFSummaryTable <- merge(lakeTable%>%select(LawaSiteID,SiteID,CouncilSiteID,LFENZID,LType,GeomorphicLType,Region,Agency,Long,Lat,Elevation,DistToSea),all.x=F,
                         NOFSummaryTable%>%select(-CouncilSiteID,-Agency,-Region,-SiteID,
                                                  -AmmoniacalMed_Band,-AmmoniacalMax_Band,
                                                  -ChlAMed_Band,-ChlAMax_Band,-EcoliBand,
                                                  -Ecoli95_Band,-EcoliRecHealth540_Band,
                                                  -EcoliRecHealth260_Band),all.y=T)


NOFSummaryTable$NitrogenMed_Band=factor(NOFSummaryTable$NitrogenMed_Band,levels=c("D", "C", "B", "A", "NA"),labels=c("D", "C", "B", "A", "NA"))
NOFSummaryTable$NitrogenMed_Band[is.na(NOFSummaryTable$NitrogenMed_Band)] <- "NA"
NOFSummaryTable$PhosphorusMed_Band=factor(NOFSummaryTable$PhosphorusMed_Band,levels=c("D", "C", "B", "A", "NA"),labels=c("D", "C", "B", "A", "NA"))
NOFSummaryTable$PhosphorusMed_Band[is.na(NOFSummaryTable$PhosphorusMed_Band)] <- "NA"
NOFSummaryTable$Ammonia_Toxicity_Band=factor(NOFSummaryTable$Ammonia_Toxicity_Band,levels=c("D", "C", "B", "A", "NA"),labels=c("D", "C", "B", "A", "NA"))
NOFSummaryTable$Ammonia_Toxicity_Band[is.na(NOFSummaryTable$Ammonia_Toxicity_Band)] <- "NA"
NOFSummaryTable$ClarityMedian_Band=factor(NOFSummaryTable$ClarityMedian_Band,levels=c("D", "C", "B", "A", "NA"),labels=c("D", "C", "B", "A", "NA"))
NOFSummaryTable$ClarityMedian_Band[is.na(NOFSummaryTable$ClarityMedian_Band)] <- "NA"
NOFSummaryTable$ChlASummaryBand=factor(NOFSummaryTable$ChlASummaryBand,levels=c("D", "C", "B", "A", "NA"),labels=c("D", "C", "B", "A", "NA"))
NOFSummaryTable$ChlASummaryBand[is.na(NOFSummaryTable$ChlASummaryBand)] <- "NA"
NOFSummaryTable$EcoliSummaryBand=factor(NOFSummaryTable$EcoliSummaryBand,levels=c("E","D", "C", "B", "A", "NA"),labels=c("E","D", "C", "B", "A", "NA"))
NOFSummaryTable$EcoliSummaryBand[is.na(NOFSummaryTable$EcoliSummaryBand)] <- "NA"

#TLI calculated on one years data, joined to NOF attributes calculated on five years of data
#Match on teh latest last year of the five
NOFSummaryTable = left_join(x=NOFSummaryTable%>%mutate(TLIYear=strFrom(Year,'to')),
                            y=TLI%>%
                              select(-FENZID,-MixingPattern,-GeomorphicType)%>%
                              mutate(TLIYear=as.character(TLIYear)),by=c("SiteID"="Lake","TLIYear"))
NOFSummaryTable$TLIBand = cut(NOFSummaryTable$TLI,c(-1,2,3,4,5,700),labels = c("A","B","C","D","E"))

  

NOFlatest = droplevels(NOFSummaryTable%>%filter(Year=="2015to2019"))%>%drop_na(TLI)

if(0){
  #Export files to recreate these plots ####
  NitrLTypeexport <- NOFlatest%>%select(Region,NitrogenMed_Band)%>%filter(NitrogenMed_Band!="NA")%>%
    group_by(Region,LandCover)%>%dplyr::summarise(A=sum(NitrogenMed_Band=="A"),
                                                  B=sum(NitrogenMed_Band=="B"),
                                                  C=sum(NitrogenMed_Band=="C"),
                                                  D=sum(NitrogenMed_Band=="D"))%>%
    ungroup%>%pivot_longer(cols = A:D,names_to = 'Band',values_to = 'Count')%>%
    mutate(Indicator="Nitr")%>%select(Region,LandCover,Indicator,Band,Count)
  
  PhosLTypeexport <- NOFlatest%>%select(Region,PhosphorusMed_Band)%>%filter(PhosphorusMed_Band!="NA")%>%
    group_by(Region,LandCover)%>%dplyr::summarise(A=sum(PhosphorusMed_Band=="A"),
                                                  B=sum(PhosphorusMed_Band=="B"),
                                                  C=sum(PhosphorusMed_Band=="C"),
                                                  D=sum(PhosphorusMed_Band=="D"))%>%
    ungroup%>%pivot_longer(cols = A:D,names_to = 'Band',values_to = 'Count')%>%
    mutate(Indicator="Phos")%>%select(Region,LandCover,Indicator,Band,Count)
  
    AmmonLTypeexport <- NOFlatest%>%select(Region,Ammonia_Toxicity_Band)%>%filter(Ammonia_Toxicity_Band!="NA")%>%
    group_by(Region,LandCover)%>%dplyr::summarise(A=sum(Ammonia_Toxicity_Band=="A"),
                                                  B=sum(Ammonia_Toxicity_Band=="B"),
                                                  C=sum(Ammonia_Toxicity_Band=="C"),
                                                  D=sum(Ammonia_Toxicity_Band=="D"))%>%
    ungroup%>%pivot_longer(cols = A:D,names_to = 'Band',values_to = 'Count')%>%
    mutate(Indicator="Ammonia")%>%select(Region,LandCover,Indicator,Band,Count)
  
  ClarLTypeexport <- NOFlatest%>%select(Region,ClarityMedian_Band)%>%filter(ClarityMedian_Band!="NA")%>%
    group_by(Region,LandCover)%>%dplyr::summarise(A=sum(ClarityMedian_Band=="A"),
                                                  B=sum(ClarityMedian_Band=="B"),
                                                  C=sum(ClarityMedian_Band=="C"),
                                                  D=sum(ClarityMedian_Band=="D"))%>%
    ungroup%>%pivot_longer(cols=A:D,names_to = 'Band',values_to = 'Count')%>%
    mutate(Indicator="Clarity")%>%select(Region,LandCover,Indicator,Band,Count)
  
  ChlLTypeexport <- NOFlatest%>%select(Region,ChlASummaryBand)%>%filter(ChlASummaryBand!="NA")%>%
    group_by(Region,LandCover)%>%dplyr::summarise(A=sum(ChlASummaryBand=="A"),
                                                  B=sum(ChlASummaryBand=="B"),
                                                  C=sum(ChlASummaryBand=="C"),
                                                  D=sum(ChlASummaryBand=="D"))%>%
    ungroup%>%pivot_longer(cols=A:D,names_to = 'Band',values_to = 'Count')%>%
    mutate(Indicator="Chlorophyll")%>%select(Region,LandCover,Indicator,Band,Count)
  
  ECOLILTypeexport <- NOFlatest%>%select(Region,EcoliSummaryBand)%>%filter(EcoliSummaryBand!="NA")%>%
    group_by(Region,LandCover)%>%dplyr::summarise(A=sum(EcoliSummaryBand=="A"),
                                                  B=sum(EcoliSummaryBand=="B"),
                                                  C=sum(EcoliSummaryBand=="C"),
                                                  D=sum(EcoliSummaryBand=="D"),
                                                  E=sum(EcoliSummaryBand=="E"))%>%
    ungroup%>%pivot_longer(cols = A:E,names_to = 'Band',values_to = 'Count')%>%
    mutate(Indicator="ECOLI")%>%select(Region,LandCover,Indicator,Band,Count)
  
NOFLTypeexport <- do.call(rbind,list(NitrLTypeexport,PhosLTypeexport,AmmonLTypeexport,ClarLTypeexport,ChlLTypeexport,ECOLILTypeexport))

# rm(PhosLTypeexport,ECOLILTypeexport,AmmonLTypeexport,ClarLTypeexport)
write.csv(NOFLTypeexport,paste0('C:/users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/Lakes/',
                             'NOFResultsLatestByRegion.csv'),row.names=F)

#Now the time history one ####
NitrNOFs = NOFSummaryTable%>%drop_na(NitrogenMed_Band)%>%filter(NitrogenMed_Band!="NA")%>%
  select(-starts_with(c('Ecoli','Ammonia','Nitra'),ignore.case = T))%>%
  dplyr::rename(Band=NitrogenMed_Band,LakeType=LType)%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)),Year=strFrom(Year,'to'))%>%filter(nY==10)%>%ungroup%>%
  group_by(Region,Year)%>%dplyr::summarise(A=sum(Band=="A"),
                                                     B=sum(Band=="B"),
                                                     C=sum(Band=="C"),
                                                     D=sum(Band=="D"))%>%ungroup%>%
  pivot_longer(cols=A:D,names_to='Band',values_to='Count')%>%
  mutate(Indicator="Nitr")%>%
  dplyr::select(Region,Indicator,Year,Band,Count)

PhosNOFs = NOFSummaryTable%>%drop_na(PhosphorusMed_Band)%>%filter(PhosphorusMed_Band!="NA")%>%
  select(-starts_with(c('Ecoli','Ammonia','Nitra'),ignore.case = T))%>%
  dplyr::rename(Band=PhosphorusMed_Band,LakeType=LType)%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)),Year=strFrom(Year,'to'))%>%filter(nY==10)%>%ungroup%>%
  group_by(Region,Year)%>%dplyr::summarise(A=sum(Band=="A"),
                                                     B=sum(Band=="B"),
                                                     C=sum(Band=="C"),
                                                     D=sum(Band=="D"))%>%ungroup%>%
  pivot_longer(cols=A:D,names_to='Band',values_to='Count')%>%
  mutate(Indicator="Phos")%>%
  dplyr::select(Region,Indicator,Year,Band,Count)
NH4NOFs = NOFSummaryTable%>%drop_na(Ammonia_Toxicity_Band)%>%filter(Ammonia_Toxicity_Band!="NA")%>%
  select(-starts_with(c('Ecoli','Phos','Nitra'),ignore.case = T))%>%
  dplyr::rename(Band=Ammonia_Toxicity_Band,LakeType=LType)%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)),Year=strFrom(Year,'to'))%>%filter(nY==10)%>%ungroup%>%
  group_by(Region,Year)%>%dplyr::summarise(A=sum(Band=="A"),
                                                     B=sum(Band=="B"),
                                                     C=sum(Band=="C"),
                                                     D=sum(Band=="D"))%>%ungroup%>%
  pivot_longer(cols=A:D,names_to='Band',values_to='Count')%>%
  mutate(Indicator="Ammonia")%>%
  dplyr::select(Region,Indicator,Year,Band,Count)
ClarNOFs = NOFSummaryTable%>%drop_na(ClarityMedian_Band)%>%filter(ClarityMedian_Band!="NA")%>%
  select(-starts_with(c('Ecoli','Phos','Nitra'),ignore.case = T))%>%
  dplyr::rename(Band=ClarityMedian_Band,LakeType=LType)%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)),Year=strFrom(Year,'to'))%>%filter(nY==10)%>%ungroup%>%
  group_by(Region,Year)%>%dplyr::summarise(A=sum(Band=="A"),
                                                    B=sum(Band=="B"),
                                                    C=sum(Band=="C"),
                                                    D=sum(Band=="D"))%>%ungroup%>%
  pivot_longer(cols=A:D,names_to='Band',values_to='Count')%>%
  mutate(Indicator="Ammonia")%>%
  dplyr::select(Region,Indicator,Year,Band,Count)
ChlNOFs = NOFSummaryTable%>%drop_na(ChlASummaryBand)%>%filter(ChlASummaryBand!="NA")%>%
  select(-starts_with(c('Ecoli','Phos','Nitra'),ignore.case = T))%>%
  dplyr::rename(Band=ChlASummaryBand,LakeType=LType)%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)),Year=strFrom(Year,'to'))%>%filter(nY==10)%>%ungroup%>%
  group_by(Region,Year)%>%dplyr::summarise(A=sum(Band=="A"),
                                                    B=sum(Band=="B"),
                                                    C=sum(Band=="C"),
                                                    D=sum(Band=="D"))%>%ungroup%>%
  pivot_longer(cols=A:D,names_to='Band',values_to='Count')%>%
  mutate(Indicator="Ammonia")%>%
  dplyr::select(Region,Indicator,Year,Band,Count)


ECOLINOFs = NOFSummaryTable%>%drop_na(EcoliSummaryBand)%>%filter(EcoliSummaryBand!="NA")%>%
  select(-starts_with(c('Phos','Ammonia','Nitra'),ignore.case = T))%>%
  dplyr::rename(Band=EcoliSummaryBand,LakeType=LType)%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)),Year=strFrom(Year,'to'))%>%filter(nY==10)%>%ungroup%>%
  group_by(Region,Year)%>%dplyr::summarise(A=sum(Band=="A"),
                                                     B=sum(Band=="B"),
                                                     C=sum(Band=="C"),
                                                     D=sum(Band=="D"),
                                                     E=sum(Band=="E"))%>%ungroup%>%
  pivot_longer(cols=A:E,names_to='Band',values_to='Count')%>%
  mutate(Indicator="ECOLI")%>%
  dplyr::select(Region,Indicator,Year,Band,Count)

NOFHistoryExport <- do.call(rbind,list(NitrNOFs,PhosNOFs,NH4NOFs,ClarNOFs,ChlNOFs,ECOLINOFs))
write.csv(NOFHistoryExport,paste0('C:/users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/Lakes/',
                                  'NOFResultsHistoryCompleteSites.csv'),row.names=F)
rm(NitrNOFs,PhosNOFs,NH4NOFs,ClarNOFs,ChlNOFs,ECOLINOFs)
}


if(!'source'%in%font_families()){
  font_add_google("Source Sans Pro",family='source')
}

labelAreas <- function(areaTable,textcex=0.75){
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
    text(colPos[ll],rowPos,areaTable[ll,],cex=textcex)
  }
}



as.hexmode(c(242,242,242))
LAWAPalette=c("#95d3db","#74c0c1","#20a7ad","#007197",
              "#e85129","#ff8400","#ffa827","#85bb5b","#bdbcbc")
OLPalette=c('#cccccc','#f2f2f2')
plot(1:9,1:9,col=LAWAPalette,cex=5,pch=16)
points(c(6,4),c(3,7),col=OLPalette,cex=7.5,pch=16)


plotLatest=NOFlatest%>%
  select(LawaSiteID,
         Long,Lat,
         Nitrogen=NitrogenMed_Band,
         Phosphorus=PhosphorusMed_Band,
         Ammonia=Ammonia_Toxicity_Band,
         Clarity=ClarityMedian_Band,
         ChlA=ChlASummaryBand,
         TLI=TLIBand,Elevation,DistToSea)

plot(NOFlatest$TLIBand,(NOFlatest$Elevation)^(1/6))
plot(NOFlatest$TLI,(NOFlatest$Elevation),xlab='TLI value',ylab='Elevation, m',
     col=LAWAPalette[c(3,8,7,6,5)][as.numeric(NOFlatest$TLIBand)],pch=16)

for(TLIthresh in seq(1.6,
                     max(NOFlatest$TLI,na.rm=T),length.out = 100)){
  altroc=pROC::roc(response=NOFlatest$TLI>=TLIthresh,
                   predictor=NOFlatest$Elevation)
  highlow=pROC::coords(roc=altroc,'best')[1]$threshold[1]
  points(TLIthresh,highlow,col='red')  
}


plot(NOFlatest$TLI,(NOFlatest$Elevation))
altroc=pROC::roc(response=NOFlatest$TLI>=4.5,
                 predictor=NOFlatest$Elevation)
highlow=pROC::coords(roc=altroc,'best')[1]$threshold
rm(altroc)
abline(h=highlow,lty=2,lwd=2)
abline(v=4.5,lty=2,lwd=2)

stopifnot(abs(highlow-170)<5)






if(0){
  tiff(paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/NOFLakeLocations.tif"),
       width = 6,height=7.5,units='in',res=300,compression='lzw',type='cairo')
  par(mar=c(1,1,1,1))
  plot(nzmap,col=OLPalette[1],border=NA,
       xaxt='n',yaxt='n',xlab='',ylab='')#,xlim=c(166.75,178.25),ylim=c(-47.81645,-33.60308))
  points(plotLatest$Long,plotLatest$Lat,asp=1,col='black',pch=16,cex=0.75)
  
  if(names(dev.cur())=='tiff'){dev.off()}
  
  file.copy(from = paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/NOFLakeLocations.tif"),
            to = "C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/Lakes",
            overwrite=T)
  
  
  barNOF=apply(plotLatest%>%select(Nitrogen:ChlA),2,FUN=function(c){
    barTab=tabulate(bin = factor(c,levels=rev(c('A','B','C','D','E'))),5)
    # barTab=tabulate(bin = factor(c,levels=rev(c('A','B','C','D','E','NA'))),6)
    print(sum(barTab))
    barTab = barTab/sum(barTab)
  })
  
  
  measCounts <- apply(plotLatest,2,function(c)sum(!c%in%c("NA","<NA>")))
  
  tiff(paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/NOFLakes.tif"),
       width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
  par(mfrow=c(1,1),xpd=T,mar=c(5,6,4,2),col.main=LAWAPalette[2],
      col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],family='source',
      mgp=c(4,2,0))
  layout(matrix(c(1,1,2,
                  1,1,2,
                  1,1,2),nrow=3,byrow=T))
  showtext_auto()
  barplot(barNOF,main="LAWA National Lake Water Quality NOF Band Scores", 
          col=LAWAPalette[c(5,6,7,8,3)],axes=F,
          ylab="",border = NA,cex.lab=4,cex.main=5,cex.names=4,
          names.arg=c(as.expression(bquote(atop(.(colnames(barNOF)[1]),'('*.(unname(measCounts[colnames(barNOF)[1]]))~'sites)'))),
                      as.expression(bquote(atop(.(colnames(barNOF)[2]),'('*.(unname(measCounts[colnames(barNOF)[2]]))~'sites)'))),
                      as.expression(bquote(atop(.(colnames(barNOF)[3]),'('*.(unname(measCounts[colnames(barNOF)[3]]))~'sites)'))),
                      as.expression(bquote(atop(.(colnames(barNOF)[4]),'('*.(unname(measCounts[colnames(barNOF)[4]]))~'sites)'))),
                      as.expression(bquote(atop(.(colnames(barNOF)[5]),'('*.(unname(measCounts[colnames(barNOF)[5]]))~'sites)')))))
  # as.expression(bquote(atop(.(colnames(barNOF)[6]),'('*.(unname(measCounts[colnames(barNOF)[6]]))~'sites)')))))
  axis(side = 2,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],cex.axis=4)
  par(mar=c(1,0,0,0))
  plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
  plotLatest%>%select(-TLI)%>%mutate(allNA = Nitrogen=="NA"&Phosphorus=="NA"&Ammonia=="NA"&Clarity=="NA"&ChlA=="NA")%>%
    filter(!allNA)%>%select(Long,Lat)%>%points(pch=16,cex=0.75)
  # points(plotLatest$Long,plotLatest$Lat,pch=16,cex=0.75)
  if(names(dev.cur())=='tiff'){dev.off()}
  
  file.copy(from = paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/NOFLakes.tif"),
            to = "C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/Lakes",
            overwrite=T)
}



#TLI Plot, latest grades ####
barTLI=apply(plotLatest%>%select(TLI),2,FUN=function(c){
  barTab=tabulate(bin = factor(c,levels=rev(c('A','B','C','D','E'))),5)
  # barTab=tabulate(bin = factor(c,levels=rev(c('A','B','C','D','E','NA'))),6)
  print(sum(barTab))
  barTab = barTab/sum(barTab)
})

write.csv(barTLI,
            paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/TLILakesLatest2019Proportions.csv"),
          row.names=c("really poor","poor","average","good","really good"))

TLICounts <- sum(!is.na(plotLatest%>%select(TLI)%>%unlist))


tiff(paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/TLILakes.tif"),
     width = 12,height=8,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),xpd=T,mar=c(5,6,4,2),col.main=LAWAPalette[2],
    col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],family='source',
    mgp=c(4,2,0))
layout(matrix(c(1,1,1,2,2,
                1,1,1,2,2,
                1,1,1,2,2),nrow=3,byrow=T))
showtext_begin()
barplot(barTLI,main="",
        col=LAWAPalette[c(5,6,7,8,3)],axes=F,width=0.1125,xlim=c(0,1),space=6,
        ylab="",border = NA,cex.lab=4,cex.main=5,cex.names=4,
        names.arg=c(as.expression(bquote(atop(.(colnames(barTLI)[1]),'('*.(TLICounts)~'sites)')))))
par(xpd=T)
title(main = "Latest TLI grades of monitored lakes (2019)",
      adj = 1,cex.main=5,family='source',col=LAWAPalette[3])
par(xpd=F)
axis(side = 2,line = -23,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],cex.axis=4)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
plotLatest%>%filter(TLI!="NA")%>%select(Long,Lat)%>%points(pch=16,cex=0.75)
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}

tiff(paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/TLILakeswCounts.tif"),
     width = 12,height=8,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),xpd=T,mar=c(5,6,4,2),col.main=LAWAPalette[2],
    col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],family='source',
    mgp=c(4,2,0))
layout(matrix(c(1,1,1,2,2,
                1,1,1,2,2,
                1,1,1,2,2),nrow=3,byrow=T))
showtext_begin()
iT <- barplot(barTLI,main="",
        col=LAWAPalette[c(5,6,7,8,3)],axes=F,width=0.1125,xlim=c(0,1),space=6,
        ylab="",border = NA,cex.lab=4,cex.main=5,cex.names=4,
        names.arg=c(as.expression(bquote(atop(.(colnames(barTLI)[1]),'('*.(TLICounts)~'sites)')))))
text(iT,apply(cbind(c(0,head(cumsum(barTLI),length(barTLI)-1)),cumsum(barTLI)),1,mean),barTLI*TLICounts,cex=4)
par(xpd=T)
title(main = "Latest TLI grades of monitored lakes (2019)",
      adj = 1,cex.main=5,family='source',col=LAWAPalette[3])
par(xpd=F)
axis(side = 2,line = -23,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],cex.axis=4)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
plotLatest%>%filter(TLI!="NA")%>%select(Long,Lat)%>%points(pch=16,cex=0.75)
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}


tiff(paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/TLILakes_col.tif"),
     width = 12,height=8,units='in',res=300,compression='lzw',type='cairo')
par(mfrow=c(1,1),xpd=T,mar=c(5,6,4,2),col.main=LAWAPalette[2],
    col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],family='source',
    mgp=c(4,2,0))
layout(matrix(c(1,1,1,2,2,
                1,1,1,2,2,
                1,1,1,2,2),nrow=3,byrow=T))
showtext_begin()
barplot(barTLI,main="",
        col=LAWAPalette[c(5,6,7,8,3)],axes=F,width=0.1125,xlim=c(0,1),space=6,
        ylab="",border = NA,cex.lab=4,cex.main=5,cex.names=4,
        names.arg=c(as.expression(bquote(atop(.(colnames(barTLI)[1]),'('*.(TLICounts)~'sites)')))))
par(xpd=T)
title(main = "Latest TLI grades of monitored lakes (2019)",
      adj = 1,cex.main=5,family='source',col=LAWAPalette[3])
par(xpd=F)
axis(side = 2,line = -23,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],cex.axis=4)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
with(plotLatest%>%filter(TLI!="NA"),points(Long,Lat,col=rev(LAWAPalette[c(5,6,7,8,3)])[TLI],pch=16,cex=1))
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}

file.copy(from = dir(path = paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d")),
                     pattern = "^TLILakes",full.names = T),
          to = "C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/Lakes",
          overwrite=T)







#Compare TLI by elevation  ####



barTLI = cbind(tabulate(bin=factor(NOFlatest$TLIBand,levels=rev(c('A','B','C','D','E'))),5)/
                 sum(!is.na(NOFlatest$TLIBand)),
  tabulate(bin = factor(NOFlatest$TLIBand[NOFlatest$Elevation<=highlow],levels=rev(c('A','B','C','D','E'))),5)/
                 sum(!is.na(NOFlatest$TLIBand[NOFlatest$Elevation<=highlow])),
               tabulate(bin = factor(NOFlatest$TLIBand[NOFlatest$Elevation>highlow],levels=rev(c('A','B','C','D','E'))),5)/
                 sum(!is.na(NOFlatest$TLIBand[NOFlatest$Elevation>highlow])))

write.csv(data.frame(barTLI)%>%dplyr::rename("all124"=X1,"lowland77"=X2,"upland47"=X3),
          paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/LakesTLIElevationPropns.csv"),
          row.names=c("Really bad","bad","moderate","great","really great"))



tiff(paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/LakesTLIbyElevation.tif"),
     width = 12,height=8,units='in',res=300,compression='lzw',type='cairo')
par(mgp=c(4,3,0),family='source',mar=c(5,6,4,2))
layout(matrix(c(1,1,1,2,2,
                1,1,1,2,2,
                1,1,1,2,2),nrow=3,byrow=T))
showtext_begin()
iT=barplot(barTLI,main="",
        col=LAWAPalette[c(5,6,7,8,3)],axes=F,
        ylab="",border = NA,cex.lab=6,col.axis=LAWAPalette[3],
        cex.names=ifelse(names(dev.cur())=='tiff',6,1))
axis(side = 1,at=iT,labels = c(expression(atop(All~elevations,'(124 sites)')),
                    expression(atop(Lowland*' < 170m','(77 sites)')),
                    expression(atop(Upland*" > 170m",'(47 sites)'))),
     lty=0,line = 1.25,col.axis=LAWAPalette[3],cex.axis=ifelse(names(dev.cur())=='tiff',6,1))
title(main="Latest TLI scores of monitored lakes (2019) by altitude",col.main=LAWAPalette[3],
      cex.main=ifelse(names(dev.cur())=='tiff',8,1.5))
par(mgp=c(4,1.5,0))
axis(side = 2,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),
     las=2,col.axis=LAWAPalette[3],col=LAWAPalette[3],cex.axis=ifelse(names(dev.cur())=='tiff',6,1))
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border='#CCCCCC',lwd=NULL)
with(NOFlatest%>%filter(TLI!="NA"),points(Long,Lat,col=c('#868686','#000000')[as.numeric(Elevation<=highlow)+1],pch=16,cex=1))
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
rm(iT)



barTLI = cbind(tabulate(bin = factor(NOFlatest$TLIBand[NOFlatest$Elevation<=highlow],levels=rev(c('A','B','C','D','E'))),5)/
                 sum(!is.na(NOFlatest$TLIBand[NOFlatest$Elevation<=highlow])),
               tabulate(bin = factor(NOFlatest$TLIBand[NOFlatest$Elevation>highlow],levels=rev(c('A','B','C','D','E'))),5)/
                 sum(!is.na(NOFlatest$TLIBand[NOFlatest$Elevation>highlow])))


tiff(paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/L",
            format(Sys.Date(),"%Y-%m-%d"),
            "/LakesTLIbyElevation2band.tif"),
     width = 12,height=8,units='in',res=300,compression='lzw',type='cairo')
par(mgp=c(4,3,0),family='source',mar=c(5,6,4,2))
layout(matrix(c(1,1,1,2,2,
                1,1,1,2,2,
                1,1,1,2,2),nrow=3,byrow=T))
showtext_begin()
iT=barplot(barTLI,main="",
           col=LAWAPalette[c(5,6,7,8,3)],axes=F,
           ylab="",border = NA,cex.lab=6,col.axis=LAWAPalette[3],
           cex.names=ifelse(names(dev.cur())=='tiff',6,1))
axis(side = 1,at=iT,labels = c(expression(atop(Lowland*' < 170m','(77 sites)')),
                               expression(atop(Upland*" > 170m",'(47 sites)'))),
     lty=0,line = 1.25,col.axis=LAWAPalette[3],cex.axis=ifelse(names(dev.cur())=='tiff',6,1))
title(main="Latest TLI scores of monitored lakes (2019) by altitude",col.main=LAWAPalette[3],
      cex.main=ifelse(names(dev.cur())=='tiff',8,1.5))
par(mgp=c(4,1.5,0))
axis(side = 2,at = seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),
     las=2,col.axis=LAWAPalette[3],col=LAWAPalette[3],cex.axis=ifelse(names(dev.cur())=='tiff',6,1))
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border='#CCCCCC',lwd=NULL)
with(NOFlatest%>%filter(TLI!="NA"),points(Long,Lat,col=c('#868686','#000000')[as.numeric(Elevation<=highlow)+1],pch=16,cex=1))
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
rm(iT)



NOFlatest%>%dplyr::filter(TLI!='NA')%>%
  dplyr::transmute(Long,Lat,uplow=factor((Elevation<=highlow),levels=c("TRUE","FALSE"),labels=c("lowland","upland")))%>%
write.csv(paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/LakeLocnElevation.csv"),row.names=F)


tiff(paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/LakesTLIbyElevationwCounts.tif"),
     width = 12,height=8,units='in',res=300,compression='lzw',type='cairo')
par(mgp=c(4,3,0),family='source',cex.lab=6,cex.main=8,mar=c(5,6,4,2))
layout(matrix(c(1,1,1,2,2,
                1,1,1,2,2,
                1,1,1,2,2),nrow=3,byrow=T))
showtext_begin()
#spineplot
iT=plot(factor(NOFlatest$Elevation<=highlow,levels=c("TRUE","FALSE"),labels=c("Lowland","Upland")),
        NOFlatest$TLIBand,ylevels=c("E","D","C","B","A"),
        main="Latest TLI scores of monitored lakes (2019) by altitude",
           col=LAWAPalette[c(5,6,7,8,3)],axes=F,xlab='',
           ylab="",border = NA)
axis(side=1,at=c(0.3,0.8),c(expression(atop(Lowland*' < 170m','(77 sites)')),
                                expression(atop(Upland*" > 170m",'(47 sites)'))),lty=0,cex.axis =6)
par(mgp=c(4,1.5,0))
axis(side = 2,at = seq(0,1,by=0.2),
     labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],cex.axis=ifelse(names(dev.cur())=='tiff',6,1),line=0)
labelAreas(iT,textcex=6)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border='#CCCCCC',lwd=NULL)
with(NOFlatest%>%filter(TLI!="NA"),points(Long,Lat,col=c('#868686','#000000')[as.numeric(Elevation<=highlow)+1],pch=16,cex=1))
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
rm(iT)

file.copy(from = paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/LakesTLIbyElevation.tif"),
          to = "C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/Lakes",
          overwrite=T)
file.copy(from = paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/LakesTLIbyElevationwCounts.tif"),
          to = "C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/Lakes",
          overwrite=T)



#Not enough complete sites to do these state over time plots

#Changes in NOF with complete sites ####
# nitrgoen phosphorus ammonia clairty chlorophyll Ecoli
xlabpos=(1:10)*0.12-0.07

# TLITrend  ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/TLITrend.tif"),
     width = 12,height=8,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
TLINOFs = NOFSummaryTable%>%filter(TLIBand!="NA")%>%drop_na(TLIBand)%>%
  select(-starts_with(c('ecoli','nitrate','phos','ammon','Clar','chl'),ignore.case=T))%>%
  select(-ends_with(c("note","type")))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSites=length(unique(TLINOFs$LawaSiteID))

layout(matrix(c(1,1,1,2,2,
                1,1,1,2,2,
                1,1,1,2,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,2),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5,xpd=F)
iT=plot(factor(TLINOFs$Year),TLINOFs$TLIBand,ylevels=c("E","D","C","B","A"),
        main='',ylab='',xlab='',col=LAWAPalette[c(5,6,7,8,3)],tol.ylab=0,
        axes=F,border = NA)
title(main="TLI scores of monitored lakes over time (2010 - 2019)",
        col=LAWAPalette[3])
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = levels(factor(strFrom(TLINOFs$Year,'to'))),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSites," continuously-monitored lakes."),line = 0.5,col=LAWAPalette[3],cex=3)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
points(TLINOFs$Long,TLINOFs$Lat,pch=16,cex=0.25,col='black')
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
iT=rbind(iT,c("Total","of",nSites,"sites"))
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/TLITrend.csv"),row.names=T)
rm(TLINOFs,nSites,iT)

tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/TLITrendwCounts.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
TLINOFs = NOFSummaryTable%>%filter(TLIBand!="NA")%>%drop_na(TLIBand)%>%
  select(-starts_with(c('ecoli','nitrate','phos','ammon','Clar','chl'),ignore.case=T))%>%
  select(-ends_with(c("note","type")))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSites=length(unique(TLINOFs$LawaSiteID))

layout(matrix(c(1,1,1,2,2,
                1,1,1,2,2,
                1,1,1,2,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,2),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5,xpd=F)
iT=plot(factor(TLINOFs$Year),TLINOFs$TLIBand,ylevels=c("E","D","C","B","A"),
        ylab='',xlab='',
        col=LAWAPalette[c(5,6,7,8,3)],tol.ylab=0,main="",
        axes=F,border = NA)
title(main="TLI scores of monitored lakes over time (2010 - 2019)",
      col=LAWAPalette[3])
labelAreas(iT)
axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
axis(1,at=xlabpos,labels = levels(factor(strFrom(TLINOFs$Year,'to'))),col=LAWAPalette[3],cex.axis=1.25)
mtext(side = 3,paste0("Showing only the ",nSites," continuously-monitored lakes."),line = 0.5,col=LAWAPalette[3],cex=2.5)
par(mar=c(1,0,0,0))
plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
points(TLINOFs$Long,TLINOFs$Lat,pch=16,cex=0.25,col='black')
showtext_end()
if(names(dev.cur())=='tiff'){dev.off()}
rm(TLINOFs,nSites,iT)

sapply(dir(path=paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d")),
           pattern="TLITrend",full.names=T),
       FUN=function(f){
         file.copy(from = f,to = "C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/Lakes",
                   overwrite=T)})


if(0){
# NOFTrendTNSum ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrendTNSum.tif"),
     width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
showtext_begin()
TNNOFs = NOFSummaryTable%>%drop_na(NitrogenMed_Band)%>%filter(NitrogenMed_Band!="NA")%>%
  select(-starts_with(c('Ecoli','Ammonia','Clarity','ChlA','Phos','TLI'),ignore.case = T))%>%
  select(-ends_with(c('Note','Type'),ignore.case=T))%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY==10)%>%ungroup%>%select(-nY)
nSites=length(unique(TNNOFs$LawaSiteID))
layout(matrix(c(1,1,2,
                1,1,2,
                1,1,2),nrow=3,byrow=T))
par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
    cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
iT=plot(factor(strFrom(TNNOFs$Year,'to')),(TNNOFs$NitrogenMed_Band),ylab='',xlab='',
        col=LAWAPalette[c(5,7,8,3)],tol.ylab=0,main="TN Summary",
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
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrendTN.csv"),row.names=T)
rm(TNNOFs,nSites,iT)

sapply(dir(path=paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d")),
           pattern="NOFTrendTN",full.names=T),
       FUN=function(f){
         file.copy(from = f,to = "C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/Lakes",
                   overwrite=T)})


# NOFTrendTPSum ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrendTPSum.tif"),
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
iT=plot(factor(strFrom(TPNOFs$Year,'to')),(TPNOFs$PhosphorusMed_Band),ylab='',xlab='',
        col=LAWAPalette[c(5,7,8,3)],tol.ylab=0,main="TP Summary",
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
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrendTP.csv"),row.names=T)
rm(TPNOFs,nSites,iT)

sapply(dir(path=paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d")),
           pattern="NOFTrendTP",full.names=T),
       FUN=function(f){
         file.copy(from = f,to = "C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/Lakes",
                   overwrite=T)})



# NOFTrendAmmoniacalTox  ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrendAmmoniacalTox.tif"),
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
iT=plot(factor(strFrom(ammonNOFs$Year,'to')),(ammonNOFs$Ammonia_Toxicity_Band),ylab='',xlab='',
        col=LAWAPalette[c(5,7,8,3)],tol.ylab=0,main="Ammonia toxicity",
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
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrendAmmonTox.csv"),row.names=T)
rm(ammonNOFs,nSites,iT)

sapply(dir(path=paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d")),
           pattern="NOFTrendAmmon",full.names=T),
       FUN=function(f){
         file.copy(from = f,to = "C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/Lakes",
                   overwrite=T)})


# NOFTrendClarity  ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrendClarity.tif"),
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
iT=plot(factor(clarNOFs$Year),clarNOFs$ClarityMedian_Band,ylab='',xlab='',
        col=LAWAPalette[c(5,7,8,3)],tol.ylab=0,main="Clarity",
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
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrendClarity.csv"),row.names=T)
rm(clarNOFs,nSites,iT)

sapply(dir(path=paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d")),
           pattern="NOFTrendCla",full.names=T),
       FUN=function(f){
         file.copy(from = f,to = "C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/Lakes",
                   overwrite=T)})



# NOFTrendChlA  ####
tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrendChlA.tif"),
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
iT=plot(factor(chlNOFs$Year),chlNOFs$ChlASummaryBand,ylab='',xlab='',
        col=LAWAPalette[c(5,7,8,3)],tol.ylab=0,main="Chlorophyll A",
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
write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrendChlA.csv"),row.names=T)
rm(chlNOFs,nSites,iT)

sapply(dir(path=paste0("H:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d")),
           pattern="NOFTrendChl",full.names=T),
       FUN=function(f){
         file.copy(from = f,to = "C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/Lakes",
                   overwrite=T)})




}

# 
# 
# # NOFTrendEcoliSummary ####
# tiff(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrendEcoliSummary.tif"),
#      width = 10,height=6,units='in',res=300,compression='lzw',type='cairo')
# showtext_begin()
# ecoliNOFs = NOFSummaryTable%>%filter(EcoliSummaryBand!="NA")%>%drop_na(EcoliSummaryBand)%>%
#   select(-starts_with(c('ammon','nitrate'),ignore.case=T))%>%
#   group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)))%>%filter(nY>=7)%>%ungroup%>%select(-nY)
# nSites=length(unique(ecoliNOFs$LawaSiteID))
# 
# layout(matrix(c(1,1,2,
#                 1,1,2,
#                 1,1,2),nrow=3,byrow=T))
# par(col.main=LAWAPalette[2],col.axis=LAWAPalette[3],col.lab=LAWAPalette[3],mar=c(4,3,3,1),mgp=c(1.75,0.5,0),
#     cex=2,cex.main=2,cex.lab=1.5,cex.axis=1.5)
# iT=plot(factor(strFrom(ecoliNOFs$Year,'to')),droplevels(ecoliNOFs$EcoliSummaryBand),ylab='',xlab='',
#         col=LAWAPalette[c(5,6,7,8,3)],tol.ylab=0,main=expression(italic(E.~coli)~Summary),
#         axes=F,border = NA)
# axis(2,at=seq(0,1,by=0.2),labels = paste(seq(0,1,by=0.2)*100,'%'),las=2,col=LAWAPalette[3],tck=-0.025)
# axis(1,at=xlabpos[-1],labels = levels(factor(strFrom(ecoliNOFs$Year,'to'))),col=LAWAPalette[3],cex.axis=1.25)
# mtext(side = 3,paste0("Showing only the ",nSites,"  sites with 7 or more results."),line = 0.5,col=LAWAPalette[3],cex=2.5)
# par(mar=c(1,0,0,0))
# plot(nzmap,col=OLPalette[1],border=OLPalette[1],lwd=NULL)
# points(ecoliNOFs$Long,ecoliNOFs$Lat,pch=16,cex=0.25,col='black')
# showtext_end()
# if(names(dev.cur())=='tiff'){dev.off()}
# iT=rbind(iT,c("Total","of",nSites,"sites",""))
# write.csv(iT,paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d"),"/NOFTrendEColi.csv"),row.names=T)
# rm(ecoliNOFs,nSites,iT)

stdise=function(x){
  (x-min(x))/max(x-min(x))
}

TLIb <- TLI%>%filter(TLIYear>2009)%>%
  group_by(Lake)%>%dplyr::mutate(n=n())%>%ungroup%>%filter(n==10)%>%select(-n)



plot(0,0,type='n',xlim=c(2010,2021),ylim=c(0,10),xlab='Year',ylab='TLI',main="TLI change over time\n114 complete lake sites")
Modes = rep(0,10)
for(yy in 2010:2019){
  ydens = TLIb%>%filter(TLIYear==yy)%>%select(TLI)%>%unlist%>%density
  ydens$y=stdise(ydens$y) 
  lines(yy+ydens$y,ydens$x)
  Modes[yy-2009]=ydens$x[which.max(ydens$y)]
}
lines(2011:2021,by(data = TLIb$TLI,INDICES = TLIb$TLIYear,FUN=median),col='blue',lwd=2)
lines(2011:2021,Modes,col='red',lwd=2)


#Copy fioles to sharepoint ####
file.copy(from = dir(path=paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/L",format(Sys.Date(),"%Y-%m-%d")),
                     pattern = "tif$|csv$",full.names = T),
          to = "C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Refresh 2021_NationalPicture/Lakes",
          overwrite = T,copy.date = T)
