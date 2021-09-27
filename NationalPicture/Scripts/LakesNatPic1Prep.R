#NOFPlots #### 

#At some point, 2020, it was decided to look at only TLI for the national picture.
#This was achieved by simply commenting out the non-TLI aspects of this lakes national picture script.


# Hi Eric,  
# 
# The lakes project team had a meeting to discuss the figures that you had generated so far for the national picture. We have decided that we are likely going to present the same as last year (overall TLI for nation, TLI split by elevation, and TLI changes over time), therefore not present the NOF bands or NOF band changes over time. However, if it is easy to generate the NOF bands graphs, then we welcome seeing them for our understanding of numbers. 
# 
# There are a couple of things I wanted to clarify with you: 
#   
#   •	TLI state – this year says 134 lakes (last year was 124)– check that we are not including multiple sites per lake, and instead are just having one site represented for the lake.   ##   sorted by correcting capitalisation EG 27/8

# •	TLI split by elevation – error with the high vs. lowland on some graphs being the wrong way around (e.g. lowland lakes having greater proportion of sites with better TLI).     ## addressed by making elevationBnad factor at the start

# •	TLI over time – stick to 10 years (2011-2020) - check multiple sites are not used, and one site used per lake.
                                                          ##done
# •	It is useful to have both versions of the graph, with and without the counts on them – counts are helpful for us to write the story, but final graph will not have the count. 
                                                        ##done
# •	Checking the numbers are correct for 2020 – some of the titles are incorrect so unsure we are accurately presenting the correct years etc.                            ##edone
# •	If running NOF band graphs – remove clarity as it is not a NOF attribute. 
# •	If running NOF band graphs – make sure the same lakes are for all attributes, otherwise not showing the same pool of samples across attributes which can be misleading. 
# 
# If you could also the analysis of % of lakes for TLI split by lake depth (20m threshold), that would be excellent! 
#   
#   Please give me a shout if there is anything I can clarify – the lakes team have our next meeting on Tues so be good to have a look through figures then. 
# 
# Thanks heaps. 
# 
# Cheers, 
# Jane
# 


rm(list=ls())
library(tidyverse)
library(areaplot)
library(lubridate)
library(doBy)
library(showtext)

setwd("H:/ericg/16666LAWA/LAWA2021/WaterQuality/")
source("h:/ericg/16666LAWA/LAWA2021/Scripts/LAWAFunctions.R")
source("K:/R_functions/nztm2WGS.r")
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
      text(colPos[ll],rowPosareaTable[ll,]>0,areaTable[ll,areaTable[ll,]>0],cex=textcex)
    }
  }
}



as.hexmode(c(242,242,242))
LAWAPalette=c("#95d3db","#74c0c1","#20a7ad","#007197",
              "#e85129","#ff8400","#ffa827","#85bb5b","#bdbcbc")
OLPalette=c('#cccccc','#f2f2f2')
NOFPal4 = LAWAPalette[c(5,7,8,3)]
NOFPal5 = LAWAPalette[c(5,6,7,8,3)]
AtoE = c("A","B","C","D","E","NA")
AtoD = c("A","B","C","D","NA")

plot(1:9,1:9,col=LAWAPalette,cex=5,pch=16)
points(c(6,4),c(3,7),col=OLPalette,cex=7.5,pch=16)

dir.create(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/DataFiles"),showWarnings = F,recursive = T)
dir.create(paste0("h:/ericg/16666LAWA/LAWA2021/NationalPicture/Lakes/L",format(Sys.Date(),"%Y-%m-%d"),"/Plots"),showWarnings = F,recursive = T)
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
lakeSiteTable$MaxDepth = FENZlake$MaxDepth[match(lakeSiteTable$LFENZID,FENZlake$LID)]
lakeSiteTable$DistToSea = FENZlake$DirectDist[match(lakeSiteTable$LFENZID,FENZlake$LID)]
thisN = which(is.na(lakeSiteTable$Elevation))
dists = sqrt((lakeSiteTable$Long[thisN]-FENZlake$long)^2+(lakeSiteTable$Lat[thisN]-FENZlake$lat)^2)
thatN = which.min(dists)
lakeSiteTable$Elevation[thisN] = FENZlake$LakeElevat[thatN]
lakeSiteTable$MaxDepth[thisN] = FENZlake$MaxDepth[thatN]
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
  dplyr::filter(!(nSite>1&
                    grepl('centre|center|mid|open',sites,ignore.case=T)&
                    !grepl('centre|center|mid|open',SiteID,ignore.case=T)))%>%
  #    These list the oens we dont want to keep
  dplyr::filter(!tolower(SiteID)%in%c("lake rotorangi l1 (mangamingi)", "lake rotorangi l3 (dam)",# (7506)
                                      "1880", "5632",# (34665)
                                      "lake rotoiti at okawa bay (integrated)", "lake rotoiti at site 4 (integrated)",# (54730)
                                      "lake rotorua at site 2 (integrated)",# (11133)
                                      "lake manapouri at stony point top", "lake manapouri near frazers beach top",#(54735)
                                      "lake george ne",#(28543)
                                      "lake te anau at south fiord top",#(52566)
                                      "lake forsyth at catons bay","lake forsyth at birdlings flat"#(47579)
  ))%>%select(-nSite,-sites)


lakeTable%>%
  group_by(LFENZID)%>%
  dplyr::summarise(Agency=unique(Agency),
                   nSite=length(unique(LawaSiteID)),
                   sites=paste0(gsub('integrated|lake','',SiteID,ignore.case = T),collapse=', '))%>%
  ungroup%>%
  arrange(desc(nSite))%>%
  filter(nSite>1)%>%
  filter(!grepl('cent|mid|open',sites))







TLI = read_csv(tail(dir(path = "h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/",
                        pattern="ITELakeTLIBYFENZ",recursive=T,full.names=T),1))
TLI <- TLI%>%select(FENZID,TLIYear,TLI)%>%distinct

TLI <- merge(x=lakeTable%>%select(LawaSiteID,SiteID,CouncilSiteID,
                                  FENZID=LFENZID,LType,GeomorphicLType,
                                  Region,Agency,Long,Lat,Elevation,MaxDepth,DistToSea),all.x=F,
             y=TLI,all.y=T)
TLI$TLIBand_orig = cut(TLI$TLI,c(-1,2,3,4,5,700),labels = c("A","B","C","D","E"))
TLI$TLIBand = cut(TLI$TLI,c(-1,2,3,4,5,700),
                  labels = c("A","B","C","D","E"),
                  right = FALSE)
TLIlatest = droplevels(TLI%>%filter(TLIYear==2020)%>%drop_na(TLI))





NOFSummaryTable=read.csv(tail(dir(path = "h:/ericg/16666LAWA/LAWA2021/Lakes/Analysis/",pattern="NOFLakesOverall",
                                  recursive=T,full.names = T),1),stringsAsFactors = F)
NOFSummaryTable <- NOFSummaryTable%>%filter(!Year%in%c('2006to2010'))  #Just, you see this way we're left with a single decade
NOFSummaryTable <- NOFSummaryTable%>%filter(SiteID%in%unique(lakeTable$SiteID)) #And this way, we have only representative sites per lake

NOFSummaryTable <- merge(lakeTable%>%select(LawaSiteID,SiteID,CouncilSiteID,LFENZID,
                                            LType,GeomorphicLType,Region,Agency,
                                            Long,Lat,Elevation,MaxDepth,DistToSea),all.x=F,
                         NOFSummaryTable%>%select(-CouncilSiteID,-Agency,-Region,-SiteID,
                                                  -AmmoniacalMed_Band,-AmmoniacalMax_Band,
                                                  -ChlAMed_Band,-ChlAMax_Band,-EcoliBand,
                                                  -Ecoli95_Band,-EcoliRecHealth540_Band,
                                                  -EcoliRecHealth260_Band),all.y=T)


NOFSummaryTable$NitrogenMed_Band=factor(NOFSummaryTable$NitrogenMed_Band,
                                        levels=AtoD,
                                        labels=AtoD)
NOFSummaryTable$NitrogenMed_Band[is.na(NOFSummaryTable$NitrogenMed_Band)] <- "NA"
NOFSummaryTable$PhosphorusMed_Band=factor(NOFSummaryTable$PhosphorusMed_Band,
                                          levels=AtoD,
                                          labels=AtoD)
NOFSummaryTable$PhosphorusMed_Band[is.na(NOFSummaryTable$PhosphorusMed_Band)] <- "NA"
NOFSummaryTable$Ammonia_Toxicity_Band=factor(NOFSummaryTable$Ammonia_Toxicity_Band,
                                             levels=AtoD,
                                             labels=AtoD)
NOFSummaryTable$Ammonia_Toxicity_Band[is.na(NOFSummaryTable$Ammonia_Toxicity_Band)] <- "NA"
NOFSummaryTable$ClarityMedian_Band=factor(NOFSummaryTable$ClarityMedian_Band,
                                          levels=AtoD,
                                          labels=AtoD)
NOFSummaryTable$ClarityMedian_Band[is.na(NOFSummaryTable$ClarityMedian_Band)] <- "NA"
NOFSummaryTable$ChlASummaryBand=factor(NOFSummaryTable$ChlASummaryBand,
                                       levels=AtoD,
                                       labels=AtoD)
NOFSummaryTable$ChlASummaryBand[is.na(NOFSummaryTable$ChlASummaryBand)] <- "NA"
NOFSummaryTable$EcoliSummaryBand=factor(NOFSummaryTable$EcoliSummaryBand,
                                        levels=AtoE,
                                        labels=AtoE)
NOFSummaryTable$EcoliSummaryBand[is.na(NOFSummaryTable$EcoliSummaryBand)] <- "NA"


#TLI calculated on one years data, joined to NOF attributes calculated on five years of data
#Match on teh latest last year of the five
NOFSummaryTable = left_join(x=NOFSummaryTable%>%mutate(TLIYear=strFrom(Year,'to')),
                            y=TLI%>%select(-FENZID,-LType)%>%
                              mutate(TLIYear=as.character(TLIYear)))#,by=c("SiteID","TLIYear"))
NOFSummaryTable$TLIBand_orig = cut(NOFSummaryTable$TLI,c(-1,2,3,4,5,700),labels = c("A","B","C","D","E"))
NOFSummaryTable$TLIBand = cut(NOFSummaryTable$TLI,c(-1,2,3,4,5,700),
                              labels = c("A","B","C","D","E"),
                              right = FALSE)


NOFlatest = droplevels(NOFSummaryTable%>%filter(Year=="2016to2020"))%>%drop_na(TLI)


plot(TLIlatest$TLIBand,(TLIlatest$Elevation)^(1/6))
plot(TLIlatest$TLI,(TLIlatest$Elevation),xlab='TLI value',ylab='Elevation, m',
     col=LAWAPalette[c(3,8,7,6,5)][as.numeric(TLIlatest$TLIBand)],pch=16)

for(TLIthresh in seq(1.6,
                     max(TLIlatest$TLI,na.rm=T),length.out = 100)){
  suppressMessages({altroc=pROC::roc(response=TLIlatest$TLI>=TLIthresh,
                   predictor=TLIlatest$Elevation)})
  highlow=pROC::coords(roc=altroc,'best')[1]$threshold[1]
  points(TLIthresh,highlow,col='red')  
}


plot(TLIlatest$TLI,(TLIlatest$Elevation))
altroc=pROC::roc(response=TLIlatest$TLI>=4.5,
                 predictor=TLIlatest$Elevation)
highlow=pROC::coords(roc=altroc,'best')[1]$threshold
rm(altroc)
abline(h=highlow,lty=2,lwd=2)
abline(v=4.5,lty=2,lwd=2)

stopifnot(abs(highlow-170)<5)


TLIlatest$elevationBand = factor(TLIlatest$Elevation>highlow,
                                 levels=c(FALSE,TRUE),labels=c("Low","High"))

shallowDeep=20
 TLIlatest$depthBand = factor(TLIlatest$MaxDepth>shallowDeep,levels=c(FALSE,TRUE),labels=c("Shallow","Deep"))

 
 
 plotLatest=NOFlatest%>%
   select(LawaSiteID,
          Long,Lat,
          Nitrogen=NitrogenMed_Band,
          Phosphorus=PhosphorusMed_Band,
          Ammonia=Ammonia_Toxicity_Band,
          Clarity=ClarityMedian_Band,
          ChlA=ChlASummaryBand,
          # TLI=TLIBand,
          Elevation,MaxDepth)%>%
   pivot_longer(cols = Nitrogen:ChlA,names_to = "NOFBand",values_to = "NOFGrade")%>%
   filter(NOFGrade!='NA')
 plotLatest <- bind_rows(plotLatest,
                         TLIlatest%>%mutate(NOFBand="TLI",NOFGrade=TLIBand)%>%
                           select(matches(names(plotLatest),ignore.case = F),elevationBand,depthBand))
 barNOF <- plotLatest%>%
   group_by(NOFBand)%>%
   summarise(n=n(),
             E=sum(NOFGrade=="E")/n,
             D=sum(NOFGrade=="D")/n,
             C=sum(NOFGrade=="C")/n,
             B=sum(NOFGrade=="B")/n,
             A=sum(NOFGrade=="A")/n)%>%
   select(-n)%>%t%>%as.data.frame
 names(barNOF) <- barNOF[1,]
 barNOF=barNOF[-1,]
 barNOF <- as.matrix(sapply(barNOF,as.numeric))
 NOFCounts <- table(plotLatest$NOFBand)
 TLIcol = which(colnames(barNOF)=="TLI")