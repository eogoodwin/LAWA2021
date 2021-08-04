rm(list=ls())
library(tidyverse)
library(sysfonts)
library(googleVis)

dir.create(paste0("H:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/",format(Sys.Date(),"%Y-%m-%d")),showWarnings = F,recursive = T)

load(tail(dir(path = "h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/",
              pattern = "RecDataF",recursive=T,full.names = T),1),verbose=T)



EndYear <- lubridate::year(Sys.Date())
StartYear5 <- EndYear - 5 + 1
# firstYear = min(wqdYear,na.rm=T)
firstYear=2005
yr <- paste0(as.character(StartYear5),'to',as.character(EndYear))
rollyrs=which(grepl('to',yr))
nonrollyrs=which(!grepl('to',yr))
reps <- length(yr)



#Deal with censored data
#Anna Madarasz-Smith 1/10/2018
#Hey guys,
# We (HBRC) have typically used the value with the >reference 
# (eg >6400 becomes 6400), and half the detection of the less than values (e.g. <1 becomes 0.5). 
# I think realistically you can treat these any way as long as its consistent beccuase 
# it shouldn?t matter to your 95th%ile.  
# These values should be in the highest and lowest of the range.
# Hope this helps Cheers
# Anna
recDataF$val=as.numeric(recDataF$val)
recDataF$fVal=recDataF$val
recDataF$fVal[recDataF$lCens]=recDataF$val[recDataF$lCens]/2
recDataF$fVal[recDataF$rCens]=recDataF$val[recDataF$rCens]
table(recDataF$lCens)
table(recDataF$rCens)



graphData <- recDataF%>%
  # filter(YW>201525)%>%
  filter(YW>paste0(StartYear5-1,"25"))%>%
  dplyr::filter(LawaSiteID!='')%>%drop_na(LawaSiteID)%>%
  dplyr::filter(property%in%c("E-coli","Enterococci"))%>%
  dplyr::filter((lubridate::yday(dateCollected)>297|month<4))#%>% #bathign season months only plus the last week of October. To catch labour weeknd.
# dplyr::group_by(LawaSiteID,YW,property)%>%
# dplyr::arrange(YW)%>%                   
# dplyr::summarise(dateCollected=first(dateCollected),
# region=unique(region),
# n=length(fVal),            #Count number of weeks recorded per season
# # fVal=first(fVal),
# bathingSeason=unique(bathingSeason))%>%
# ungroup->graphData
#45239  15/10/20
#45465 16/10/2020
#45577 20/10/20
#50452 28/10/20
#50277 29/10/20
#50254 3/11/20
#50316 9/11/20
#47460 2/7/21
#55828 29/7/2021
#48134 5/8/21

graphData$bathingSeason <- factor(graphData$bathingSeason)


# Calculate CISHSiteSummary ####
###############################
CISHsiteSummary <- graphData%>%
  select(-dateCollected)%>%
  group_by(LawaSiteID,property)%>%            #For each site
  dplyr::summarise(.groups = 'keep',
                   region=unique(region),
                   nBS=length(unique(bathingSeason)),
                   nPbs=paste(as.numeric(table(bathingSeason)),collapse=','),
                   uBS=paste(sort(unique(bathingSeason)),collapse=','),
                   totSampleCount=n(),
                   min=min(fVal,na.rm=T),
                   max=max(fVal,na.rm=T),
                   haz95=quantile(fVal,probs = 0.95,type = 5,na.rm = T),
                   haz50=quantile(fVal,probs = 0.5,type = 5,na.rm = T))%>%ungroup
#709 15/10/20
#708 16/10/2020
#709 20/10/20
#708 28/10/20
#708 29-10-20
#707 3*11*20
#708  9/11/20
#535 2/7/21
#637 29/7/21
#627 5/8/21

#https://environment.govt.nz/publications/microbiological-water-quality-guidelines-for-marine-and-freshwater-recreational-areas/
#https://niwa.co.nz/sites/niwa.co.nz/files/Swimmability%20Paper%2010%20May%202017%20FINAL.pdf     says 550
#and quotes old NOF 
#
#a new one shows a change from 550 to 540
#
##For enterococci, marine:                      E.coli, freshwater
#table D1 A is 95th %ile <= 40       table E1        <= 130
#         B is 95th %ile <41-200                  131 - 260
#         C is 95th %ile <201-500                 261 - 540
#         D is 95th %ile >500                    >540


# NPSFM tables 9 and 22

# Table 9    p        q       r      s
# A Blue    <5%     <20%    <=130  <=540
# B Green   5-10%   20-30%  <=130  <=1000
# C Yellow  10-20%  30-34%  <=130  <=1200
# D Orange  20-30%  >34%    >130   >1200
# E Red     >30%    >50%    >260   >1200
# 
# p = % exceedances over 540/100mL
# q = % exceedances over 260/100mL
# r = Median conc / 100 mL
# s = 95th% of E.coli/100 mL
# 
# Table 22 of NPSFM
# Exc   <=130
# Good  >130 & <=260
# Fair  >260 & <=540
# Poor  >540

#### 17/Nov2020 NOF Definitions ####
library(parallel)
library(doParallel)
source("h:/ericg/16666LAWA/LAWA2021/WaterQuality/scripts/SWQ_NOF_Functions.R")
NOFbandDefinitions <- read.csv("H:/ericg/16666LAWA/LAWA2021/WaterQuality/Metadata/NOFbandDefinitions3.csv", header = TRUE, stringsAsFactors=FALSE)
NOFbandDefinitions <- NOFbandDefinitions[,1:11]
uLAWAids = unique(graphData$LawaSiteID)


if(exists("NOFSummaryTable")) { rm("NOFSummaryTable") }
startTime=Sys.time()
workers <- makeCluster(7)
registerDoParallel(workers)
clusterCall(workers,function(){
  library(magrittr)
  # library(doBy)
  library(plyr)
  library(dplyr)
  source('H:/ericg/16666LAWA/LAWA2021/scripts/LAWAFunctions.R',echo = F)
})
startTime=Sys.time()

foreach(i = 1:length(uLAWAids),.combine=rbind,.errorhandling="stop",.inorder=F)%dopar%{
  
  # for(i in 1:length(uLAWAids)){
  Com_NOF <- data.frame (LawaSiteID               = rep(uLAWAids[i],reps),
                         Year                     = yr,
                         EcoliMed                 = as.numeric(rep(NA,reps)),
                         EcoliMed_Band            = rep(as.character(NA),reps),
                         Ecoli95                  = as.numeric(rep(NA,reps)),
                         Ecoli95_Band             = rep(as.character(NA),reps),
                         EcoliRecHealth540        = as.numeric(rep(NA,reps)),
                         EcoliRecHealth540_Band   = rep(as.character(NA),reps),
                         EcoliRecHealth260        = as.numeric(rep(NA,reps)),
                         EcoliRecHealth260_Band   = rep(as.character(NA),reps),
                         EcoliAnalysisNote        = rep('',reps),
                         stringsAsFactors = FALSE)
  
  suppressWarnings(rm(ecosite,rightSite,value,val)  )
  rightSite <- graphData%>%
    dplyr::filter(LawaSiteID==uLAWAids[i])%>%
    tidyr::drop_na(val)%>%
    mutate(Year=format(dateCollected,'%Y'))%>%mutate(Value=val)
  rightSite$YearQuarter=paste0(quarters(rightSite$dateCollected),year(rightSite$dateCollected))
  
  nBS=length(unique(rightSite$bathingSeason))
  nPbs=paste(as.numeric(table(rightSite$bathingSeason)),collapse=',')
  uBS=paste(sort(unique(rightSite$bathingSeason)),collapse=',')
  totSampleCount=sum(!is.na(rightSite$val))
  nInLatestBS = as.numeric(tail(unlist(strsplit(nPbs,',')),1))
  nInPreviousBS = as.numeric(head(tail(unlist(strsplit(nPbs,',')),2),1))
  notRecentlyMonitored = nInLatestBS==0 & nInPreviousBS==0
  notEnough50 = totSampleCount<50
  tooFew = any(c(notRecentlyMonitored,notEnough50)) #,notEnoughBS
  
  suppressWarnings(rm(cnEc_Band,cnEc95_Band,cnEcRecHealth540_Band,cnEcRecHealth260_Band,ecosite,ecosite))
  ecosite=rightSite%>%dplyr::filter(property=="E-coli")
  
  #E coli median and  95th percentile
  if(!tooFew){
    Com_NOF$EcoliMed = quantile(ecosite$val,prob=0.5,type=5,na.rm=T)
    Com_NOF$EcoliMed_Band=NOF_FindBand(Com_NOF$EcoliMed,bandColumn = NOFbandDefinitions$E..coli)
    Com_NOF$Ecoli95 = quantile(ecosite$val,prob=0.95,type=5,na.rm=T)
    Com_NOF$Ecoli95_Band=NOF_FindBand(Com_NOF$Ecoli95,bandColumn = NOFbandDefinitions$Ecoli95)
    ecv = ecosite$val[!is.na(ecosite$val)]
    Com_NOF$EcoliRecHealth540=sum(ecv>540)/length(ecv)*100
    Com_NOF$EcoliRecHealth260=sum(ecv>260)/length(ecv)*100
    
    Com_NOF$EcoliRecHealth540_Band <- NOF_FindBand(Com_NOF$EcoliRecHealth540,bandColumn=NOFbandDefinitions$EcoliRec540)
    Com_NOF$EcoliRecHealth260_Band <- NOF_FindBand(Com_NOF$EcoliRecHealth260,bandColumn=NOFbandDefinitions$EcoliRec260)
  }
  
  rm(ecosite)
  rm(rightSite) 
  return(Com_NOF)
}->NOFSummaryTable
stopCluster(workers)
rm(workers)
cat(Sys.time()-startTime)  

# 1.4s 2/7/21
# 1.7s 29/7/21

# NOFSummaryTable$EcoliMed_Band <- sapply(NOFSummaryTable$EcoliMed,NOF_FindBand,bandColumn=NOFbandDefinitions$E..coli)

#These contain the best case out of these scorings, the worst of which contributes.
suppressWarnings(cnEc_Band <- sapply(NOFSummaryTable$EcoliMed_Band,FUN=function(x){
  min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEc95_Band <- sapply(NOFSummaryTable$Ecoli95_Band,FUN=function(x){
  min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEcRecHealth540_Band <- sapply(NOFSummaryTable$EcoliRecHealth540_Band,FUN=function(x){
  min(unlist(strsplit(x,split = '')))}))
suppressWarnings(cnEcRecHealth260_Band <- sapply(NOFSummaryTable$EcoliRecHealth260_Band,FUN=function(x){
  min(unlist(strsplit(x,split = '')))}))  
NOFSummaryTable$EcoliSummaryband = as.character(apply(cbind(pmax(cnEc_Band,cnEc95_Band,cnEcRecHealth540_Band,cnEcRecHealth260_Band)),1,max))
rm("cnEc_Band","cnEc95_Band","cnEcRecHealth540_Band","cnEcRecHealth260_Band")

NOFSummaryTable$siteType = graphData$siteType[match(NOFSummaryTable$LawaSiteID,graphData$LawaSiteID)]

with(NOFSummaryTable%>%filter(Year=="2017to2021"),table(siteType,EcoliSummaryband,useNA='if')%>%addmargins())
# EcoliSummaryband
# siteType     A   B   C   D   E <NA> Sum
# Beach     32   6   4  13   2  198 255
# LakeSite  40   2   0   5   0    8  55
# Site      45  34  10 110  11   25 235
# Sum      117  42  14 128  13  231 545
#### \17/Nov/2020


CISHsiteSummary$MACmarineEnt=cut(x=CISHsiteSummary$haz95,breaks = c(-0.1,40,200,500,Inf),labels=c("A","B","C","D"))
CISHsiteSummary$MACmarineEnt[CISHsiteSummary$property!='Enterococci'] <- NA
CISHsiteSummary$NOFfwEcoli=cut(x=CISHsiteSummary$haz95,breaks = c(-0.1,130,260,540,Inf),labels=c("A","B","C","D"))
CISHsiteSummary$NOFfwEcoli[CISHsiteSummary$property!='E-coli'] <- NA

table(CISHsiteSummary$MACmarineEnt)
# A   B   C   D 
# 36 110  72  33    8/10/20
# 55 156 85 42      15/10/20
# 55 156 85 42      16/10/20
# 55 156 85 42      20/10/20
# 53 154 92 39      28/10/20
# 53 156 92 37      29
# 53 156 92 37      3/11
# 53 156 92 37
# 53 156 92 37    9/11
# 43 114 82 33     2/7/21
# 47 120 83 35    29/7/21
table(CISHsiteSummary$NOFfwEcoli)
# A   B   C   D 
# 24  27  33 176    8/10/20
# 52 34  63 222    15/10/20
# 52 32 67 219     16/10/20 
# 53 32 66 220      20/10/20
# 48 35 65 222     28/10/20
# 48 35 66 221    29
# 48 35 66 220    3/11/20
# 48 35 63 223
# 48 35 63 223   9/11
# 36 32 53 142     2/7/21
# 45 35 59 213    29/7/21

#NOF table 22
#Excellent                          <=130
#Good                               >130 <=260
#Fair                               >260 <=540
#Poor                               >540


#For LAWA bands in 2020
#50 needed over 5 seasons 
#Sites must be still recently monitored - if no data fro 19/20, 
#then must have been last monitored in 18/19. 
#Anythign older not considered
#              marine               fresh
#              enterococci          e.coli
#A              <=40                 <=130
#B             41 - 200              131<260
#C             201-500              261-540
#D                >500                 >540

CISHsiteSummary$LawaBand = ifelse(is.na(CISHsiteSummary$MACmarineEnt),
                                  as.character(CISHsiteSummary$NOFfwEcoli),
                                  as.character(CISHsiteSummary$MACmarineEnt))

table(CISHsiteSummary$LawaBand)
# A   B   C   D 
# 128  84  90 209   8/10/2020
# 107 190 148 264   15/10/20
# 107 188 152 261   16/10/20
# 108 188 151 262   20/10/20
# 101 189 157 261   28/10/20
# 101 191 158 257    3/11/20
# 101 191 155 260    4/11/20
# 101 191 155 260    9/11/20
# 79  146 135 175    2/7/21
# 92  155 142 248   29/7/21 


#Calculate data abundance
#number per bathing season
#This is the old previous rules, about needing ten per season, and every season represented
if(lubridate::year(Sys.time())<2021){
  #Email, Abi L 15/10/2019
  #2)	Greater Wellington and minimum sample size
  #   So as a starter for 10, can we propose alternative rule if the above isn?t met, that:
  #   If in one year, there is less than 10 but at least 7 data points, 
  #   then as long as the total number of data points over 3 years are at least 35, 
  #   then these will be eligible for an overall bacto risk grade?  
  #   This would ensure that the majority of sites in the spreadsheet attached would be eligible for a grade - 
  #   but wanted to test with you that this is a statistically robust approach before we sign off on this.
  
  # nPbs=do.call(rbind,lapply(CISHsiteSummary$nPbs,FUN = function(x)lapply(strsplit(x,split = ','),as.numeric))) #get the per-season counts
  # tooFew = which(do.call('rbind',lapply(nPbs,function(x){
  #   any(x<10)|length(x)<3
  # })))  #see if any per-season counts are below ten, or there are fewer than three seasons
  # tooFewNew = which(do.call('rbind',lapply(nPbs,function(x){
  #   (any(x<10)&sum(x,na.rm=T)<35)|(length(x)<3)|any(x<7)
  # }))) 
  # 
  # CISHsiteSummary$Region[tooFew[!tooFew%in%tooFewNew]]
  # cbind(CISHsiteSummary$Region[tooFew[!tooFew%in%tooFewNew]],t(sapply((tooFew[!tooFew%in%tooFewNew]),FUN=function(x)nPbs[[x]])))
  # 
  # 
  # 
  # CISHsiteSummarySiteName = recDataF$SiteName[match(CISHsiteSummary$LawaId,recDataF$LawaId)]
  # CISHsiteSummarySiteName[tooFew[!tooFew%in%tooFewNew]]
  # 
  # CISHsiteSummary$LawaBand[tooFewNew]=NA #set those data-poor sites' grade to NA
}else{
  #50 needed over 5 seasons 
  #Sites must be still recently monitored - if no data fro 19/20, 
  #then must have been last monitored in 18/19. 
  #Anythign older not considered
  nInLatestBS = unname(sapply(CISHsiteSummary$nPbs,FUN=function(s)as.numeric(tail(unlist(strsplit(s,',')),1))))
  nInPreviousBS = unname(sapply(CISHsiteSummary$nPbs,FUN=function(s)as.numeric(head(tail(unlist(strsplit(s,',')),2),1))))
  notRecentlyMonitored = which(nInLatestBS==0 & nInPreviousBS==0)
  notEnough50 = which(CISHsiteSummary$totSampleCount<50)
  # notEnoughBS = which(CISHsiteSummary$nBS<5)
  tooFew = unique(c(notRecentlyMonitored,notEnough50)) #  INDICES, NOT LOGICALS
  rm(nInLatestBS,nInPreviousBS,notRecentlyMonitored,notEnough50)
  table(CISHsiteSummary$LawaBand[-tooFew])
  table(CISHsiteSummary$LawaBand[tooFew])
  CISHsiteSummary$MACmarineEnt[tooFew]=NA
  CISHsiteSummary$NOFfwEcoli[tooFew]=NA
  CISHsiteSummary$LawaBand[tooFew]=NA
}
table(CISHsiteSummary$LawaBand,useNA='if')
#    A    B    C    D <NA> 
#   69  178  126  201  135  20/10/20
#   64  179  136  203  126  28/10/20
#   64  181  135  201  127  29/10/20   
#   64  181  135  201  126   3/11/20
#   64  181  132  204  126   4/11/20
#   64  181  132  204  126   9/11/20
#   57  139  117  141  81    2/7/21
#   62  145  120  200  110  29/7/21

CISHsiteSummary$siteName = recDataF$siteName[match(CISHsiteSummary$LawaSiteID,recDataF$LawaSiteID)]
CISHsiteSummary$siteType = recDataF$siteType[match(CISHsiteSummary$LawaSiteID,recDataF$LawaSiteID)]
CISHsiteSummary$SiteID = ssm$SiteName[match(CISHsiteSummary$siteName,make.names(ssm$callID))]


#Export here 
downloadSummary = CISHsiteSummary%>%select(region,siteName,SiteID,LawaSiteID,
                                           siteType,property,totSampleCount,
                                           minVal=min,maxVal=max,Hazen95=haz95,
                                           LongTermGrade=NOFfwEcoli)
#LongTermGrade is set to Ecoli just above, set it to enterococci for enterocicci rows
downloadSummary$LongTermGrade[downloadSummary$property=='Enterococci']=
  CISHsiteSummary$MACmarineEnt[CISHsiteSummary$property=="Enterococci"]
#Dont save out yet, need to add the "GradeSubstitue" column


CISHsiteSummary <- CISHsiteSummary%>%select(LawaSiteID,SiteID,siteName,region,property,siteType,
                                            nBS,nPbs,uBS,totSampleCount,min,max,haz95,haz50,
                                            MACmarineEnt,NOFfwEcoli,LawaBand)



#Simplify. ####
#  If sites have different bands for multiple properties then keep them, otherwise,
# let's just you know, we dont need to keep both do we
singleProp <- CISHsiteSummary%>%
  group_by(LawaSiteID)%>%
  dplyr::mutate(nProp=length(unique(property)))%>%
  ungroup%>%
  filter(nProp==1)%>%select(-nProp)
#535 3/11
#371 2/7/21
#453 29/7/21
multiProp <- CISHsiteSummary%>%
  group_by(LawaSiteID)%>%
  dplyr::mutate(nProp=length(unique(property)),
                nNA=sum(is.na(LawaBand)))%>%
  ungroup%>%
  filter(nProp>1)%>%
  filter(!(.$nNA==1&is.na(.$LawaBand)))%>%  #we're only looking at sites that have two FIBs, so,
  #if a row is NA in lawaband, and has only 1 NA< the other FIB must be good.
  group_by(LawaSiteID)%>%
  dplyr::mutate(nProp=length(unique(property)),
                nNA=sum(is.na(LawaBand))) #113 3/11
moresingles = multiProp%>%filter(nProp==1)%>%select(-nProp,-nNA)%>%distinct
#59 3/11
#110 2/7/21
#60  29/7/21

singleProp=full_join(singleProp,moresingles)  #513
rm(moresingles)

multiProp <- multiProp%>%filter(nProp!=1)%>%
  group_by(LawaSiteID)%>%
  dplyr::mutate(combGr=paste0(unique(LawaBand),collapse=''),
                ncg=nchar(combGr))%>%
  ungroup 
#56 2/7/21
#64 29/7/21

moresingles=multiProp%>%filter(ncg==1|combGr=="NA")%>%select(-combGr,-nProp,-nNA,-ncg)%>%distinct
#26 2/7
#34 29/7/21

#These are now same grade in ecoli and enterococci, but we do need to keep only the appropriate numeric values
moresingles <- moresingles%>%filter((property=="E-coli"&siteType=='Site')|(property=="Enterococci"&siteType=='Beach'))
#13 2/7
#17 29/7

singleProp=full_join(singleProp,moresingles) #530
rm(moresingles)

multiProp <- multiProp%>%  dplyr::filter(ncg>1&combGr!='NA') #30 x 19

multiProp%>%select(LawaSiteID,region,siteType,combGr)%>%distinct

#Some of these, the appropriate FIB will be the worst case, in which case, all good, right?

moresingles = multiProp%>%select(LawaSiteID:region,siteType,property,nBS:LawaBand)%>%
  pivot_wider(id_cols = LawaSiteID:siteType,
              names_from = property,values_from = nBS:LawaBand)%>%
  filter((siteType=='Site'&`LawaBand_E-coli`>=LawaBand_Enterococci)|
           (siteType=='Beach'&`LawaBand_E-coli`<=LawaBand_Enterococci))
#Have to keep this two-step I think
moresingles = multiProp%>%filter(LawaSiteID%in%moresingles$LawaSiteID)%>%
  filter((siteType=='Beach'&property=="Enterococci")|
           (siteType=='Site'&property=="E-coli"))%>%
  select(-(nProp:ncg))
#4 2/7/21
#4 29/7/21

multiProp <- multiProp%>%filter(!LawaSiteID%in%moresingles$LawaSiteID) #22 (==11 sites)

singleProp=full_join(singleProp,moresingles) #534
rm(moresingles)

storeForCouncilInfo=multiProp


#There are 11 sites here.  Give them each their worst grade, regardless of water type
stopifnot(all(multiProp$siteType=='Beach'))
moresingles <- multiProp%>%filter(property=='Enterococci')%>%
  mutate(LawaBand = sapply(combGr,FUN=function(s){max(unlist(strsplit(s,split = '')))}))%>%
  select(-(nProp:ncg)) #11

singleProp = full_join(singleProp,moresingles)%>% #545
  arrange(region)
rm(moresingles,multiProp)




CISHsiteSummary = singleProp          #637 rows to 545

# rm(singleProp)


#Can we check here for sites that have different LawaBand in the CISH than in the download ####
downloadSummary <- left_join(downloadSummary,
                             CISHsiteSummary%>%select(LawaSiteID,property,LawaBand))
downloadSummary$GradeSubstituted = !downloadSummary$LongTermGrade==downloadSummary$LawaBand
downloadSummary <- downloadSummary%>%select(-LawaBand)
downloadSummary$GradeSubstituted[is.na(downloadSummary$GradeSubstituted)] <- FALSE

downloadSummary$siteType[downloadSummary$siteType=="Site"] <- "River"
downloadSummary$siteType[downloadSummary$siteType=="LakeSite"] <- "Lake"
downloadSummary$siteType[downloadSummary$siteType=="Beach"] <- "Coastal"



LAWAPalette=c("#95d3db","#74c0c1","#20a7ad","#007197",
              "#e85129","#ff8400","#ffa827","#85bb5b","#bdbcbc")

AbisPivotData = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2020/CanISwimHere/Data//LAWA Recreational water quality monitoring dataset_Nov2020_pivot draft.xlsx',sheet= "Weekly monitoring dataset")

AbisPivotData <- AbisPivotData %>% filter(property!='Cyanobacteria')%>%droplevels()

AbisPivotData$Grade = factor(AbisPivotData$`Swim guidelines test result description`,
                             levels=c("Suitable for swimming","Caution advised","Unsuitable for swimming","Not available"))

RiverGrades=AbisPivotData%>%
  dplyr::filter(siteType=='River',Grade!="Not available")%>%droplevels%>%
  dplyr::select(Grade)%>%table%>%as.data.frame
LakeGrades=AbisPivotData%>%
  dplyr::filter(siteType=='Lake',Grade!="Not available")%>%droplevels%>%
  dplyr::select(Grade)%>%table%>%as.data.frame
CoastalGrades=AbisPivotData%>%
  dplyr::filter(siteType=='Coastal',Grade!="Not available")%>%droplevels%>%
  dplyr::select(Grade)%>%table%>%as.data.frame

library(showtext)
if(!'source'%in%font_families()){
  sysfonts::font_add_google("Source Sans Pro",family='source')
}


tiff('H:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/Summary.tif',
     width=15,height=12,res=300,compression='lzw',type='cairo',units='in')
showtext::showtext_begin()
layout(matrix(c(1,1,1,1,1,1,1,1,1,
                2,2,2,3,3,3,4,4,4,
                2,2,2,3,3,3,4,4,4,
                5,5,5,6,6,6,7,7,7),nrow=4,byrow=T))
par(family='source',mar=c(0,0,0,0),bg='white')
par(xpd=NA)
plot(0,0,bty='n',type='n',xlab='',ylab='',xaxt='n',yaxt='n',xlim=c(0,10),ylim=c(0,10))
text(5,6,"New Zealand swim spot water quality summary",cex=20)
text(5,3,expression('Recreational water quality over five years'^'*'),cex=15)
pie(RiverGrades$Freq,labels=NA,col=c('#85bb5b','#ffa827','#e85129'),border='white',radius=0.9)
title(main = 'River',line=-4,cex.main=12)
points(0,0,pch=16,cex=50,col='white')
pie(LakeGrades$Freq,labels=NA,col=c('#85bb5b','#ffa827','#e85129'),border='white',radius=0.9)
title(main = 'Lake',line=-4,cex.main=12)
points(0,0,pch=16,cex=50,col='white')
pie(CoastalGrades$Freq,labels=NA,col=c('#85bb5b','#ffa827','#e85129'),border='white',radius=0.9)
title(main = 'Coastal',line=-4,cex.main=12)
points(0,0,pch=16,cex=50,col='white')

plot(0,0,bty='n',type='n',xlab='',ylab='',xaxt='n',yaxt='n',xlim=c(0,10),ylim=c(0,10))
text(1,7+3,paste0(round(RiverGrades$Freq[1]/sum(RiverGrades$Freq)*100,0),'%'),col='#85bb5b',cex=12)
text(1,5+3,paste0(round(RiverGrades$Freq[2]/sum(RiverGrades$Freq)*100,0),'%'),col='#ffa827',cex=12)
text(1,3+3,paste0(round(RiverGrades$Freq[3]/sum(RiverGrades$Freq)*100,0),'%'),col='#e85129',cex=12)
text(2,7+3,"Suitable for swimming",cex=8,pos = 4)
text(2,5+3,"Caution advised",cex=8,pos = 4)
text(2,3+3,"Unsuitable for swimming",cex=8,pos = 4)
text(1,1+3,paste0("Summary from ",sum(RiverGrades$Freq)," samples."),cex=6,pos=4)
plot(0,0+3,bty='n',type='n',xlab='',ylab='',xaxt='n',yaxt='n',xlim=c(0,10),ylim=c(0,10))
text(1,7+3,paste0(round(LakeGrades$Freq[1]/sum(LakeGrades$Freq)*100,0),'%'),col='#85bb5b',cex=12)
text(1,5+3,paste0(round(LakeGrades$Freq[2]/sum(LakeGrades$Freq)*100,0),'%'),col='#ffa827',cex=12)
text(1,3+3,paste0(round(LakeGrades$Freq[3]/sum(LakeGrades$Freq)*100,0),'%'),col='#e85129',cex=12)
text(2,7+3,"Suitable for swimming",cex=8,pos = 4)
text(2,5+3,"Caution advised",cex=8,pos = 4)
text(2,3+3,"Unsuitable for swimming",cex=8,pos = 4)
text(1,1+3,paste0("Summary from ",sum(LakeGrades$Freq)," samples."),cex=6,pos=4)
text(5,2,expression(""^'*'*"Faecal indicator bacterial test results (excludes predicted data) supplied to LAWA. Data were collected over"),cex=8)
text(5,1,"the recreational bathing season (last week Oct – end of Mar) during 2016 – 2021  from regularly monitored swim sites.",cex=8)
plot(0,0+3,bty='n',type='n',xlab='',ylab='',xaxt='n',yaxt='n',xlim=c(0,10),ylim=c(0,10))
text(1,7+3,paste0(round(CoastalGrades$Freq[1]/sum(CoastalGrades$Freq)*100,0),'%'),col='#85bb5b',cex=12)
text(1,5+3,paste0(round(CoastalGrades$Freq[2]/sum(CoastalGrades$Freq)*100,0),'%'),col='#ffa827',cex=12)
text(1,3+3,paste0(round(CoastalGrades$Freq[3]/sum(CoastalGrades$Freq)*100,0),'%'),col='#e85129',cex=12)
text(2,7+3,"Suitable for swimming",cex=8,pos = 4)
text(2,5+3,"Caution advised",cex=8,pos = 4)
text(2,3+3,"Unsuitable for swimming",cex=8,pos = 4)
text(1,1+3,paste0("Summary from ",sum(CoastalGrades$Freq)," samples."),cex=6,pos=4)
showtext::showtext_end()
if(names(dev.cur())=='tiff')dev.off()

riverpie = gvisPieChart(data=RiverGrades,
                        options=list(
                          title='River',
                          width=1800,height=200,
                          pieHole=0.6,
                          colors="[ '#85bb5b','#ffa827','#e85129']",
                          pieSliceText='none',
                          pieStartAngle=90,
                          fontSize=20,
                          fontName='arial',
                          chartArea="{left:10,top:50,width:'40%',height:'90%'}",
                          legend="{position:'right',alignment:'center',maxLines:3}"),
                        chartid='riverCISHgrades')
lakepie = gvisPieChart(data=LakeGrades,
                       options=list(
                         title="Lake",
                         width=1800,height=200,
                         pieHole=0.6,
                         colors="[ '#85bb5b','#ffa827','#e85129']",
                         pieSliceText='none',
                         pieStartAngle=90,
                         fontSize=20,
                         fontName='arial',
                         chartArea="{left:10,top:40,width:'40%',height:'90%'}",
                         legend="{position:'righh',alignment:'center',maxLines:3}"),
                       chartid='lakeCISHgrades')
coastalpie = gvisPieChart(data=CoastalGrades,
                          options=list(
                            title="Coastal",
                            width=1800,height=200,
                            pieHole=0.6,
                            colors="[ '#85bb5b','#ffa827','#e85129']",
                            pieSliceText='none',
                            pieStartAngle=90,
                            fontSize=20,
                            fontName='arial',
                            chartArea="{left:10,top:30,width:'40%',height:'90%'}",
                            legend="{position:'right',alignment:'center',maxLines:3}"),
                          chartid='coastalCISHgrades')

# legendpie = gvisBarChart(data=RiverGrades,options=list(
#   title="Legend",
#   width=400,height=400,piehole=0.9,
#   fontName='arial',
#   chartArea="{left:100,top:100,width:'10%',height:'100%'}",
#   pieSliceText='none',fontSize=20,legend="{position:'top',alignment:'start',maxLines:5}"
# ),
# chartid='legendOnly')
# 
# plot()

rlpie = gvisMerge(riverpie,lakepie,horizontal=F)
rlcpie=gvisMerge(rlpie,coastalpie,horizontal=F,chartid='CISHgrades')
plot(rlcpie)


print(rlcpie,file=paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/",
                         format(Sys.Date(),'%Y-%m-%d'),
                         "/SwimSpot.html"))



write.csv(downloadSummary,
          file = paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data/",
                        format(Sys.Date(),'%Y-%m-%d'),
                        "/CISHdownloadSummary",format(Sys.time(),'%Y-%b-%d'),".csv"),row.names = F) 


write.csv(CISHsiteSummary,file = paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                                        "/CISHsiteSummary",format(Sys.time(),'%Y-%b-%d'),".csv"),row.names = F)
# CISHsiteSummary=read.csv(tail(dir(path="h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/",pattern="CISHsiteSummary",recursive = T,full.names = T,ignore.case = T),1),stringsAsFactors = F)
file.copy(from=paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                      "/CISHsiteSummary",format(Sys.time(),'%Y-%b-%d'),".csv"),
          to = "c:/users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021/Can I Swim here/Analysis/",overwrite=T)


#Write individual regional files: data from recDataF and scores from CISHsiteSummary 
uReg=unique(recDataF$region)
for(reg in seq_along(uReg)){
  toExport=recDataF%>%filter(region==uReg[reg],dateCollected>(Sys.time()-lubridate::years(5)))
  write.csv(toExport,file=paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                                 "/recData_",
                                 gsub(' ','',gsub(pattern = ' region',replacement = '',x = uReg[reg])),
                                 ".csv"),row.names = F)
  toExport=CISHsiteSummary%>%filter(region==uReg[reg])%>%as.data.frame
  write.csv(toExport,file=paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                                 "/recScore_",
                                 gsub(' ','',gsub(pattern = ' region',replacement = '',x = uReg[reg])),
                                 ".csv"),row.names = F)
}
rm(reg,uReg,toExport)







#Write export file for ITEffect to use for the website

recDataITE <- CISHsiteSummary%>%
  transmute(LAWAID=LawaSiteID,
            Region=region,
            Site=siteName,
            Hazen=haz95,
            NumberOfPoints=unlist(lapply(str_split(nPbs,','),function(x)sum(as.numeric(x)))),
            DataMin=min,
            DataMax=max,
            RiskGrade=LawaBand,
            Module=siteType)
recDataITE$Module[recDataITE$Module=="Site"] <- "River"
recDataITE$Module[recDataITE$Module=="LakeSite"] <- "Lake"
recDataITE$Module[recDataITE$Module=="Beach"] <- "Coastal"
write.csv(recDataITE,paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                            "/ITERecData",format(Sys.time(),'%Y-%b-%d'),".csv"),row.names = F)  

file.copy(from=paste0("h:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/",format(Sys.Date(),'%Y-%m-%d'),
                      "/ITERecData",format(Sys.time(),'%Y-%b-%d'),".csv"),
          to = "c:/users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021/Can I Swim here/Analysis/",overwrite=T)





























#7/12/2020
#Hi Eric

# Here is hopefully a quick job for Monday, if you can please.
# 
# Would it be possible to regenerate the below figure, but only use samples from the last swim season? (last week of Oct 2019 through to the end of Mar 2021).
# 
# Needs to go be ready to go out with the LAWA media release on first thing Tuesday 8th Dec.  It’s short notice, so if this isn’t doable, please let us know, and we will revise what we send out.
# 
# Thanking you kindly
# 
# Have a lovely weekend.
# 
# Cheers
# Abi


AbisPivotData = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2021/CanISwimHere/Data//LAWA Recreational water quality monitoring dataset_Nov2021_pivot draft.xlsx',sheet= "Weekly monitoring dataset")

AbisPivotData <- AbisPivotData %>% filter(property!='Cyanobacteria')%>%droplevels()
AbisPivotData <- AbisPivotData %>% filter(dateCollected>lubridate::dmy('22-10-2019')&dateCollected<=lubridate::dmy('31-3-2020'))

AbisPivotData$Grade = factor(AbisPivotData$`Swim guidelines test result description`,
                             levels=c("Suitable for swimming","Caution advised","Unsuitable for swimming","Not available"))

RiverGrades=AbisPivotData%>%
  dplyr::filter(siteType=='River',Grade!="Not available")%>%droplevels%>%
  dplyr::select(Grade)%>%table%>%as.data.frame
LakeGrades=AbisPivotData%>%
  dplyr::filter(siteType=='Lake',Grade!="Not available")%>%droplevels%>%
  dplyr::select(Grade)%>%table%>%as.data.frame
CoastalGrades=AbisPivotData%>%
  dplyr::filter(siteType=='Coastal',Grade!="Not available")%>%droplevels%>%
  dplyr::select(Grade)%>%table%>%as.data.frame

library(showtext)
if(!'source'%in%font_families()){
  sysfonts::font_add_google("Source Sans Pro",family='source')
}


tiff('H:/ericg/16666LAWA/LAWA2021/CanISwimHere/Analysis/OneYearSummary.tif',
     width=15,height=12,res=300,compression='lzw',type='cairo',units='in')
showtext::showtext_begin()
layout(matrix(c(1,1,1,1,1,1,1,1,1,
                2,2,2,3,3,3,4,4,4,
                2,2,2,3,3,3,4,4,4,
                5,5,5,6,6,6,7,7,7),nrow=4,byrow=T))
par(family='source',mar=c(0,0,0,0),bg='white')
par(xpd=NA)
plot(0,0,bty='n',type='n',xlab='',ylab='',xaxt='n',yaxt='n',xlim=c(0,10),ylim=c(0,10))
text(5,6,"New Zealand swim spot water quality summary",cex=20)
text(5,3,expression('Recreational water quality 2019 - 2021'^'*'),cex=15)
pie(RiverGrades$Freq,labels=NA,col=c('#85bb5b','#ffa827','#e85129'),border='white',radius=0.9)
title(main = 'River',line=-4,cex.main=12)
points(0,0,pch=16,cex=50,col='white')
pie(LakeGrades$Freq,labels=NA,col=c('#85bb5b','#ffa827','#e85129'),border='white',radius=0.9)
title(main = 'Lake',line=-4,cex.main=12)
points(0,0,pch=16,cex=50,col='white')
pie(CoastalGrades$Freq,labels=NA,col=c('#85bb5b','#ffa827','#e85129'),border='white',radius=0.9)
title(main = 'Coastal',line=-4,cex.main=12)
points(0,0,pch=16,cex=50,col='white')

plot(0,0,bty='n',type='n',xlab='',ylab='',xaxt='n',yaxt='n',xlim=c(0,10),ylim=c(0,10))
text(1,7+3,paste0(round(RiverGrades$Freq[1]/sum(RiverGrades$Freq)*100,0),'%'),col='#85bb5b',cex=12)
text(1,5+3,paste0(round(RiverGrades$Freq[2]/sum(RiverGrades$Freq)*100,0),'%'),col='#ffa827',cex=12)
text(1,3+3,paste0(round(RiverGrades$Freq[3]/sum(RiverGrades$Freq)*100,0),'%'),col='#e85129',cex=12)
text(2,7+3,"Suitable for swimming",cex=8,pos = 4)
text(2,5+3,"Caution advised",cex=8,pos = 4)
text(2,3+3,"Unsuitable for swimming",cex=8,pos = 4)
text(1,1+3,paste0("Summary from ",sum(RiverGrades$Freq)," samples."),cex=6,pos=4)
plot(0,0+3,bty='n',type='n',xlab='',ylab='',xaxt='n',yaxt='n',xlim=c(0,10),ylim=c(0,10))
text(1,7+3,paste0(round(LakeGrades$Freq[1]/sum(LakeGrades$Freq)*100,0),'%'),col='#85bb5b',cex=12)
text(1,5+3,paste0(round(LakeGrades$Freq[2]/sum(LakeGrades$Freq)*100,0),'%'),col='#ffa827',cex=12)
text(1,3+3,paste0(round(LakeGrades$Freq[3]/sum(LakeGrades$Freq)*100,0),'%'),col='#e85129',cex=12)
text(2,7+3,"Suitable for swimming",cex=8,pos = 4)
text(2,5+3,"Caution advised",cex=8,pos = 4)
text(2,3+3,"Unsuitable for swimming",cex=8,pos = 4)
text(1,1+3,paste0("Summary from ",sum(LakeGrades$Freq)," samples."),cex=6,pos=4)
text(5,2,expression(""^'*'*"Faecal indicator bacterial test results (excludes predicted data) supplied to LAWA. Data were collected over"),cex=8)
text(5,1,"the recreational bathing season (last week Oct – end of Mar) during 2019 – 2021  from regularly monitored swim sites.",cex=8)
plot(0,0+3,bty='n',type='n',xlab='',ylab='',xaxt='n',yaxt='n',xlim=c(0,10),ylim=c(0,10))
text(1,7+3,paste0(round(CoastalGrades$Freq[1]/sum(CoastalGrades$Freq)*100,0),'%'),col='#85bb5b',cex=12)
text(1,5+3,paste0(round(CoastalGrades$Freq[2]/sum(CoastalGrades$Freq)*100,0),'%'),col='#ffa827',cex=12)
text(1,3+3,paste0(round(CoastalGrades$Freq[3]/sum(CoastalGrades$Freq)*100,0),'%'),col='#e85129',cex=12)
text(2,7+3,"Suitable for swimming",cex=8,pos = 4)
text(2,5+3,"Caution advised",cex=8,pos = 4)
text(2,3+3,"Unsuitable for swimming",cex=8,pos = 4)
text(1,1+3,paste0("Summary from ",sum(CoastalGrades$Freq)," samples."),cex=6,pos=4)
showtext_end()
if(names(dev.cur())=='tiff')dev.off()
