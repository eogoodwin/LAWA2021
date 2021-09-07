
######################################################################################
#Calculate trends ####
GWtrendData <- GWdata%>%filter(Measurement%in%c("Nitrate nitrogen","Chloride",
                                                "Dissolved reactive phosphorus",
                                                "Electrical conductivity/salinity",
                                                "E.coli","Ammoniacal nitrogen"))
library(parallel)
library(doParallel)

workers <- makeCluster(2)
registerDoParallel(workers)
startTime=Sys.time()
clusterCall(workers,function(){
  library(magrittr)
  library(plyr)
  library(dplyr)
  
})
foreach(py = c(10,15),.combine=rbind,.errorhandling="stop")%dopar%{
  pn=0.75
  tredspy <- split(GWtrendData,GWtrendData$siteMeas)%>%
    purrr::map(~trendCore(.,periodYrs=py,proportionNeeded=pn,valToUse = 'Result-edited')) #Proportion needed is greater or equal
  tredspy <- do.call(rbind.data.frame,tredspy)
  row.names(tredspy)=NULL
  return(tredspy)
}->GWtrends
stopCluster(workers)
rm(workers)
Sys.time()-startTime           #2.9 mins 13/8/21
#8592 of 39  11-11
#8382 of 39  14-11
#8478 of 39  22-11
#8836 of 39  25-11
#8886 of 39  13-03-2021
#8888 of 39  27-03-2021
#9070       04/08/20
#10542      10/8/20
#11436   14-8-20
#11524
#11534    28-8-20
#11544
#6354      3-8-21
#9928   6*8*21
#10986 13/8/21
#10456 20/8/21
#10916 27/8/21

GWtrends$Source = GWdata$Source[match(GWtrends$LawaSiteID,GWdata$LawaSiteID)]


GWtrends$ConfCat <- cut(GWtrends$Cd, breaks=  c(-0.1, 0.1,0.33,0.67,0.90, 1.1),
                        labels = rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
GWtrends$ConfCat=factor(GWtrends$ConfCat,levels=rev(c("Very likely improving","Likely improving","Indeterminate","Likely degrading","Very likely degrading")))
GWtrends$TrendScore=as.numeric(GWtrends$ConfCat)-3
GWtrends$TrendScore[is.na(GWtrends$TrendScore)]<-(NA)

fGWt = GWtrends%>%dplyr::filter(!grepl('^unassess',GWtrends$frequency)&
                                  !grepl('^Insufficient',GWtrends$MKAnalysisNote)&
                                  !grepl('^Insufficient',GWtrends$SSAnalysisNote))

# 2476 of 41  11-7
# 2515 of 41  11-14
# 2405 of 41  11-22
# 2572 of 41  11-25
# 2585 of 31  03-13-2021
# 2560 of 31  03-27-2021
# 1537 of 41  04/08/2021
# 1615 of 41  10/8/20
# 1815
# 1938 of 41  24/8/2021
# 1935 28-8-20
# 1977
# 2252 14-9-20
# 757 3-8-21
# 2265 6-8-21
# 2594 13-8-21
# 2543 20/8/21
# 2580 27/8/21

#    nitrate nitrogen, chloride, DRP, electrical conductivity and E. coli.
# fGWt = fGWt%>%filter(Measurement %in% c("Nitrate nitrogen","Chloride","Dissolved reactive phosphorus",
#                                         "Electrical conductivity/salinity","E.coli"))

# table(GWtrends$proportionNeeded,GWtrends$period)
# knitr::kable(table(fGWt$proportionNeeded,fGWt$period),format='rst')

if(plotto){
  with(fGWt,knitr::kable(table(MKAnalysisNote,period),format='rst'))
  with(fGWt,knitr::kable(table(SSAnalysisNote,period),format='rst'))
  
  knitr::kable(with(fGWt%>%filter(period==10 & proportionNeeded==0.75)%>%
                      droplevels,table(Measurement,TrendScore)),format='rst')
  
  table(GWtrends$numQuarters,GWtrends$period)
  
  table(GWtrends$Measurement,GWtrends$ConfCat,GWtrends$proportionNeeded,GWtrends$period)
  table(fGWt$Measurement,fGWt$ConfCat,fGWt$proportionNeeded,fGWt$period)
  
  # with(GWtrends[GWtrends$period==5,],table(Measurement,ConfCat))
  with(GWtrends[GWtrends$period==10,],table(Measurement,ConfCat))
  with(GWtrends[GWtrends$period==15,],table(Measurement,ConfCat))
  
  table(GWtrends$Measurement,GWtrends$period)
  table(GWtrends$ConfCat,GWtrends$period)
  
  knitr::kable(table(GWtrends$ConfCat,GWtrends$Measurement,GWtrends$period))
}

#Export Trend values
write.csv(GWtrends,file = paste0('h:/ericg/16666LAWA/LAWA2021/Groundwater/Analysis/',format(Sys.Date(),"%Y-%m-%d"),
                                 '/ITEGWTrend',format(Sys.time(),"%d%b%Y"),'.csv'),row.names = F)
write.csv(fGWt,file = paste0('h:/ericg/16666LAWA/LAWA2021/Groundwater/Analysis/',format(Sys.Date(),"%Y-%m-%d"),
                             '/ITEGWTrendSuccess',format(Sys.time(),"%d%b%Y"),'.csv'),row.names = F)

write.csv(GWtrends,file=paste0("c:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021/Groundwater Quality/EffectDelivery/",
                               "ITEGWTrend",format(Sys.time(),"%d%b%Y"),'.csv'),row.names = F)
write.csv(fGWt,file = paste0("c:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021/Groundwater Quality/EffectDelivery/",
                             '/ITEGWTrendSuccess',format(Sys.time(),"%d%b%Y"),'.csv'),row.names = F)

# 
# #Put the state on the trend?
# 
# GWtrends$Calcmedian = GWmedians$median[match(x = paste(GWtrends$LawaSiteID,GWtrends$Measurement),
#                                              table = paste(GWmedians$LawaSiteID,GWmedians$Measurement))]
# GWtrends$Calcmedian[GWtrends$Calcmedian<=0] <- NA
# par(mfrow=c(2,3))
# for(meas in unique(GWtrends$Measurement)){
#   dfp=GWtrends%>%dplyr::filter(Measurement==meas)
#   plot(factor(dfp$ConfCat),dfp$Calcmedian,main=meas,log='y')
# }
# 
# head(GWmedians%>%filter(!Exclude)%>%select(-Exclude))
