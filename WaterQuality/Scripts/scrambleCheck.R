
agencies= c("ac","boprc","ecan","es","gdc","gwrc","hbrc","hrc","mdc","ncc","niwa","nrc","orc","tdc","trc","wcrc","wrc")
wqdata=loadLatestDataRiver()
wqdYear=lubridate::year(dmy(wqdata$Date))

StartYear5 <- lubridate::year(Sys.Date())-5  #2016
EndYear <- lubridate::year(Sys.Date())-1    #2020
wqdata <- wqdata[which((wqdYear>=(StartYear5) & wqdYear<=EndYear)),]

for(agency in agencies){
  wqdata%>%filter(Agency==agency)%>%mutate(sitemeas=paste(LawaSiteID,Measurement,sep='_'))%>%
    group_by(sitemeas)%>%dplyr::summarise(meas=unique(Measurement),
                                          valMean=round(mean(Value,na.rm=T),6),
                                          valSD=round(sd(Value,na.rm=T),6),
                                          count=n(),
                                          nonCens = sum(!Censored))%>%
    filter(count>1)%>%
    ungroup->agStat
  agStat$key = paste0(agStat$valMean,'_',agStat$valSD,'-',agStat$count,'_',agStat$nonCens)
  agStat$dup=duplicated(agStat$key)
  agStat%>%filter(key%in%agStat$key[agStat$dup])%>%arrange(key)%>%as.data.frame()
  cat(agency,'\t',sum(agStat$dup),'\n')
}
  
  
#WRC
wrcScram = wqdata%>%filter(LawaSiteID%in%c("ew-00009","ew-00066"),Measurement=='NH4')
#Duplicates confrimed on server


#HRC
#resolved by repull


#ecan
#e.g. ecan-00128 NO3N and ecan-00128 TON
#e.g. lawa-102822 DIN and lawa-102822 TN

agStat%>%filter(key%in%agStat$key[agStat$dup],!meas%in%c("NO3N","TON"))%>%arrange(key)








mfl%>%mutate(sitemeas=paste(CouncilSiteID,Measurement,sep='_'))%>%
  group_by(sitemeas)%>%dplyr::summarise(meas=unique(Measurement),
                                        valMean=round(mean(Value,na.rm=T),6),
                                        valSD=round(sd(Value,na.rm=T),6),
                                        count=n(),
                                        nonCens = sum(!Censored))%>%
  filter(count>1)%>%
  ungroup->agStat
agStat$key = paste0(agStat$valMean,'_',agStat$valSD,'-',agStat$count,'_',agStat$nonCens)
agStat$dup=duplicated(agStat$key)
agStat%>%filter(key%in%agStat$key[agStat$dup])%>%filter(!meas%in%c("NO3N","TON"))%>%arrange(key)%>%as.data.frame()







wqdata <- merge(wqdata,
                siteTable%>%select(CouncilSiteID,LawaSiteID))
