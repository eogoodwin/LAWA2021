
#                 Export files to recreate these plots ####


#DIN ####
DINLUexport <- NOFlatest%>%select(Region,LandCover=gRecLC,DIN_Summary_Band)%>%
  filter(!DIN_Summary_Band%in%c("","NA")&!is.na(DIN_Summary_Band))
DINLUexport <- DINLUexport%>%group_by(Region,LandCover)%>%dplyr::summarise(.groups='keep',
                                                                           A=sum(DIN_Summary_Band=="A"),
                                                                           B=sum(DIN_Summary_Band=="B"),
                                                                           C=sum(DIN_Summary_Band=="C"),
                                                                           D=sum(DIN_Summary_Band=="D"))%>%ungroup
DINLUexport <- DINLUexport%>%pivot_longer(cols = A:D,names_to = 'Band',values_to = 'Count')%>%
  mutate(Indicator="DIN")%>%select(Region,LandCover,Indicator,Band,Count)


#NO3 ####
NOxLUexport <- NOFlatest%>%select(Region,LandCover=gRecLC,Nitrate_Toxicity_Band)%>%
  filter(!Nitrate_Toxicity_Band%in%c("","NA")&!is.na(Nitrate_Toxicity_Band))
NOxLUexport <- NOxLUexport%>%group_by(Region,LandCover)%>%dplyr::summarise(.groups='keep',
                                                                           A=sum(Nitrate_Toxicity_Band=="A"),
                                                                           B=sum(Nitrate_Toxicity_Band=="B"),
                                                                           C=sum(Nitrate_Toxicity_Band=="C"),
                                                                           D=sum(Nitrate_Toxicity_Band=="D"))%>%ungroup
NOxLUexport <- NOxLUexport%>%pivot_longer(cols = A:D,names_to = 'Band',values_to = 'Count')%>%
  mutate(Indicator="NO3N")%>%select(Region,LandCover,Indicator,Band,Count)

#DRP ####
DRPLUexport <- NOFlatest%>%select(Region,LandCover=gRecLC,DRP_Summary_Band)%>%
  filter(!DRP_Summary_Band%in%c("","NA")&!is.na(DRP_Summary_Band))
DRPLUexport <- DRPLUexport%>%group_by(Region,LandCover)%>%dplyr::summarise(.groups='keep',
                                                                           A=sum(DRP_Summary_Band=="A"),
                                                                           B=sum(DRP_Summary_Band=="B"),
                                                                           C=sum(DRP_Summary_Band=="C"),
                                                                           D=sum(DRP_Summary_Band=="D"))%>%ungroup
DRPLUexport <- DRPLUexport%>%pivot_longer(cols = A:D,names_to = 'Band',values_to = 'Count')%>%
  mutate(Indicator="DRP")%>%select(Region,LandCover,Indicator,Band,Count)
#ECOLI ####
ECOLILUexport <- NOFlatest%>%select(Region,LandCover=gRecLC,EcoliSummaryband)%>%
  filter(!EcoliSummaryband%in%c("","NA")&!is.na(EcoliSummaryband))
ECOLILUexport <- ECOLILUexport%>%group_by(Region,LandCover)%>%dplyr::summarise(.groups='keep',
                                                                               A=sum(EcoliSummaryband=="A"),
                                                                               B=sum(EcoliSummaryband=="B"),
                                                                               C=sum(EcoliSummaryband=="C"),
                                                                               D=sum(EcoliSummaryband=="D"),
                                                                               E=sum(EcoliSummaryband=="E"))%>%ungroup
ECOLILUexport <- ECOLILUexport%>%pivot_longer(cols = A:E,names_to = 'Band',values_to = 'Count')%>%
  mutate(Indicator="ECOLI")%>%select(Region,LandCover,Indicator,Band,Count)
#NH4 ####
NH4LUexport <- NOFlatest%>%select(Region,LandCover=gRecLC,Ammonia_Toxicity_Band)%>%
  filter(!Ammonia_Toxicity_Band%in%c("","NA")&!is.na(Ammonia_Toxicity_Band))
NH4LUexport <- NH4LUexport%>%group_by(Region,LandCover)%>%dplyr::summarise(.groups='keep',
                                                                           A=sum(Ammonia_Toxicity_Band=="A"),
                                                                           B=sum(Ammonia_Toxicity_Band=="B"),
                                                                           C=sum(Ammonia_Toxicity_Band=="C"),
                                                                           D=sum(Ammonia_Toxicity_Band=="D"))%>%ungroup
NH4LUexport <- NH4LUexport%>%pivot_longer(cols = A:D,names_to = 'Band',values_to = 'Count')%>%
  mutate(Indicator="NH4")%>%select(Region,LandCover,Indicator,Band,Count)
#Sed ####
SedLUexport <- NOFlatest%>%select(Region,LandCover=gRecLC,SusSedBand)%>%
  filter(!SusSedBand%in%c("","NA")&!is.na(SusSedBand))
SedLUexport <- SedLUexport%>%group_by(Region,LandCover)%>%dplyr::summarise(.groups='keep',
                                                                           A=sum(SusSedBand=="A"),
                                                                           B=sum(SusSedBand=="B"),
                                                                           C=sum(SusSedBand=="C"),
                                                                           D=sum(SusSedBand=="D"))%>%ungroup
SedLUexport <- SedLUexport%>%pivot_longer(cols = A:D,names_to = 'Band',values_to = 'Count')%>%
  mutate(Indicator="SusSed")%>%select(Region,LandCover,Indicator,Band,Count)

#MCI ####
MCILUexport <- MCINOF%>%filter(sYear==2020)%>%drop_na(gRecLC)%>%select(Region,LandCover=gRecLC,MCIband)%>%
  filter(!MCIband%in%c("","NA")&!is.na(MCIband))
MCILUexport <- MCILUexport%>%group_by(Region,LandCover)%>%dplyr::summarise(.groups='keep',
                                                                           A=sum(MCIband=="A"),
                                                                           B=sum(MCIband=="B"),
                                                                           C=sum(MCIband=="C"),
                                                                           D=sum(MCIband=="D"))%>%ungroup
MCILUexport <- MCILUexport%>%pivot_longer(cols=A:D,names_to = 'Band',values_to = 'Count')%>%
  mutate(Indicator="MCI")%>%select(Region,LandCover,Indicator,Band,Count)
#Combine and export ####

NOFLUexport <- do.call(rbind,list(NOxLUexport,DRPLUexport,ECOLILUexport,NH4LUexport,SedLUexport,MCILUexport))

rm(NOxLUexport,DRPLUexport,ECOLILUexport,NH4LUexport,SedLUexport,MCILUexport)
write.csv(NOFLUexport,paste0('H:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/',format(Sys.Date(),"%Y-%m-%d"),
                             '/DownloadDataFiles/NOFResultsLatestByLandUseAndRegion.csv'),row.names=F)
#spaceer ####





#                      Now the time history one ####

#DIN ####
DINNOFs = NOFSummaryTable%>%drop_na(DIN_Summary_Band)%>%filter(!DIN_Summary_Band%in%c("","NA"))%>%
  select(-starts_with(c('Ecoli','Ammonia','Nitra'),ignore.case = T))%>%
  dplyr::rename(Band=DIN_Summary_Band,LandCover=gRecLC)%>%
  distinct%>%group_by(LawaSiteID)%>%
  dplyr::mutate(nY=length(unique(Year)),Year=strFrom(Year,'to'))%>%filter(nY==10)%>%ungroup%>%
  group_by(Region,LandCover,Year)%>%dplyr::summarise(.groups='keep',A=sum(Band=="A"),
                                                     B=sum(Band=="B"),
                                                     C=sum(Band=="C"),
                                                     D=sum(Band=="D"))%>%ungroup%>%
  pivot_longer(cols=A:D,names_to='Band',values_to='Count')%>%
  mutate(Indicator="DIN")%>%
  dplyr::select(Region,LandCover,Indicator,Year,Band,Count)

#NO3N ####
NOxNOFs = NOFSummaryTable%>%drop_na(Nitrate_Toxicity_Band)%>%filter(!Nitrate_Toxicity_Band%in%c("","NA"))%>%
  select(-starts_with(c('Ecoli','Ammonia'),ignore.case = T))%>%
  dplyr::rename(Band=Nitrate_Toxicity_Band,LandCover=gRecLC)%>%
  distinct%>%group_by(LawaSiteID)%>%
  dplyr::mutate(nY=length(unique(Year)),Year=strFrom(Year,'to'))%>%filter(nY==10)%>%ungroup%>%
  group_by(Region,LandCover,Year)%>%dplyr::summarise(.groups='keep',A=sum(Band=="A"),
                                                     B=sum(Band=="B"),
                                                     C=sum(Band=="C"),
                                                     D=sum(Band=="D"))%>%ungroup%>%
  pivot_longer(cols=A:D,names_to='Band',values_to='Count')%>%
  mutate(Indicator="NO3N")%>%
  dplyr::select(Region,LandCover,Indicator,Year,Band,Count)
#DRP ####
DRPNOFs = NOFSummaryTable%>%drop_na(DRP_Summary_Band)%>%filter(!DRP_Summary_Band%in%c("","NA"))%>%
  select(-starts_with(c('Ecoli','Ammonia','Nitra'),ignore.case = T))%>%
  dplyr::rename(Band=DRP_Summary_Band,LandCover=gRecLC)%>%
  distinct%>%group_by(LawaSiteID)%>%
  dplyr::mutate(nY=length(unique(Year)),Year=strFrom(Year,'to'))%>%filter(nY==10)%>%ungroup%>%
  group_by(Region,LandCover,Year)%>%dplyr::summarise(.groups='keep',A=sum(Band=="A"),
                                                     B=sum(Band=="B"),
                                                     C=sum(Band=="C"),
                                                     D=sum(Band=="D"))%>%ungroup%>%
  pivot_longer(cols=A:D,names_to='Band',values_to='Count')%>%
  mutate(Indicator="DRP")%>%
  dplyr::select(Region,LandCover,Indicator,Year,Band,Count)
#ECOLI ####
ECOLINOFs = NOFSummaryTable%>%drop_na(EcoliSummaryband)%>%filter(!EcoliSummaryband%in%c("","NA"))%>%
  select(-starts_with(c('DRP','Ammonia','Nitra'),ignore.case = T))%>%
  dplyr::rename(Band=EcoliSummaryband,LandCover=gRecLC)%>%
  distinct%>%group_by(LawaSiteID)%>%
  dplyr::mutate(nY=length(unique(Year)),Year=strFrom(Year,'to'))%>%filter(nY==10)%>%ungroup%>%
  group_by(Region,LandCover,Year)%>%dplyr::summarise(.groups='keep',
                                                     A=sum(Band=="A"),
                                                     B=sum(Band=="B"),
                                                     C=sum(Band=="C"),
                                                     D=sum(Band=="D"),
                                                     E=sum(Band=="E"))%>%ungroup%>%
  pivot_longer(cols=A:E,names_to='Band',values_to='Count')%>%
  mutate(Indicator="ECOLI")%>%
  dplyr::select(Region,LandCover,Indicator,Year,Band,Count)
#Sediment ####
SedNOFs = NOFSummaryTable%>%drop_na(SusSedBand)%>%filter(!SusSedBand%in%c("","NA"))%>%
  select(-starts_with(c('Ecoli','DRP','Nitra'),ignore.case = T))%>%
  dplyr::rename(Band=SusSedBand,LandCover=gRecLC)%>%
  distinct%>%group_by(LawaSiteID)%>%
  dplyr::mutate(nY=length(unique(Year)),Year=strFrom(Year,'to'))%>%filter(nY==10)%>%ungroup%>%
  group_by(Region,LandCover,Year)%>%dplyr::summarise(.groups='keep',A=sum(Band=="A"),
                                                     B=sum(Band=="B"),
                                                     C=sum(Band=="C"),
                                                     D=sum(Band=="D"))%>%ungroup%>%
  pivot_longer(cols=A:D,names_to='Band',values_to='Count')%>%
  mutate(Indicator="SusSed")%>%
  dplyr::select(Region,LandCover,Indicator,Year,Band,Count)
#NH4 ####
NH4NOFs = NOFSummaryTable%>%drop_na(Ammonia_Toxicity_Band)%>%filter(!Ammonia_Toxicity_Band%in%c("","NA"))%>%
  select(-starts_with(c('Ecoli','DRP','Nitra'),ignore.case = T))%>%
  dplyr::rename(Band=Ammonia_Toxicity_Band,LandCover=gRecLC)%>%
  distinct%>%group_by(LawaSiteID)%>%
  dplyr::mutate(nY=length(unique(Year)),Year=strFrom(Year,'to'))%>%
  filter(nY==10)%>%ungroup%>%
  group_by(Region,LandCover,Year)%>%dplyr::summarise(.groups='keep',A=sum(Band=="A"),
                                                     B=sum(Band=="B"),
                                                     C=sum(Band=="C"),
                                                     D=sum(Band=="D"))%>%ungroup%>%
  pivot_longer(cols=A:D,names_to='Band',values_to='Count')%>%
  mutate(Indicator="Ammonia")%>%
  dplyr::select(Region,LandCover,Indicator,Year,Band,Count)
#MCI ####

MCINOFsexp = MCINOF%>%filter(sYear>=2011,!MCIband%in%c("","NA"))%>%drop_na(MCIband)%>%
  # distinct%>%
  group_by(LawaSiteID)%>%
  dplyr::mutate(nY=length(unique(sYear)))%>%filter(nY==10)%>%ungroup%>%
  dplyr::rename(Band=MCIband,LandCover=gRecLC)%>%droplevels%>%
  group_by(Region,LandCover,sYear)%>%dplyr::summarise(.groups='keep',
                                                      A=sum(Band=="A"),
                                                      B=sum(Band=="B"),
                                                      C=sum(Band=="C"),
                                                      D=sum(Band=="D"))%>%ungroup%>%
  pivot_longer(cols=A:D,names_to='Band',values_to='Count')%>%
  mutate(Indicator="MCI")%>%
  dplyr::select(Region,LandCover,Indicator,Year=sYear,Band,Count)
#Combine and export ####
NOFHistoryExport <- do.call(rbind,list(NOxNOFs,DRPNOFs,ECOLINOFs,SedNOFs,NH4NOFs,MCINOFsexp))
write.csv(NOFHistoryExport,
          paste0('H:/ericg/16666LAWA/LAWA2021/NationalPicture/SWQ/',format(Sys.Date(),"%Y-%m-%d"),
                 '/DownloadDataFiles/NOFResultsHistoryCompleteSites.csv'),row.names=F)

#spacer ####


