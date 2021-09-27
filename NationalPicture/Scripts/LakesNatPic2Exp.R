

#Export files to recreate these plots ####
if(0){
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
  write.csv(NOFLTypeexport,paste0('C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/LakesNationalPicture/',
                                  'NOFResultsLatestByRegion.csv'),row.names=F)
}

#Now the time history one ####
NitrNOFs = NOFSummaryTable%>%drop_na(NitrogenMed_Band)%>%filter(NitrogenMed_Band!="NA")%>%
  select(-starts_with(c('Ecoli','Ammonia','Nitra'),ignore.case = T))%>%
  dplyr::rename(Band=NitrogenMed_Band,LakeType=LType)%>%
  group_by(LawaSiteID)%>%dplyr::mutate(nY=length(unique(Year)),Year=strFrom(Year,'to'))%>%filter(nY==10)%>%ungroup%>%
  group_by(Region,Year)%>%dplyr::summarise(.groups='keep',A=sum(Band=="A"),
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
  group_by(Region,Year)%>%dplyr::summarise(.groups='keep',A=sum(Band=="A"),
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
  group_by(Region,Year)%>%dplyr::summarise(.groups='keep',A=sum(Band=="A"),
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
  group_by(Region,Year)%>%dplyr::summarise(.groups='keep',A=sum(Band=="A"),
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
  group_by(Region,Year)%>%dplyr::summarise(.groups='keep',A=sum(Band=="A"),
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
  group_by(Region,Year)%>%dplyr::summarise(.groups='keep',A=sum(Band=="A"),
                                           B=sum(Band=="B"),
                                           C=sum(Band=="C"),
                                           D=sum(Band=="D"),
                                           E=sum(Band=="E"))%>%ungroup%>%
  pivot_longer(cols=A:E,names_to='Band',values_to='Count')%>%
  mutate(Indicator="ECOLI")%>%
  dplyr::select(Region,Indicator,Year,Band,Count)

NOFHistoryExport <- do.call(rbind,list(NitrNOFs,PhosNOFs,NH4NOFs,ClarNOFs,ChlNOFs,ECOLINOFs))
write.csv(NOFHistoryExport,paste0('C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021 - project team collab/LakesNationalPicture/',
                                  'NOFResultsHistoryCompleteSites.csv'),row.names=F)
rm(NitrNOFs,PhosNOFs,NH4NOFs,ClarNOFs,ChlNOFs,ECOLINOFs)
