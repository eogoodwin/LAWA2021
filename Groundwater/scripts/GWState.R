#Groudnwater state analysis
rm(list=ls())
library(tidyverse)
source('H:/ericg/16666LAWA/LAWA2021/Scripts/LAWAFunctions.R')
source('H:/ericg/16666LAWA/LWPTrends_v2101/LWPTrends_v2101.R')
EndYear <- 2020#year(Sys.Date())-1
startYear5 <- EndYear - 5+1
startYear10 <- EndYear - 10+1
startYear15 <- EndYear - 15+1
plotto=F
applyDataAbundanceFilters=F




dir.create(paste0("H:/ericg/16666LAWA/LAWA2021/Groundwater/Data/", format(Sys.Date(),"%Y-%m-%d")),showWarnings = F)

# GWdata = readxl::read_xlsx(paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021/Groundwater Quality/",
#                                   "GWExport_20210914.xlsx"),sheet=1,guess_max = 50000)%>%
#   filter(Variable_aggregated%in%c("Nitrate nitrogen","Chloride",
#                                   "Dissolved reactive phosphorus",
#                                   "Electrical conductivity/salinity",
#                                   "E.coli","Ammoniacal nitrogen"))%>%as.data.frame
GWdata = readxl::read_xlsx(tail(dir(path="H:/ericg/16666LAWA/LAWA2021/Groundwater/Data/",
                                    pattern="GWExport_.*xlsx",full.names = T,recursive = T),1),
                           sheet=1,guess_max = 50000)%>%
  filter(Variable_aggregated%in%c("Nitrate nitrogen","Chloride",
                                  "Dissolved reactive phosphorus",
                                  "Electrical conductivity/salinity",
                                  "E.coli","Ammoniacal nitrogen"))%>%as.data.frame

siteTab=GWdata%>%drop_na(Site_ID)%>%select(Source,Site_ID,LAWA_ID,RC_ID,Latitude,Longitude)%>%distinct

these=which(is.na(GWdata$Site_ID))
if(length(these)>0)GWdata$Site_ID[these]=siteTab$Site_ID[match(GWdata$LAWA_ID[these],siteTab$LAWA_ID)]
these=which(is.na(GWdata$RC_ID))
if(length(these)>0)GWdata$RC_ID[these]=siteTab$RC_ID[match(GWdata$LAWA_ID[these],siteTab$LAWA_ID)]
these=which(is.na(GWdata$Latitude))
if(length(these)>0)GWdata$Latitude[these]=siteTab$Latitude[match(GWdata$LAWA_ID[these],siteTab$LAWA_ID)]
these=which(is.na(GWdata$Longitude))
if(length(these)>0)GWdata$Longitude[these]=siteTab$Longitude[match(GWdata$LAWA_ID[these],siteTab$LAWA_ID)]
rm(these)
#206482 of 17 4-8-20
#234751 of 17 10-8-20
#254451  14-8-20
#262451 24-8-20
#261854 28-8-20
#262749 04-9-20
#216873 14-9-20
#197088 3/8/21
#194562 6/8/21
#215206 13/8/21
#208293 20/8/21
#213592 27/8/21 
#212767 03/09/21

GWdata%>%split(f=.$Source)%>%purrr::map(.f = function(x)any(apply(x,2,FUN=function(y)any(grepl('<|>',y,ignore.case=T)))))
#ECAN GDC HBRC HRC MDC ORC ES TRC TDC GWRC WCRC do
#AC BOPRC NCC NRC WRC dont

#Auckland censoring info
#PK at WRC says bit posns 13 and 14 are < and >
table(bitwAnd(2^13,as.numeric(GWdata$Qualifier))==2^13)
table(GWdata$Source,bitwAnd(2^14,as.numeric(GWdata$Qualifier))==2^14)

if(bitwAnd(2^13,as.numeric(GWdata$Qualifier))==2^13){}
if(bitwAnd(2^14,as.numeric(GWdata$Qualifier))==2^14){}

CensLeft = which(bitwAnd(2^14,as.numeric(GWdata$Qualifier))==2^14)  
CensRight = which(bitwAnd(2^13,as.numeric(GWdata$Qualifier))==2^13)  

if(length(CensLeft)>0){
with(GWdata[CensLeft,],table(Variable_aggregated,`Result-raw`))
GWdata$`Result-raw`[CensLeft]=paste0('<',GWdata$`Result-raw`[CensLeft])
GWdata$`Result-prefix`[CensLeft]='<'
}
rm(CensLeft)

if(length(CensRight)>0){
  GWdata$`Result-raw`[CensRight]=paste0('>',GWdata$`Result-raw`[CensLeft])
GWdata$`Result-prefix`[CensLeft]='>'
}
rm(CensRight)



#6/3/20
#CHeck BOPRC for censored ecoli
GWdata%>%filter(Source=="Bay of Plenty")%>%grepl(pattern = '<',x = .$`Result-prefix`)%>%sum
if(0){
#BOP censoring should be indicated in the result-prefix column
bopcens = readxl::read_xlsx('h:/ericg/16666LAWA/LAWA2021/Groundwater/Data/BOPRC E coli QT datasets.xlsx',sheet = 2)%>%
  dplyr::rename(RC_ID=Site,'Result-prefix'=Qualifiers)%>%
  mutate(Date=Time-hours(12))%>%
  select(RC_ID,Date,`Result-prefix`)
bopcens$`Result-prefix` = as.character(factor(bopcens$`Result-prefix`,levels=c("<DL",">DL"),labels=c('<','>')))
bopdat=GWdata%>%filter(Source=='Bay of Plenty'&Variable_aggregated=="E.coli")%>%select(-`Result-prefix`)
bopdat = left_join(bopdat,bopcens,by=c("RC_ID","Date"))
rm(bopcens)
GWdata=full_join(GWdata%>%filter(!(Source=='Bay of Plenty'&Variable_aggregated=="E.coli")),bopdat)%>%arrange(Source)
rm(bopdat)

# 3/9/20
GWdata%>%filter(Source=="Waikato")%>%grepl(pattern = '<',x = .$`Result-prefix`)%>%sum
waikcens = readxl::read_xlsx('h:/ericg/16666LAWA/LAWA2021/Groundwater/Data/E coli BOP and Waikato.xlsx',sheet=1)%>%
  dplyr::filter(Region=="Waikato")%>%
  dplyr::rename(LawaSiteID=`Lawa ID`,Date=`Result-date`)%>%
  select(LawaSiteID,Date,`Result-prefix`)%>%
  arrange(Date)
waikdat=GWdata%>%filter(Source=='Waikato'&Variable_aggregated=="E.coli")%>%select(-`Result-prefix`)%>%arrange(Date)
waikdat$`Result-prefix` <- waikcens$`Result-prefix`
rm(waikcens)
GWdata <- full_join(GWdata%>%filter(!(Source=="Waikato"&Variable_aggregated=="E.coli")),waikdat)%>%arrange(Source)
rm(waikdat)
}


c("Nitrate nitrogen",
  "Chloride",
  "Dissolved reactive phosphorus",
  "Electrical conductivity/salinity",
  "E.coli",
  "Ammoniacal nitrogen")

# This copied from SWQ, needs fixing up to GW context

  #Censor the data there that came in.
  # Datasource or Parameter Type	Measurement or Timeseries Name	Units	AQUARIUS Parameter	              Detection Limit
  # Total Oxidised Nitrogen	      Total Oxidised Nitrogen	        g/m3	Nitrite Nitrate _as N__LabResult	0.001
  # Total Nitrogen	              Total Nitrogen	                g/m3	N _Tot__LabResult	                0.01
  # Ammoniacal Nitrogen	          Ammoniacal Nitrogen	            g/m3	Ammoniacal N_LabResult	          0.002
  # Dissolved Reactive Phosphorus	DRP	                            g/m3	DRP_LabResult	                    0.001
  # Total Phosphorus	            TP	                            g/m3	P _Tot__LabResult	                0.001
  # Turbidity	                    Turbidity	                      NTU	  Turbidity, Nephelom_LabResult	    0.1
  # pH	                          pH  	                      pH units	pH_LabResult	                    0.2
  # Visual Clarity	              BDISC	                            m	  Water Clarity_LabResult	          0.01
  # Escherichia coli	            Ecoli	                      /100 ml	  E coli_LabResult	                1
  # These next from Lisa N 15/9/2021
  # Chloride	                                                          Cl _Dis__LabResult	              0.5 g/m3  value from Paul Scholes email
  # Nitrate nitrogen	                                                 Nitrate _N__LabResult	            0.001
  # Nitrogen - Other	                                                 Nitrite _as N__LabResult	          0.001
  # Dissolved reactive phosphorus	                                     DRP_LabResult	                    0.001
  # Electrical conductivity/salinity                                   Conductivity_LabResult	            0.001
  # Ammoniacal nitrogen	                                               Ammoniacal N_LabResult	            0.002
  # E.coli	                                                           E coli QT_LabResult	              1
  # Nitrogen - Other                                                   N _Tot__LabResult	                0.01
  # Nitrogen - Other	                                                 Nitrite Nitrate _as N__LabResult	  0.001

  cenNH4 = which(GWdata$Source=='Bay of Plenty'&
                   GWdata$Variable_aggregated=="Ammoniacal nitrogen"&
                   as.numeric(GWdata$`Result-raw`)<0.002)
  cenCL = which(GWdata$Source=='Bay of Plenty'&
                  GWdata$Variable_aggregated=="Chloride"&
                  as.numeric(GWdata$`Result-raw`<0.5))
  cenDRP = which(GWdata$Source=='Bay of Plenty'&
                   GWdata$Variable_aggregated=="Dissolved reactive phosphorus"&
                   as.numeric(GWdata$`Result-raw`)<0.001)
  cenCond = which(GWdata$Source=='Bay of Plenty'&
                  GWdata$Variable_aggregated=="Electrical conductivity/salinity"&
                  as.numeric(GWdata$`Result-raw`<0.001))
  cenNit = which(GWdata$Source=='Bay of Plenty'&
                  GWdata$Variable_aggregated=="Nitrate nitrogen"&
                  as.numeric(GWdata$`Result-raw`<0.001))
  cenECOLI=which(GWdata$Source=='Bay of Plenty'&
                   GWdata$Variable_aggregated=="E.coli"&
                   as.numeric(GWdata$`Result-raw`)<1)

  if(length(cenNH4)>0){
    GWdata$`Result-raw`[cenNH4] <- '<0.002'
    GWdata$`Result-prefix`[cenNH4] <- '<'
    GWdata$`Result-edited`[cenNH4] <- 0.002
  }
  if(length(cenCL)>0){
    GWdata$`Result-raw`[cenCL] <- '<0.5'
    GWdata$`Result-prefix`[cenCL] <- '<'
    GWdata$`Result-edited`[cenCL] <- 0.5
  }
  if(length(cenDRP)>0){
    GWdata$`Result-raw`[cenDRP] <- '<0.001'
    GWdata$`Result-prefix`[cenDRP] <- '<'
    GWdata$`Result-edited`[cenDRP] <- 0.001
  }
  if(length(cenCond)>0){
    GWdata$`Result-raw`[cenCond] <- '<0.001'
    GWdata$`Result-prefix`[cenCond] <- '<'
    GWdata$`Result-edited`[cenCond] <- 0.001
  }
  if(length(cenNit)>0){
    GWdata$`Result-raw`[cenNit] <- '<0.001'
    GWdata$`Result-prefix`[cenNit] <- '<'
    GWdata$`Result-edited`[cenNit] <- 0.001
  }
  if(length(cenECOLI)>0){
    GWdata$`Result-raw`[cenECOLI] <- '<1'
    GWdata$`Result-prefix`[cenECOLI] <- '<'
    GWdata$`Result-edited`[cenECOLI] <- 1
  }
  
  
  rm(cenNH4,cenDRP,cenECOLI,cenCL,cenCond,cenNit)



GWdata <- GWdata%>%mutate(LawaSiteID=`LAWA_ID`,
                          Measurement=`Variable_aggregated`,
                          Region=`Source`,
                          # Date = `Date`,
                          Value=`Result-edited`,
                          siteMeas=paste0(LAWA_ID,'.',Variable_aggregated))

#Drop QC-flagged problems
stopifnot(all(bitwAnd(as.numeric(GWdata$Qualifier),255)%in%c(10,30,42,43,151,NA)))  #See email from Vanitha Pradeep 13-8-2021
table(GWdata$Source[!bitwAnd(as.numeric(GWdata$Qualifier),255)%in%c(10,30,42,43,151,NA)])
GWdata$Qualifier = bitwAnd(as.numeric(GWdata$Qualifier),255)
GWdata <- GWdata%>%dplyr::filter(!Qualifier%in%c(42,151))  #42 means poor quality, 151 means missing
#206482 of 22 4-8-20
#234751 of 22 10-8-20
#254451        14-8-20
#262432 of 23 24-8-20
#261832 of 23 28-8-20
#262727  4/9/20
#263248 of 23 14/9/2021
#197063 of 23 3/8/2021
#194537 6/8/21
#215184 13/8/21
#208268 20/8/21
#213567 27/8/21
#212742 3/9/21


noActualData = which(is.na(GWdata$Site_ID)&is.na(GWdata$`Result-raw`)&is.na(GWdata$Date))
if(length(noActualData)>0){
  GWdata <- GWdata[-noActualData,]
}
rm(noActualData) #215176
GWdata <- GWdata%>%distinct
#214254 of 31 11-7
#212401 of 31 11-14
#225242 of 22 11-22
#225898 of 22 11-25
#228933 of 22 13-3-2021
#229104 of 22 20-3-20
#229050 of 22 27-3-20
#206382 of 22 04/08/20
#234621 of 22 10-8-20
#254315     14-8-20
#262225 of 23 24-8-20  #qualifier column added
#261775 of 23 28-8-20
#262670  4/9/20
#216665 14-9-20
#196964 3/8/21
#194437 6/8/21
#215097 13/8/21
#208174 20/8/21
#213480 27/8/21
#212655 03/09/21

GWdata$Value[which(GWdata$`Result-prefix`=='<')] <- GWdata$`Result-edited`[which(GWdata$`Result-prefix`=='<')]*0.5
GWdata$Value[which(GWdata$`Result-prefix`=='>')] <- GWdata$`Result-edited`[which(GWdata$`Result-prefix`=='>')]*1.1

if(plotto){
  table(GWdata$Value==GWdata$`Result-edited`)
  table(GWdata$`Result-prefix`==""|is.na(GWdata$`Result-prefix`))
  GWdata[which(GWdata$`Result-prefix`!="" & GWdata$Value==GWdata$`Result-edited`),]
}

GWdata$Censored=FALSE
GWdata$Censored[grepl(pattern = '^[<|>]',GWdata$`Result-prefix`)]=TRUE
GWdata$CenType='not'
GWdata$CenType[grepl(pattern = '^<',GWdata$`Result-prefix`)]='lt'
GWdata$CenType[grepl(pattern = '^>',GWdata$`Result-prefix`)]='gt'


if(plotto){
table(GWdata$Source,GWdata$CenType)
  table(GWdata$Region,GWdata$Measurement)
}


#Conductivity is in different units ####
table(GWdata$Variable_units[GWdata$Variable_aggregated=="Electrical conductivity/salinity"])
table(GWdata$Variable_units[GWdata$Variable_aggregated=="Electrical conductivity/salinity"],
      GWdata$Source[GWdata$Variable_aggregated=="Electrical conductivity/salinity"])
par(mfrow=c(2,1),mar=c(10,4,4,2))
with(GWdata[GWdata$Variable_aggregated=="Electrical conductivity/salinity"&GWdata$Value>0,],
     plot(as.factor(Variable_units),(Value),log='y',las=2))
#Set all units to S/m
these = which(GWdata$Variable_units%in%c('µS/cm','us/cm','uS/cm'))
GWdata$Variable_units[these] <- 'µS/cm'

# these = which(GWdata$Variable_units%in%c('mS/cm','ms/cm'))
# GWdata$Variable_units[these] <- 'µS/cm'
# GWdata$Value[these] = GWdata$Value[these]*1000

these = which(GWdata$Variable_units=='mS/m')
GWdata$Variable_units[these] <- 'µS/cm'
GWdata$Value[these] = GWdata$Value[these]*1000/100

these = which(GWdata$Variable_units%in%c('mS/m @25 deg C','mS/m @25°C','ms/m@25C','mS/m@20C','mS/m@25C'))
GWdata$Variable_units[these] <- 'µS/cm'
GWdata$Value[these] = GWdata$Value[these]*1000/100

with(GWdata[GWdata$Variable_aggregated=="Electrical conductivity/salinity"&GWdata$Value>0,],
     plot(as.factor(Variable_units),Value,log='y',las=2))
mtext(side = 1,text='µS/cm')



#FreqCheck expects a one called "Date" ####
GWdata$myDate <- as.Date(as.character(GWdata$Date))
GWdata <- GetMoreDateInfo(GWdata)
GWdata$monYear = base::format.Date(GWdata$myDate,"%b-%Y")
GWdata$quYear = paste0(quarters(GWdata$myDate),'-',base::format.Date(GWdata$myDate,'%Y'))

write.csv(GWdata,paste0('h:/ericg/16666LAWA/LAWA2021/Groundwater/Data/',
                        format(Sys.Date(),'%Y-%m-%d'),'/GWdata.csv'),row.names=F)
# GWdata = read_csv(tail(dir('h:/ericg/16666LAWA/LAWA2021/Groundwater/Data/','GWdata.csv',recursive=T,full.names=T),1),guess_max = 5000)



#STATE ####
#Carl Hanson 11/9/2019:  We only need state and trend for five parameters: 
#    nitrate nitrogen, chloride, DRP, electrical conductivity and E. coli.
periodYrs=5
GWdataRelevantVariables <- GWdata%>%
  filter(Measurement%in%c("Nitrate nitrogen","Chloride",
                          "Dissolved reactive phosphorus",
                          "Electrical conductivity/salinity",
                          "E.coli","Ammoniacal nitrogen"))%>%
  filter(`Result-raw`!='*')%>%         #This excludes non-results discussed by Carl Hanson by email April 2020
  dplyr::filter(lubridate::year(myDate)>=(EndYear-periodYrs+1)&lubridate::year(myDate)<=EndYear)%>%  
  select(-'Result-raw',-'Result-metadata',-'Variable')%>%
  dplyr::group_by(siteMeas,monYear)%>%  #Month and year
  dplyr::summarise(.groups='keep',
                   LawaSiteID=unique(LawaSiteID),
                   Measurement=unique(Measurement),
                   Year=unique(Year),
                   Qtr=unique(Qtr),
                   Month=unique(Month),
                   Value=median(Value,na.rm=T),
                   Date=first(Date,1),
                   myDate=first(myDate,1),
                   LcenLim = suppressWarnings({max(`Result-edited`[`Result-prefix`=='<'],na.rm=T)}),
                   RcenLim = suppressWarnings({min(`Result-edited`[`Result-prefix`=='>'],na.rm=T)}),
                   CenType = ifelse(Value<LcenLim,'lt',ifelse(Value>RcenLim,'gt',NA)))%>%
  ungroup%>%distinct
#68163
#32382 3/8/21
#65305 13/8/21
#62173 20/8/21
#64958 27/8/21
#64979

freqs <- split(x=GWdataRelevantVariables,
               f=GWdataRelevantVariables$siteMeas)%>%purrr::map(~freqCheck(.))%>%unlist
table(freqs)
# freqs
# bimonthly    monthly quarterly     
#        19       190       3547   11-14
#        37       196       3649   11-22
#        36       192       3829   11-25
#        36       192       4000   03-06-2021
#        36       180       3992   03-13-2021
#        32       180       3917   03-20-2021
#        35       180       3994   03-27-2021
#        35       180       3993   04-23-2021
#        38       130       4151   04/08/2021
#        43       130       4880    10/8/20
#        43       134       5229    14-8-20
#        44       134       5349    24-8-20
#        43       134       5355    28-8-20
#        42       134       5356
#        42       134       5366    14-9-20
#        14       58        3041     3-8-21
#        32       65        4758     6-8-21
#        32       77        5245    13-8-21
#        32       74        5004    20-8-21
#        32       75        5214    27/8/21
#        32       75        5221    3/9/21
GWdataRelevantVariables$Frequency=freqs[GWdataRelevantVariables$siteMeas]  
rm(freqs)

GWdataRelevantVariables$CenBin = 0
GWdataRelevantVariables$CenBin[GWdataRelevantVariables$CenType=='lt'] = 1
GWdataRelevantVariables$CenBin[GWdataRelevantVariables$CenType=='gt'] = 2



#Calculate state (median) ####
#Get a median value per site/Measurement combo
GWmedians <- GWdataRelevantVariables%>%
  group_by(LawaSiteID,Measurement)%>%
  dplyr::summarise(.groups='keep',
                   median=quantile(Value,probs = 0.5,type=5,na.rm=T),
                   MAD = quantile(abs(median-Value),probs=0.5,type=5,na.rm=T),
                   count = n(),
                   minPerYear = min(as.numeric(table(factor(as.character(lubridate::year(myDate)),
                                                            levels=as.character(startYear5:EndYear))))),  
                   nYear = length(unique(Year)),
                   nQuart=length(unique(Qtr)),
                   nMonth=length(unique(Month)),
                   # censoredCount = sum(!is.na(CenType)),
                   censoredCount = sum(Value<=(0.5*LcenLim) | Value>=(1.1*RcenLim)),
                   CenType = Mode(CenBin),
                   Frequency=unique(Frequency))%>%
  ungroup%>%
  mutate(censoredPropn = censoredCount/count)
#5328 of 13

GWmedians$CenType = as.character(GWmedians$CenType)
GWmedians$CenType[GWmedians$CenType==1] <- '<'
GWmedians$CenType[GWmedians$CenType==2] <- '>'
GWmedians$censMedian = GWmedians$median
GWmedians$censMedian[GWmedians$censoredPropn>=0.5 & GWmedians$CenType=='<'] <- 
  paste0('<',2*GWmedians$median[GWmedians$censoredPropn>=0.5 & GWmedians$CenType=='<'])
GWmedians$censMedian[GWmedians$censoredPropn>=0.5 & GWmedians$CenType=='>'] <- 
  paste0('>',GWmedians$median[GWmedians$censoredPropn>=0.5 & GWmedians$CenType=='>']/1.1)

#E coli detection ####
#1 is detect, 2 is non-detect
# GWmedians$EcoliDetect=NA
# GWmedians$EcoliDetect[which(GWmedians$Measurement=="E.coli")] <- "1"  #Detect
# GWmedians$EcoliDetect[which(GWmedians$Measurement=="E.coli"&
#                               (GWmedians$censoredPropn>0.5|
#                                  GWmedians$median==0|
#                                  is.na(GWmedians$median)))] <- "2"  #Non-detect
# table(GWmedians$EcoliDetect)   
#Changing the 'censoredCount' above.  
#When it's based on censoredCount = sum(!is.na(CenType)), I get 88 detects, 688 non detects
#When it's based on censoredCount = sum(Value<=(0.5*LcenLim) | Value>=(1.1*RcenLim)) I get 89 detects, 687 non detects


GWmedians$EcoliDetectAtAll=NA
GWmedians$EcoliDetectAtAll[which(GWmedians$Measurement=="E.coli")] <- "1"  #Detect
GWmedians$EcoliDetectAtAll[which(GWmedians$Measurement=="E.coli"&
                                   (GWmedians$censoredPropn==1|
                                      GWmedians$median==0|
                                      is.na(GWmedians$median)))] <- "2"  #Non-detect
table(GWmedians$EcoliDetectAtAll)
#Changing the 'censoredCount' above.
#When it's based on censoredCount = sum(!is.na(CenType)), I get 391 detects, 385 non detects
#When it's based on censoredCount = sum(Value<=(0.5*LcenLim) | Value>=(1.1*RcenLim)) I get 394 detects, 382 non detects



# table(GWmedians$EcoliDetect,GWmedians$EcoliDetectAtAll)


#3845 of 11 11-7
#3756 of 11 11-14
#3882 of 11 11-22
#4057 of 11 11-25
#4228 of 11 03-06-20
#4208 of 12 03-13-20
#4208 of 13 03-20-20  Added ecoliDetect   Emails in LAWA 2019 groudnwater folder
#4209 of 15 03-27-20
#4208 of 16 04-23-20 added ecoliDetectAtAll
#4319 of 16 04/08/20
#5053 of 16 10/8/20
#5473       14/8/20
#5527       24/8/20
#5532       28
#5532
#5542       14-9-20
#3113       3-8-21
#4855    6-8-21
#5354   13/8/21
#5321  27/8/21
#5328  3/9/21

GWmedians$meas = factor(GWmedians$Measurement,
                        levels=c("Ammoniacal nitrogen","Chloride","Dissolved reactive phosphorus",
                                 "E.coli","Electrical conductivity/salinity","Nitrate nitrogen"),
                        labels=c("NH4","Cl","DRP","ECOLI","NaCl","NO3"))


#Plotting
if(plotto){
  with(GWmedians[GWmedians$Frequency=='quarterly',],table(minPerYear,count))
  GWmedians[which(GWmedians$Frequency=='quarterly'&GWmedians$count==20),]
  par(mfrow=c(1,1))
  plot(GWmedians$median,GWmedians$MAD,log='xy',xlab='Median',ylab='MAD',
       col=as.numeric(factor(GWmedians$Measurement)),pch=16,cex=1)
  abline(0,1)
  GWmedians%>%filter(MAD>median)
  plot(GWmedians$median,GWmedians$MAD/GWmedians$median,log='x',xlab='Median',ylab='MAD/Median',
       col=as.numeric(factor(GWmedians$Measurement)),pch=16,cex=1)

  #Show data abundance with original filter cutoffs:
  par(mfrow=c(3,1))
  with(GWmedians%>%filter(Frequency=='monthly'),hist(count,main='monthly'));abline(v=30,lwd=2,col='red')
  with(GWmedians%>%filter(Frequency=='bimonthly'),hist(count,main='bimonthly',breaks = 20));abline(v=15,lwd=2,col='red')
  with(GWmedians%>%filter(Frequency=='quarterly'),hist(count,main='quarterly'));abline(v=10,lwd=2,col='red')

  par(mfrow=c(3,1))
  with(GWmedians%>%filter(Frequency=='monthly'),hist(minPerYear,main='monthly'));abline(v=6,lwd=2,col='red')
  with(GWmedians%>%filter(Frequency=='bimonthly'),hist(minPerYear,main='bimonthly',breaks = 20));abline(v=3,lwd=2,col='red')
  with(GWmedians%>%filter(Frequency=='quarterly'),hist(minPerYear,main='quarterly'));abline(v=2,lwd=2,col='red')
  
  par(mfrow=c(3,1))
  with(GWmedians%>%filter(Frequency=='monthly'),hist(nMonth,main='monthly'));abline(v=11.5,lwd=2,col='red')
  with(GWmedians%>%filter(Frequency=='bimonthly'),hist(nMonth,main='bimonthly',breaks=12));abline(v=5,lwd=2,col='red')
  with(GWmedians%>%filter(Frequency=='quarterly'),hist(nQuart,main='quarterly'));abline(v=3.5,lwd=2,col='red')
}
if(plotto){
  par(mfrow=c(3,3))
  plot(GWmedians$median,GWmedians$MAD/(GWmedians$median*GWmedians$count^0.5),
       log='xy',pch=c(1,16)[as.numeric(GWmedians$Exclude)+1],col=as.numeric(factor(GWmedians$Measurement)),cex=sqrt(GWmedians$count))
  GWmedians%>%split(GWmedians$Measurement)%>%
    purrr::map(~plot(.$median,.$MAD/(.$median*.$count^0.5),
                     log='xy',pch=c(1,16)[as.numeric(.$Exclude)+1],main=unique(.$Measurement),cex=sqrt(.$count)))
  
  par(mfrow=c(3,3))
  plot(GWmedians$median,GWmedians$MAD/(GWmedians$median),
       log='xy',lwd=c(2,1)[as.numeric(GWmedians$Exclude)+1],col=as.numeric(factor(GWmedians$Measurement)),cex=sqrt(GWmedians$count))
  abline(h=1)
  GWmedians%>%split(GWmedians$Measurement)%>%
    purrr::map(~{plot(.$median,.$MAD/(.$median),
                      log='xy',lwd=c(2,1)[as.numeric(.$Exclude)+1],main=unique(.$Measurement),cex=sqrt(.$count),ylim=c(0.001,5))
      abline(h=1)})
  
  par(mfrow=c(3,3))
  plot(GWmedians$median,GWmedians$MAD,
       log='xy',pch=c(1,16)[as.numeric(GWmedians$Exclude)+1],col=as.numeric(factor(GWmedians$Measurement)),cex=sqrt(GWmedians$count))
  abline(0,1)
  GWmedians%>%split(GWmedians$Measurement)%>%
    purrr::map(~{plot(.$median,.$MAD,
                      log='xy',pch=c(1,16)[as.numeric(.$Exclude)+1],main=unique(.$Measurement),cex=sqrt(.$count))
      abline(0,1)})
  
  table(GWmedians$count)
}
if(plotto){
  table(GWmedians$count)
  table(GWmedians$count,GWmedians$meas)
  table(GWmedians$count,GWmedians$Frequency)
  
  par(mfrow=c(1,1))
  plot(density(GWmedians$count,from=min(GWmedians$count),adjust=2),xlim=range(GWmedians$count))
  
  
  par(mfrow=c(3,3))
  uMeasures=unique(GWmedians$Measurement)
  for(measure in uMeasures){
    dtp = GWmedians%>%filter(Measurement==measure,median>=0)
    rtp = GWdataRelevantVariables%>%filter(Measurement==measure,Value>=0)
    if(!measure%in%('Water Level')){
      if(measure%in%c('Ammoniacal nitrogen','E.coli')){adjust=2}else{adjust=1}
      plot(density(log(dtp$median),na.rm=T,adjust=adjust),main=measure,xaxt='n',xlab='')
      lines(density(log(rtp$Value),na.rm=T,adjust=2*adjust),col='grey')
      rug(log(dtp$median))
      axis(side = 1,at = pretty(log(dtp$median)),labels=signif(exp(pretty(log(dtp$median))),2))
    }else{
      plot(density(dtp$median,na.rm=T),main=measure,xlab='')
      rug(dtp$median)
      lines(density(rtp$Value,na.rm=T),col='grey')
    }
  }
  rm(dtp,rtp,measure)
  #pick a site, pick a measurement
  nExample=0
  while(nExample<3){
    site=sample(x = unique(GWmedians$LawaSiteID),size = 1)
    meas=sample(x = unique(GWmedians$Measurement),size = 1)
    if(length(which(GWmedians$LawaSiteID==site&GWmedians$Measurement==meas))>0){
      toPlot = GWdataRelevantVariables%>%filter(LawaSiteID==site&Measurement==meas)%>%select(Value)%>%drop_na%>%unlist
      plot(density(toPlot,na.rm=T,from=0),xlab='',main=paste(site,meas))
      rug(toPlot)
      abline(v = GWmedians%>%filter(LawaSiteID==site&Measurement==meas)%>%select(median))
      nExample=nExample+1
    }
  }
  rm(site,meas)
}


GWmedians$Source = GWdata$Source[match(GWmedians$LawaSiteID,GWdata$LawaSiteID)]

GWmedians$StateVal = GWmedians$median
GWmedians$StateVal[GWmedians$Measurement=="E.coli"] <- GWmedians$EcoliDetectAtAll[GWmedians$Measurement=="E.coli"]

#Export Median values
dir.create(paste0("h:/ericg/16666LAWA/LAWA2021/Groundwater/Analysis/",format(Sys.Date(),"%Y-%m-%d")))
write.csv(GWmedians,file = paste0('h:/ericg/16666LAWA/LAWA2021/Groundwater/Analysis/',format(Sys.Date(),"%Y-%m-%d"),
                                  '/ITEGWState',format(Sys.time(),"%d%b%Y"),'.csv'),row.names = F)
write.csv(GWmedians,file = paste0("c:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2021/Groundwater Quality/EffectDelivery/",
                                  '/ITEGWState',format(Sys.time(),"%d%b%Y"),'.csv'),row.names = F)
rm(GWdataRelevantVariables)
if(exists('GWdataReducedTemporalResolution')){rm(GWdataReducedTemporalResolution)}

GWmedians <- read.csv(tail(dir(path='./Analysis',pattern='ITEGWState',full.names = T,recursive = T),1),stringsAsFactors = F)





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
