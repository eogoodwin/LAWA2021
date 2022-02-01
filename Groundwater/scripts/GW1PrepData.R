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
write.csv(siteTab,"h:/ericg/16666LAWA/LAWA2021/Groundwater/Metadata/SiteTable.csv",row.names=F)


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
#219692 17/9/21

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
#219667 17/9/21


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
#219582

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



#Prep state dataset ####
#Carl Hanson 11/9/2019:  We only need state and trend for five parameters: 
#    nitrate nitrogen, chloride, DRP, electrical conductivity and E. coli.
startTime=Sys.time()
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
                   Units = unique(Variable_units),
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
Sys.time()-startTime
#68163
#32382 3/8/21
#65305 13/8/21
#62173 20/8/21
#64958 27/8/21
#64979
#66278 17/9/21 21 secs
#66278 18/11   1.1 mins

GWdataRelevantVariables$CenBin = 0
GWdataRelevantVariables$CenBin[GWdataRelevantVariables$CenType=='lt'] = 1
GWdataRelevantVariables$CenBin[GWdataRelevantVariables$CenType=='gt'] = 2
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
#        35       77        5339   17/9/21
GWdataRelevantVariables$Frequency=freqs[GWdataRelevantVariables$siteMeas]  
rm(freqs)


write.csv(GWdataRelevantVariables,paste0('h:/ericg/16666LAWA/LAWA2021/Groundwater/Data/',
                                         format(Sys.Date(),'%Y-%m-%d'),'/GWdataRV.csv'),row.names=F)