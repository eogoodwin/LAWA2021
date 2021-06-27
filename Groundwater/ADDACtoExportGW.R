#Add auckland data, censored appropriately and QC-filtered, to the data export file


GWExportData = readxl::read_xlsx('H:/ericg/16666LAWA/LAWA2020/Groundwater/Data/lawa-gwq-download-dataset-2005-2019.xlsx',
                                 sheet=1,col_types = c('text','text','text','text','numeric','numeric','text','text','text','date','text','text','numeric'))

addmargins(table(GWExportData$Agency,GWExportData$Symbol,useNA = 'always'))
#AC have no censored symbols

ACdata = read_csv(tail(dir('h:/ericg/16666LAWA/LAWA2020/Groundwater/Data/','GWdata.csv',recursive=T,full.names=T),1),
                  col_types = cols(
                    .default = col_character(),
                    Imported_On_UTC = col_datetime(format = ""),
                    Latitude = col_double(),
                    Longitude = col_double(),
                    Date = col_datetime(format = ""),
                    `Result-edited` = col_double(),
                    Year = col_double(),
                    Value = col_double(),
                    Censored = col_logical(),
                    myDate = col_date(format = "")
                  ))%>%
  filter(Source=="Auckland")%>%
  filter(Variable_aggregated%in%unique(GWExportData$Indicator))

table(ACdata$Variable_aggregated)


ACdata <- ACdata%>%filter(Date>=min(GWExportData$`Sample Date`,na.rm=T)&Date<=max(GWExportData$`Sample Date`,na.rm=T))

ACdata$GWQZone = GWExportData$`GWQ Zone`[match(ACdata$LawaSiteID,GWExportData$`LAWA Site ID`)]

ACdata$Variable_units[grepl("^g/m",ACdata$Variable_units)] <- 'g/m3'
ACdata$Variable_units[grepl("S/cm$",ACdata$Variable_units)] <- 'uS/cm'


ACdata <- ACdata %>% transmute(`Region Name`=Region,Agency=Source,`LAWA Site ID`=LAWA_ID,
                            `LAWA Well Name`=Site_ID,Latitude=Latitude,Longitude=Longitude,
                            `GWQ Zone`=GWQZone,Indicator=Measurement,`Indicator Unit of Measure`=Variable_units,
                            `Sample Date`=Date,`Raw Value`=`Result-raw`,Symbol=`Result-prefix`,`Censored Value`=Value)

GWExportDataOut <- rbind(GWExportData%>%filter(`Region Name`!="Auckland"),
                         ACdata)

write.csv(GWExportDataOut,'h:/ericg/16666LAWA/LAWA2020/Groundwater/Data/LAWA-GWQ-Download-Dataset-2005-2019_AC.csv',row.names=F)

library(rgdal)
nzmap <- readOGR('S:/New_S/Basemaps_Vector/NZ_Coastline/WGS_84/coast_wgs84.shp')
plot(nzmap,xlim=range(ACdata$Longitude),ylim=range(ACdata$Latitude))
points(ACdata$Longitude,ACdata$Latitude,pch=16,col='red')




allGWdata = readxl::read_xlsx(paste0("C:/Users/ericg/Otago Regional Council/Abi Loughnan - LAWA Annual Water Refresh 2020/Groundwater Quality/",
                                     "GWExport_20200914.xlsx"),sheet=1,guess_max = 50000)%>%
  filter(Variable_aggregated%in%c("Nitrate nitrogen","Chloride",
                                  "Dissolved reactive phosphorus",
                                  "Electrical conductivity/salinity",
                                  "E.coli","Ammoniacal nitrogen"))%>%as.data.frame
