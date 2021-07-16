rm(list=ls())
gc()
library(rgdal)
library(rgeos)
source('H:/ericg/16666LAWA/LAWA2021/scripts/LAWAFunctions.R')
source('k:/R_functions/nzmg2WGS.r')
source('k:/R_functions/wgs2NZTM.r')

lcdb5 <- rgdal::readOGR("D:/RiverData/LCDB-v50/lcdb-v50-land-cover-database-version-50-mainland-new-zealand.csv")

lcdbPolys <- unlist(sapply(lcdb5$WKT,FUN=readWKT))

siteTable=loadLatestSiteTableRiver()
# rec=read_csv("d:/RiverData/RECnz.txt")
recReach=read_csv("d:/RiverData/FWGroupings.txt")%>%select(NZREACH,easting,northing)
rrwgs = nzmg2wgs(East =  recReach$easting,North = recReach$northing)
rrtm = wgs2nztm(ln = rrwgs[,2],lt = rrwgs[,1])
colnames(rrtm) <- c('x','y')
rrtm <- apply(rrtm,2,round)

rrpts = sp::SpatialPoints(coords = rrtm)
rm(rrwgs,rrtm)

# rec2=read_csv('D:/RiverData/River_Environment_Classification_(REC2)_New_Zealand.csv')


polareas = unname(sapply(lcdbPolys,FUN=function(poly){
  c(poly@polygons[[1]]@area)
}))

which.max(polareas)

r1pts <- raster::intersect(x=rrpts,y = lcdbPolys[[510851]])
plot(lcdbPolys[[510851]])
points(r1pts,cex=0.25,col='red')

rrptsID <- apply(rrpts@coords,1,FUN=function(r){
  paste0(as.character(round(r)),collapse='')
})

recReach$landUse2018=NA
thisPoly=1
for(thisPoly in thisPoly:511104){
  if(thisPoly%%100 == 0){cat('.')}
  if(thisPoly%%10000 == 0){
    write.csv(recReach,'ReachLCDB5.csv',row.names=F)
    cat('\n')
  }
  rpts <- raster::intersect(x=rrpts,y=lcdbPolys[[thisPoly]])
  rptsID <- apply(rpts@coords,1,FUN=function(r){
    paste0(as.character(round(r)),collapse='')
  })
  thesePoints <- match(rptsID,rrptsID)
  recReach$landUse2018[thesePoints] <- lcdb5$Name_2018[thisPoly]
}


# 
# library(parallel)
# library(doParallel)
# workers=makeCluster(5)
# registerDoParallel(workers)
# clusterCall(workers,function(){
#   library(rgdal)
#   library(rgeos)
#   library(raster)
# })
# foreach(thisPoly = 1:511,.combine = rbind,.errorhandling = "stop")%dopar%{
#   rpts = raster::intersect(x=rrpts,y=lcdbPolys[[thisPoly]])
#   if(length(rpts)>0){
#     rptsID = apply(rpts@coords,1,FUN=function(r){
#       paste0(as.character(round(r)),collapse='')
#     })
#     thesePoints = match(rptsID,rrptsID)
#     return(data.frame(pointIDs=thesePoints,landUse=lcdb5$Name_2018[thisPoly]))
#   }else{
#     return(NULL)
#   }
# }->indicesLandUse
# stopCluster(workers)
# rm(workers)
