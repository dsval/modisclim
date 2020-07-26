#' getModisClim
#'
#' download and gapfill MOD07 and MYD07 atmospheric profiles (air temperature and humidity), calculate saturated vapour pressure using dowsncaled LST from Microwave SSMI and Modis IR, if used with the option use.clouds=TRUE, additional files from MOD06 and MYD06 cloud product will be used to infer temperature and actual vapour pressure under the clouds (only works it the top of the cloud is below the tropopause)
#' @param   coords: lat,lon
#' @param   start, end : data range
#' @param   usr, pass : credentials for NASA EOSDIS/LAADS
#' @import raster
#' @import gdalUtils
#' @import rgdal
#' @import httr
#' @import xml2
#' @keywords modis, air temperature, vapour pressure
#' @export
#' @examples
#' getModisClim()

getModisClim<-function(lat,lon,start,end,DNB='DB',outdir=getwd(), tmpdir=dirname(rasterTmpFile()),usr='usr',pass='pass'){
	# testing
	on.exit(endCluster())
	clcheck<-try(cl<-getCluster(), silent=TRUE)
	if(class(clcheck)=="try-error"){
		cl<-NULL
		message('Only using one core, use first beginCluster() if you want to run in parallel!!')
		
	}
		
	rasterOptions(todisk=T)
	# end testing
########################################################################
#1.get the urls
########################################################################
	
	start.time<-Sys.time()
	#build the query
	use.clouds=TRUE
	cat('Retrieving the urls',"\n")
 url<- "http://modwebsrv.modaps.eosdis.nasa.gov/axis2/services/MODAPSservices/"
 askids<-function(start, end,DNB){
	query_par <- list(products = gsub(" ", "", toString(c('MOD07_L2','MYD07_L2','MOD06_L2','MYD06_L2'))),
		collection = '61',
		startTime = start,
		endTime = end,
		north = lat+0.1,
		south = lat-0.1,
		east = lon+0.1,
		west = lon-0.1,
		coordsOrTiles = 'coords',
		dayNightBoth = DNB)
	query_out<-httr::GET(url = paste0(url, "searchForFiles"),query = query_par)
	## NASA API apparently doesn't like too many requests, but it works after few trials
	while (query_out$status_code != 200){
		query_out<-httr::GET(url = paste0(url, "searchForFiles"),query = query_par)
	}
	# get the fileids
	fileIds <- httr::content(query_out, as = "text")
	# fileIds <- xml2::read_xml(fileIds)
	fileIds<- xml2::read_html(fileIds)
	fileIds <- do.call(rbind, xml2::as_list(fileIds)[[1]][[1]][[1]])
	if(fileIds[1,] == 'No results') {
		warning(paste('No records between',start,'and',end,'!!'))
		fileIds[1,]<-NA
	}
	
	return(fileIds)
 }
########################################################################
#1.1 create monthly intervals, the server doesn't like too many requests
######################################################################## 
range<-seq(as.Date(start),as.Date(end),by='month')
if(length(range)>12){
	beg<-format(as.Date(start),'%Y')
	til<-format(as.Date(end),'%Y')
	datestarts<-format(seq(as.Date(paste0(beg,'-01-01')),as.Date(paste0(til,'-12-01')),by='month'),format='%Y-%m-%d')
	dateends<-format(seq(as.Date(paste0(beg,'-02-1')),as.Date(paste0(as.numeric(til)+1,'-01-01')),by='month')-1,format='%Y-%m-%d')
}else{
	
	datestarts<-format(seq(as.Date(start),as.Date(end),by='day'),format='%Y-%m-%d')
	dateends<-datestarts
	
}

#### request the fileids###
fileIds<-mapply(askids,datestarts,dateends,MoreArgs = list(DNB=DNB))
 


	# get the urls
	get_urls<-function(fileid){
		if(is.na(fileid)){
			fileurl<-NA	
		}else{
			files_md<-httr::GET(url = paste0(url,"getFileUrls"),query = list(fileIds=fileid))
			fileurl <- httr::content(files_md, as = "text")
			# fileurl <- xml2::read_xml(fileurl)
			fileurl <- xml2::read_html(fileurl)
			fileurl <- do.call(rbind, xml2::as_list(fileurl)[[1]][[1]][[1]])	
		}
		
		fileurl
	}
	##request urls by chunks, otherwise the server will hang up on too many requests
	file_urls<-lapply(fileIds,FUN=function(x){mapply(FUN=get_urls,x)})
	file_urls<-do.call(c,file_urls)
	file_urls<-do.call(c,file_urls)
	file_urls<-file_urls[!is.na(file_urls)]
	zdates<-do.call(rbind,strsplit(file_urls,'.',fixed=T))
	# length(zdates[1,])-4
	zdates<-strptime(paste0(zdates[,length(zdates[1,])-4],zdates[,length(zdates[1,])-3]),format='A%Y%j%H%M')
	file_urls<-cbind(file_urls,as.character(zdates))
	file_urls<- file_urls[order(file_urls[,2]),]
	########################################################################
	#2.get monthly MODIS LST urls
	########################################################################
	qextent<-list(products = 'MOD11B3',
		collection = '6',
		startTime = start,
		endTime = end,
		north = lat+0.1,
		south = lat-0.1,
		east = lon+0.1,
		west = lon-0.1,
		coordsOrTiles = 'coords',
		dayNightBoth = 'D')
	qextent[[3]]<-paste0(format(as.Date(start),'%Y'),'-',format(as.Date(start),'%m'),'-01')
	qextent[[4]]<-paste0(format(as.Date(end),'%Y'),'-',format(as.Date(end),'%m'),'-01')
	query_oute<-httr::GET(url = paste0(url, "searchForFiles"),query = qextent)
	while(query_oute$status_code != 200){
		query_oute<-httr::GET(url = paste0(url, "searchForFiles"),query = qextent)
	}
	fileIdse <- httr::content(query_oute, as = "text")
	fileIdse <- xml2::read_html(fileIdse)
	fileIdse <- do.call(rbind, xml2::as_list(fileIdse)[[1]][[1]][[1]])
	if(fileIdse[1,] == 'No results') stop("No records whitin the time range")
	# urlext<-get_urls(fileIdse[,1])
	urlext<-mapply(FUN=get_urls,fileIdse)
	urlext<-do.call(c,urlext)
	hv<-do.call(rbind,strsplit(urlext,'.',fixed = T))
	hv<-hv[,length(hv[1,])-3]
	beg<-format(as.Date(start),'%Y%j')
	til<-format(as.Date(end),'%Y%j')
	urlext<-do.call(c,regmatches(urlext, gregexpr(paste0('.*.',hv[1],'.*.'),urlext)))
	# file_urls<-file_urls[!duplicated(file_urls[,2]),]
	########################################################################
	#1.get the urls for lst microwave SSM/I-SSMIS Pathfinder
	########################################################################
	# start="2012-01-01";end="2013-12-31";

	
########################################################################
#2.download the files
########################################################################
	# tmpdir<-dirname(rasterTmpFile())
	setwd(tmpdir)
	# setwd('C:/Rcalculations')
	destfiles<-basename(file_urls[,1])
	# pb <- pbCreate(length(file_urls[,1]))
	pb <- txtProgressBar(min=1,max = length(file_urls[,1]), style = 3)
	# Loop through all files
	########################################################################
	#2.download atmosphere
	########################################################################
	cat('downloading elevation from GMTED',"\n")
	dem<-raster('/vsicurl/https://data.earthenv.org/topography/elevation_5KMmd_GMTEDmd.tif')
	cat('downloading atmospheric profiles',"\n")
	for (i in 1:length(file_urls[,1])) {
		if(!file.exists(destfiles[i])){
			# Write file to disk (authenticating with netrc) using the current directory/filename
			response <- httr::GET(as.character(file_urls[i,1]), write_disk(destfiles[i], overwrite = TRUE), 
				authenticate(usr, pass))
			while(file.size(destfiles[i])<=200){
				response <- httr::GET(as.character(file_urls[i,1]), write_disk(destfiles[i], overwrite = TRUE), 
					authenticate(usr, pass))
			}
			
			# Check to see if file downloaded correctly
			if (response$status_code == 200) {
				cat(destfiles[i],"downloaded","\n")
			} else {
				cat("error downloading, make sure usr/password are correct","\n")
				errordwn<-response$status_code
			}
		}
	
		setTxtProgressBar(pb,i)
	}
	close(pb)
	gc()	
	# 
	########################################################################
	#2.download MODIS LST
	########################################################################
	destfileext<-basename(as.character(urlext))
		
	if(length(destfileext)>1){pb <- txtProgressBar(min=1,max = length(destfileext), style = 3)}
	
	# destfiles<-basename(file_urls[,1])
	cat(' ',"\n")
	cat('downloading MODIS LST',"\n")
	for(i in 1:length(destfileext)){
		if(!file.exists(destfileext[i])){
		respon <-httr::GET(as.character(urlext[i]), write_disk(destfileext[i], overwrite = TRUE), 
			authenticate(usr, pass))
		while(file.size(destfileext[i])<=200){
			respon <-httr::GET(as.character(urlext[i]), write_disk(destfileext[i], overwrite = TRUE), 
				authenticate(usr, pass))
		}
		# Check to see if file downloaded correctly
		if (respon$status_code == 200) {
			cat(' ',"\n")
			cat(destfileext[i],"downloaded","\n")
		} else {
			cat("error downloading, make sure usr/password are correct","\n")
		}
		if(length(destfileext)>1){setTxtProgressBar(pb,i)}
	}
	}
	

end.time<-Sys.time()
cat("time taken downloading:",difftime(end.time,start.time,'mins'),'mins',"\n")
start.time<-Sys.time()
########################################################################
#3.read the data, ....mapply is working faster than overay wth a rasterstack
########################################################################	
# prepare the filenames
filenames<-paste0(tmpdir,'/',destfiles)
if (use.clouds==TRUE){
	filenames_mod06<-do.call(c,regmatches(filenames, gregexpr('.*.MOD06.*.',filenames)))
	filenames_myd06<-do.call(c,regmatches(filenames, gregexpr('.*.MYD06.*.',filenames)))
	filenames_cld<-c(filenames_mod06,filenames_myd06)
	zdates_cld<-do.call(rbind,strsplit(filenames_cld,'.',fixed=T))
	# zdates_cld<-strptime(paste0(zdates_cld[,2],zdates_cld[,3]),format='A%Y%j%H%M')
	zdates_cld<-as.POSIXct(paste0(zdates_cld[,length(zdates_cld[1,])-4],zdates_cld[,length(zdates_cld[1,])-3]),format='A%Y%j%H%M', tz = "GMT")
	filenames_cld<-filenames_cld[order(zdates_cld)]
}


filenames_mod07<-do.call(c,regmatches(filenames, gregexpr('.*.MOD07.*.',filenames)))
filenames_myd07<-do.call(c,regmatches(filenames, gregexpr('.*.MYD07.*.',filenames)))
filenames_atm<-c(filenames_mod07,filenames_myd07)
zdates_atm<-do.call(rbind,strsplit(filenames_atm,'.',fixed=T))
# zdates_atm<-strptime(paste0(zdates_atm[,2],zdates_atm[,3]),format='A%Y%j%H%M')
zdates_atm<-as.POSIXct(paste0(zdates_atm[,length(zdates_atm[1,])-4],zdates_atm[,length(zdates_atm[1,])-3]),format='A%Y%j%H%M', tz = "GMT")
filenames_atm<-filenames_atm[order(zdates_atm)]
zdates_atm<-zdates_atm[order(zdates_atm)]
filenamlst<-paste0(tmpdir,'/',destfileext)


############################# some constants ############################
kG <- 9.80665       # gravitational acceleration, m/s^2 (Allen, 1973)
kL <- 0.0065        # adiabatic lapse rate, K/m (Cavcar, 2000)
kMa <- 0.028963     # molecular weight of dry air, kg/mol (Tsilingiris, 2008)
kPo <- 101325       # standard atmosphere, Pa (Allen, 1973)
kR <- 8.31447       # universal gas constant, J/mol/K (Moldover et al., 1988)
kTo <- 288.15       # base temperature, K (Berberan-Santos et al., 1997)
kcp <- 1010		# specific heat dry air at constant pressure J/Kg/K (Oke, 1996)
kcpw<- 1880		# specific heat water vapour at constant pressure J/Kg/K (Oke, 1996)
############################# create the functions to read the data############################
# ************************************************************************
# Name:     elev2pres
# Inputs:   double (z), meters
# Returns:  double, Pa
# Features: Calculates atmospheric pressure for a given elevation
# Depends:  - kPo ............ base pressure, Pa
#           - kTo ............ base temperature, C
#           - kL ............. temperature lapse rate, K/m
#           - kR ............. universal gas constant, J/mol/K
#           - kMa ............ molecular weight of dry air, kg/mol
#           - kG ............. gravity, m/s^2
# Ref:      Allen et al. (1998)
# ************************************************************************


elev2pres <- function(z) {
	############################# some constants ############################
	kG <- 9.80665       # gravitational acceleration, m/s^2 (Allen, 1973)
	kL <- 0.0065        # adiabatic lapse rate, K/m (Cavcar, 2000)
	kMa <- 0.028963     # molecular weight of dry air, kg/mol (Tsilingiris, 2008)
	kPo <- 101325       # standard atmosphere, Pa (Allen, 1973)
	kR <- 8.31447       # universal gas constant, J/mol/K (Moldover et al., 1988)
	kTo <- 288.15       # base temperature, K (Berberan-Santos et al., 1997)
	kPo*(1 - kL*z/kTo)^(kG*kMa/(kR*kL))
}
p_dryair<-function(elev,t){
	#calc dry air density [kg/m3] using pv=nRT
	kMa <- 0.028963     # molecular weight of dry air, kg/mol (Tsilingiris, 2008)
	kR <- 8.31447       # universal gas constant, J/mol/K (Moldover et al., 1988)
	(elev2pres(elev)*kMa)/(kR*(t+273.15))
}

############################# func to read modis lst############################
readlst<-function(filename){
	# filename=filenamlst[1]
	lst<-raster(get_subdatasets(filename)[1])
	lst<-projectRaster(lst,crs="+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0")
	calc(lst,fun=function(x)x-273.15,filename=paste0(tempfile(pattern = "file", tmpdir = tmpdir),'.grd'))
	}
######################################BitFlagDecoder###############################
BitFlagDecoder <- function(x,bits=16,obj=15){
	i <- 1
	result <- rep(0,bits)
	if(length(obj)>1){
		ind <- max(obj):min(obj)+1 # bit index
	}else{
		ind <- obj + 1 
	}
	if(is.na(x)){
		result[1:bits] <- NA
	}else{
		while(x > 0){
			result[i]  <- x %% 2
			x <- x %/% 2
			i <- i + 1
		}
	}
	return(as.numeric(paste(result[ind],collapse = '')))
}
############################# func to read modis Atmosphere Profiles############################
readMOD07<-function(filename){
	# testting
	# filename=filenames_atm[1]
	# end testing
	#read datatsets
	sds <- get_subdatasets(filename)
	md<-gdalinfo(filename)
	north<-do.call(c,regmatches(md, gregexpr(paste0('.*.','NORTHBOUNDINGCOORDINATE','.*.'),md)))
	south<-do.call(c,regmatches(md, gregexpr(paste0('.*.','SOUTHBOUNDINGCOORDINATE','.*.'),md)))
	east<-do.call(c,regmatches(md, gregexpr(paste0('.*.','EASTBOUNDINGCOORDINATE','.*.'),md)))
	west<-do.call(c,regmatches(md, gregexpr(paste0('.*.','WESTBOUNDINGCOORDINATE','.*.'),md)))
	bbox<-c(west,east,south,north)
	# bbox<-c(md[77],md[18],md[66],md[45])
	bbox<-do.call(rbind,strsplit(bbox,'='))
	bbox<-as.numeric(bbox[,2])
	wgs<-"+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0"
	# QA<-readGDAL(sds[27], as.is = TRUE,silent = T)
	# reading heigth at different geopotentials [masl]
	tropohgt <- calc(brick(readGDAL(sds[18], as.is = TRUE,silent = T)),fun=function(x){x+32500})
	# subsetting readings max aronud ~8KM (lower than tropopause)
	tropohgt<-subset(tropohgt,13:20)
	# reading air temperature profile [C]
	ta_press <- calc(brick(readGDAL(sds[15], as.is = TRUE,silent = T)), fun = function(x){((x+15000) * 0.009999999776482582)-273.15})
	# subsetting readings max aronud ~8KM (lower than tropopause)
	ta_press<-subset(ta_press,13:20)
	extent(ta_press)<-extent(bbox)
	projection(ta_press)<-wgs
	extent(tropohgt)<-extent(bbox)
	projection(tropohgt)<-wgs
	#mixing ratio profile(Kg water vapour/Kg dry air)  !not total air mass!!https://disc.gsfc.nasa.gov/information/glossary?title=Giovanni%20Parameter%20Definitions
	vw<-calc(brick(readGDAL(sds[17], as.is = TRUE,silent = T)),fun=function(x){x*(0.001000000047497451)/1000})
	# subsetting readings max aronud ~8KM (lower than tropopause)
	vw<-subset(vw,13:20)
	#error reading QA fixing, negative humidity
	# vw[vw<0]<-0
	vw<-reclassify(vw, cbind(-Inf,0, 0), right=TRUE)
	extent(vw)<-extent(bbox)
	projection(vw)<-wgs
	#calc adiabatic lapse rates
	fr<-function(x,y) {
		
		frvect<-function(x,y){
			if((sum(is.na(x))==length(x))||(sum(is.na(x))!=sum(is.na(y)))){
				c(NA,NA)
			}else{
				coefs<-.lm.fit(cbind(1, na.omit(x)), na.omit(y))
				#coefs<-lm(y~x)
				# coefs<-mapply(function(x,y)lm(y~x)$coefficients,split(x, row(x)) ,split(y, row(y)))
				# set NAs values anomalous values if(coefs$coefficients[2]<0.0098)
				return(coefs$coefficients)
			}
		}
		coefs<-t(mapply(frvect,split(x, row(x)) ,split(y, row(y)),SIMPLIFY = T))
		return(coefs)
	}
	calc_lr<-function(x,y){overlay(x,y,fun=fr,filename=paste0(tempfile(pattern = "file", tmpdir = tmpdir),'.grd'))}
	
	LR_csky<-calc_lr(tropohgt,ta_press)
	VR_csky<-calc_lr(tropohgt,vw)
	
	gc()
	# return(list(ta_prof=ta_press,tropohgt=tropohgt,vw_prof=vw))
	return(list(LR_csky=LR_csky,VR_csky=VR_csky))
			
}


############################# func to read modis cloud############################
readMOD06<-function(filename){
	# returns a list with 3 datasets: cth= cloud top heigth (m),cl_bs=cloud base height(m) ,tcloud=cloud top temperature (C)
	# testting
	# filename=filenames_cld[1]
	# end testing
	#read datatsets
	sds <- get_subdatasets(filename)
	md<-gdalinfo(filename)
	north<-do.call(c,regmatches(md, gregexpr(paste0('.*.','NORTHBOUNDINGCOORDINATE','.*.'),md)))
	south<-do.call(c,regmatches(md, gregexpr(paste0('.*.','SOUTHBOUNDINGCOORDINATE','.*.'),md)))
	east<-do.call(c,regmatches(md, gregexpr(paste0('.*.','EASTBOUNDINGCOORDINATE','.*.'),md)))
	west<-do.call(c,regmatches(md, gregexpr(paste0('.*.','WESTBOUNDINGCOORDINATE','.*.'),md)))
	bbox<-c(west,east,south,north)
	# bbox<-c(md[86],md[26],md[76],md[55])
	# QA<-raster(readGDAL(sds[110], as.is = TRUE,silent = T))
	bbox<-do.call(rbind,strsplit(bbox,'='))
	bbox<-as.numeric(bbox[,2])
	wgs<-"+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0"
	#get elevation from surface pressure https://www.engineeringtoolbox.com/air-altitude-pressure-d_462.html
	#elev<-calc(raster(readGDAL(sds[16], as.is = TRUE,silent = T)),function(x){(1/2.25577e-5)*(1-(x*10/101325)^(1/5.25588))})
	# read cloud top height (masl)
	cth <- raster(readGDAL(sds[58], as.is = TRUE,silent = T))
	extent(cth)<-extent(bbox)
	projection(cth) <- wgs
	#get rid of readings above the tropopause
	cth<- reclassify(cth, cbind(8000, +Inf, NA), right=TRUE,filename=paste0(tempfile(pattern = "file", tmpdir = tmpdir),'.grd'))
	# read cloud optical thickness
	copt<- calc(raster(readGDAL(sds[73], as.is = TRUE,silent = T)),fun = function(x){x*0.009999999776482582})
	extent(copt)<-extent(bbox)
	projection(copt) <- wgs
	# read cloud effective radius
	cefr<- calc(raster(readGDAL(sds[67], as.is = TRUE,silent = T)),fun = function(x){x*0.009999999776482582})
	extent(cefr)<-extent(bbox)
	projection(cefr) <- wgs
	# calculate cloud base height(masl)  Welch, et al. 2008 doi 10.1175/2007JAMC1668.1
	calc_bh<-function(cot,cer,clth){
		cldbh<-clth-((4/3)*cot*cer*(0.002)^-1)^0.5
		ifelse(cldbh<0,0.0,cldbh)
	}
	cl_bs<-overlay(copt,cefr,cth,fun=calc_bh,filename=paste0(tempfile(pattern = "file", tmpdir = tmpdir),'.grd'))
	# read cloud top temperature
	tcloud<-calc(raster(readGDAL(sds[104], as.is = TRUE,silent = T)),fun=function(x){((x+15000)* 0.009999999776482582)-273.15})
	extent(tcloud)<-extent(bbox)
	projection(tcloud) <- wgs
	#tcloud<-projectRaster(tcloud,cth)
	# QF<-readGDAL(sds[124], as.is = TRUE,silent = T)
	# rgadl error reading QA flags, remove bad vlues
	tcloud<-reclassify(tcloud, cbind(-Inf,-40, NA), right=TRUE)
	#get rid of readings above the tropopause
	tcloud<-mask(tcloud,cth,filename=paste0(tempfile(pattern = "file", tmpdir = tmpdir),'.grd'))
	######air temperature at the base of the cloud######
	calc_Tbh<-function(cth,cl_bs,tcloud){
		# cloud thickness
		z_cld<-cth-cl_bs
		#temperature in kelvin
		TclK<-tcloud+273.15
		# get cloud water vap pressure [pa]
		es<-0.6108*1000*exp((17.27*tcloud)/(tcloud+237.3))
		#calc mixing ratio at the top of the cloud http://glossary.ametsoc.org/wiki/Mixing_ratio
		mr<-(0.622*es)/(elev2pres(cth)-es)
		# get saturated adiabatic lapse rate K/m http://glossary.ametsoc.org/wiki/Saturation-adiabatic_lapse_rate
		L<-9.8*((287*(TclK)^2+ 2501000*mr*(TclK))/(1003.5*287*(TclK)^2+2501000^2*mr*0.622))
		#calc air temperature at the base of the cloud
		Ta_bh<-tcloud+(L*z_cld)
		return(Ta_bh)
	}
	
	Ta_bh<-overlay(cth,cl_bs,tcloud,fun=calc_Tbh,filename=paste0(tempfile(pattern = "file", tmpdir = tmpdir),'.grd'))
	gc()
	return(list(cth=cth,cl_bs=cl_bs,tcloud=tcloud,Ta_bh=Ta_bh))
	
}
############################# func to read SSM lst############################
read_ssm<-function(filename){
	# testing
	# filename=filenames_SSM[1]
	# end testing
	con <- gzfile(filename, open = "rb")
	raw <- readBin(con, what = "int",size=2,n=1e7)
	close(con)
	raw[raw==0]<-NA
	raw<- matrix(data = raw, nrow = 586, ncol = 1383,byrow = TRUE)
	r<-raster(raw, xmn=-17324563.84, xmx=17324563.84, ymn=-7338939.46, ymx=7338939.46,crs='+init=epsg:3410')
	r<-projectRaster(r,crs="+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0")
	# get brightness temperature 37gz V polarization Holmes, et al. 2008 doi 10.1029/2008JD010257
	calc(r,function(x){y=(x/10);(1.11*y-15.2)-273.15},filename=paste0(tempfile(pattern = "file", tmpdir = tmpdir),'.grd'))
}
############################# reading the data ############################
##reading temperature and humidity profiles
cat(' ...',"\n")
if(is.null(cl)){
	cat('reading temperature and humidity profiles, computing ELRs',"\n")
	atm<-mapply(FUN=readMOD07,filenames_atm,SIMPLIFY = F)
	cat('reading cloud info, computing cloud base heigths and base temperature',"\n")
	clds<-mapply(FUN=readMOD06,filenames_cld,SIMPLIFY = F)
	cat(' ',"\n")
	cat('reading modis LST',"\n")
	lst_mod<-mapply(FUN=readlst,filenamlst,SIMPLIFY = F)
}else{
	cat('starting cluster for parallel computation',"\n")
	parallel:::clusterEvalQ(cl, lapply(c('raster','rgdal','gdalUtils','MASS'), library, character.only = TRUE))
	parallel:::clusterExport(cl, c("readMOD07","readMOD06",'filenames_atm','filenames_cld','elev2pres','readlst','filenamlst','rasterOptions','tmpdir'),envir=environment())
	cat('reading temperature and humidity profiles, computing ELRs',"\n")
	atm<-parallel:::clusterMap(cl = cl, fun=readMOD07, filename=filenames_atm)
	cat('reading cloud info, computing cloud base height and base temperature',"\n")
	clds<-parallel:::clusterMap(cl = cl, fun=readMOD06, filename=filenames_cld)
	cat(' ',"\n")
	cat('reading modis LST',"\n")
	if(length(urlext)>1){
		lst_mod<-parallel:::clusterMap(cl = cl, fun=readlst, filename=filenamlst)
	}else{
		lst_mod<-mapply(FUN=readlst,filenamlst,SIMPLIFY = F)
	}
	
	
}

end.time<-Sys.time()
cat("time taken reading the datasets:",difftime(end.time,start.time,'mins'),'mins',"\n")
########################################################################
#3.Organize the layers
########################################################################
start.time<-Sys.time()
# stack the grids

LR_csky<-list()
for(i in 1:length(atm)){LR_csky[[i]]<-atm[[i]]$LR_csky}
VR_csky<-list()
for(i in 1:length(atm)){VR_csky[[i]]<-atm[[i]]$VR_csky}
#cloud top height
cl_top_hgt<-list()
for(i in 1:length(clds)){cl_top_hgt[[i]]<-clds[[i]]$cth}
# cloud base heigth
cl_b_hgt<-list()
for(i in 1:length(clds)){cl_b_hgt[[i]]<-clds[[i]]$cl_bs}
# cloud top temperature
cl_t<-list()
for(i in 1:length(clds)){cl_t[[i]]<-clds[[i]]$tcloud}
# cloud base temperature
cl_b_T<-list()
for(i in 1:length(clds)){cl_b_T[[i]]<-clds[[i]]$Ta_bh}

rm(atm)
rm(clds)
gc()
# fix formats and extent

if(is.null(cl)){
	tiddyup<-function(rastrlist,mold){
		result<-mapply(FUN=extend,rastrlist,MoreArgs=list(y=mold),SIMPLIFY = F)
		result<-mapply(FUN=crop,result,MoreArgs=list(y=mold),SIMPLIFY = F)
		result<-mapply(FUN=projectRaster,result,MoreArgs=list(to=mold),SIMPLIFY = F)
		#result<-mapply(FUN=mask,result,MoreArgs=list(mask=mold),SIMPLIFY = F)
		# stack(result)
		result
	}
	LR_csky<-tiddyup(LR_csky,lst_mod[[1]])
	VR_csky<-tiddyup(VR_csky,lst_mod[[1]])
	cl_top_hgt<-tiddyup(cl_top_hgt,lst_mod[[1]])
	cl_b_hgt<-tiddyup(cl_b_hgt,lst_mod[[1]])
	cl_t<-tiddyup(cl_t,lst_mod[[1]])
	cl_b_T<-tiddyup(cl_b_T,lst_mod[[1]])
}else{
	tiddyup_par<-function(rastrlist,mold){
		files<-lapply(rastrlist,filename)
		parallel:::clusterExport(cl, c('rastrlist',"mold",'files'),envir=environment())
		result<-parallel:::clusterMap(cl = cl, fun=extend,rastrlist,filename=files,MoreArgs=list(y=mold,overwrite=TRUE))
		parallel:::clusterExport(cl, c('result'),envir=environment())
		result<-parallel:::clusterMap(cl = cl, fun=crop,result,filename=files,MoreArgs=list(y=mold,overwrite=TRUE))
		parallel:::clusterExport(cl, c('result'),envir=environment())
		result<-parallel:::clusterMap(cl = cl, fun=projectRaster,result,filename=files,MoreArgs=list(to=mold,overwrite=TRUE))
		result
	}
	LR_csky<-tiddyup_par(LR_csky,lst_mod[[1]])
	VR_csky<-tiddyup_par(VR_csky,lst_mod[[1]])
	cl_top_hgt<-tiddyup_par(cl_top_hgt,lst_mod[[1]])
	cl_b_hgt<-tiddyup_par(cl_b_hgt,lst_mod[[1]])
	cl_t<-tiddyup_par(cl_t,lst_mod[[1]])
	cl_b_T<-tiddyup_par(cl_b_T,lst_mod[[1]])
	}
	end.time<-Sys.time()
	cat("time taken organizing the layers:",difftime(end.time,start.time,'mins'),'mins'	,"\n")
########################################################################
#3.gap fill daily adiabatic Lapse rate asuming meain in 3 neigthbour pixels
########################################################################

gapfill<-function(x){
	fill.na <- function(x, i=5) {
		if( is.na(x)[i] ) {
			return( mean(x, na.rm=TRUE) )
		} else {
			return(x[i])
		}
	}
	fillLR<-focal(x,w = matrix(1,3,3), fun = fill.na, pad = TRUE, na.rm = FALSE)
	fillLR<-focal(fillLR,w = matrix(1,3,3), fun = fill.na, pad = TRUE, na.rm = FALSE,filename=filename(x),overwrite=T) 
	# for (i in 1:2){
	# 	fillLR<-focal(fillLR,w = matrix(1,3,3), fun = fill.na, pad = TRUE, na.rm = FALSE)
	# }
	return(fillLR)
}

crs(dem)<-crs(lst_mod[[1]])
dem<-projectRaster(dem,lst_mod[[1]])

# dem_hr<-crop(dem_hr,dem)

gc()

########################################################################
#3.calculate  temp lapse rate under the clouds and map orographic inmmersion following Nair et al., 2008 doi: 10.1175/2007JAMC1819.1
########################################################################
cat("calculating ELRs under the clouds","\n")
start.time<-Sys.time()
setwd(tmpdir)
calc_ub_cld<-function(cl_b_t,cld_b_ht,dem){
	############################# some constants ############################
	kG <- 9.80665       # gravitational acceleration, m/s^2 (Allen, 1973)
	kL <- 0.0065        # adiabatic lapse rate, K/m (Cavcar, 2000)
	kMa <- 0.028963     # molecular weight of dry air, kg/mol (Tsilingiris, 2008)
	kPo <- 101325       # standard atmosphere, Pa (Allen, 1973)
	kR <- 8.31447       # universal gas constant, J/mol/K (Moldover et al., 1988)
	kTo <- 288.15       # base temperature, K (Berberan-Santos et al., 1997)
	kcp <- 1010		# specific heat dry air at constant pressure J/Kg/K (Oke, 1996)
	kcpw<- 1880		# specific heat water vapour at constant pressure J/Kg/K (Oke, 1996)
	
	# h=cloud altitude (m)
	h<-cl_b_t-dem
	h<-ifelse(h<0,0,h)
	# dew temperature lapse rate constant http://glossary.ametsoc.org/wiki/Dewpoint_formula -1.8 C/Km
	Dlr<-0.0018
	# T_dew_s: dew temperature at surface:
	T_dew_s<-cl_b_t+Dlr*h
	# # ea: vapor pressure at suface from Tdew
	a=6.112
	b=17.62
	c=243.12
	# # ea in mbars
	ea=exp(log(a)+b/(1+(c/T_dew_s)))
	# # ea in Pa:
	ea<-ea*100
	# # vw mixing ratio kg/kg at surface solving Oke and pV=nRT
	vw=(2.17*1e-3)*(ea*kR/(elev2pres(dem)*kMa))
	# # air specific heat at surface with humidity vw Seinfield and Pandis, 1998 eq 14.7
	cp=(1-vw)*kcp+vw*kcpw
	# #Dry lapse rate Seinfield and Pandis, 1998 eq 14.6
	dlr<-(-kG/cp)
	#Ta air temperature at surface under the cloud
	Ta<-h*(dlr-Dlr)+ cl_b_t
	TclK<-Ta+273.15
	#ELR environmental (unsaturated) lapse rate Seinfield and Pandis, 1998 eq 14.6
	
	# get adiabatic lapse rate K/m http://glossary.ametsoc.org/wiki/Saturation-adiabatic_lapse_rate
	elr<--kG*((287*(TclK)^2+ 2501000*vw*(TclK))/(kcp*287*(TclK)^2+2501000^2*vw*0.622))
	# calc the intercept of the elr
	To<-Ta-elr*dem
	#calc vapour pressure at cloud base [Pa]
	es_cld<-0.6108*1000*exp((17.27*cl_b_t)/(cl_b_t+237.3))
	# es_cld=exp(log(a)+b/(1+(c/cl_b_t)))*100
	#mixing ratio at cloud base kg/kg
	# vw_b=(2.17*1e-3)*(es_cld*kR/(elev2pres(cld_b_ht)*kMa))
	vw_b=(0.622*es_cld)/(elev2pres(cld_b_ht)-es_cld)
	#rate mixing ratio
	vwlr<-(vw_b-vw)/h
	#correct for cloud immersion
	vwlr<-ifelse(is.infinite(vwlr),0,vwlr)
	vwlr<-ifelse(vwlr>0,0,vwlr)
	#vw intercept
	vo<-vw-vwlr*dem
	vo<-ifelse(vo<0,0,vo)
	result<-cbind(To,elr,vo,vwlr)
	return(result)
	
	#get vapour density [kg/m3](Oke, 1996)
}
calc_ub_cld_rast<-function(cl_b_t,cld_b_ht,dem){overlay(cl_b_t,cld_b_ht,dem,fun=calc_ub_cld,filename=paste0(tempfile(pattern = "file", tmpdir = tmpdir),'.grd'))}


if(is.null(cl)){
	LR_uclds<-mapply(FUN=calc_ub_cld_rast,cl_b_t=cl_b_T,cld_b_ht=cl_b_hgt,MoreArgs = list(dem=dem))
}else{
	parallel:::clusterExport(cl, c("calc_ub_cld_rast","calc_ub_cld",'cl_b_T','cl_b_hgt','dem'),envir=environment())
	LR_uclds<-parallel:::clusterMap(cl = cl, fun=calc_ub_cld_rast, cl_b_t=cl_b_T,cld_b_ht=cl_b_hgt,MoreArgs = list(dem=dem))	
}
end.time<-Sys.time()
cat("time taken computing ELR under the clouds:",difftime(end.time,start.time,'mins'),'mins'	,"\n")
start.time<-Sys.time()
########################################################################
#3.calculate average LR between cSky and cloudy sky
########################################################################

#function to calc average between raster objects, ignoring Nas
calc_avgTa<-function(x,y){overlay(x,y,fun=function(x,y){rowMeans(cbind(x,y),na.rm = T)},filename=paste0(tempfile(pattern = "file", tmpdir = tmpdir),'.grd'))}
#function to loop averages between clear sky and cloudy data

calc_lr_avg<-function(cs,uc){stack(calc_avgTa(cs[[1]],uc[[1]]),calc_avgTa(cs[[2]],uc[[2]]))}

#merged LR cloudy and clear data
if(is.null(cl)){
	LR_avg<-mapply(calc_lr_avg,LR_csky,LR_uclds)
}else{
	parallel:::clusterExport(cl, c("calc_lr_avg","calc_avgTa",'LR_csky','LR_uclds'),envir=environment())
	LR_avg<-parallel:::clusterMap(cl = cl, fun=calc_lr_avg,LR_csky,LR_uclds)	
}


#intercept daily Ta 
int_day<-list()
LR_day<-list()
for(i in 1:length(LR_avg)){int_day[[i]]<-LR_avg[[i]][[1]]};
for(i in 1:length(LR_avg)){LR_day[[i]]<-LR_avg[[i]][[2]]};
#rm(LR_avg)
gc()
end.time<-Sys.time()
cat("time taken averaging ELR from clear sky and under the clouds:",difftime(end.time,start.time,'mins'),'mins'	,"\n")
start.time<-Sys.time()
########################################################################
#3.gap fill daily adiabatic Lapse rate asuming meain in 3 neigthbour pixels
########################################################################
#Gapfilling temperature parameters
if(is.null(cl)){
	int_day<-mapply(gapfill,int_day)
	LR_day<-mapply(gapfill,LR_day)
}else{
	parallel:::clusterExport(cl,c("gapfill","int_day",'LR_day'),envir=environment())
	int_day<-parallel:::clusterMap(cl = cl, fun=gapfill, int_day)	
	LR_day<-parallel:::clusterMap(cl = cl, fun=gapfill, LR_day)
}
gc()
end.time<-Sys.time()
cat("time taken gapfilling:",difftime(end.time,start.time,'mins'),'mins'	,"\n")
start.time<-Sys.time()
#averaging to daily



if(is.null(cl)){
	int_day<-setZ(stack(int_day),zdates_atm)
	int_day<-zApply(int_day,as.Date(zdates_atm),mean)
	LR_day<-setZ(stack(LR_day),zdates_atm)
	LR_day<-zApply(LR_day,as.Date(zdates_atm),mean)
}else{
	int_day<-setZ(stack(int_day),zdates_atm)
	int_day<-aggRaster(int_day,func = "mean",xout='daily',cl=cl,varnam='int_day',longname='int_day_gaps', varunit='C')
	LR_day<-setZ(stack(LR_day),zdates_atm)
	LR_day<-aggRaster(LR_day,func = "mean",xout='daily',cl=cl,varnam='LR_day',longname='LR_day_gaps', varunit='C/m')
}
gc()

gapfillgauss<-function(x){
	fill.na <- function(x, i=5) {
		if( is.na(x)[i] ) {
			return( mean(x, na.rm=TRUE) )
		} else {
			return(x[i])
		}
	}
	fillLR<-focal(x,w = matrix(1,3,3), fun = fill.na, pad = TRUE, na.rm = FALSE)
	fillLR<-focal(fillLR,w = matrix(1,3,3), fun = fill.na, pad = TRUE, na.rm = FALSE)
	fillLR<-focal(fillLR,w = matrix(1,3,3), fun = fill.na, pad = TRUE, na.rm = FALSE)
	gf <- focalWeight(fillLR, 0.15, "Gauss")
	fillLR <- focal(fillLR, w=gf,na.rm=T,filename=paste0(tempfile(pattern = "file", tmpdir = tmpdir),'.grd')) 
	# for (i in 1:2){
	# 	fillLR<-focal(fillLR,w = matrix(1,3,3), fun = fill.na, pad = TRUE, na.rm = FALSE)
	# }
	return(fillLR)
}



if(is.null(cl)){
	int_day<-mapply(gapfillgauss,as.list(int_day))
	LR_day<-mapply(gapfillgauss,as.list(LR_day))
}else{
	parallel:::clusterExport(cl, c("gapfill","int_day",'LR_day'),envir=environment())
	int_day<-parallel:::clusterMap(cl = cl, fun=gapfillgauss, as.list(int_day))	
	LR_day<-parallel:::clusterMap(cl = cl, fun=gapfillgauss, as.list(LR_day))
}


int_day<-stack(int_day)
LR_day<-stack(LR_day)
int_day<-setZ(int_day,unique(as.Date(zdates_atm)))
LR_day<-setZ(LR_day,unique(as.Date(zdates_atm)))

########################################################################
#3.process humidity
########################################################################


calc_vr_avg<-function(cs,uc){stack(calc_avgTa(cs[[1]],uc[[3]]),calc_avgTa(cs[[2]],uc[[4]]))}

#merged LR cloudy and clear data



#merged LR cloudy and clear data
if(is.null(cl)){
	VR_avg<-mapply(calc_vr_avg,VR_csky,LR_uclds)
}else{
	parallel:::clusterExport(cl,c("calc_vr_avg",'VR_csky','LR_uclds'),envir=environment())
	VR_avg<-parallel:::clusterMap(cl = cl, fun=calc_vr_avg,VR_csky,LR_uclds)
	
}



#calc daily averages
int_VR_day<-list()
VR_day<-list()
for(i in 1:length(VR_avg)){int_VR_day[[i]]<-VR_avg[[i]][[1]]};
for(i in 1:length(VR_avg)){VR_day[[i]]<-VR_avg[[i]][[2]]};
#rm(VR_avg)
gc()
#Gapfilling humidity parameters
if(is.null(cl)){
	int_VR_day<-mapply(gapfill,int_VR_day)
	VR_day<-mapply(gapfill,VR_day)
}else{
	parallel:::clusterExport(cl, c("gapfill","int_VR_day",'VR_day'),envir=environment())
	int_VR_day<-parallel:::clusterMap(cl = cl, fun=gapfill, int_VR_day)	
	VR_day<-parallel:::clusterMap(cl = cl, fun=gapfill, VR_day)
}

#averaging to daily



if(is.null(cl)){
	int_VR_day<-setZ(stack(int_VR_day),zdates_atm)
	int_VR_day<-zApply(int_VR_day,as.Date(zdates_atm),mean)
	VR_day<-setZ(stack(VR_day),zdates_atm)
	VR_day<-zApply(VR_day,as.Date(zdates_atm),mean)
}else{
	int_VR_day<-setZ(stack(int_VR_day),zdates_atm)
	int_VR_day<-aggRaster(int_VR_day,func = "mean",xout='daily',cl=cl,varnam='int_VR_day',longname='int_VR_day_gaps', varunit='kg/kg')
	VR_day<-setZ(stack(VR_day),zdates_atm)
	VR_day<-aggRaster(VR_day,func = "mean",xout='daily',varnam='VR_day',cl=cl,longname='VR_day_gaps', varunit='kg/kg/m')
}
gc()
if(is.null(cl)){
	int_VR_day<-mapply(gapfillgauss,as.list(int_VR_day))
	VR_day<-mapply(gapfillgauss,as.list(VR_day))
}else{
	parallel:::clusterExport(cl, c("gapfill","int_VR_day",'VR_day'),envir=environment())
	int_VR_day<-parallel:::clusterMap(cl = cl, fun=gapfillgauss, as.list(int_VR_day))	
	VR_day<-parallel:::clusterMap(cl = cl, fun=gapfillgauss, as.list(VR_day))
}

int_VR_day<-stack(int_VR_day)
VR_day<-stack(VR_day)

int_VR_day<-setZ(int_VR_day,unique(as.Date(zdates_atm)))
VR_day<-setZ(VR_day,unique(as.Date(zdates_atm)))
end.time<-Sys.time()
cat("time taken computing daily average ELR per pixel:",end.time-start.time	,"\n")
start.time<-Sys.time()
# stopCluster(cl)

Ta.int<-calc(int_day,fun=function(x){x*10},filename=paste0(outdir,'/','Ta.int_',hv[1],'.',beg,'.',til,'.nc'),format="CDF",datatype='INT2S',force_v4=TRUE,compression=7,overwrite=TRUE,varname="Ta_int", varunit="C", longname="intercept of Ta lm, factor 10", xname="lon", yname="lat", zname="time", zunit=paste("days","since",paste0(as.numeric(format(as.Date(start),'%Y'))-1,"-",12,"-",31)))

Ta.lr<-calc(LR_day,fun=function(x){x*10000},filename=paste0(outdir,'/','Ta.lr_',hv[1],'.',beg,'.',til,'.nc'),format="CDF",datatype='INT2S',force_v4=TRUE,compression=7,overwrite=TRUE,varname="Ta_lr", varunit="C/m", longname="Ta lapse rate factor 1e5", xname="lon", yname="lat", zname="time", zunit=paste("days","since",paste0(as.numeric(format(as.Date(start),'%Y'))-1,"-",12,"-",31)))

MR.int<-calc(int_VR_day,fun=function(x){x*10000},filename=paste0(outdir,'/','MR.int_',hv[1],'.',beg,'.',til,'.nc'),format="CDF",datatype='INT2S',force_v4=TRUE,compression=7,overwrite=TRUE,varname="MR_int", varunit="kg/kg", longname="intercept of the mixing ratio lm, factor 1e5", xname="lon", yname="lat", zname="time", zunit=paste("days","since",paste0(as.numeric(format(as.Date(start),'%Y'))-1,"-",12,"-",31)))


MR.slp<-calc(VR_day,fun=function(x){x*10000000},filename=paste0(outdir,'/','MR.slp_',hv[1],'.',beg,'.',til,'.nc'),format="CDF",datatype='INT4S',force_v4=TRUE,compression=7,overwrite=TRUE,varname="MR_slp", varunit="kg/kg/m", longname="variation of MR with elevation, factor 1e8", xname="lon", yname="lat", zname="time", zunit=paste("days","since",paste0(as.numeric(format(as.Date(start),'%Y'))-1,"-",12,"-",31)))
########################################################################
#3.apply regressions
########################################################################
return(list(Ta.lr=Ta.lr,Ta.int=Ta.int,MR.slp=MR.slp,MR.int=MR.int))									
					
			
}




























