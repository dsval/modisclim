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

getModisClim<-function(lat,lon,start,end,DNB='DB',options=list(cl=NULL,tile=TRUE,monthly=FALSE),dem,outdir=getwd(), tmpdir=dirname(rasterTmpFile()),usr='usr',pass='pass'){
	# testing
	on.exit(traceback(1))
	if(!is.null(options$cl)){
		on.exit(stopCluster(options$cl))
	}
	
	rasterOptions(todisk=F)
	# end testing
########################################################################
#1.get the urls
########################################################################
	dem_hr<-dem
	#build the query
	options$use.clouds=TRUE
	cat('Retrieving the urls',"\n")
 url<- "http://modwebsrv.modaps.eosdis.nasa.gov/axis2/services/MODAPSservices/"
 if (options$use.clouds==TRUE){
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
 }else{
	query_par <- list(products = gsub(" ", "", toString(c('MOD07_L2','MYD07_L2'))),
		collection = 61,
		startTime = start,
		endTime = end,
		north = lat+0.1,
		south = lat-0.11,
		east = lon+0.1,
		west = lon-0.1,
		coordsOrTiles = 'coords',
		dayNightBoth = DNB)
 }

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
	if(fileIds[1,] == 'No results') stop("No records whitin the time range")
	# get the urls
	get_urls<-function(fileid){
		files_md<-httr::GET(url = paste0(url,"getFileUrls"),query = list(fileIds=fileid))
		fileurl <- httr::content(files_md, as = "text")
		# fileurl <- xml2::read_xml(fileurl)
		fileurl <- xml2::read_html(fileurl)
		fileurl <- do.call(rbind, xml2::as_list(fileurl)[[1]][[1]][[1]])
		fileurl
	}
	
	file_urls<-mapply(FUN=get_urls,fileIds)
	file_urls<-do.call(c,file_urls)
	zdates<-do.call(rbind,strsplit(file_urls,'.',fixed=T))
	# length(zdates[1,])-4
	zdates<-strptime(paste0(zdates[,length(zdates[1,])-4],zdates[,length(zdates[1,])-3]),format='A%Y%j%H%M')
	file_urls<-cbind(file_urls,as.character(zdates))
	file_urls<- file_urls[order(file_urls[,2]),]
	########################################################################
	#1.get monthly MODIS LST urls
	########################################################################
	qextent<-query_par
	qextent[[1]]<-'MOD11B3'
	qextent[[2]]<-6
	qextent[[10]]<-'D'
	format(as.Date(start),'%m')
	format(as.Date(start),'%Y')
	qextent[[3]]<-paste0(format(as.Date(start),'%Y'),'-',format(as.Date(start),'%m'),'-01')
	qextent[[4]]<-paste0(format(as.Date(end),'%Y'),'-',format(as.Date(end),'%m'),'-01')
	query_oute<-httr::GET(url = paste0(url, "searchForFiles"),query = qextent)
	while(query_out$status_code != 200){
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
	if(options$monthly==FALSE){
		dateseq<-format(seq(as.Date(start),as.Date(end),by='day'),format='%Y.%m.%d')
		urlsSSM<-paste0('https://n5eil01u.ecs.nsidc.org/PM/NSIDC-0032.002/',as.character(dateseq),'/')
		
		get_ssmurl<-function(dayurl,DNB){
			url_file_SSM<-GET(dayurl,authenticate(usr, pass))
			if(url_file_SSM$status_code==404){
				return(NA)
			}else{
				url_file_SSM <- httr::content(url_file_SSM, as = "parsed")
				url_file_SSM <-xml2::as_list(url_file_SSM)
				url_file_SSM<-do.call(rbind,url_file_SSM$html$body[[4]]$table)
				# get brightness temperature 37gz V polarization Holmes, et al. 2008 doi 10.1029/2008JD010257
				if(DNB=='DB'|DNB=='D'){
					url_file_SSM<-regmatches(unlist(url_file_SSM[,2]), gregexpr('.*.ML.*.A.*.37V.*.gz$', unlist(url_file_SSM[,2])))
				}else{
					url_file_SSM<-regmatches(unlist(url_file_SSM[,2]), gregexpr('.*.ML.*.D.*.37V.*.gz$', unlist(url_file_SSM[,2])))
				}
				
				url_file_SSM<-do.call(c,url_file_SSM)
				return(paste0(dayurl,url_file_SSM))
			}
			
		}
		SSM_url<-mapply(get_ssmurl,urlsSSM,MoreArgs = list(DNB=DNB),SIMPLIFY = T)
		SSM_url<-SSM_url[!is.na(SSM_url)]	
		
		
	}
	
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
	#2.download athmosphere
	########################################################################
	
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
	########################################################################
	#2.download SSM LST
	########################################################################
	if(options$monthly==FALSE){
		destfiles_ssm<-basename(SSM_url)
		cat(' ',"\n")
		cat('downloading SSM/I, SSMIS LST',"\n")
		pb <- txtProgressBar(min=1,max = length(destfiles_ssm), style = 3)
		for(i in 1:length(destfiles_ssm)){
			if(!file.exists(destfiles_ssm[i])){
				respon <-httr::GET(as.character(SSM_url[i]), write_disk(destfiles_ssm[i], overwrite = TRUE), 
					authenticate(usr, pass))
				# Check to see if file downloaded correctly
				if (response$status_code == 200) {
					cat(' ',"\n")
					cat(destfiles_ssm[i],"downloaded","\n")
				} else {
					cat("error downloading, make sure usr/password are correct","\n")
				}
				if(length(destfiles_ssm)>1){setTxtProgressBar(pb,i)}
			}
		}
		filenames_SSM<-paste0(tmpdir,'/',destfiles_ssm)	
	}
	
	

########################################################################
#3.read the data, ....mapply is working faster than overay wth a rasterstack
########################################################################	
# prepare the filenames
filenames<-paste0(tmpdir,'/',destfiles)
if (options$use.clouds==TRUE){
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
	(elev2pres(elev)*kMa)/(kR*(t+273.15))
}

############################# func to read modis lst############################
readlst<-function(filename){
	# filename=filenamlst[1]
	lst<-raster(get_subdatasets(filename)[1])
	lst<-projectRaster(lst,crs="+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0")
	lst-273.15
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
	tropohgt <- brick(readGDAL(sds[18], as.is = TRUE,silent = T))+32500
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
	vw[vw<0]<-0
	extent(vw)<-extent(bbox)
	projection(vw)<-wgs
	fr<-function(x,y) {
		
		frvect<-function(x,y){
			if(sum(is.na(x))==length(x)){
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
	calc_lr<-function(x,y){overlay(x,y,fun=fr)}
	
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
	# filename=filenames_cld[4]
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
	# read cloud top height (masl)
	cth <- raster(readGDAL(sds[58], as.is = TRUE,silent = T))
	extent(cth)<-extent(bbox)
	projection(cth) <- wgs
	#get rid of readings above the tropopause
	cth<- reclassify(cth, cbind(8000, +Inf, NA), right=TRUE)
	# read cloud optical thickness
	copt<- raster(readGDAL(sds[73], as.is = TRUE,silent = T))*0.009999999776482582
	extent(copt)<-extent(bbox)
	projection(copt) <- wgs
	# read cloud effective radius
	cefr<- raster(readGDAL(sds[67], as.is = TRUE,silent = T))*0.009999999776482582
	extent(cefr)<-extent(bbox)
	projection(cefr) <- wgs
	# calculate cloud base height(masl)  Welch, et al. 2008 doi 10.1175/2007JAMC1668.1
	calc_bh<-function(cot,cer,clth){
		cldbh<-clth-((4/3)*cot*cer*(0.002)^-1)^0.5
		ifelse(cldbh<0,0.0,cldbh)
	}
	cl_bs<-overlay(copt,cefr,cth,fun=calc_bh)
	# read cloud top temperature
	tcloud<-brick(readGDAL(sds[104], as.is = TRUE,silent = T))
	tcloud<-((tcloud+15000)* 0.009999999776482582)-273.15
	extent(tcloud)<-extent(bbox)
	projection(tcloud) <- wgs
	tcloud<-projectRaster(tcloud,cth)
	# rgadl error reading QA flags, remove bad vlues
	tcloud<-reclassify(tcloud, cbind(-Inf,-30, NA), right=TRUE)
	#get rid of readings above the tropopause
	tcloud<-mask(tcloud,cth)
	# cloud thickness
	z_cld<-cth-cl_bs
	######air temperature at the base of the cloud######
	# temperature in kelvin
	TclK<-tcloud+273.15
	# get cloud water vap pressure [pa]
	es<-0.6108*1000*exp((17.27*tcloud)/(tcloud+237.3))
	#calc mixing ratio at the top of the cloud http://glossary.ametsoc.org/wiki/Mixing_ratio
	mr<-(0.622*es)/(elev2pres(cth)-es)
	# get saturated adiabatic lapse rate K/m http://glossary.ametsoc.org/wiki/Saturation-adiabatic_lapse_rate
	L<-9.8*((287*(TclK)^2+ 2501000*mr*(TclK))/(1003.5*287*(TclK)^2+2501000^2*mr*0.622))
	#calc air temperature at the base of the cloud
	Ta_bh<-tcloud+(L*z_cld)
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
	calc(r,function(x){y=(x/10);(1.11*y-15.2)-273.15})
}
############################# reading the data ############################
##reading temperature and humidity profiles
cat(' ',"\n")
if(is.null(options$cl)){
	cat('reading temperature and humidity profiles',"\n")
	atm<-mapply(FUN=readMOD07,filenames_atm,SIMPLIFY = F)
	cat('reading cloud info',"\n")
	clds<-mapply(FUN=readMOD06,filenames_cld,SIMPLIFY = F)
}else{
	doSNOW::registerDoSNOW(options$cl)
	snow::clusterEvalQ(options$cl, lapply(c('raster','rgdal','gdalUtils'), library, character.only = TRUE))
	snow::clusterExport(options$cl, list=c("readMOD07","readMOD06",'filenames_atm','filenames_cld','elev2pres'),envir=environment())
	cat('reading temperature and humidity profiles',"\n")
	atm<-snow::clusterMap(cl = options$cl, fun=readMOD07, filename=filenames_atm)
	cat('reading cloud info',"\n")
	clds<-snow::clusterMap(cl = options$cl, fun=readMOD06, filename=filenames_cld)
	
}

gc()

##read modis lst
cat(' ',"\n")
cat('reading modis LST',"\n")
lst_mod<-mapply(FUN=readlst,filenamlst,SIMPLIFY = F)
##read ssm lst
if(options$monthly==FALSE){
	cat(' ',"\n")
	cat('reading SSM LST',"\n")
	##read ssm lst
	lst_ssm<-mapply(FUN=read_ssm,filenames_SSM,SIMPLIFY = F)
	lst_ssm<-mapply(crop,x=lst_ssm,MoreArgs = list(y=lst_mod[[1]]),SIMPLIFY = F)
	lst_ssm<-approxNA(stack(lst_ssm),rule=2)
	########################################################################
	#1. downscale lst
	########################################################################
	# get nday per month
	nds<-format(as.Date(dateseq,format='%Y.%m.%d'),'%m')
	nds<-as.data.frame(table(nds))
	# get array to do mapply
	lsmpd<-unlist(mapply(replicate,nds$Freq,lst_mod,SIMPLIFY = F))
	# get funct to downs
	cat('downscaling LST',"\n")
	dowsn<-function(x,y){downscaleRLM(x,y)$downscale}
	# gapfill
	lst_ssm<-mapply(dowsn,as.list(lst_ssm),lsmpd,SIMPLIFY = F)
}

gc()
########################################################################
#3.Organize the layers
########################################################################
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
	fillLR<-focal(fillLR,w = matrix(1,3,3), fun = fill.na, pad = TRUE, na.rm = FALSE) 
	# for (i in 1:2){
	# 	fillLR<-focal(fillLR,w = matrix(1,3,3), fun = fill.na, pad = TRUE, na.rm = FALSE)
	# }
	return(fillLR)
}

#gaptest<-gapfill(int_VR_day[[1]])

gc()
dem<-projectRaster(dem,lst_mod[[1]])
dem_hr<-crop(dem_hr,dem)

gc()

########################################################################
#3.calculate  temp lapse rate under the clouds and map orographic inmmersion following Nair et al., 2008 doi: 10.1175/2007JAMC1819.1
########################################################################
setwd(tmpdir)
calc_ub_cld<-function(cl_b_t,cld_b_ht,dem){
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
calc_ub_cld_rast<-function(cl_b_t,cld_b_ht,dem){overlay(cl_b_t,cld_b_ht,dem,fun=calc_ub_cld)}

LR_uclds<-mapply(FUN=calc_ub_cld_rast,cl_b_t=cl_b_T,cld_b_ht=cl_b_hgt,MoreArgs = list(dem=dem))


#calc_ub_cld_rast not working in parallel, weird error vectorized formula??
# if(is.null(cl)){
# 	LR_uclds<-mapply(FUN=calc_ub_cld_rast,cl_b_t=cl_b_T,cld_b_ht=cl_b_hgt,MoreArgs = list(dem=dem))
# }else{
# 	snow::clusterExport(options$cl, list=c("calc_ub_cld_rast","calc_ub_cld",'cl_b_T','cl_b_hgt','dem'),envir=environment())
# 	LR_uclds<-snow::clusterMap(cl = options$cl, fun=calc_ub_cld_rast, cl_b_t=cl_b_T,cld_b_ht=cl_b_hgt,MoreArgs = list(dem=dem))	
# }



#function to calc average between raster objects, ignoring Nas
calc_avgTa<-function(x,y){overlay(x,y,fun=function(x,y){rowMeans(cbind(x,y),na.rm = T)})}
#function to loop averages between clear sky and cloudy data

calc_lr_avg<-function(cs,uc){stack(calc_avgTa(cs[[1]],uc[[1]]),calc_avgTa(cs[[2]],uc[[2]]))}

#merged LR cloudy and clear data
LR_avg<-mapply(calc_lr_avg,LR_csky,LR_uclds)

#intercept daily Ta 
int_day<-list()
LR_day<-list()
for(i in 1:length(LR_avg)){int_day[[i]]<-LR_avg[[i]][[1]]};
for(i in 1:length(LR_avg)){LR_day[[i]]<-LR_avg[[i]][[2]]};
rm(LR_avg)
gc()
#Gapfilling temperature parameters
if(is.null(options$cl)){
	int_day<-mapply(gapfill,int_day)
	LR_day<-mapply(gapfill,LR_day)
}else{
	snow::clusterExport(options$cl, list=c("gapfill","int_day",'LR_day'),envir=environment())
	int_day<-snow::clusterMap(cl = options$cl, fun=gapfill, int_day)	
	LR_day<-snow::clusterMap(cl = options$cl, fun=gapfill, LR_day)
}

#averaging to daily

int_day<-setZ(stack(int_day),zdates_atm)
int_day<-zApply(int_day,as.Date(zdates_atm),mean)
LR_day<-setZ(stack(LR_day),zdates_atm)
LR_day<-zApply(LR_day,as.Date(zdates_atm),mean)


########################################################################
#3.gap fill daily adiabatic Lapse rate asuming meain in 3 neigthbour pixels
########################################################################

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
	fillLR <- focal(fillLR, w=gf,na.rm=F) 
	# for (i in 1:2){
	# 	fillLR<-focal(fillLR,w = matrix(1,3,3), fun = fill.na, pad = TRUE, na.rm = FALSE)
	# }
	return(fillLR)
}



if(is.null(options$cl)){
	int_day<-mapply(gapfillgauss,as.list(int_day))
	LR_day<-mapply(gapfillgauss,as.list(LR_day))
}else{
	snow::clusterExport(options$cl, list=c("gapfill","int_day",'LR_day'),envir=environment())
	int_day<-snow::clusterMap(cl = options$cl, fun=gapfillgauss, as.list(int_day))	
	LR_day<-snow::clusterMap(cl = options$cl, fun=gapfillgauss, as.list(LR_day))
}


int_day<-stack(int_day)
LR_day<-stack(LR_day)
int_day<-setZ(int_day,unique(as.Date(zdates_atm)))
LR_day<-setZ(LR_day,unique(as.Date(zdates_atm)))

########################################################################
#3.process humidity
########################################################################
#intercept vw regression

calc_lr_avg<-function(cs,uc){stack(calc_avgTa(cs[[1]],uc[[3]]),calc_avgTa(cs[[2]],uc[[4]]))}

#merged LR cloudy and clear data
VR_avg<-mapply(calc_lr_avg,VR_csky,LR_uclds)
#calc daily averages
int_VR_day<-list()
VR_day<-list()
for(i in 1:length(VR_avg)){int_VR_day[[i]]<-VR_avg[[i]][[1]]};
for(i in 1:length(VR_avg)){VR_day[[i]]<-VR_avg[[i]][[2]]};
rm(VR_avg)
gc()
#Gapfilling humidity parameters
if(is.null(options$cl)){
	int_VR_day<-mapply(gapfill,int_VR_day)
	VR_day<-mapply(gapfill,VR_day)
}else{
	snow::clusterExport(options$cl, list=c("gapfill","int_VR_day",'VR_day'),envir=environment())
	int_VR_day<-snow::clusterMap(cl = options$cl, fun=gapfill, int_VR_day)	
	VR_day<-snow::clusterMap(cl = options$cl, fun=gapfill, VR_day)
}

#averaging to daily

int_VR_day<-setZ(stack(int_VR_day),zdates_atm)
int_VR_day<-zApply(int_VR_day,as.Date(zdates_atm),mean)
VR_day<-setZ(stack(VR_day),zdates_atm)
VR_day<-zApply(VR_day,as.Date(zdates_atm),mean)


if(is.null(options$cl)){
	int_VR_day<-mapply(gapfillgauss,as.list(int_VR_day))
	VR_day<-mapply(gapfillgauss,as.list(VR_day))
}else{
	snow::clusterExport(options$cl, list=c("gapfill","int_VR_day",'VR_day'),envir=environment())
	int_VR_day<-snow::clusterMap(cl = options$cl, fun=gapfillgauss, as.list(int_VR_day))	
	VR_day<-snow::clusterMap(cl = options$cl, fun=gapfillgauss, as.list(VR_day))
}

int_VR_day<-stack(int_VR_day)
VR_day<-stack(VR_day)

int_VR_day<-setZ(int_VR_day,unique(as.Date(zdates_atm)))
VR_day<-setZ(VR_day,unique(as.Date(zdates_atm)))


# stopCluster(cl)

# function to apply the regression parameters

calc_regr<-function(x,m,yo){
	yo+m*x
}

#project to dem resolution
# stopCluster(options$cl)
gc()
int_VR_day<-projectRaster(int_VR_day,dem_hr)
VR_day<-projectRaster(VR_day,dem_hr)

int_day<-projectRaster(int_day,dem_hr)
LR_day<-projectRaster(LR_day,dem_hr)
## compute daily air temperature and humidity at surface

Ta<-overlay(dem_hr,LR_day,int_day,fun=calc_regr)

vr<-overlay(dem_hr,VR_day,int_VR_day,fun=calc_regr)

#compute ea [Pa] from mixing ratio
calc_ea<-function(a,t,dem){
	#dry air density[kg/m3]
	pair<-p_dryair(dem,t)
	#get vapour density [kg/m3](Oke, 1996)
	pvap<-a*pair
	ea<-pvap*461.5*(273.15+t)
	es<-0.6108*1000*exp((17.27*t)/(t+237.3))
	ea<-ifelse(ea>es,es,ea)
	ea<-ifelse(ea<0,0,ea)
	ea
	
}
ea<-overlay(vr,Ta,dem_hr,fun=calc_ea)

if(options$monthly==TRUE){
	# get es* [pa]
	es<-calc(stack(lst_mod),fun=function(ts)0.6108*1000*exp((17.27*ts)/(ts+237.3)))
	es<-approxNA(es,rule=2)
	es<-projectRaster(es,Ta)
	Ta<-setZ(Ta,unique(as.Date(zdates_atm)))
	ea<-setZ(ea,unique(as.Date(zdates_atm)))
	monthind<-format(getZ(Ta),'%Y-%m')
	Ta<-zApply(Ta,monthind,fun=mean,na.rm=T)
	ea<-zApply(ea,monthind,fun=mean,na.rm=T)
	dateseq_month<-as.Date(format(seq(as.Date(start),as.Date(end),by='month'),format='%Y-%m-%d'))
	es<-setZ(es,dateseq_month)
	ea<-setZ(ea,dateseq_month)
	Ta<-setZ(Ta,dateseq_month)
	
	Ta<-writeRaster(Ta,paste0(outdir,'/','Ta_month_',hv[1],'.',beg,'.',til,'.nc'),format="CDF",overwrite=TRUE,varname="Ta", varunit="C", longname="air temperature", xname="lon", yname="lat", zname="time")
	ea<-writeRaster(ea,paste0(outdir,'/','ea_month_',hv[1],'.',beg,'.',til,'.nc'),format="CDF",overwrite=TRUE,varname="ea", varunit="Pa", longname="actual vapor pressure", xname="lon", yname="lat", zname="time")
	es<-writeRaster(es,paste0(outdir,'/','es_month_',hv[1],'.',beg,'.',til,'.nc'),format="CDF",overwrite=TRUE,varname="es", varunit="Pa", longname="saturation vapor pressure(from LST)", xname="lon", yname="lat", zname="time")
}else{
	es<-calc(stack(lst_ssm),fun=function(ts)0.6108*1000*exp((17.27*ts)/(ts+237.3)))
	# es<-projectRaster(es,Ta)
	Ta<-writeRaster(Ta,paste0(outdir,'/','Ta_day_',hv[1],'.',beg,'.',til,'.nc'),format="CDF",overwrite=TRUE,varname="Ta", varunit="C", longname="air temperature", xname="lon", yname="lat", zname="time", zunit=paste("days","since",paste0(as.numeric(format(as.Date(start),'%Y'))-1,"-",12,"-",31)))
	ea<-writeRaster(ea,paste0(outdir,'/','ea_day_',hv[1],'.',beg,'.',til,'.nc'),format="CDF",overwrite=TRUE,varname="ea", varunit="Pa", longname="actual vapor pressure", xname="lon", yname="lat", zname="time", zunit=paste("days","since",paste0(as.numeric(format(as.Date(start),'%Y'))-1,"-",12,"-",31)))
	es<-projectRaster(es,Ta,filename=paste0(outdir,'/','es_day_',hv[1],'.',beg,'.',til,'.nc'),format="CDF",overwrite=TRUE,varname="es", varunit="Pa", longname="saturation vapor pressure (from LST)", xname="lon", yname="lat", zname="time", zunit=paste("days","since",paste0(as.numeric(format(as.Date(start),'%Y'))-1,"-",12,"-",31)))
}


return(list(Ta=Ta,ea=ea,es=es))									
					
			
}




























