% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/getModisClim.R
\name{getModisClim}
\alias{getModisClim}
\title{getModisClim}
\usage{
getModisClim(lat, lon, start, end, outmode = list(cl = NULL, tile = TRUE,
  monthly = FALSE), dem, outdir = getwd(),
  tmpdir = dirname(rasterTmpFile()), usr = "usr", pass = "pass")
}
\arguments{
\item{start, }{end : data range}

\item{usr, }{pass : credentials for NASA EOSDIS/LAADS}

\item{coords:}{lat,lon}
}
\description{
download and gapfill MOD07 and MYD07 atmospheric profiles (air temperature and humidity), calculate saturated vapour pressure using dowsncaled LST from Microwave SSMI and Modis IR, if used with the option use.clouds=TRUE, additional files from MOD06 and MYD06 cloud product will be used to infer temperature and actual vapour pressure under the clouds (only works it the top of the cloud is below the tropopause)
}
\examples{
getModisClim()
}
\keyword{air}
\keyword{modis,}
\keyword{pressure}
\keyword{temperature,}
\keyword{vapour}
