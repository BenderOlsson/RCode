





matrix.to.df <- function(x, header = NA,...){  
  
  if(is.numeric(header)){
    colnames(x) = x[header,]
    x = x[-header,]
  }  
  as.data.frame(x,...)  
}

has.Mcap <- function (x, which = FALSE) 
{
  colAttr <- attr(x, "Mcap")
  if (!is.null(colAttr)) 
    return(if (which) colAttr else TRUE)
  loc <- grep("Mcap", colnames(x), ignore.case = TRUE)
  if (!identical(loc, integer(0))) {
    return(if (which) loc else TRUE)
  }
  else FALSE
}

Mcap <- function (x){
  if (has.Mcap(x)) 
    return(x[, grep("Mcap", colnames(x), ignore.case = TRUE)])
  stop("subscript out of bounds: no column name containing \"Mcap\"")
}

rollapply.partial <- function(x, width = 10, FUN = mean, max.na = 0.1, ...){  
  n = round((max.na)*width)
  tmp = rollapply(x, width, FUN, na.rm=T, fill = NA,...)
  isna = rollapply(is.na(x), width, sum, fill = NA, ...)    
  tmp[isna > n] = NA
  tmp  
}

diff.by.days <- function(x, y = NULL, nlag = 1, period.fn = days){
  library(data.table)
  library(lubridate)
  
  tbl = as.data.table(as.data.frame(x),keep.rownames=T)
  setnames(tbl,c("Date","Value"))
  tbl[,Date:=as.Date(Date)]
  setkey(tbl, Date)
  
  if(!is.null(y)){
    tbl2 = as.data.table(as.data.frame(y),keep.rownames=T)
    setnames(tbl2,c("Date","LagValue"))
    tbl2[,Date:=as.Date(Date)]
    setkey(tbl2,Date)  
    
    tbl2 = tbl2[,Date:=Date+period.fn(nlag)]    
  } else {    
    tbl2 = tbl[,list(Date=Date+period.fn(nlag),LagValue = Value)]      
  }
  
  setkey(tbl2,Date)  
  tbl2 = tbl2[J(tbl[,Date]),roll=T,rollends=c(T,F)]
  
  tbl3 = tbl[tbl2,roll=F][,list(Date,Value=Value-LagValue)]      
  xts(tbl3[,Value],tbl3[,Date])
}