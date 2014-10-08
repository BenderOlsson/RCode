





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
