





matrix.to.df <- function(x, header = NA,...){  
  
  if(is.numeric(header)){
    colnames(x) = x[header,]
    x = x[-header,]
  }  
  as.data.frame(x,...)  
}