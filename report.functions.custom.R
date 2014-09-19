add.start.values <- 
function(x,base = 0){
  rbind(xts(matrix(base,nrow=1,ncol(x),dimnames=list(NULL,colnames(x))),start(x)-1),x)
}

get.stats <- 
function(x){
  x = na.omit(x)
  hr = 100*length(which(x>0))/length(x)
  rtnAverage <- mean(x)
  rtnPositive <- mean(x[which(x > 0)])
  rtnNegative <- mean(x[which(x < 0)])
  worstPer <- min(x)
  bestPer <- max(x)  
  
  return(list(
    HitRate = hr,
    RtnAvg = rtnAverage,
    RtnPos = rtnPositive,
    RtnNeg = rtnNegative,
    WorstPer = worstPer,
    BestPer =  bestPer   
  ))
}

create.barplot <- 
function(x,plotLabels = NULL,plotTitle = ""){
  tmp = as.matrix(x)
  row.names(tmp) = plotLabels
  #colorVec = ifelse(x> 0, 1, 2)  
  colorVec = rep(1,length(x))
  
  bp = barplot(t(tmp),
   border=NA,
   col=as.vector(colorVec),
   ylim=range(floor(min(c(x),0))-1,ceiling(max(x))+1),
   main=plotTitle
  )
  text(bp, 
  ifelse(as.vector(x)<0,0,round(x,2)),
  labels=formatC(as.numeric(x),digits=2,format="f"),  
  pos=3) 
}


compute.returs <- 
function(
  x,
  geometric = T,
  addZeros = T,
  percent = T){
  
  if(!geometric){}
  
  if(addZeros) x = add.start.values(x)
  
  x = exp(cumsum(na.fill(x,0)))-1
  
  if(percent) x = x * 100
  
  return(x)
}

to.period.mult <- 
function(
	x, period = "months", k = 1, indexAt = NULL, name = NULL, OHLC = TRUE, ...
)
{
  tmp = NULL
  for(i in 1:ncol(x)){
    tmp = cbind(tmp,to.period(x[,i],period=period,k=k,indexAt=indexAt,name=name,OHLC=OHLC,...))
  }
  return(tmp)
}

period.apply.mult <- 
function(
	x, FUN, period = "months",...
)
{  
  ep <- endpoints(x,period)  
  tmp = NULL
  for(i in 1:ncol(x)){
    tmp = cbind(tmp, period.apply(x[,i], ep, FUN, ...))
  }
  return(tmp)
}

compute.drawdown <- 
function(x)
{
  return(x / cummax(c(1,x))[-1] - 1)
}

#compute.max.drawdown <- function(x)
#{
#  as.double( min(compute.drawdown(x)) )
#}

if(FALSE){
  iif <- function(cond,truepart, falsepart){
    if(length(cond) == 1) { 
      if(cond) truepart else falsepart 
    } else {
      if(length(falsepart) == 1) {
        temp = falsepart
        falsepart = cond
        falsepart[] = temp
      }
      if(length(truepart) == 1) {
        falsepart[cond] = truepart
      } else {
        cond = ifna(cond,F)
        falsepart[cond] = truepart[cond]
      }
      return(falsepart);
    }
  }
  
  ifna <- function( x,y) {
    return(iif(is.na(x) | is.nan(x) | is.infinite(x), y, x))
  }
  
  bt.apply.matrix <- function( b,xfun=Cl,...){
    out = b
    out[] = NA
    nsymbols = ncol(b)
    for( i in 1:nsymbols ) {
      msg = try( match.fun(xfun)( coredata(b[,i]),... ) , silent=T);
      if (class(msg)[1] != 'try-error') {
        out[,i] = msg
      } else {
        cat(i, msg, '\n')
      }
    }
    return(out)
  }
  
  
  # calculate factor equity
  equity <- bt.apply.matrix(1 + ifna(-factor.ret,0), cumprod)
  equity <- make.xts(equity, index(ret)) 
}
