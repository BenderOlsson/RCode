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


sanitizeLatexS <- 
function(
	str
){
	#gsub('([#$%&~_\\^\\\\{}])', '\\\\\\\\\\1', str, perl = TRUE);
	gsub('([#$%&~_\\^\\\\{}])', '\\\\\\1', str, perl = TRUE);
}


create.performance.report <- 
function(
	inputDataFile, 
	outputFile,
	templateFile
)
{  
  library(knitr)
  
  # Create path to temp data file
  tmpDataFile = "PerformanceData.csv"
  
  # Copy data file to a temp file
  if(!file.exists(inputDataFile)) stop("input data file is missing")
  file.copy(from = inputDataFile, to = tmpDataFile, overwrite = T)
  
  # Check template
  # f = paste(templateDir,"performanceReport_AssetAllocation.Rnw",sep="\\")  
  if(!file.exists(templateFile)) stop("Template is missing")
  templateFile = chartr("\\", "/", templateFile)
  
  # Convert TeX to Pdf
  inputPath <<- dirname(inputDataFile)
  inputFile <<- basename(inputDataFile)
  
  knit2pdf(templateFile,output='PerformanceReport.tex',quiet=T)
  
  tmp_pdf = "PerformanceReport.pdf"
  if(file.exists(tmp_pdf)){
    cat("Report created... copying...\n")
    file.copy(from = tmp_pdf, to = outputFile, overwrite = T) 
    
    if(file.exists(outputFile))
      file.remove(tmp_pdf)
    
    if(file.exists('PerformanceReport.tex'))
      file.remove('PerformanceReport.tex')  
    
    if(file.exists('PerformanceReport.aux'))
      file.remove('PerformanceReport.aux')
    
    file.remove(tmpDataFile)
    
    # ToDo Spara ifall det har chrashat?
    if(file.exists('PerformanceReport.log'))
      file.remove('PerformanceReport.log')
  }
}

performance.report <- 
function(
  inputPath = inputPath,
  inputFile = inputFile,
  columnList = list(
    prt = 1,
    bmk = 2,
    rf = 3,
    eq = 4
  )  
){
	library(knitr)
	library(PerformanceAnalytics)
	library(xtable)
	library(xts)
	library(xtsExtra)
	library(RColorBrewer)
	library(lubridate)

	
	if(!file.exists(file.path(inputPath,inputFile))) stop(file.path(inputPath,inputFile), " is missing")
	
	data <- read.csv(file.path(inputPath,inputFile),sep=",")

	dataDaily = xts(data[,-1],as.Date(data[,1]))
	days = index(dataDaily)
	years = unique(years(days)) 
	months = sort(unique(substring(days,1,7))) 
	  
	# Strings ----
	cyrStr = as.numeric(substring(Sys.Date(),1,4))
	ytdStr = paste0(cyrStr,"::")

	# Calc data ----
	dailyRtn = as.xts(Return.calculate(dataDaily,"log"),days)
	dailyDD = as.xts(apply(dataDaily*100,2,compute.drawdown)*100,days)
	maxDD =  apply(dailyDD,2,min,na.rm=T)

	monthlyRtn = (exp(apply.monthly(dailyRtn,FUN=colSums,na.rm=T))-1)
	yearlyRtn = (exp(apply.yearly(dailyRtn,FUN=colSums,na.rm=T))-1) 



	#rets = Return.calculate(dataDaily[c(1,endpoints(x = dataDaily,"months")[-1])],"discrete")

	#table.CalendarReturns(rets[,c(prt,bmk)],geometric=T,digits=4,as.perc=F)



	# Select data ----
	n = ncol(dataDaily)

	prt = columnList$prt

	rf = columnList$rf
	if(rf > n) rf = NULL

	bmk = columnList$bmk
	if(bmk > n) bmk = NULL

	eq = columnList$eq
	if(eq > n) eq = bmk

	dailyRfRtn = NULL
	if(!is.null(rf) && ncol(dailyRtn) >= rf) dailyRfRtn = dailyRtn[,rf]

	dailyEqRtn = NULL
	if(!is.null(eq) && ncol(dailyRtn) >= eq) dailyEqRtn = dailyRtn[,eq]

	dailyBmkRtn = NULL
	if(!is.null(bmk) && ncol(dailyRtn) >= bmk) dailyBmkRtn = dailyRtn[,bmk]


	# Performance Table ----
	tmpTbl = table.CalendarReturns(monthlyRtn[,c(prt,bmk)],geometric=T,digits=4,as.perc=T)
	colnames(tmpTbl)[13:14] = c("Prt","Bmk")

	xtablePerfMonthly = xtable(tmpTbl,caption="Monthly Percentage Return (net of fees)",digits=2)  
	print(xtablePerfMonthly,caption.placement="top",include.rownames=T,latex.environment="center",size="\\scriptsize")

	# Timeseries plots ----
	par(mfrow=c(3,2),cex=0.5,mex=0.3)


	#plot.xts((exp(cumsum(na.fill(dailyRtn[,c(prt,bmk)],0)))-1)*100,screens=1,col = c("blue","black"),auto.legend=T,minor.ticks=F,major.format="%Y-%m-01",major.ticks="months",main="Equity Curve - Since Inception (%)")

	tmp_y = (exp(cumsum(na.fill(dailyRtn,0)))-1)*100
	tmp_x = time(tmp_y)
	plot(tmp_x,tmp_y[,prt,drop=F],type="l",main="Equity Curve - Since Inception (%)",xlab="",ylab="",ylim=range(tmp_y[,c(prt,bmk)]),lwd=1,)
	lines(tmp_x,tmp_y[,bmk,drop=F],type="l",col="darkgrey",lty=1)
	lines(tmp_x,tmp_y[,prt,drop=F],type="l",col="black",lty=1,lwd=2)
	grid(col="dark grey")

	legend( "topleft",legend=c("Prt","Bmk"),bty="n",horiz=T,col=c("black","darkgrey"),pch=NA,lty=1,title=NULL,cex=1,lwd=2)


	tmp_y = compute.returs(dailyRtn[ytdStr])
	tmp_x = time(tmp_y)
	plot(tmp_x,tmp_y[,prt,drop=F],type="l",main="Equity Curve - YTD (%)",xlab="",ylab="",ylim = range(tmp_y[,c(prt,bmk)]),lwd=1)
	lines(tmp_x,tmp_y[,bmk,drop=F],type="l",col="darkgrey",lty=1)
	lines(tmp_x,tmp_y[,prt,drop=F],type="l",col="black",lty=1,lwd=2)
	grid(col="dark grey")
	legend( "topleft",legend=c("Prt","Bmk"),bty="n",horiz=T,col=c("black","darkgrey"),pch=NA,lty=1,title=NULL,cex=1,lwd=2)


	tmp_y = dailyDD
	tmp_x = time(tmp_y)
	plot(days,dailyDD[,prt,drop=F],type="l",xlab="",ylab="",main="maximum DrawDown - Since Inception (%)",ylim = range(tmp_y[,c(prt,bmk)]),lwd=1)
	lines(tmp_x,tmp_y[,bmk,drop=F],type="l",col="darkgrey",lty=1)
	lines(tmp_x,tmp_y[,prt,drop=F],type="l",col="black",lty=1,lwd=2)
	grid(col="dark grey")
	legend("bottomleft",legend=c("Prt","Bmk"),bty="n",horiz=F,col=c("black","darkgrey"),pch=NA,lty=1,title=NULL,cex=1,lwd=2)

	tmp_x = days[year(days)==cyrStr]
	tmp_y = as.xts(apply(dataDaily[ytdStr]*100,2,compute.drawdown)*100,tmp_x)
	plot(tmp_x,tmp_y[,prt,drop=F],type="l",xlab="",ylab="", main="maximum DrawDown - YTD (%)",ylim = range(tmp_y[,c(prt,bmk)]),lwd=1)
	lines(tmp_x,tmp_y[,bmk,drop=F],type="l",col="darkgrey",lty=1)
	lines(tmp_x,tmp_y[,prt,drop=F],type="l",col="black",lty=1,lwd=2)
	grid(col="dark grey")
	legend("bottomleft",legend=c("Bmk","Prt"),bty="n",horiz=F,col=c("black","darkgrey"),pch=NA,lty=1,title=NULL,cex=1,lwd=2)

	# Barplots ----
	maxDD = maxDD[prt]
	dailyDD = dailyDD[,prt]

	create.barplot(
	  yearlyRtn[,prt]*100,
	  plotLabels = year(yearlyRtn[,prt]),
	  plotTitle = "Yearly Return - Since Inception (%)"
	)

	create.barplot(
	  monthlyRtn[ytdStr,prt]*100,
	  plotLabels = format(index(monthlyRtn[ytdStr,prt]),"%b"),
	  plotTitle = "Monthly Return - YTD (%)"
	)


	# Stats table ----

	#inc = !(is.na(dailyRtn[,prt]) | dailyRtn[,prt] == 0)

	annRet = Return.annualized(dailyRtn[-1,prt],geometric=T)*100
	annVol = sd.annualized(dailyRtn[,prt])*100
	sr = SharpeRatio.annualized(dailyRtn[,prt],Rf=dailyRtn[,rf])[1]

	maxDDIdx = match(maxDD,dailyDD)
	maxDDDate = days[maxDDIdx]  

	nextHigh = days[maxDDIdx:length(dailyDD)][which(dailyDD[maxDDIdx:length(dailyDD)] == 0)]
	recTime = nextHigh[1] - maxDDDate

	monthlyStats = get.stats(monthlyRtn[,prt]*100)
	dailyStats = get.stats(dailyRtn[,prt]*100)

	# Create table----

	captionColumn1 = c("Ann.Return","Ann.Volatility","Sharpe Ratio")
	valueColumn1 = c(formatC(c(annRet,annVol,sr),digits=2,format = "f"))

	captionColumn2 = c("maxDD","maxDD Date","Time to Recover")
	valueColumn2 = c(formatC(maxDD,digits=2,format="f"),as.character(maxDDDate),paste(recTime,"days"))


	tradingStatistics1 = cbind(captionColumn1,valueColumn1,captionColumn2,valueColumn2)

	colnames(tradingStatistics1) = c("Performance","(%)","Draw Down","(%)")
	row.names(tradingStatistics1) = NULL

	xtableResult1 = xtable(tradingStatistics1,caption="Trading Statistics",digits=2,align = "llrlr")

	print(xtableResult1,caption.placement="top",include.rownames=F,size="\\scriptsize")  

	# By Months
	captionColumn3 = c("Hit Rate","Mean Return","Mean > 0","Mean < 0","Worst","Best")
	valueColumn3 = formatC(unlist(monthlyStats),digits=2,format="f")

	captionColumn4 = c("Up Number","Down Number","Up Out.Perf","Down OutPerf","","")
	valueColumn4 = c(formatC(100*UpDownRatios(monthlyRtn[,prt],monthlyRtn[,eq],method=c("Number","Percent"),side=c("Up","Down")),digits=2,format="f"),"","")

	# By Days
	captionColumn5 = c("Hit Rate","Mean Return","Mean > 0","Mean < 0","Worst","Best")
	valueColumn5 = formatC(unlist(dailyStats),digits=2,format="f")

	captionColumn6 = c("Up Number","Down Number","Up OutPer","Down Out.Perf","","")
	valueColumn6 = c(formatC(100*UpDownRatios(dailyRtn[,prt],dailyRtn[,eq],method=c("Number","Percent"),side=c("Up","Down")),digits=2,format="f"),"","")


	tradingStatistics2 = cbind(captionColumn3,valueColumn3,captionColumn4,valueColumn4,captionColumn5,valueColumn5,captionColumn6,valueColumn6)

	colnames(tradingStatistics2) = c("Monthly","(%)","vs Eq","(%)","Daily","(%)","vs Eq","(%)")
	row.names(tradingStatistics2) = NULL

	xtableResult2 = xtable(tradingStatistics2,caption="Period Statistics",digits=2,align = "llrlrlrlr")
	  
	print(xtableResult2,caption.placement="top",include.rownames=F,size="\\scriptsize")  

}