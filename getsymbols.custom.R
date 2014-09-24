

convert.time.series <- 
function (
	fr, return.class
)
{
	if ("quantmod.OHLC" %in% return.class) {
		class(fr) <- c("quantmod.OHLC", "zoo")
		return(fr)
	} else if ("xts" %in% return.class) {
		return(fr)
	}

	if ("zoo" %in% return.class) {
		return(as.zoo(fr))
	} else if ("ts" %in% return.class) {
		fr <- as.ts(fr)
		return(fr)
	} else if ("data.frame" %in% return.class) {
		fr <- as.data.frame(fr)
		return(fr)
	} else if ("matrix" %in% return.class) {
		fr <- as.data.frame(fr)
		return(fr)
	} else if ("its" %in% return.class) {
		if ("package:its" %in% search() || suppressMessages(require("its", quietly = TRUE))) {
			fr.dates <- as.POSIXct(as.character(index(fr)))
			fr <- its::its(coredata(fr), fr.dates)
			return(fr)
		} else {
			warning(paste("'its' from package 'its' could not be loaded:", " 'xts' class returned"))
		}
	} else if ("timeSeries" %in% return.class) {
		if ("package:fSeries" %in% search() || suppressMessages(require("fSeries", quietly = TRUE))) {
			fr <- timeSeries(coredata(fr), charvec = as.character(index(fr)))
			return(fr)
		} else {
			warning(paste("'timeSeries' from package 'fSeries' could not be loaded:", " 'xts' class returned"))
		}
	}
}

get.symbols <- 
function(
	x,env
)
{
    sapply(x ,FUN = function(x,env) env[[x]], env = env)                         
}  

remove.weekends <- 
function(x){  
  library(lubridate)
  x[!wday(x)%in%c(1,7),]  
}

na.rm.by.col <- 
function(x,cols = 1:ncol(x)){
  x[!apply(is.na(x[,cols]),1,any),]  
}

bdh.to.xts <- 
function(
	x,...
)
{  
  dots = list(...)     
  
  if("ticker"%in%colnames(x)){
    x = reshape(x, timevar = "ticker", idvar = "date", direction = "wide")
    xts(x[,-1],as.Date(x[,"date"],...))  
  } else {   
    #print(dots)
    x = xts(x[,dots$field],as.Date(x[,"date"]))  
    
	if(!is.null(dots$name))
		colnames(x) = dots$name
    x
  }
}

scale.one <- 
function(
  x,
  overlay = F,
  main.index = which(!is.na(x[1,]))[1]
)
{
  index = 1:nrow(x)
  if( overlay )
    x / rep.row(apply(x, 2,
      function(v) {
        i = index[!is.na(v)][1]
        v[i] / as.double(x[i,main.index])
      }
    ), nrow(x))
  else
    x / rep.row(apply(x, 2, function(v) v[index[!is.na(v)][1]]), nrow(x))
}

getSymbols.CSV2 <- 
function (
	Symbols, env, 
	dir = "", 
	return.class = "xts", 
	extension = "csv", 
	cols = list(
	  Date = 1,
	  Open = 2,
	  High = 3,
	  Low = 4,   
	  Close = 5,
	  Volume = 6,
	  Adjusted = 7      
	),
	from = "",
	to = Sys.Date(),
	verbose = F,
	auto.assign = T,
	...
)
{
	importDefaults("getSymbols.csv")
	this.env = environment()
	for (var in names(list(...))) {
	  assign(var, list(...)[[var]], this.env)
	}

	default.tz = Sys.timezone()
	default.return.class = return.class
	default.dir = dir
	default.extension = extension
	default.cols = cols
	
	if (missing(verbose))
	  verbose = FALSE

	if (missing(auto.assign))
	  auto.assign = TRUE

	for (i in 1:length(Symbols)) {
	  return.class = getSymbolLookup()[[Symbols[[i]]]]$return.class
	  return.class = ifelse(is.null(return.class),default.return.class,return.class)
	  
	  dir = getSymbolLookup()[[Symbols[[i]]]]$dir
	  dir = ifelse(is.null(dir),default.dir,dir)
	  
	  extension = getSymbolLookup()[[Symbols[[i]]]]$extension
	  extension = ifelse(is.null(extension),default.extension,extension)
	  
	  tz = getSymbolLookup()[[Symbols[[i]]]]$tz
	  tz = ifelse(is.null(tz), default.tz, tz)
	  
	  cols = getSymbolLookup()[[Symbols[[i]]]]$cols
	  if(is.null(cols)) cols = default.cols
	  
	  filename = getSymbolLookup()[[Symbols[[i]]]]$filename
	  filename = ifelse(is.null(filename),Symbols[[i]],filename)
	  
	  if (verbose)
		cat("loading ",Symbols[[i]],".....")
	  
	  if (dir == "") {
		sym.file = paste(filename,extension,sep = ".")
	  } else {
		sym.file = file.path(dir,paste(filename,extension,sep = "."))
	  }
	  
	  if (!file.exists(sym.file)) {
		cat("\nfile ",paste(filename,"csv",sep=".")," does not exist ","in ",dir,"....skipping\n")
		next
	  }
	  fr = read.csv(sym.file,header=T,stringsAsFactors=F) #added header=TRUE      

	  if(is.null(cols$Date))
		cols$Date = 1 #grep(pattern="date|index",x=colnames(fr),ignore.case=T)  
		
	  cols = lapply(cols,function(x,y) if(is.numeric(x)) x else match(x,y),y=colnames(fr))      
	  cols[cols > ncol(fr)] = NA
	  
	  dates = as.Date(fr[,cols$Date],tz=tz)  
	  cols$Date = NULL
	  
	  if (verbose)
		cat("done.\n")

	  fr = sapply(cols,FUN=function(x,fr) if(is.na(x)) rep(NA,nrow(fr)) else as.numeric(fr[,x]),fr=fr)	
	  fr = xts(fr,dates,src = "csv", updated = Sys.time())              	  	  	  
	  fr = fr[gsub("-","",paste(from,to,sep="::"))]
	  
	  fr = convert.time.series(fr = fr,return.class = return.class)
	  Symbols[[i]] = toupper(gsub("\\^", "", Symbols[[i]]))
	  
	  if (auto.assign)
		assign(Symbols[[i]], fr, env)
	}
	if (auto.assign)
	  return(Symbols)

	return(fr)
}      
  

getSymbols.Rbbg <- 
function (
	Symbols, 
	env, 
	return.class = "xts", 
	fields = list(	  
	  Open = "PX_OPEN",
	  High = "PX_HIGH",
	  Low = "PX_LOW",   
	  Close = "PX_LAST",
	  Volume = "VOLUME",
	  Adjusted = ""      
	),
	from = "2007-01-01",
	to = Sys.Date(),
	verbose = F,
	auto.assign = T,
	conn = NULL,
...){

    importDefaults("getSymbols.Rbbg")
	this.env = environment()
	for (var in names(list(...))) {
	  assign(var, list(...)[[var]], this.env)
	}

	default.tz = Sys.timezone()
	default.return.class = return.class
	default.fields = fields
	
	if (missing(verbose))
	  verbose = FALSE

	if (missing(auto.assign))
	  auto.assign = TRUE

	if(!'package:Rbbg' %in% search() || !require('Rbbg',quietly=TRUE)) {
		stop(paste("package:",dQuote('Rbbg'),"cannot be loaded."))
	}
	   
	close_conn = FALSE
	if(is.null(conn)){
		close_conn = TRUE
		conn = blpConnect()
	}
	fromStr = as.POSIXct(from)
	toStr = as.POSIXct(to)
		
	for (i in 1:length(Symbols)) {
	  return.class = getSymbolLookup()[[Symbols[[i]]]]$return.class
	  return.class = ifelse(is.null(return.class), default.return.class,return.class)
	  
	  tz = getSymbolLookup()[[Symbols[[i]]]]$tz
	  tz = ifelse(is.null(tz),default.tz,tz)
	  	
	  ticker = getSymbolLookup()[[Symbols[[i]]]]$ticker
	  ticker = ifelse(is.null(ticker),Symbols[[i]],ticker)
		
	  fields = getSymbolLookup()[[Symbols[[i]]]]$fields
	  if(is.null(fields)) fields = default.fields
	  
	  ok_fields = sapply(fields, FUN = function(x) !is.na(x) & is.character(x) & x != "")
	  ok_fields[names(ok_fields)=="Date"] = FALSE	  
	  dl_fields = unlist(fields[ok_fields])
		
	  op_nam = NULL
	  op_val = NULL
	
	  # if(ticker_ccy[j] != "LOC"){
      #		op_nam = "currency"
      #		op_val = ticker_ccy[j]        
	  # }
	  
	  if (verbose){
		cat("loading ", Symbols[[i]], ".....\n")
		cat("loading ", dl_fields, ".....\n")
	  }
	  
	  tmp = try(bdh(conn,ticker,dl_fields,fromStr,toStr,option_names = op_nam, option_values = op_val),silent=T)
	  if(inherits(tmp,"try-error")){		
		cat(ticker," failed to download....\n\n",fr, "\n\n....skipping\n")
		next
	  }
	  tmp = bdh.to.xts(tmp,field = dl_fields)
	  
	  fr = xts(matrix(NA,ncol = length(fields),nrow=nrow(tmp),dimnames=list(NULL,names(fields))),index(tmp),src="bbg",updated = Sys.time())
	  fr[,ok_fields] = tmp
	  
	  fr = convert.time.series(fr = fr, return.class = return.class)
	  Symbols[[i]] = toupper(gsub("\\^", "", Symbols[[i]]))
	  
	  if (auto.assign)
		assign(Symbols[[i]], fr, env)
	}
	if(	close_conn)
	   blpDisconnect(conn)
	
	if (auto.assign)
	  return(Symbols)

	return(fr)
}      

getSymbols.camprod <- 
function(
	Symbols,
	env,
	return.class='xts',
	dbfields = list(	  
	  #Open = 90,	  
	  Close = 29,
	  #Volume = 32,
	  Adjusted = 134      
	),
		
	from = "2007-01-01",
	to = Sys.Date(),
	verbose = F,
	auto.assign = T,
	dbconn = NULL,
	...) 
{
     importDefaults("getSymbols.MySQL")
     this.env <- environment()
     for(var in names(list(...))) {
        # import all named elements that are NON formals
        assign(var, list(...)[[var]], this.env)
     }
	 
	 if(!'package:RODBC'%in% search() || !require('RODBC',quietly=TRUE)) {         
		stop(paste("package:",dQuote('RODBC'),"cannot be loaded."))
	 }
    
     if(is.null(dbconn)) {
		stop(paste('At least one connection argument (',sQuote('dbconn'),") is not set"))
	 }
	
	 default.tz = Sys.timezone()
	 default.return.class = return.class
	 default.dbfields = dbfields
	 default.dsid = '1'
	 	
     if(missing(verbose)) verbose = FALSE
     if(missing(auto.assign)) auto.assign = TRUE
	 
	 fromStr = as.POSIXct(from)
	 toStr = as.POSIXct(to)
	 ccyid = -1		
	 
	 db = odbcConnect(dbconn)
	                           		
	 for (i in 1:length(Symbols)) {
		return.class = getSymbolLookup()[[Symbols[[i]]]]$return.class
		return.class = ifelse(is.null(return.class), default.return.class,return.class)
	  
		tz = getSymbolLookup()[[Symbols[[i]]]]$tz
		tz = ifelse(is.null(tz),default.tz,tz)
	  	
		iid = getSymbolLookup()[[Symbols[[i]]]]$iid
		iid = ifelse(is.null(iid),Symbols[[i]],iid)
		
		dbfields = getSymbolLookup()[[Symbols[[i]]]]$dbfields
		if(is.null(dbfields)) dbfields = default.dbfields
	  
	  	dsid = getSymbolLookup()[[Symbols[[i]]]]$dsid
		if(is.null(dsid)) dsid = default.dsid
		
	  
		if(verbose) {
			cat(paste('Loading ',Symbols[[i]],paste(rep('.',10-nchar(Symbols[[i]])),collapse=''),sep=''))
		}
				
		fr = NULL		
		for(j in 1:length(dbfields)){
		
			tmp = switch(names(dbfields[j]),				
				Close = {
					query = paste0("CALL GetPeriodPrices(",iid,",",ccyid,",'",fromStr,"','",toStr,"','",dsid,"',0,0,0,1);")			
					rs = sqlQuery(db,query)				
					xts(as.matrix(rs[,2]), order.by=as.Date(rs[,1],origin='1970-01-01'),src='db',updated=Sys.time())			
				},		
				Adjusted = {
					query = paste0("CALL GetPeriodPrices(",iid,",",ccyid,",'",fromStr,"','",toStr,"','",dsid,"',1,0,0,1);")			
					rs = sqlQuery(db,query)	
					rs[,3] = ifelse(rs[,3] == "raw(0)" | is.na(rs[,3]),0,rs[,3])														
					rs = xts(as.matrix(rs[,2:3]), order.by=as.Date(rs[,1],origin='1970-01-01'), src='db',updated=Sys.time())				
					cumprod(c(rs[1,1],((rs[,2]+rs[,1])/lag(rs[,1]))[-1]))				
				},
				NA
			)					
			fr = cbind(fr,tmp)			
		}		
		names(fr) = names(dbfields)
		  		  		        
		fr <- convert.time.series(fr=fr,return.class=return.class)
		if(auto.assign)
		  assign(Symbols[[i]],fr,env)
		  
		if(verbose) cat('done\n')
	}
	odbcClose(db)
	if(auto.assign)
	  return(Symbols)
	return(fr)
}

###############################################################################
#' Helper function to extend functionality of getSymbols
#'
#' Syntax to specify tickers:
#' * Basic : XLY
#' * Rename: BOND=TLT
#' * Extend: XLB+RYBIX
#' * Mix above: XLB=XLB+RYBIX+FSDPX+FSCHX+PRNEX+DREVX
#' Symbols = spl('XLY, BOND=TLT,XLY+RYBIX,XLB=XLB+RYBIX+FSDPX+FSCHX+PRNEX+DREVX')

#' tickers=spl('XLB+RYBIX+FSDPX+FSCHX+PRNEX+DREVX,
#' XLE+RYEIX+VGENX+FSENX+PRNEX+DREVX,
#' XLF+FIDSX+SCDGX+DREVX,
#' XLI+FSCGX+VFINX+FEQIX+DREVX,
#' XLK+RYTIX+KTCBX+FSPTX+FTCHX+FTRNX+DREVX,
#' XLP+FDFAX+FSPHX+OARDX+DREVX,
#' XLU+FSUTX+DREVX,
#' XLV+VGHCX+VFINX+DREVX,
#' XLY+FSRPX+DREVX,
#' BOND+IEI+VFIUX+VFITX+FSTGX+FGOVX+STVSX+FGMNX+FKUSX')
#' 
#' data <- new.env()
#'   getSymbols.extra(tickers, src = 'yahoo', from = '1980-01-01', env = data, auto.assign = T)
#' bt.start.dates(data)
#' 
#' @export 
################################################################################
getSymbols.extra.new <- function 
(
	Symbols = NULL, 
	env = parent.frame(), 
	getSymbols.fn = getSymbols,
	raw.data = new.env(),		# extra pre-loaded raw data
	set.symbolnames = F,
	auto.assign = T,  
	...
) 
{
	if(is.character(Symbols)) Symbols = spl(Symbols)
	if(len(Symbols) < 1) return(Symbols)
	
	Symbols = toupper(gsub('\n','',Symbols))
		
	# split
	map = list()
	for(s in Symbols) {
		name = iif(len(spl(s, '=')) > 1, spl(s, '=')[1], spl(s, '\\+')[1])
		values = spl(iif(len(spl(s, '=')) > 1, spl(s, '=')[2], s), '\\+')
		map[[trim(name)]] = trim(values)
	}
	Symbols = unique(unlist(map))
	
	# find overlap with raw.data
	Symbols = setdiff(Symbols, ls(raw.data))
	
	# download
	data <- new.env()
	if(len(Symbols) > 0) match.fun(getSymbols.fn)(Symbols, env=data, auto.assign = T, ...)
	for(n in ls(raw.data)) data[[n]] = raw.data[[n]]
	
	# reconstruct, please note getSymbols replaces ^ symbols
	if (set.symbolnames) env$symbolnames = names(map)
	for(s in names(map)) {
		env[[ s ]] = data[[ gsub('\\^', '', map[[ s ]][1]) ]]
		if( len(map[[ s ]]) > 1)
			for(i in 2:len(map[[ s ]])) 
				env[[ s ]] = extend.data.new(env[[ s ]], data[[ gsub('\\^', '', map[[ s ]][i]) ]], scale=T) 			
		if (!auto.assign)
       		return(env[[ s ]])			
	}			
}

# gold = extend.GLD(data$GLD)
# comm = extend.data(data$DBC, get.CRB(), scale=T)
#' @export 
extend.data.new <- function
(
	current,
	hist,
	scale = F
) 
{
	colnames(current) = sapply(colnames(current), function(x) last(spl(x,'\\.')))
	colnames(hist) = sapply(colnames(hist), function(x) last(spl(x,'\\.')))

	# find Close in hist
	close.index.current = has.Cl(current,T)	
	close.index.hist = has.Cl(hist,T)		
	if(!close.index.current) close.index.current = 1
	if(!close.index.hist) close.index.hist = 1
	
	adjusted.index.current = has.Ad(current,T)	
	adjusted.index.hist = has.Ad(hist,T)		
	if(!adjusted.index.current) adjusted.index.current = close.index.current
	if(!adjusted.index.hist) adjusted.index.hist = close.index.hist
	
	if(scale) {
		# find first common observation in current and hist series
		common = merge(current[,close.index.current], hist[,close.index.hist], join='inner')		
		scale = as.numeric(common[1,1]) / as.numeric(common[1,2])
			
		if( close.index.hist == adjusted.index.hist )	
			hist = hist * scale
		else {
			hist[,-adjusted.index.hist] = hist[,-adjusted.index.hist] * scale
			
			common = merge(current[,adjusted.index.current], hist[,adjusted.index.hist], join='inner')
			scale = as.numeric(common[1,1]) / as.numeric(common[1,2])
			hist[,adjusted.index.hist] = hist[,adjusted.index.hist] * scale
		}
	}
	
	# subset history before current
	hist = hist[format(index(current[1])-1,'::%Y:%m:%d'),,drop=F]
	
	
	if( ncol(hist) != ncol(current) )	
		#hist = make.xts( rep.col(hist[,close.index], ncol(current)), index(hist))
		hist = hist[, colnames(current)]
	else
		hist = hist[, colnames(current)]
	
	colnames(hist) = colnames(current)
		
	rbind( hist, current )
}

if(FALSE){
getSymbols.MySQL <- 
function(
	Symbols,
	env,
	return.class='xts',
	db.fields=c('date','o','h','l','c','v','a'),
	field.names = NULL,
    user=NULL,
	password=NULL,
	dbname=NULL,
	host='localhost',
	port=3306,
	...) 
{
     importDefaults("getSymbols.MySQL")
     this.env <- environment()
     for(var in names(list(...))) {
        # import all named elements that are NON formals
        assign(var, list(...)[[var]], this.env)
     }
     if(missing(verbose)) verbose <- FALSE
     if(missing(auto.assign)) auto.assign <- TRUE
        if('package:DBI' %in% search() || require('DBI',quietly=TRUE)) {
          if('package:RMySQL' %in% search() || require('RMySQL',quietly=TRUE)) {
          } else { warning(paste("package:",dQuote("RMySQL"),"cannot be loaded" )) }
        } else {
          stop(paste("package:",dQuote('DBI'),"cannot be loaded."))
        }
        if(is.null(user) || is.null(password) || is.null(dbname)) {
          stop(paste(
              'At least one connection argument (',sQuote('user'),
              sQuote('password'),sQuote('dbname'),
              ") is not set"))
        }
        con <- dbConnect("MySQL",user=user,password=password,dbname=dbname,host=host,port=port)
        db.Symbols <- dbListTables(con)
        if(length(Symbols) != sum(Symbols %in% db.Symbols)) {
          missing.db.symbol <- Symbols[!Symbols %in% db.Symbols]
                warning(paste('could not load symbol(s): ',paste(missing.db.symbol,collapse=', ')))
                Symbols <- Symbols[Symbols %in% db.Symbols]
        }
        for(i in 1:length(Symbols)) {
            if(verbose) {
                cat(paste('Loading ',Symbols[[i]],paste(rep('.',10-nchar(Symbols[[i]])),collapse=''),sep=''))
            }
            query <- paste("SELECT ",paste(db.fields,collapse=',')," FROM ",Symbols[[i]]," ORDER BY date")
            rs <- dbSendQuery(con, query)
            fr <- fetch(rs, n=-1)
            #fr <- data.frame(fr[,-1],row.names=fr[,1])
            fr <- xts(as.matrix(fr[,-1]), order.by=as.Date(fr[,1],origin='1970-01-01'), src=dbname,updated=Sys.time())
            colnames(fr) <- paste(Symbols[[i]], c('Open','High','Low','Close','Volume','Adjusted'),   sep='.')
            fr <- convert.time.series(fr=fr,return.class=return.class)
            if(auto.assign)
              assign(Symbols[[i]],fr,env)
            if(verbose) cat('done\n')
        }
        dbDisconnect(con)
        if(auto.assign)
          return(Symbols)
        return(fr)
}


GetBloombergData <- 
function
(
	tickers,
	ticker.names = tickers,
	fields = "PX_LAST",
	currency = NULL,
	from,
	to = Sys.Date()-1,
	periodicity = "DAILY"
) {
	require(Rbbg)	
	
	oNames = c("nonTradingDayFillOption", "nonTradingDayFillMethod","periodicitySelection",if(!is.null(currency)) "currency" )
	oVal = c("ALL_CALENDAR_DAYS", "PREVIOUS_VALUE",periodicity,if(!is.null(currency)) currency ) #DAILY,WEEKLY	
	
	conn <- blpConnect()		
	bd = bdh(conn,tickers,fields,as.Date(from),as.Date(to),option_names = oNames,option_values = oVal,dates.as.row.names = FALSE,include.non.trading.days = F)
	blpDisconnect(conn)
	
	bdDates = unique(na.omit(bd)$date)
	
	allData = unstack(bd, paste(fields[1]," ~ ticker",sep=""))
	colnames(allData) = paste(ticker.names,fields[1],sep=".")

	if(length(fields) > 1){
		for(x in fields[-1]){
			tempData = unstack(bd, paste(x," ~ ticker",sep=""))
			colnames(tempData) = paste(ticker.names,x,sep=".")
			allData = cbind(allData,tempData)			
		}
	}

	if(class(allData)!="data.frame"){

	} else {		
		allData <- xts(allData, order.by=as.Date(bdDates,origin='1970-01-01'), src="blp",updated=Sys.time())
	}
	allData
}

}