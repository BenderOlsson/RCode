

convert.time.series <- 
function (fr, return.class)
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

bdh.to.xts <- 
function(x,...){  
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
...)
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

	if('package:Rbbg' %in% search() || require('Rbbg',quietly=TRUE)) {
		{}
	} else {
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

getSymbols.SQLite <- 
function(
	Symbols,
	env,
	return.class='xts',
	db.fields=c('row_names','Open','High','Low','Close','Volume','Adjusted'),
	field.names = NULL,
	dbname=NULL,
	POSIX = TRUE,
	...) {
     importDefaults("getSymbols.SQLite")
     this.env <- environment()
     for(var in names(list(...))) {
        # import all named elements that are NON formals
        assign(var, list(...)[[var]], this.env)
     }
     if(missing(verbose)) verbose <- FALSE
     if(missing(auto.assign)) auto.assign <- TRUE
        if('package:DBI' %in% search() || require('DBI',quietly=TRUE)) {
          if('package:RSQLite' %in% search() || require('RSQLite',quietly=TRUE)) {
          } else { warning(paste("package:",dQuote("RSQLite"),"cannot be loaded" )) }
        } else {
          stop(paste("package:",dQuote('DBI'),"cannot be loaded."))
        }
        drv <- dbDriver("SQLite")
        con <- dbConnect(drv,dbname=dbname)
        db.Symbols <- dbListTables(con)
        if(length(Symbols) != sum(Symbols %in% db.Symbols)) {
          missing.db.symbol <- Symbols[!Symbols %in% db.Symbols]
                warning(paste('could not load symbol(s): ',paste(missing.db.symbol,collapse=', ')))
                Symbols <- Symbols[Symbols %in% db.Symbols]
        }
        for(i in 1:length(Symbols)) {
            if(verbose) {
                cat(paste('Loading ',Symbols[[i]],  paste(rep('.',10-nchar(Symbols[[i]])),collapse=''),  sep=''))
            }
            query <- paste("SELECT ",  paste(db.fields,collapse=',')," FROM ",Symbols[[i]], " ORDER BY row_names")
            rs <- dbSendQuery(con, query)
            fr <- fetch(rs, n=-1)
            #fr <- data.frame(fr[,-1],row.names=fr[,1])
            if(POSIX) {
              d <- as.numeric(fr[,1])
              class(d) <- c("POSIXt","POSIXct")
              fr <- xts(fr[,-1],order.by=d)
            } else {
              fr <- xts(fr[,-1],order.by=as.Date(as.numeric(fr[,1]),origin='1970-01-01'))
            }
            colnames(fr) <- paste(Symbols[[i]], c('Open','High','Low','Close','Volume','Adjusted'),  sep='.')
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
	require(RBloomberg)	
	
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

