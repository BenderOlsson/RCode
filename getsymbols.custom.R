

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
        if ("package:its" %in% search() || suppressMessages(require("its",    quietly = TRUE))) {
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

getSymbols.CSV2 <- 
function (
Symbols, env, dir = "", return.class = "xts", 
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
verbose = F,
auto.assign = T,
...)
{
importDefaults("getSymbols.csv")
this.env <- environment()
for (var in names(list(...))) {
  assign(var, list(...)[[var]], this.env)
}

default.tz = Sys.timezone()
default.return.class <- return.class
default.dir <- dir
default.extension <- extension
default.cols <- cols
if (missing(verbose))
  verbose <- FALSE

if (missing(auto.assign))
  auto.assign <- TRUE

for (i in 1:length(Symbols)) {
  return.class <- getSymbolLookup()[[Symbols[[i]]]]$return.class
  return.class <- ifelse(is.null(return.class), default.return.class,return.class)
  dir <- getSymbolLookup()[[Symbols[[i]]]]$dir
  dir <- ifelse(is.null(dir), default.dir, dir)
  extension <- getSymbolLookup()[[Symbols[[i]]]]$extension
  extension <- ifelse(is.null(extension), default.extension,extension)
  tz <- getSymbolLookup()[[Symbols[[i]]]]$tz
  tz <- ifelse(is.null(tz), default.tz, tz)
  
  cols <- getSymbolLookup()[[Symbols[[i]]]]$cols
  cols <- ifelse(is.null(cols), default.cols, cols)
  
  if (verbose)
	cat("loading ", Symbols[[i]], ".....")
  
  if (dir == "") {
	sym.file <- paste(Symbols[[i]], extension, sep = ".")
  } else {
	sym.file <- file.path(dir, paste(Symbols[[i]], extension,sep = "."))
  }
  
  if (!file.exists(sym.file)) {
	cat("\nfile ",paste(Symbols[[i]],"csv",sep=".")," does not exist ","in ",dir,"....skipping\n")
	next
  }
  fr = read.csv(sym.file,header=T,stringsAsFactors=F ) #added header=TRUE      

  if(is.null(cols$Date))
	cols$Date = grep(pattern="date|index",x=colnames(fr),ignore.case=T)  

  cols = lapply(cols,function(x,y) if(is.numeric(x)) x else match(x,y),y=colnames(fr))      
  cols[cols > ncol(fr)] = NA
  
  dates = as.Date(fr[,cols$Date],tz=tz)  
  cols$Date = NULL
  
  if (verbose)
	cat("done.\n")

  fr = do.call("cbind",sapply(cols,FUN=function(x,fr) if(is.na(x)) NA else as.numeric(fr[,x]),fr=fr))
  fr = xts(fr,dates,src = "csv", updated = Sys.time())              
  
  fr <- convert.time.series(fr = fr, return.class = return.class)
  Symbols[[i]] <- toupper(gsub("\\^", "", Symbols[[i]]))
  
  if (auto.assign)
	assign(Symbols[[i]], fr, env)
}
if (auto.assign)
  return(Symbols)

return(fr)
}      
  
getSymbols.John <- 
function (
	Symbols, 
	env, 
	dir = "", 
	return.class = "xts", 
	extension = "csv",
	verbose=FALSE,
	auto.assign=TRUE,
...)
{
    importDefaults("getSymbols.csv")
    this.env <- environment()
    for (var in names(list(...))) {
        assign(var, list(...)[[var]], this.env)
    }
    default.return.class <- return.class
    default.dir <- dir
    default.extension <- extension
    if (missing(verbose))
        verbose <- FALSE
    if (missing(auto.assign))
        auto.assign <- TRUE
    for (i in 1:length(Symbols)) {
        return.class <- getSymbolLookup()[[Symbols[[i]]]]$return.class
        return.class <- ifelse(is.null(return.class), default.return.class, return.class)
        dir <- getSymbolLookup()[[Symbols[[i]]]]$dir
        dir <- ifelse(is.null(dir), default.dir, dir)
        extension <- getSymbolLookup()[[Symbols[[i]]]]$extension
        extension <- ifelse(is.null(extension), default.extension,
            extension)
        if (verbose)
            cat("loading ", Symbols[[i]], ".....")
        if (dir == "") {
            sym.file <- paste(Symbols[[i]], extension, sep = ".")
        }
        else {
            sym.file <- file.path(dir, paste(Symbols[[i]], extension,sep = "."))
        }
        if (!file.exists(sym.file)) {
            cat("\nfile ", paste(Symbols[[i]], "csv", sep = ".")," does not exist ", "in ", dir, "....skipping\n")
            next
        }
        fr <- read.csv(sym.file, header=TRUE) #added header=TRUE
        if (verbose)
            cat("done.\n")
        fr <- xts(matrix(fr$price, dimnames=list(index(x),'price')),
			as.Date(fr$date),   # added for getSymbols.John
            src = "csv", updated = Sys.time()) # added for getSymbols.John

# removed colnames call from original getSymbols.csv

        fr <- convert.time.series(fr = fr, return.class = return.class)
        Symbols[[i]] <- toupper(gsub("\\^", "", Symbols[[i]]))
        if (auto.assign)
            assign(Symbols[[i]], fr, env)
    }
    if (auto.assign)
        return(Symbols)
    return(fr)
}




	 
getSymbols.Bloomberg <- 
function(
	Symbols,
	env,
	return.class='xts',
	from = as.POSIXlt(Sys.time()-60*60,"GMT"), 
	to = as.POSIXlt(Sys.time(),"GMT"), 
	bb.suffix="Equity", 
	bb.interval="5",
	verbose=FALSE,
	auto.assign=TRUE,
		# intraday = F,
		# fields = NULL,		
...) {

    importDefaults("getSymbols.Bloomberg")
    this.env <- environment()
    for(var in names(list(...))) {
        # import all named elements that are NON formals  
		assign(var, list(...)[[var]], this.env)
    }
    if ((class(from)=="Date" && class(to)=="Date") || (class(from)=="character" && length(from)<=8 && class(to)=="character" && length(to)<=8 )) {
       bb.intraday <- FALSE
       bb.call <- 
       bb.fields <- c("OPEN", "HIGH", "LOW", "PX_LAST", "VOLUME")
    } else {
       bb.intraday <- TRUE
       bb.call <- bar
       bb.fields <- "TRADE"
    }
    if(missing(verbose)) verbose <- FALSE
    if(missing(auto.assign)) auto.assign <- TRUE
       if('package:RBloomberg' %in% search() || require('RBloomberg',quietly=TRUE)) {
         {}
       } else {
         stop(paste("package:",dQuote('RBloomberg'),"cannot be loaded."))
       }
       bbconn <- blpConnect()
       for(i in 1:length(Symbols)) {
           bbsym <- paste(Symbols[[i]],bb.suffix)

           if(verbose) {
               cat(paste('Loading ',bbsym, ' from BB ', from,' to ',to, paste(rep('.',18-nchar(Symbols[[i]])),collapse=''),sep=''))
           }
           tryCatch (
             {
               if (bb.intraday) {
                 fromStr <- paste(as.character(from),".000",sep="")
                 toStr <- paste(as.character(to),".000",sep="")
                 b <- bb.call(bbconn, bbsym, bb.fields,fromStr, toStr, bb.interval)
                 b$datetime <- as.POSIXct(strptime(b$time,format="%Y-%m-%dT%H:%M:%S"))
                 bxo <- as.xts(b$open, order.by=b$datetime)
                 fr <- merge(bxo,  b$high, b$low, b$close, b$volume)
               } else {
                 if (class(from)=="character") {
                   fromStr <- from
                 } else {
                   fromStr <- strftime(from,format="%Y%m%d")
                 }
                 if (class(to)=="character") {
                   toStr <- to
                 } else {
                   toStr <- strftime(to,format="%Y%m%d")
                 }
                 b <- bb.call(bbconn, bbsym, bb.fields, fromStr, toStr)
                 b$datetime <- as.POSIXct(strptime(b$date,format="%Y-%m-%d"))
                 bxo <- as.xts(b$OPEN, order.by=b$datetime)
                 fr <- merge(bxo,  b$HIGH, b$LOW, b$PX_LAST, b$VOLUME)
               }

               if(verbose) {
                 cat(paste(length(fr),'points '))
               }
               colnames(fr) <- paste(Symbols[[i]],c('Open','High','Low','Close','Volume'),sep='.')
               fr <- convert.time.series(fr=fr,return.class=return.class)
               if(auto.assign)
                 assign(Symbols[[i]],fr,env)
             },
             error=function(e) {print(e);fr <- data.frame()}, finally=function () {if(verbose) {cat('done\n')}}
           )
       }
       blpDisconnect(bbconn)
       if(auto.assign)
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

