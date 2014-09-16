


getSymbols.John
function (Symbols, env, dir = "", return.class = "xts", extension = "csv",
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
        return.class <- ifelse(is.null(return.class), default.return.class,
            return.class)
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
            sym.file <- file.path(dir, paste(Symbols[[i]], extension,
                sep = "."))
        }
        if (!file.exists(sym.file)) {
            cat("\nfile ", paste(Symbols[[i]], "csv", sep = "."),
                " does not exist ", "in ", dir, "....skipping\n")
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