get.symbols <- function(x,env){
    sapply(x ,FUN = function(x,env) env[[x]], env = env)                         
}  