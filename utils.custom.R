get.symbols <- 
function(x,env){
    sapply(x ,FUN = function(x,env) env[[x]], env = env)                         
}  

remove.weekends <- 
function(x){  
  library(lubridate)
  x[!wday(x)%in%c(1,7),]  
}