### =========================================================================
### Java utilities
### -------------------------------------------------------------------------
###

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Wrapper objects for when there is no R equivalent
###

setClass("JavaOption", slots=c(value="ANY"))
setClass("JavaSet", slots=c(value="ANY"))

joption <- function(x) new("JavaOption", value=x)
jset <- function(x) new("JavaSet", value=x)
