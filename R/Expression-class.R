### =========================================================================
### Expression objects
### -------------------------------------------------------------------------
###
### Temporarily copy/pasted from rsolr
###

setClassUnion("Expression", "language")

setClass("SimpleExpression",
         slots=c(expr="character"),
         prototype=prototype(expr=""),
         validity=function(object) {
             if (!isSingleString(object@expr)) {
                 "'expr' must be a single, non-NA string"
             }
         })
setIs("SimpleExpression", "Expression")

setClassUnion("Symbol", "name")
setIs("Symbol", "Expression")

.SimpleSymbol <- setClass("SimpleSymbol",
                          slots=c(name="character"),
                          validity=function(object) {
                              if (!isSingleString(object@name))
                                  "'name' must be a single, non-NA string"
                          })
setIs("SimpleSymbol", "Symbol")

setClassUnion("Call", "call")
setIs("Call", "Expression")

## Could sit above SolrFunctionCall
.SimpleCall <- setClass("SimpleCall",
                        slots=c(name="character",
                                args="list"),
                        validity=function(object) {
                            if (!isSingleString(object@name))
                                "'name' of a function call must be a string"
                        })
setIs("SimpleCall", "Call")

setClass("MethodCall")
setIs("MethodCall", "Call")


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors
###

SimpleSymbol <- function(name) {
    .SimpleSymbol(name=as.character(name))
}

SimpleCall <- function(name, args) {
    .SimpleCall(name=as.character(name), args=as.list(args))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Factory
###

setGeneric("expressionClass", function(x) standardGeneric("expressionClass"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

setMethod("as.character", "SimpleCall", function(x) {
    paste0(name(x), "(", paste(args, collapse=", "), ")")
})
