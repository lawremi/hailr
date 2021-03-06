### =========================================================================
### Expression objects
### -------------------------------------------------------------------------
###
### Temporarily copy/pasted from rsolr
###

setClassUnion("Expression", "language")

setClass("ConstantExpression",
         slots=c(value="ANY"))
setIs("ConstantExpression", "Expression")

setClassUnion("Symbol", "name")
##setIs("Symbol", "Expression")

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
.SimpleCall <- setClass("SimpleCall", slots=c(name="Symbol", args="list"))
setIs("SimpleCall", "Call")

setClass("MethodCall")
setIs("MethodCall", "Call")

.SimpleMethodCall <- setClass("SimpleMethodCall",
                              slots=c(target="Expression"),
                              contains="SimpleCall")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors
###

SimpleSymbol <- function(name) {
    .SimpleSymbol(name=as.character(name))
}

SimpleCall <- function(name, args) {
    .SimpleCall(name=name, args=as.list(args))
}

SimpleMethodCall <- function(target, name, args) {
    .SimpleMethodCall(SimpleCall(name, args), target=target)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

name <- function(x) x@name
target <- function(x) x@target
args <- function(x) x@args

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Factory
###

setGeneric("languageClass", function(x) standardGeneric("languageClass"))

setGeneric("expressionClass", function(x) standardGeneric("expressionClass"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

setMethod("as.character", "SimpleCall", function(x) {
    paste0(name(x), "(", paste(args(x), collapse=", "), ")")
})

setMethod("as.character", "SimpleSymbol", function(x) {
    name(x)
})
