### =========================================================================
### Expression objects
### -------------------------------------------------------------------------
###
### Temporarily copy/pasted from rsolr
###

setClassUnion("Expression", "language")

setClass("SimpleExpression",
         representation(expr="character"),
         prototype(expr=""),
         validity=function(object) {
             if (!isSingleString(object@expr)) {
                 "'expr' must be a single, non-NA string"
             }
         })
setIs("SimpleExpression", "Expression")

setClassUnion("Symbol", "name")
setIs("Symbol", "Expression")

setClass("SimpleSymbol",
         representation(name="character"),
         validity=function(object) {
             if (!isSingleString(object@name))
                 "'name' must be a single, non-NA string"
         })
setIs("SimpleSymbol", "Symbol")
