### =========================================================================
### HailExpression objects
### -------------------------------------------------------------------------
###
### Expressions in the Hail language.
###

setClass("HailExpression", contains=c("Expression", "VIRTUAL"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Symbol classes
###

setClass("HailSymbol", contains=c("SimpleSymbol", "HailExpression"))

setClass("ColumnSymbol", contains="HailSymbol")

setClass("GlobalSymbol", contains="HailSymbol")


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

