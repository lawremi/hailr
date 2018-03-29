### =========================================================================
### SparkDriverObject objects
### -------------------------------------------------------------------------
###
### Wrapper of object from driver implementation.
###

setClass("SparkDriverObject", slots=c(impl="ANY"))

setGeneric("callMethod",
           function(target, name, ...) standardGeneric("callMethod"),
           signature="target")

setMethod("callMethod", "SparkDriverObject",
          function(target, name, args) {
              callMethod(toDriver(target), name, args)
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Generic coercions
###

setGeneric("toDriver", function(x) standardGeneric("toDriver"))

setMethod("toDriver", "ANY", function(x) x)

setMethod("toDriver", "SparkDriverObject", function(x) impl(x))

setGeneric("fromDriver", function(x) standardGeneric("fromDriver"))

setMethod("fromDriver", "ANY", function(x) x)
