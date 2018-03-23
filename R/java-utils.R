### =========================================================================
### Java utilities
### -------------------------------------------------------------------------
###

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors for complicated Java/Scala things
###

joption <- function(x) spark()$scala$Option$apply(x)
jset <- function(x) {
    utils <- spark()$is$hail$utils$Py4jUtils
    arrayList <- utils$arrayToArrayList(x)
    utils$arrayListToSet(arrayList)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### toJava: conversion of R objects to corresponding Java objects
###

setGeneric("toJava", function(x) standardGeneric("toJava"))

setMethod("toJava", "ANY", function(x) x)
setMethod("toJava", "list", function(x) lapply(x, toJava))
setMethod("toJava", "SparkObject", function(x) impl(x))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### fromJava: conversion of Java objects to corresponding R objects
###

setGeneric("fromJava", function(x) standardGeneric("fromJava"))

setMethod("fromJava", "SparkDriverObject",
          function(x) {
              downcast(SparkObject(x))
          })

