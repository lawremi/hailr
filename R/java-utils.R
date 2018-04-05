### =========================================================================
### Java utilities
### -------------------------------------------------------------------------
###

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors for complicated Java/Scala things
###

setClass("ScalaOption", slots=c(value="ANY"))
setClass("ScalaSet", slots=c(value="vector"))

ScalaOption <- function(x) new("ScalaOption", value=x)
ScalaSet <- function(x) new("ScalaSet", value=x)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### toJava: conversion of R objects to corresponding Java objects
###

setMethod("toJava", "ANY", function(x, jvm) x)
setMethod("toJava", "list", function(x, jvm) lapply(x, toJava, jvm))

setMethod("toJava", "ScalaOption",
          function(x, jvm) toJava(jvm$scala$Option$apply(x@value)))

setMethod("toJava", "ScalaSet",
          function(x, jvm)  {
              arrayList <- jvm$is$hail$utils$arrayToArrayList(array(x@value))
              toJava(jvm$is$hail$utils$arrayListToSet(arrayList))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### fromJava: conversion of Java objects to corresponding R objects
###

setGeneric("fromJava", function(x) standardGeneric("fromJava"))

setMethod("fromJava", "ANY", function(x) x)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Helpers
###

## Could be made generic if needed, as a backend might not require
## direct reflection.
getFieldValue <- function(x, src, name) {
    if (is.character(src)) {
        class <- x$java$lang$Class$forName(src)
        src <- NULL
    } else {
        class <- src$getClass()
    }
    class$getField(name)$get(src)
}

pathToClassName <- function(x) paste(x, collapse=".")

