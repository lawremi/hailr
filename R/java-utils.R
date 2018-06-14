### =========================================================================
### Java utilities
### -------------------------------------------------------------------------
###

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors for complicated Java/Scala things
###

setClass("ScalaOption", slots=c(value="ANY"))
setClass("ScalaSet", slots=c(value="vector"))
setClass("JavaArrayList", slots=c(value="vector"))
setClass("JavaHashMap", slots=c(value="vector"),
         validity=function(object) {
             if (length(object@value) > 0L && is.null(names(object@value)))
                 "vector/list requires names to become a java.util.HashMap"
         })

ScalaOption <- function(x = NULL) new("ScalaOption", value=x)
ScalaSet <- function(x = list()) new("ScalaSet", value=x)
JavaList <- JavaArrayList <- function(x = list()) new("JavaArrayList", value=x)
JavaMap <- JavaHashMap <- function(x = list()) new("JavaHashMap", value=x)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### toJava: conversion of R objects to corresponding Java objects
###

setMethod("toJava", "ANY", function(x, jvm) x)
setMethod("toJava", "list", function(x, jvm) lapply(x, toJava, jvm))

setMethod("toJava", "ScalaOption",
          function(x, jvm) toJava(jvm$scala$Option$apply(x@value)))

setMethod("toJava", "ScalaSet",
          function(x, jvm)  {
              arrayList <- toJava(JavaArrayList(x), jvm)
              toJava(jvm$is$hail$utils$arrayListToSet(arrayList))
          })

setMethod("toJava", "JavaArrayList",
          function(x, jvm)  {
              toJava(jvm$is$hail$utils$arrayToArrayList(array(x@value)))
          })

setMethod("toJava", "JavaHashMap",
          function(x, jvm)  {
              list2env(as.list(x@value))
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

scala_object <- function(x) x$"MODULE$"()
