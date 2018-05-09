### =========================================================================
### JVM objects
### -------------------------------------------------------------------------
###
### Abstraction for the JVM hosting Spark. Implementations call static
### Java methods, and return Spark contexts.
###

setClass("JVM", slots=c(impl="ANY"), contains="JavaMethodTarget")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

JVM <- function(.impl, ...) {
    if (missing(.impl)) {
        .impl <- SparkDriverConnection(...)
    }
    new("JVM", impl=.impl)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setGeneric("sparkContext", function(x, ...) standardGeneric("sparkContext"))

setMethod("sparkContext", "JVM",
          function(x) JavaObject(sparkContext(impl(x))))

setMethod("jvm", "JVM", function(x) x)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Transmitting objects to the JVM
###

setMethod("transmit", c("ANY", "JVM"), function(x, dest, ...) {
    copy(x, impl(jvm), ...)
})
