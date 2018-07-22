### =========================================================================
### JVM objects
### -------------------------------------------------------------------------
###
### Abstraction for the JVM hosting Spark. Implementations call static
### Java methods, and return Spark contexts.
###

.JVM <- setClass("JVM", slots=c(impl="ANY"), contains="JavaMethodTarget")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

JVM <- function(.impl, ...) {
    if (missing(.impl)) {
        .impl <- SparkDriverConnection(...)
    }
    .JVM(impl=.impl)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### JavaObject factory method
###

setMethod("constructObject", "JVM", function(target, path, args) {
    constructObject(impl(target), path, args)
})

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

setMethod("transmit", c("ANY", "JVM"), function(x, dest) {
    copy(x, impl(jvm))
})
