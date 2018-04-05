### =========================================================================
### SparkObject objects
### -------------------------------------------------------------------------
###
### High-level abstraction around Spark objects. The hierarchy under
### SparkObject should mirror the Java hierarchy. Every SparkObject
### has an underlying implementing instance, which is polymorphic
### based on its driver.
###

setClass("SparkObject", slots = c(impl="ANY"), contains="JavaMethodTarget",
         validity = function(object) {
             class <-
                 tryCatch(sparkConnection(object)$Class$forName(class(object)),
                          error=function(e) { })
             if (!is.null(class) && !class$isAssignableFrom(object$getClass()))
             {
                 paste0("class '", class$getName(), "' not assignable from '",
                        object$getClass()$getName(), "'")
             }
         })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

SparkObject <- function(impl) {
    new("SparkObject", impl=impl)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

impl <- function(x) x@impl

setMethod("[[", "SparkObject", function (x, i, j, ...) {
    stopifnot(missing(j), missing(...))
    stopifnot(is.character(i) && length(i) == 1L && !is.na(i))
    x$getClass()$getField(i)$get(x)
})

setGeneric("sparkConnection", function(x) standardGeneric("sparkConnection"))

setMethod("sparkConnection", "SparkObject",
          function(x) SparkConnection(sparkConnection(impl(x))))

setMethod("jvm", "SparkObject", function(x) sparkConnection(x))

setMethod("toJava", "SparkObject", function(x, jvm) impl(x))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Utilities
###

java_superclasses <- function(x) {
    ### NOTE: cannot use '$' here, since it downcasts and infinitely recurses
    class <- callMethod(x, "getClass")
    supers <- character()
    while(!is.null(class)) {
        supers <- c(supers, callMethod(class, "getName"))
        class <- callMethod(class, "getSuperclass")
    }
    supers
}

S4_subclasses <- function(x) {
    ## should get all recursive subclasses if information is complete
    vapply(getClass(class(x))@subclasses, slot, character(1L), "subClass")
}

downcast <- function(x) {
    candidates <- c(class(x), S4_subclasses(x))
    deepest <- which.min(match(candidates, java_superclasses(x)))
    if (length(deepest) > 0L)
        as(x, candidates[deepest])
    else x
}
