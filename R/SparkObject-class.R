### =========================================================================
### SparkObject objects
### -------------------------------------------------------------------------
###
### High-level abstraction around Spark objects. The hierarchy under
### SparkObject should mirror the Java hierarchy. Every SparkObject
### has an underlying implementing instance, which is polymorphic
### based on its driver.
###

setClass("SparkObject", slots = c(impl="SparkDriverObject"),
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

SparkObject <- function(impl) {
    downcast(new("SparkObject", impl=fromDriver(impl)))
}

impl <- function(x) x@impl

setMethod("$", "SparkObject", function(x, name) JavaPath(x, name))

setGeneric("sparkConnection", function(x) standardGeneric("sparkConnection"))

setMethod("sparkConnection", "SparkObject",
          function(x) SparkConnection(sparkConnection(impl(x))))

java_superclasses <- function(x) {
    class <- x$getClass()
    supers <- character()
    while(!is.null(class)) {
        classes <- c(classes, class$getName())
        class <- class$getSuperClass()
    }
    supers
}

S4_subclasses <- function(x) {
    ## should get all recursive subclasses if information is complete
    vapply(getClass(class(x))@subclasses, `@`, character(1L), "subClass")
}

downcast <- function(x) {
    candidates <- c(class(x), S4_subclasses(x))
    deepest <- which.min(match(candidates, superclasses(x)))
    if (length(deepest) > 0L)
        as(x, candidates[deepest])
    else x
}
