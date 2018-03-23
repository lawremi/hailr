setClass("JavaPath", contains="function")

setGeneric("callMethod",
           function(target, name, ...) standardGeneric("callMethod"),
           signature="target")

JavaPath <- function(target, ...) {
    path <- c(...)
    new("JavaPath", function(...) {
        fromJava(callMethod(impl(target), path, toJava(list(...))))
    })
}

path <- function(x) environment(x)$path

`path<-` <- function(x, value) {
    environment(x)$path <- value
    x
}

setMethod("$", "JavaPath", function(x, name) {
    path(x) <- c(path(x), name)
    x
})
