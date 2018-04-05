setClass("JavaPath", contains="function")

JavaPath <- function(target, ...) {
    path <- c(...)
    new("JavaPath", function(...) {
        downcast(callMethod(target, path, list(...)))
    })
}

path <- function(x) environment(x)$path

`path<-` <- function(x, value) {
    environment(x)$path <- value
    x
}

target <- function(x) environment(x)$target

setMethod("$", "JavaPath", function(x, name) {
    path(x) <- c(path(x), name)
    x
})

setMethod("[[", "JavaPath", function (x, i, j, ...) {
    stopifnot(missing(j), missing(...))
    stopifnot(is.character(i), length(i) == 1L && !is.na(i))
    getFieldValue(target(x), pathToClassName(path(x)), i)
})
