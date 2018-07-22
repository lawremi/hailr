### =========================================================================
### JavaPath objects
### -------------------------------------------------------------------------
###
### Provides syntactic sugar for referencing Java classes and calling
### methods by chaining with '$()'
###

.JavaPath <- setClass("JavaPath", contains="function")

JavaPath <- function(target, ...) {
    path <- c(...)
    .JavaPath(function(...) {
        args <- list(...)
        if (identical(tail(path, 1L), "new"))
            ans <- constructObject(target, head(path, -1L), args)
        else ans <- callMethod(target, path, args)
        downcast(ans)
    })
}

path <- function(x) environment(x)$path

`path<-` <- function(x, value) {
    environment(x)$path <- value
    x
}

target <- function(x) environment(x)$target

setMethod("$", "JavaPath", function(x, name) {
    x[[name]]
})

setMethod("[[", "JavaPath", function (x, i, j, ...) {
    stopifnot(missing(j), missing(...))
    stopifnot(is.character(i), length(i) == 1L && !is.na(i))
    path(x) <- c(path(x), i)
    x
})
