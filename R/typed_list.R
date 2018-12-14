### =========================================================================
### S3 typed list objects
### -------------------------------------------------------------------------
###
### Yes, we already have S4 typed lists, but S3 class labels let us
### transmit typed lists to Hail through data.frames.
###

setOldClass(c("integer_list", "typed_list", "list"))
setOldClass(c("numeric_list", "typed_list", "list"))
setOldClass(c("character_list", "typed_list", "list"))
setOldClass(c("factor_list", "typed_list", "list"))
setOldClass(c("logical_list", "typed_list", "list"))

setMethod("elementType", "typed_list",
          function(x) sub("_list$", "", class(x)[1L]))

setMethod("elementType", "list", function(x) {
    cls <- unique(vapply(x, function(xi) class(xi)[1L], character(1L)))
    if (length(cls) == 0L)
        return("ANY")
    ans <- cls[1L]
    ### NOTE: assumes S4 class definitions
    for (cl in cls[-1L]) {
        if (extends(ans, cl)) {
            ans <- cl
        } else if (!extends(cl, ans)) {
            ans <- "ANY"
        }
    }
    ans
})

integer_list <- function(...) {
    typed_list("integer", ...)
}

character_list <- function(...) {
    typed_list("character", ...)
}

typed_list <- function(type, ...) {
    as.typed_list(list(...), type)
}

`unlist<-` <- function(x, value) {
    relist(value, x)
}

as.typed_list <- function(x, type) UseMethod("as.typed_list")

as.typed_list.list <- function(x, type) {
    if (!extends(elementType(x), type)) {
        x <- unclass(x)
        unlist(x) <- as(unlist(x), type)
    }
    class(x) <- c(paste0(type, "_list"), "typed_list", "list")
    x
}

as.data.frame.typed_list <- function(x, row.names = NULL, optional = FALSE, ...)
{
    df <- as.data.frame(I(x), optional = optional, row.names = row.names, ...)
    class(df[[1L]]) <- class(x)
    df
}
