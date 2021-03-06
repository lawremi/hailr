### =========================================================================
### HailContext singleton management
### -------------------------------------------------------------------------
###

setClassUnion("HailContext_OR_NULL", c("HailContext", "NULL"))

HailContextManager_getContext <- function(.self) {
    ctx <- .self$context
    if (is.null(ctx)) {
        ctx <- .self$setHailContext(initHailContext())
    }
    ctx
}

HailContextManager_setContext <- function(.self, ctx) {
    .self$context <- ctx
    ctx
}

HailContextManager <- 
    setRefClass("HailContextManager", fields=c(context="HailContext_OR_NULL"),
                methods=list(getHailContext=HailContextManager_getContext,
                             setHailContext=HailContextManager_setContext))()

use_hail_context <- function(context) {
    HailContextManager$setContext(context)
}

hail_context <- function() {
    HailContextManager$getHailContext()
}

hail <- hail_context
