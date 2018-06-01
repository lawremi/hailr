### =========================================================================
### Promise objects
### -------------------------------------------------------------------------
###
### Temporarily copy/pasted from rsolr
###

setClass("Promise")

setClass("SimplePromise",
         slots=c(expr="Expression",
                 context="Context"),
         contains="Promise")
