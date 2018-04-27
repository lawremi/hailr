### =========================================================================
### JavaMethodTarget objects
### -------------------------------------------------------------------------
###
### Objects that are targeted by Java methods, i.e., JavaObject and
### JVM (static methods)
###

setClass("JavaMethodTarget")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Method invocation
###

setMethod("$", "JavaMethodTarget", function(x, name) x[[name]])

setMethod("[[", "JavaMethodTarget", function (x, i, j, ...) {
    stopifnot(missing(j), missing(...))
    stopifnot(is.character(i), length(i) == 1L && !is.na(i))
    JavaPath(x, name)
})

setGeneric("callMethod",
           function(target, path, args = list()) standardGeneric("callMethod"),
           signature="target")

setMethod("callMethod", "JavaMethodTarget",
          function(target, path, args) {
              fromJava(callMethod(impl(target), path,
                                  toJava(args, jvm(target))))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Object construction
###

setGeneric("constructObject",
           function(target, path, args) standardGeneric("constructObject"),
           signature="target")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### jvm() accessor
###

setGeneric("jvm", function(x) standardGeneric("jvm"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

setGeneric("toJava", function(x, jvm) standardGeneric("toJava"),
           signature="x")

