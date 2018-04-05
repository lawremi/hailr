### =========================================================================
### JavaMethodTarget objects
### -------------------------------------------------------------------------
###
### Objects that are targeted by Java methods, i.e., SparkObject and
### SparkConnection (static methods)
###

setClass("JavaMethodTarget")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Method invocation
###

setMethod("$", "JavaMethodTarget", function(x, name) JavaPath(x, name))

setGeneric("callMethod",
           function(target, path, args = list()) standardGeneric("callMethod"),
           signature="target")

setMethod("callMethod", "JavaMethodTarget",
          function(target, path, args) {
              fromJava(callMethod(impl(target), path,
                                  toJava(args, jvm(target))))
          })

setGeneric("jvm", function(x) standardGeneric("jvm"))

setGeneric("toJava", function(x, jvm) standardGeneric("toJava"),
           signature="x")

