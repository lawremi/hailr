### =========================================================================
### JavaObject objects
### -------------------------------------------------------------------------
###
### High-level abstraction around Java objects. The hierarchy under
### JavaObject should mirror the Java hierarchy. Every JavaObject
### has an underlying implementing instance, which is polymorphic
### based on its driver.
###

.JavaObject <-
    setClass("JavaObject", slots = c(impl="ANY"),
             contains="JavaMethodTarget",
             validity = function(object) {
                 if (class(object) != "JavaObject") {
                     class <- jvm(object)$Class$forName(class(object))
                     if (!class$isAssignableFrom(object$getClass()))
                     {
                         paste0("class '", class$getName(),
                                "' not assignable from '",
                                object$getClass()$getName(), "'")
                     }
                 }
             })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

JavaObject <- function(impl) {
    .JavaObject(impl=impl)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

impl <- function(x) x@impl

setMethod("jvm", "JavaObject", function(x) JVM(jvm(impl(x))))

setMethod("toJava", "JavaObject", function(x, jvm) impl(x))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Transmitting objects to the JVM
###

setMethod("marshal", c("ANY", "JavaObject"), function(x, dest) {
    marshal(x, jvm(dest))
})

setMethod("transmit", c("ANY", "JavaObject"), function(x, dest) {
    transmit(transmit(x, jvm(dest)), dest)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Utilities
###

find_first_ancestor <- function(x, candidates) {
    ### NOTE: cannot use '$' here, since it downcasts and infinitely recurses
    class <- callMethod(x, "getClass")
    while(!is.null(class)) {
        name <- callMethod(class, "getName")
        if (name %in% candidates)
            return(name)
        class <- callMethod(class, "getSuperclass")
    }
}

S4_subclasses <- function(x) {
    ## should get all recursive subclasses if information is complete
    vapply(getClass(class(x))@subclasses, slot, character(1L), "subClass")
}

setGeneric("downcast", function(x) x)

setMethod("downcast", "JavaObject",  function(x) {
    candidates <- c(class(x), S4_subclasses(x))
    deepest <- find_first_ancestor(x, candidates)
    if (!is.null(deepest))
        as(x, deepest)
    else x
})

setMethod("downcast", "list", function(x) {
    lapply(x, downcast)
})
