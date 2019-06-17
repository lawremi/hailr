### =========================================================================
### Hail function declarations
### -------------------------------------------------------------------------
###

## Utility for generating these definitions from the Python registration call
if (FALSE) {
convert <- function(x) {
    s <- substitute(x)
    name <- paste0("ht_", s[[2L]])
    args <- eval(s[[3L]], list(dtype=identity))
    ret <- s[[4L]][[2L]]
    substring(args, 1L, 1L) <- toupper(substring(args, 1L, 1L))
    args <- paste0("T", args)
    ret <- as.name(paste0("T", toupper(ret)))
    print(substitute(setGeneric(name, function(x) standardGeneric(name))))
    print(substitute(setMethod(name, args, function(x) ret)))
    invisible(NULL)
}
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercions
###

setGeneric("ht_toFloat32", function(x) standardGeneric("ht_toFloat32"))
setMethod("ht_toFloat32", "TNumeric", function(x) TFLOAT32)
setMethod("ht_toFloat32", "TString", function(x) TFLOAT32)
setMethod("ht_toFloat32", "TBoolean", function(x) TFLOAT32)

setGeneric("ht_toFloat64", function(x) standardGeneric("ht_toFloat64"))
setMethod("ht_toFloat64", "TNumeric", function(x) TFLOAT64)
setMethod("ht_toFloat64", "TString", function(x) TFLOAT64)
setMethod("ht_toFloat64", "TBoolean", function(x) TFLOAT64)

setGeneric("ht_toInt32", function(x) standardGeneric("ht_toInt32"))
setMethod("ht_toInt32", "TNumeric", function(x) TINT32)
setMethod("ht_toInt32", "TString", function(x) TINT32)
setMethod("ht_toInt32", "TBoolean", function(x) TINT32)

setGeneric("ht_toInt64", function(x) standardGeneric("ht_toInt64"))
setMethod("ht_toInt64", "TNumeric", function(x) TINT64)
setMethod("ht_toInt64", "TString", function(x) TINT64)
setMethod("ht_toInt64", "TBoolean", function(x) TINT64)

setGeneric("ht_toBoolean", function(x) standardGeneric("ht_toBoolean"))
setMethod("ht_toBoolean", "TString", function(x) TBOOLEAN)

setGeneric("ht_toSet", function(x) standardGeneric("ht_toSet"))
setMethod("ht_toSet", "TArray", function(x) TSet(elementType(x)))

setGeneric("ht_dictToArray", function(x) standardGeneric("ht_dictToArray"))
setMethod("ht_dictToArray", "TDict",
          function(x) TArray(TTuple(keyType(x), valueType(x))))

setGeneric("ht_dict", function(x) standardGeneric("ht_dict"))
setMethod("ht_dict", "TIterable", function(x) {
    stopifnot(is(elementType(x), "TTuple"))
    TDict(elementType(x)[[1L]], elementType(x)[[2L]])
})

setGeneric("ht_str", function(x) TSTRING)

setGeneric("ht_json", function(x) TSTRING)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Set arithmetic
###

# like setdiff()
setGeneric("ht_difference", function(x, y) standardGeneric("ht_difference"))
setMethod("ht_difference", c("TSet", "TSet"), function(x, y) {
    stopifnot(identical(elementType(x), elementType(y)))
    x
})

setGeneric("ht_remove", function(x, y) standardGeneric("ht_remove"))
setMethod("ht_remove", "TSet", function(x, y) {
    stopifnot(identical(y, elementType(x)))
    x
})

setGeneric("ht_add", function(x, y) standardGeneric("ht_add"))
setMethod("ht_add", "TSet", function(x, y) {
    stopifnot(identical(y, elementType(x)))
    x
})

setGeneric("ht_contains", function(x, key) standardGeneric("ht_contains"))
setMethod("ht_contains", "TDict", function(x, key) {
    stopifnot(identical(key, keyType(x)))
    TBOOLEAN
})
setMethod("ht_contains", "TSet", function(x, key) {
    stopifnot(identical(key, elementType(x)))
    TBOOLEAN
})

# like all(%in%)
setGeneric("ht_isSubset", function(x, y) standardGeneric("ht_isSubset"))
setMethod("ht_isSubset", c("TSet", "TSet"), function(x, y) TBOOLEAN)

# S4Vectors has an isEmpty()
setGeneric("ht_isEmpty", function(x) standardGeneric("ht_isEmpty"))
setMethod("ht_isEmpty", "TContainer", function(x) TBOOLEAN)

setGeneric("ht_union", function(x, y) standardGeneric("ht_union"))
setMethod("ht_union", c("TSet", "TSet"), function(x, y) {
    stopifnot(identical(elementType(x), elementType(y)))
    x
})

setGeneric("ht_intersection",
           function(x, y) standardGeneric("ht_intersection"))
setMethod("ht_intersection", c("TSet", "TSet"), function(x, y) {
    stopifnot(identical(elementType(x), elementType(y)))
    x
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Statistics
###

###
setGeneric("ht_median", function(x) standardGeneric("ht_median"))
setMethod("ht_median", "TContainer", function(x) {
    stopifnot(is(elementType(x), "TNumeric"))
    elementType(x)
})

# like which.min()
setGeneric("ht_argmin", function(x) standardGeneric("ht_argmin"))
setMethod("ht_argmin", "TArray", function(x) TINT32)
# like which.min() but returns empty on ties; useful?
setGeneric("ht_uniqueMinIndex",
           function(x) standardGeneric("ht_uniqueMinIndex"))
setMethod("ht_uniqueMinIndex", "TArray", function(x) TINT32)

# like which.max()
setGeneric("ht_argmax", function(x) standardGeneric("ht_argmax"))
setMethod("ht_argmax", "TArray", function(x) TINT32)
# like which.max() but returns empty on ties; useful?
setGeneric("ht_uniqueMaxIndex",
           function(x) standardGeneric("ht_uniqueMaxIndex"))
setMethod("ht_uniqueMaxIndex", "TArray", function(x) TINT32)

setGeneric("ht_mean", function(x) standardGeneric("ht_mean"))
setMethod("ht_mean", "TContainer", function(x) {
    stopifnot(is(elementType(x), "TNumeric"))
    TFLOAT64
})

setGeneric("ht_max", function(x, y) {
    stopifnot(identical(x, y))
    x
})
setMethod("ht_max", c("TContainer", "missing"), function(x, y) {
    stopifnot(is(elementType(x), "TNumeric"))
    elementType(x)
})

setGeneric("ht_min", function(x, y) {
    stopifnot(identical(x, y))
    x
})
setMethod("ht_min", c("TContainer", "missing"), function(x, y) {
    stopifnot(is(elementType(x), "TNumeric"))
    elementType(x)
})

setGeneric("ht_product", function(x) standardGeneric("ht_product"))
setMethod("ht_product", "TContainer", function(x) {
    stopifnot(is(elementType(x), "TNumeric"))
    elementType(x)
})

setGeneric("ht_sum", function(x) standardGeneric("ht_sum"))
setMethod("ht_sum", "TContainer", function(x) {
    stopifnot(is(elementType(x), "TNumeric"))
    elementType(x)
})

setGeneric("ht_corr", function(x, y) standardGeneric("ht_corr"))
setMethod("ht_corr", c("TArray", "TArray"), function(x, y) {
    stopifnot(is(elementType(x), "TFloat64"),
              is(elementType(y), "TFloat64"))
    TFLOAT64
})

setGeneric("ht_entropy", function(x) standardGeneric("ht_entropy"))
setMethod("ht_entropy", "TString", function(x) TFLOAT64)

setGeneric("ht_hamming", function(x, y) standardGeneric("ht_hamming"))
setMethod("ht_hamming", c("TString", "TString"), function(x, y) TINT32)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Arithmetic
###

setOpFunction <- function(name) {
    name <- paste0("ht_", name)
    eval(substitute(setGeneric(name, function(x, y) standardGeneric(name))))
    setMethod(name, c("TArray", "TArray"), function(x, y) {
        stopifnot(is(elementType(x), "TNumeric"),
                  is(elementType(y), "TNumeric"))
        merge(elementType(x), elementType(y))
    })
    setMethod(name, c("TArray", "TNumeric"), function(x, y) {
        stopifnot(is(elementType(x), "TNumeric"))
        merge(elementType(x), y)
    })
    setMethod(name, c("TNumeric", "TArray"), function(x, y) {
        stopifnot(is(elementType(y), "TNumeric"))
        merge(x, elementType(y))
    })
}

setOpFunction("+")
setOpFunction("-")
setOpFunction("%") # like %%
setOpFunction("*")
setOpFunction("//") # like %/%?

# like ^()
setGeneric("ht_**", function(x, y) standardGeneric("ht_**"))
setMethod("ht_**", c("TArray", "TArray"), function(x, y) {
    stopifnot(is(elementType(x), "TNumeric"),
              is(elementType(y), "TNumeric"))
    TArray(TFLOAT64)
})
setMethod("ht_**", c("TArray", "TNumeric"), function(x, y) {
    stopifnot(is(elementType(x), "TNumeric"))
    TArray(TFLOAT64)
})
setMethod("ht_**", c("TNumeric", "TArray"), function(x, y) {
    stopifnot(is(elementType(y), "TNumeric"))
    TArray(TFLOAT64)
})
setMethod("ht_**", c("TNumeric", "TNumeric"), function(x, y) TFLOAT64)

setGeneric("ht_/", function(x, y) standardGeneric("ht_/"))
ht_divide <- function(x, y) {
    if (is(x, "TFloat64") || is(y, "TFloat64"))
        TArray(TFLOAT64)
    else TArray(TFLOAT32)
}
setMethod("ht_/", c("TArray", "TArray"), function(x, y) {
    ht_divide(elementType(x), elementType(y))
})
setMethod("ht_/", c("TNumeric", "TArray"), function(x, y) {
    ht_divide(x, elementType(y))
})
setMethod("ht_/", c("TArray", "TNumeric"), function(x, y) {
    ht_divide(elementType(x), y)
})

setGeneric("ht_addone", function(x) standardGeneric("ht_addone"))
setMethod("ht_addone", "TInt32", function(x) TINT32)

setGeneric("ht_log10", function(x) standardGeneric("ht_log10"))
setMethod("ht_log10", "TFloat64", function(x) TFLOAT64)

setGeneric("ht_log", function(x, base) standardGeneric("ht_log"))
setMethod("ht_log", c("TFloat64", "TFloat64"), function(x, base) TFLOAT64)

setGeneric("ht_exp", function(x) standardGeneric("ht_exp"))
setMethod("ht_exp", "TFloat64", function(x) TFLOAT64)

###
setGeneric("ht_abs", function(x) standardGeneric("ht_abs"))
setMethod("ht_abs", "TNumeric", function(x) x)

###
setGeneric("ht_sign", function(x) standardGeneric("ht_sign"))
setMethod("ht_sign", "TNumeric", function(x) x)

setGeneric("ht_is_finite", function(x) standardGeneric("ht_is_finite"))
setMethod("ht_is_finite", "TFloat32", function(x) TBOOLEAN)
setMethod("ht_is_finite", "TFloat64", function(x) TBOOLEAN)

setGeneric("ht_is_infinite", function(x) standardGeneric("ht_is_infinite"))
setMethod("ht_is_infinite", "TFloat32", function(x) TBOOLEAN)
setMethod("ht_is_infinite", "TFloat64", function(x) TBOOLEAN)

setGeneric("ht_isnan", function(x) standardGeneric("ht_isnan"))
setMethod("ht_isnan", "TFloat32", function(x) TBOOLEAN)
setMethod("ht_isnan", "TFloat64", function(x) TBOOLEAN)

setGeneric("ht_floor", function(x) standardGeneric("ht_floor"))
setMethod("ht_floor", "TFloat64", function(x) TFLOAT64)
setMethod("ht_floor", "TFloat32", function(x) TFLOAT32)

setGeneric("ht_ceil", function(x) standardGeneric("ht_ceil"))
setMethod("ht_ceil", "TFloat64", function(x) TFLOAT64)
setMethod("ht_ceil", "TFloat32", function(x) TFLOAT32)

setGeneric("ht_sqrt", function(x) standardGeneric("ht_sqrt"))
setMethod("ht_sqrt", "TFloat64", function(x) TFLOAT64)


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Comparison
###

## like all.equal()
setGeneric("ht_approxEqual", function(x, y, tolerance, absolute, nanSame)
    standardGeneric("ht_approxEqual"))
setMethod("ht_approxEqual",
          c("TFloat64", "TFloat64", "TFloat64",  "TBoolean", "TBoolean"),
          function(x, y, tolerance, absolute, nanSame) TBOOLEAN)

setGeneric("ht_valuesSimilar", function(x, y, tolerance, absolute)
    standardGeneric("ht_valuesSimilar"))
setMethod("ht_valuesSimilar",
          c("ANY", "ANY", "TFloat64", "TBoolean"),
          function(x, y, tolerance, absolute) TBOOLEAN)

setGeneric("ht_compare", function(x, y) standardGeneric("ht_compare"))
setMethod("ht_compare", c("TInt32", "TInt32"), function(x, y) TINT32)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Container accessors
###

# like unname() on a list/env
setGeneric("ht_values", function(x) standardGeneric("ht_values"))
setMethod("ht_values", "TDict", function(x) TArray(valueType(x)))

# like names()
setGeneric("ht_keys", function(x) standardGeneric("ht_keys"))
setMethod("ht_keys", "TDict", function(x) TArray(keyType(x)))

# like names() on an env
setGeneric("ht_keySet", function(x) standardGeneric("ht_keySet"))
setMethod("ht_keySet", "TDict", function(x) TSet(keyType(x)))

# like tail() on a list with negative 'n'
setGeneric("ht_[*:]", function(x, n) standardGeneric("ht_[*:]"))
setMethod("ht_[*:]", c("TArray", "TInt32"), function(x, n) x)
setMethod("ht_[*:]", c("TString", "TInt32"), function(x, n) x)

# like head() on a list
setGeneric("ht_[:*]", function(x, n) standardGeneric("ht_[:*]"))
setMethod("ht_[:*]", c("TArray", "TInt32"), function(x, n) x)
setMethod("ht_[:*]", c("TString", "TInt32"), function(x, n) x)

# like window()
setGeneric("ht_[*:*]", function(x, start, end) standardGeneric("ht_[*:*]"))
setMethod("ht_[*:*]", c("TString", "TInt32", "TInt32"),
          function(x, start, end) TSTRING)
setMethod("ht_[*:*]", c("TArray", "TInt32", "TInt32"),
          function(x, start, end) x)

# sort of like [,i], but not necessarily rectangular
setGeneric("ht_indexArray", function(x, i) standardGeneric("ht_indexArray"))
setMethod("ht_indexArray", c("TArray", "TInt32"),
          function(x, i) elementType(x))

setGeneric("ht_get", function(x, key, default) standardGeneric("ht_get"))
setMethod("ht_get", "TDict", function(x, key, default) {
    stopifnot(identical(key, keyType(x)))
    if (!missing(default))
        stopifnot(identical(default, valueType(x)))
    elementType(x)
})
setGeneric("ht_[]", function(x, key) standardGeneric("ht_[]"))
setMethod("ht_[]", "TDict", function(x, key) {
    stopifnot(identical(key, keyType(x)))
    elementType(x)
})
# like substring()
setMethod("ht_[]", c("TString", "TInt32"), function(x, key) TSTRING)

# like pc() and c(), note: we need a contextual[Parallel]Bind()
setGeneric("ht_extend", function(x, y) standardGeneric("ht_extend"))
setMethod("ht_extend", c("TArray", "TArray"), function(x, y) {
    stopifnot(identical(x, y))
    x
})

# sort() seems to happen through ArraySort() now?

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Statistical distributions and tests
###

setGeneric("ht_qnorm", function(x) standardGeneric("ht_qnorm"))
setMethod("ht_qnorm", "TFloat64", function(x) TFLOAT64)

setGeneric("ht_dpois",
           function(x, lambda, log_p=TBOOLEAN) standardGeneric("ht_dpois"))
setMethod("ht_dpois", c("TFloat64", "TFloat64", "TBoolean"),
          function(x, lambda, log_p) TFLOAT64)

setGeneric("ht_ppois",
           function(x, lambda, lower_tail=TBOOLEAN, log_p=TBOOLEAN)
               standardGeneric("ht_ppois"))
setMethod("ht_ppois",
          c("TFloat64", "TFloat64", "TBoolean", "TBoolean"),
          function(x, lambda, lower_tail, log_p) TFLOAT64)

setGeneric("ht_qchisqtail", function(p, df) standardGeneric("ht_qchisqtail"))
setMethod("ht_qchisqtail", c("TFloat64", "TFloat64"), function(p, df) TFLOAT64)

setGeneric("ht_pchisqtail", function(p, df) standardGeneric("ht_pchisqtail"))
setMethod("ht_pchisqtail", c("TFloat64", "TFloat64"), function(p, df) TFLOAT64)

setGeneric("ht_binomTest", function(nSuccess, n, p, alternative)
    standardGeneric("ht_binomTest"))
setMethod("ht_binomTest", c("TInt32", "TInt32", "TFloat64", "TInt32"),
          function(nSuccess, n, p, alternative) TFLOAT64)

setGeneric("ht_qpois", function(x, lambda) standardGeneric("ht_qpois"))
setMethod("ht_qpois", c("TFloat64", "TFloat64"), function(x, lambda) TINT32)

setGeneric("ht_dbeta", function(x, a, b) standardGeneric("ht_dbeta"))
setMethod("ht_dbeta", c("TFloat64", "TFloat64", "TFloat64"), 
          function(x, a, b) TFLOAT64)

setGeneric("ht_gamma", function(x) standardGeneric("ht_gamma"))
setMethod("ht_gamma", "TFloat64", function(x) TFLOAT64)

setGeneric("ht_fisher_exact_test",
           function(c1, c2, c3, c4) standardGeneric("ht_fisher_exact_test"))
setMethod("ht_fisher_exact_test", c("TInt32", "TInt32", "TInt32",  "TInt32"),
          function(c1, c2, c3, c4)
              TStruct(p_value=TFLOAT64, odds_ratio=TFLOAT64,
                      ci_95_lower = TFLOAT64, ci_95_upper = TFLOAT64))

setGeneric("ht_chi_squared_test",
           function(c1, c2, c3, c4) standardGeneric("ht_chi_squared_test"))
setMethod("ht_chi_squared_test",
          c("TInt32", "TInt32", "TInt32", "TInt32"),
          function(c1, c2, c3, c4)
              TStruct(p_value=TFLOAT64, odds_ratio=TFLOAT64))

setGeneric("ht_hardy_weinberg_test",
           function(nHomRef, nHet, nHomVar)
               standardGeneric("ht_hardy_weinberg_test"))
setMethod("ht_hardy_weinberg_test",
          c("TInt32", "TInt32", "TInt32"),
          function(nHomRef, nHet, nHomVar)
              TStruct(het_freq_hwe=TFLOAT64, p_value=TFLOAT64))

setGeneric("ht_pnorm", function(x) standardGeneric("ht_pnorm"))
setMethod("ht_pnorm", "TFloat64", function(x) TFLOAT64)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### String manipulation
###

###
## like paste(collapse=)
setGeneric("ht_mkString", function(x, collapse) standardGeneric("ht_mkString"))
setMethod("ht_mkString", c("TContainer", "TString"), function(x, collapse) {
    stopifnot(is(elementType(x), "TString"))
    TSTRING
})

## like paste0()
setGeneric("ht_+", function(x, y) standardGeneric("ht_+"))
setMethod("ht_+", c("TString", "TString"), function(x, y) TSTRING)

## like sub()
setGeneric("ht_replace",
           function(pattern, replacement, x) standardGeneric("ht_replace"))
setMethod("ht_replace", c("TString", "TString", "TString"),
          function(pattern, replacement, x) TSTRING)

setGeneric("ht_split", function(x, delim, n) standardGeneric("ht_split"))
setMethod("ht_split", c("TString", "TString", "TInt32"),
          function(x, delim, n) TArray(TSTRING))
setMethod("ht_split", c("TString", "TString", "missing"),
          function(x, delim, n) TArray(TSTRING))

setGeneric("ht_endswith", function(x, suffix) standardGeneric("ht_endswith"))
setMethod("ht_endswith", c("TString", "TString"), function(x, suffix) TBOOLEAN)

setGeneric("ht_startswith",
           function(x, prefix) standardGeneric("ht_startswith"))
setMethod("ht_startswith", c("TString", "TString"),
          function(x, prefix) TBOOLEAN)

setGeneric("ht_lower", function(x) standardGeneric("ht_lower"))
setMethod("ht_lower", "TString", function(x) TSTRING)

setGeneric("ht_upper", function(x) standardGeneric("ht_upper"))
setMethod("ht_upper", "TString", function(x) TSTRING)

## like grepl(fixed=TRUE)
setMethod("ht_contains", c("TString", "TString"), function(x, key) TBOOLEAN)

setGeneric("ht_strip", function(x) standardGeneric("ht_strip"))
setMethod("ht_strip", "TString", function(x) TSTRING)

## like regmatches(regexec()), utils:::strextract()
setGeneric("ht_firstMatchIn",
           function(x, pattern) standardGeneric("ht_firstMatchIn"))
setMethod("ht_firstMatchIn", c("TString", "TString"),
          function(x, pattern) TArray(TSTRING))

## like grepl(fixed=FALSE)
setGeneric("ht_~", function(x, pattern) standardGeneric("ht_~"))
setMethod("ht_~", c("TString", "TString"), function(x, pattern) TBOOLEAN)

## like sprintf()
setGeneric("ht_format", function(x, args) standardGeneric("ht_format"))
setMethod("ht_format", c("TString", "TTuple"), function(x, args) TSTRING)

setGeneric("ht_escapeString", function(x) standardGeneric("ht_escapeString"))
setMethod("ht_escapeString", "TString", function(x) TSTRING)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Genetics
###

setGeneric("ht_oneHotAlleles",
           function(x, n_alleles) standardGeneric("ht_oneHotAlleles"))
setMethod("ht_oneHotAlleles", c("TCall", "TInt32"),
          function(x, n_alleles) TArray(TINT32))

setGeneric("ht_ploidy", function(x) standardGeneric("ht_ploidy"))
setMethod("ht_ploidy", "TCall", function(x) TINT32)

setGeneric("ht_isHet", function(x) standardGeneric("ht_isHet"))
setMethod("ht_isHet", "TCall", function(x) TBOOLEAN)

setGeneric("ht_isHetRef", function(x) standardGeneric("ht_isHetRef"))
setMethod("ht_isHetRef", "TCall", function(x) TBOOLEAN)

setGeneric("ht_isAutosomalOrPseudoAutosomal",
           function(x) standardGeneric("ht_isAutosomalOrPseudoAutosomal"))
setMethod("ht_isAutosomalOrPseudoAutosomal", "TLocus", function(x) TBOOLEAN)

setGeneric("ht_isPhased", function(x) standardGeneric("ht_isPhased"))
setMethod("ht_isPhased", "TCall", function(x) TBOOLEAN)

setGeneric("ht_isHomVar", function(x) standardGeneric("ht_isHomVar"))
setMethod("ht_isHomVar", "TCall", function(x) TBOOLEAN)

setGeneric("ht_plDosage", function(x) standardGeneric("ht_plDosage"))
setMethod("ht_plDosage", "TArray", function(x) {
    stopifnot(is(elementType(x), "TInt32"))
    TFLOAT64
})

setGeneric("ht_triangle", function(x) standardGeneric("ht_triangle"))
setMethod("ht_triangle", "TInt32", function(x) TINT32)

setGeneric("ht_min_rep",
           function(locus, alleles) standardGeneric("ht_min_rep"))
setMethod("ht_min_rep", c("TLocus", "TArray"),
          function(locus, alleles) {
              stopifnot(is(elementType(alleles), "TString"))
              TStruct(locus=x, alleles=TArray(TSTRING))
          })

setGeneric("ht_haplotype_freq_em",
           function(x) standardGeneric("ht_haplotype_freq_em"))
setMethod("ht_haplotype_freq_em", "TArray",
          function(x) {
              stopifnot(is(elementType(x), "TInt32"))
              TArray(TFLOAT64)
          })

setGeneric("ht_filtering_allele_frequency",
           function(ac, an, ci)
               standardGeneric("ht_filtering_allele_frequency"))
setMethod("ht_filtering_allele_frequency",
          c("TInt32", "TInt32", "TFloat64"),
          function(ac, an, ci) TFLOAT64)

setGeneric("ht_gqFromPL", function(x) standardGeneric("ht_gqFromPL"))
setMethod("ht_gqFromPL", "TArray", function(x) {
    stopifnot(is(elementType(x), "TInt32"))
    TINT32
})

setGeneric("ht_dosage", function(x) standardGeneric("ht_dosage"))
setMethod("ht_dosage", "TArray", function(x) {
    stopifnot(is(elementType(x), "TFloat64"))
    TFLOAT64
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Logic
###

setGeneric("ht_||", function(x, y) standardGeneric("ht_||"))
setMethod("ht_||", c("TBoolean", "TBoolean"), function(x, y) TBOOLEAN)

setGeneric("ht_&&", function(x, y) standardGeneric("ht_&&"))
setMethod("ht_&&", c("TBoolean", "TBoolean"), function(x, y) TBOOLEAN)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Ranges
###

setGeneric("ht_Interval", function(start, end, includeStart, includeEnd)
    standardGeneric("ht_Interval"))
setMethod("ht_Interval", c("ANY", "ANY", "TBoolean", "TBoolean"),
          function(start, end, includeStart, includeEnd) {
              stopifnot(identical(start, end))
              TInterval(start)
          })

setGeneric("ht_includesEnd", function(x) standardGeneric("ht_includesEnd"))
setMethod("ht_includesEnd", "TInterval", function(x) TBOOLEAN)

setGeneric("ht_includesStart", function(x) standardGeneric("ht_includesStart"))
setMethod("ht_includesStart", "TInterval", function(x) TBOOLEAN)

setGeneric("ht_position", function(x) standardGeneric("ht_position"))
setMethod("ht_position", "TLocus", function(x) TINT32)

setGeneric("ht_contig", function(x) standardGeneric("ht_contig"))
setMethod("ht_contig", "TLocus", function(x) TSTRING)

setGeneric("ht_inYPar", function(x) standardGeneric("ht_inYPar"))
setMethod("ht_inYPar", "TLocus", function(x) TBOOLEAN)

setGeneric("ht_inYNonPar", function(x) standardGeneric("ht_inYNonPar"))
setMethod("ht_inYNonPar", "TLocus", function(x) TBOOLEAN)

setGeneric("ht_inXPar", function(x) standardGeneric("ht_inXPar"))
setMethod("ht_inXPar", "TLocus", function(x) TBOOLEAN)

setGeneric("ht_inXNonPar", function(x) standardGeneric("ht_inXNonPar"))
setMethod("ht_inXNonPar", "TLocus", function(x) TBOOLEAN)

setGeneric("ht_isMitochondrial",
           function(x) standardGeneric("ht_isMitochondrial"))
setMethod("ht_isMitochondrial", "TLocus", function(x) TBOOLEAN)

setGeneric("ht_isAutosomal", function(x) standardGeneric("ht_isAutosomal"))
setMethod("ht_isAutosomal", "TLocus", function(x) TBOOLEAN)


setMethod("ht_contains", c("TInterval", "ANY"), function(x, key) {
    stopifnot(identical(representationType(x), key))
    TBOOLEAN
})

setMethod("ht_isEmpty", "TInterval", function(x) TBOOLEAN)

setGeneric("ht_overlaps", function(x, y) standardGeneric("ht_overlaps"))
setMethod("ht_overlaps", c("TInterval", "TInterval"), 
          function(x, y) TBOOLEAN)

setGeneric("ht_end", function(x) standardGeneric("ht_end"))
setMethod("ht_end", "TInterval", function(x) representationType(x))

setGeneric("ht_start", function(x) standardGeneric("ht_start"))
setMethod("ht_start", "TInterval", function(x) representationType(x))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Variant/genotype calls
###

setGeneric("ht_Call", function(a, b, phased) standardGeneric("ht_Call"))
setMethod("ht_Call", c("missing", "missing", "TBoolean"),
          function(a, b, phased) TCALL)
setMethod("ht_Call", c("TString", "missing", "TBoolean"),
          function(a, b, phased) TCALL)
setMethod("ht_Call", c("TInt32", "missing", "TBoolean"),
          function(a, b, phased) TCALL)
setMethod("ht_Call", c("TInt32", "TInt32", "TBoolean"),
          function(a, b, phased) TCALL)
setMethod("ht_Call", c("TArray", "missing", "TBoolean"),
          function(a, b, phased) {
              stopifnot(is(elementType(a), "TInt32"))
          })

setGeneric("ht_UnphasedDiploidGtIndexCall", function(x)
    standardGeneric("ht_UnphasedDiploidGtIndexCall"))
setMethod("ht_UnphasedDiploidGtIndexCall", "TInt32", function(x) TCALL)

setGeneric("ht_[]", function(x, i) standardGeneric("ht_[]"))
setMethod("ht_[]", c("TCall", "TInt32"), function(x, i) TINT32)

setGeneric("ht_unphasedDiploidGtIndex",
           function(x) standardGeneric("ht_unphasedDiploidGtIndex"))
setMethod("ht_unphasedDiploidGtIndex", "TCall", function(x) TINT32)

setGeneric("ht_isNonRef", function(x) standardGeneric("ht_isNonRef"))
setMethod("ht_isNonRef", "TCall", function(x) TBOOLEAN)

setGeneric("ht_isHetNonRef", function(x) standardGeneric("ht_isHetNonRef"))
setMethod("ht_isHetNonRef", "TCall", function(x) TBOOLEAN)

setGeneric("ht_isHomRef", function(x) standardGeneric("ht_isHomRef"))
setMethod("ht_isHomRef", "TCall", function(x) TBOOLEAN)

setGeneric("ht_nNonRefAlleles",
           function(x) standardGeneric("ht_nNonRefAlleles"))
setMethod("ht_nNonRefAlleles", "TCall", function(x) TINT32)

setGeneric("ht_downcode", function(x, i) standardGeneric("ht_downcode"))
setMethod("ht_downcode", c("TCall", "TInt32"), function(x, i) TCALL)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Seeded (RNG) functions
###

setGeneric("ht_seeded_rand_pois", function(n, lambda)
    standardGeneric("ht_seeded_rand_pois"))
setMethod("ht_seeded_rand_pois", c("missing", "TFloat64"), function(n, lambda)
    TFLOAT64)
setMethod("ht_seeded_rand_pois", c("TInt32", "TFloat64"), function(n, lambda)
    TArray(TFLOAT64))

setGeneric("ht_seeded_rand_unif", function(n, p)
    standardGeneric("ht_seeded_rand_unif"))
setMethod("ht_seeded_rand_unif", c("TFloat64", "TFloat64"),
          function(n, p) TFLOAT64)

setGeneric("ht_seeded_rand_bool", function(x)
    standardGeneric("ht_seeded_rand_bool"))
setMethod("ht_seeded_rand_bool", "TFloat64", function(x) TBOOLEAN)

setGeneric("ht_seeded_rand_cat", function(x)
    standardGeneric("ht_seeded_rand_cat"))
setMethod("ht_seeded_rand_cat", "TFloat64", function(x) TINT32)

setGeneric("ht_seeded_rand_gamma", function(shape, scale)
    standardGeneric("ht_seeded_rand_gamma"))
setMethod("ht_seeded_rand_gamma", c("TFloat64", "TFloat64"),
          function(shape, scale) TFLOAT64)

setGeneric("ht_seeded_rand_beta",
           function(a, b, lower=TFLOAT64, upper=TFLOAT64)
               standardGeneric("ht_seeded_rand_beta"))
setMethod("ht_seeded_rand_beta",
          c("TFloat64", "TFloat64", "TFloat64", "TFloat64"),
          function(a, b, lower, upper) TFLOAT64)

setGeneric("ht_seeded_rand_norm",
           function(mean, sd) standardGeneric("ht_seeded_rand_norm"))
setMethod("ht_seeded_rand_norm", c("TFloat64", "TFloat64"),
          function(mean, sd) TFLOAT64)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Aggregators
###

setGeneric("ht_agg_Fraction", function(x) standardGeneric("ht_agg_Fraction"))
setMethod("ht_agg_Fraction", "TBoolean", function(x) TFLOAT64)

setGeneric("ht_agg_Statistics", function(x)
    standardGeneric("ht_agg_Statistics"))
setMethod("ht_agg_Statistics", "TNumeric", function(x)
    TStruct(mean=TFLOAT64, stdev=TFLOAT64, min=TFLOAT64, max=TFLOAT64, n=TINT64,
            sum=TFLOAT64))

setGeneric("ht_agg_InfoScore", function(x) standardGeneric("ht_agg_InfoScore"))
setMethod("ht_agg_InfoScore", "TArray", function(x) {
    if (!is(elementType(x), "TNumeric"))
        stop("InfoScore() aggregator only supports numeric arrays")
    TStruct(score=TFLOAT64, n_included=TINT32)
})

setGeneric("ht_agg_Collect", function(x) TArray(x))

setGeneric("ht_agg_CollectAsSet", function(x) TSet(x))

setGeneric("ht_agg_Sum", function(x) standardGeneric("ht_agg_Sum"))
setMethod("ht_agg_Sum", "TIntegral", function(x) TINT64)
setMethod("ht_agg_Sum", "TNumeric", function(x) TFLOAT64)
setMethod("ht_agg_Sum", "TArray", function(x) {
    if (!is(elementType(x), "TNumeric"))
        stop("Sum() aggregator only supports numeric arrays")
    elementType(x)
})

setGeneric("ht_agg_Product", function(x) standardGeneric("ht_agg_Product"))
setMethod("ht_agg_Product", "TIntegral", function(x) TINT64)
setMethod("ht_agg_Product", "TNumeric", function(x) TFLOAT64)

setGeneric("ht_agg_HardyWeinberg",
           function(x) standardGeneric("ht_agg_HardyWeinberg"))
setMethod("ht_agg_HardyWeinberg", "TCall", function(x) {
    TStruct(het_freq_hwe=FLOAT64, p_value=TFLOAT64)
})

setGeneric("ht_agg_Max", function(x) standardGeneric("ht_agg_Max"))
setMethod("ht_agg_Max", "TNumeric", function(x) x)
setMethod("ht_agg_Max", "TBoolean", function(x) TBOOLEAN)

setGeneric("ht_agg_Min", function(x) standardGeneric("ht_agg_Min"))
setMethod("ht_agg_Min", "TNumeric", function(x) x)
setMethod("ht_agg_Min", "TBoolean", function(x) TBOOLEAN)

ht_agg_Count <- function() TINT64

setGeneric("ht_agg_Counter", function(x) TDict(x, TINT64))

setGeneric("ht_agg_Take", function(x) TArray(x))

setGeneric("ht_agg_TakeBy", function(x, key) TArray(x))

setGeneric("ht_agg_Histogram", function(x) standardGeneric("ht_agg_Histogram"))

setMethod("ht_agg_Histogram", "TNumeric",
          function(x)
              TStruct(bin_edges=TArray(TFLOAT64),
                      bin_freq=TArray(TINT64), n_smaller=TINT64,
                      n_larger=TINT64))

setGeneric("ht_agg_Downsample",
           function(x, y, label) standardGeneric("ht_agg_Downsample"))
setMethod("ht_agg_Downsample", c("TNumeric", "TNumeric", "TArray"),
          function(x, y, label) {
              TArray(TTuple(TFLOAT64, TFLOAT64, TArray(TSTRING)))
          })

setGeneric("ht_agg_CallStats", function(x) standardGeneric("ht_agg_CallStats"))
setMethod("ht_agg_CallStats", "TCall",
          function(x) {
              TStruct(AC=TArray(TINT32), AF=TArray(TFLOAT64), AN=TINT32,
                      homozygote_count=TArray(TINT32))
          })

setGeneric("ht_agg_Inbreeding",
           function(x, prior) standardGeneric("ht_agg_Inbreeding"))
setMethod("ht_agg_Inbreeding", c("TCall", "TNumeric"), function(x, prior) {
    TStruct(f_stat=TFLOAT64, n_called=TINT64, expected_homs=TFLOAT64,
            observed_homs=TINT64)
})

setGeneric("ht_agg_LinearRegression",
           function(y, x) standardGeneric("ht_agg_LinearRegression"))
setMethod("ht_agg_LinearRegression", c("TNumeric", "TArray"), function(y, x) {
    if (!is(elementType(x), "TNumeric"))
        stop("LinearRegression() aggregator only supports numeric arrays")
    TSTruct(beta=TArray(TFLOAT64), standard_error=TArray(TFLOAT64),
            t_stat=TArray(TFLOAT64), p_value=TArray(TFLOAT64),
            multiple_standard_error=TFLOAT64, multiple_r_squared=TFLOAT64,
            adjusted_r_squared=TFLOAT64, f_stat=TFLOAT64,
            multiple_p_value=TFLOAT64,
            n=TINT64)
})

setGeneric("ht_agg_PearsonCorrelation",
           function(x, y) standardGeneric("ht_agg_PearsonCorrelation"))
setMethod("ht_agg_PearsonCorrelation", c("TNumeric", "TNumeric"),
          function(x, y) TFLOAT64)

setGeneric("ht_agg_PrevNonnull", function(x) x)
