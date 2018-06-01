\name{HailDataFrame-class}
\docType{class}

\alias{class:HailDataFrame}
\alias{HailDataFrame-class}

% Accessors:
\alias{colnames,HailDataFrame-method}
\alias{rownames,HailDataFrame-method}
\alias{nrow,HailDataFrame-method}
\alias{ncol,HailDataFrame-method}
\alias{length,HailDataFrame-method}
\alias{names,HailDataFrame-method}
\alias{[<-,HailDataFrame,ANY,ANY,ANY-method}
\alias{[[<-,HailDataFrame-method}
\alias{[,HailDataFrame-method}
\alias{[[,HailDataFrame,ANY-method}

% Constructor:
\alias{HailDataFrame}

% Manipulation:
\alias{rename}
\alias{subset,HailDataFrame-method}
\alias{transform,HailDataFrame-method}
\alias{sort,HailDataFrame-method}
\alias{xtabs,HailDataFrame-method}
\alias{aggregate,formula-method}
\alias{unique,HailDataFrame-method}
\alias{aggregate,HailDataFrame-method}
\alias{sort,HailDataFrame-method}
\alias{summary,HailDataFrame-method}
\alias{unique,HailDataFrame-method}
\alias{head,HailDataFrame-method}
\alias{tail,HailDataFrame-method}
\alias{window,HailDataFrame-method}
\alias{group}
\alias{group,HailDataFrame,NULL-method}
\alias{group,HailDataFrame,formula-method}
\alias{ungroup,HailDataFrame-method}
\alias{rename,HailDataFrame-method}

% Evaluation:
\alias{eval,HailExpression,HailDataFrame-method}
\alias{eval,HailAggregateCall,HailDataFrame-method}
\alias{eval,language,HailDataFrame-method}
\alias{with,HailDataFrame-method}
\alias{within,HailDataFrame-method}

% Show:
\alias{show,HailDataFrame-method}

\title{HailDataFrame}
\description{
  The \code{HailDataFrame} class extends
  \code{\link[S4Vectors]{DataFrame}} to make a Hail Table object
  accessible through a data.frame-like interface.
}

\details{
  A \code{HailDataFrame} should more or less behave analogously to a data
  frame. It provides the same basic accessors (\code{\link{nrow}},
  \code{\link{ncol}}, \code{\link{length}}, \code{\link{rownames}},
  \code{\link{colnames}}, \code{\link{[}}, \code{\link{[<-}},
  \code{\link{[[}}, \code{\link{[[<-}}, \code{\link{$}},
  \code{\link{$<-}}, \code{\link{head}}, \code{\link{tail}}, etc) and
  can be coerced to an actual data frame via
  \code{\link{as.data.frame}}. Supported types of data manipulations
  include \code{\link{subset}}, \code{\link{transform}},
  \code{\link{sort}}, \code{\link{xtabs}}, \code{\link{aggregate}},
  \code{\link{unique}}, \code{\link{summary}}, etc.

  A special feature of \code{HailDataFrame}, vs. an ordinary data frame, is
  that it can be \code{group}ed into a
  \code{\linkS4class{GroupedHailDataFrame}}, where every column is modeled
  as a list, split by some combination of grouping factors. This is
  useful for aggregation and supports the implementation of the
  \code{aggregate} method, which is the recommended high-level
  interface.

  Another interesting feature is laziness: all column retrieval, e.g.,
  via \code{$} or \code{eval}, returns a
  \code{\linkS4class{HailPromise}} object. Many operations on promises
  are deferred, until they are finally \code{fulfill}ed by being shown
  or through explicit coercion to an R vector.
}

\section{Accessors}{
  These are some accessors that \code{HailDataFrame} adds on top of the
  basic data frame accessors. Most of these are for advanced use only.
  
  \describe{
  }
}

\section{Extended API}{
  Most of the typical data frame accessors and data manipulation
  functions will work analogously on \code{HailDataFrame} (see
  Details). Below, we list some of the non-standard methods that might
  be seen as an extension of the data frame API.

  \describe{
    \item{}{\code{aggregate(x, data, FUN, ..., subset, na.action,
        simplify = TRUE, count = FALSE)}: If \code{x} is a formula,
        aggregates \code{data}, grouping by \code{x}, by either applying
        \code{FUN}, or evaluating an aggregating expression in \dots, on
        each group. If \code{count} is \code{TRUE}, a \dQuote{count}
        column is added with the number of elements in each group. The
        rest of the arguments behave like those for the base
        \code{\link{aggregate}}.

        There are two main modes: aggregating with \code{FUN}, or, as an
        extension to the base \code{aggregate}, aggregating with
        expressions in \code{\dots}, similar to the interface for
        \code{transform}. If \code{FUN} is specified, then behavior is
        much like the original, except one can omit the LHS on the
        formula, in which case the entire frame is passed to
        \code{FUN}. In the second mode, there is a column in the result
        for each argument in \dots, and there must not be an LHS on the
        formula.

        See the documentation for the underlying \code{\link{facet}}
        function for details on what is supported on the formula RHS.

        For global aggregation, simply pass the \code{HailDataFrame} as
        \code{x}, in which case the \code{data} argument does not exist.

        Note that the function or expressions are only
        \emph{conceptually} evaluated on each group. In reality, the
        computations occur on grouped columns/promises, which are
        modeled as lists. Thus, there is potential for conflict, in
        particular with \code{length}, which return the number of
        groups, instead of operating group-wise. One should use the
        abstraction \code{ndoc} instead of \code{length}, since
        \code{ndoc} always returns document counts, and thus will return
        the size of each group.
    }
    \item{}{\code{rename(x, ...)}: Renames the columns of \code{x},
      where the names and character values of \dots indicates the
      mapping (\code{newname = oldname}). 
    }
    \item{}{\code{group(x, by)}: Returns a
      \code{\linkS4class{GroupedHailDataFrame}} that is grouped by the
      factors in \code{by}, typically a formula. To get back to
      \code{x}, call \code{ungroup(x)}.
    }
    \item{}{\code{grouping(x)}: Just returns \code{NULL}, since a
      \code{HailDataFrame} is not grouped (unless extended to be groupable).
    }
    \item{}{\code{defer(x)}: Returns a \code{HailDataFrame} that yields
      \code{\linkS4class{HailPromise}} objects instead of vectors
      whenever a field is retrieved
    }
  }
}

\section{Getting data into Hail}{
  There are two primary ways of getting a table of data into Hail:
  \enumerate{
    \item{\link[=copy]{Copy} the data to Hail from an R data.frame-like
      object},
    \item{Instruct Hail to \link[=readHailDataFrame]{read} the data from
      disk}
  }
  See the example section below for a demonstration of both methods.
}

%% \section{Constructor}{
  
  %% Low-level constructor, not sure if we want to expor this
  %% \describe{
  %%   \item{}{
  %%     \code{HailDataFrame(table)}: Constructs a new \code{HailDataFrame}
  %%     instance, given a Hail \code{table} to wrap.
  %%   }
  %% }
%% }


\section{Evaluation}{
  \describe{
    \item{}{
      \code{eval(expr, envir, enclos)}: Evaluates \code{expr} in the
      \code{HailDataFrame} \code{envir}, using \code{enclos} as the
      enclosing environment. The \code{expr} can be an R language object
      or a \code{HailExpression}, either of which are lazily evaluated
      if \code{defer} has been called on \code{envir}.
    }
  }
}

\section{Coercion}{
  \describe{
    \item{}{
      \code{as.data.frame(x, row.names=NULL, optional=FALSE)}:
      Downloads the data into an actual data.frame.
    }
    \item{}{
      \code{as.list(x)}: Essentially \code{as.list(as.data.frame(x))},
      except returns a list of promises.
    }
  }
}

\author{ Michael Lawrence }

\seealso{
  % TODO
}

\examples{
data_dir <- system.file("extdata", package="hailr")
tsv1 <- file.path(data_dir, "kt_example1.tsv")

## read directly from file into Hail
df <- readHailDataFrameFromText(tsv1, header=TRUE)

## read file into R, copy to Hail
df <- copy(read.table(tsv1, header=TRUE), hail_context())
}

\keyword{methods}
\keyword{classes}
