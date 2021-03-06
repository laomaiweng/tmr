TMR - Tcl Map-Reduce
====================

Tcl implementation of the map-reduce data processing algorithm.

Available concurrency models:
* sequential
* threads (using the _tpool_ package)


## Usage

Usage is rather straightforward once the map and reduce procedures have been defined:
````Tcl
package require tmr
set results [::tmr::mapreduce -threads <threads #> <input data> <map proc> <reduce proc>]
````

The _input data_ must be a dictionary of input (key,value) pairs.

The _map proc_ must be a procedure with the following signature (_oKey*_ values need not be unique):
````Tcl
proc mapproc {iKey iValue} {
  # do stuff
  return [list oKey1 oValue1 ... oKeyN oValueN]
}
````

The _reduce proc_ must be a procedure with the following signature:
````Tcl
proc reduceproc {iKey iValue} {
  # do stuff
  return [list oValue1 ... oValueN]
}
````

Due to limitations in the current implementation of TMR, both these procedures *must* be self-contained: load their required packages and define any procedure they may use (if not provided by a package).


## Requirements

* Tcl 8.6 (with threading enabled)
* QUtils package from https://github.com/laomaiweng/scripts (in _tcl/qutils_)

Easily adaptable to Tcl 8.5 (with Thread extension): _try{}_ constructs to replace by simple _catch{}_ constructs.


## Samples

### _wordcount_

Count the occurences of all words in a text.

Sample usage:
````Tcl
# Split the input text in blocks of 100 lines
set text [::wc::slab $text 100]
# Execute the word-count map-reduce algorithm on 4 threads
set words [::tmr::mapreduce -threads 4 $text ::wc::map ::wc::reduce]
# Sort the words from most to least occurring
set words [lsort -integer -decreasing -stride 2 -index 1 $words]
# Dump the Top-5 words to stdout
foreach {w c} [lrange $words 0 9] {
    puts "$w\t$c"
}
````


## Further information

On map-reduce:
> http://en.wikipedia.org/wiki/MapReduce

On TMR:
> The _::tmr::mrstage_ procedure implements a single stage of a map-reduce pipeline. It has the following syntax:
> ````Tcl
::tmr::mrstage ?-pool <thread pool name (tpool)>? ?-reduce? <data> <procedure>
````

> A multi-stage pipeline can be constructed manually by calling this procedure
> multiple times in sequence and passing the results of each stage on to the next stage.

> If the last stage is a reduce stage, the _-reduce_ argument must be specified.
