#############################################################################
#   tmr - Tcl Map-Reduce                                                    #
#                                                                           #
# Author: quentin <quentin AT minster DOT io>                               #
#                                                                           #
# Tcl implementation of the map-reduce data processing algorithm.           #
#                                                                           #
# Features:                                                                 #
# * sequential implementation                                               #
# * parallel implementation using the tpool package                         #
#                                                                           #
# History:                                                                  #
#   v0.1    Initial version: basic sequential and parallel implementation   #
#############################################################################

package provide tmr 0.1


# Package dependencies
package require qutils
package require Thread
package require cmdline


# Define the tmr namespace and export some of its procedures
namespace eval ::tmr {
    namespace export mapreduce mrstage
}


#############################################################################
#############################################################################
#
# Procedures
#
#############################################################################
#############################################################################

#############################################################################
# Standard 2-stage map-reduce pipeline.
#
# Options:
#   -threads    number of threads to use (0 runs in the main thread)
#
# Arguments:
#   data        input data as a dict
#   mapproc     map procedure - must return a list of (key,value) pairs
#                               (flat, 2-stride list)
#   reduceproc  reduce procedure - must return a list of results for the
#                                  given key and value list
#############################################################################
proc ::tmr::mapreduce {args} {
    # Usage text and options
    set usage "::tmr::mapreduce ?options? data mapproc reduceproc"
    set options {
        {threads.arg    0   "number of threads to use (0 runs in the main thread)"}
    }

    # Parse the options
    array set params [::cmdline::getoptions args $options "$usage\noptions:"]
    # Check the number of arguments
    if {[llength $args] != 3} {
        return -code error "wrong number of arguments: should be \"$usage\""
    }
    # Get the arguments
    lassign $args data mapproc reduceproc
    # Check the arguments
    if {![string is dict $data]} {
        return -code error "data argument is not a dict"
    }
    if {![info pcexists -proc $mapproc]} {
        return -code error "map procedure does not exist: $mapproc"
    }
    if {![info pcexists -proc $reduceproc]} {
        return -code error "reduce procedure does not exist: $reduceproc"
    }
    # TODO: -ignore => ignore errors in map/reduce stages
    # TODO: separate map/reduce thread counts
    # TODO: noop map/reduce procs
    # TODO: -initscript
    # TODO: multi-stage pipeline

    # Setup the thread pool, if necessary
    set stageargs {}
    if {$params(threads) > 0} {
        # Generate the init script to define the map/reduce procs in the worker threads
        set initscript \
            "namespace eval [namespace qualifiers $mapproc] {}
             proc $mapproc {[info args $mapproc]} {[info body $mapproc]}
             namespace eval [namespace qualifiers $reduceproc] {}
             proc $reduceproc {[info args $reduceproc]} {[info body $reduceproc]}"
        # TODO: duplicate package requires
        # TODO: package require ttrace (in case people use it)
        # Create the thread pool
        set pool [::tpool::create -minworkers $params(threads) -maxworkers $params(threads) -initcmd $initscript]
        ::tpool::preserve $pool
        # Pass the thread pool name to the map-reduce stages
        lappend stageargs -pool $pool
    }

    try {
        # Map stage
        set results [::tmr::mrstage {*}$stageargs $data $mapproc]
        # Reduce stage
        set results [::tmr::mrstage {*}$stageargs -reduce $results $reduceproc]
    } finally {
        # Release the thread pool
        if {$params(threads) > 0} {
            ::tpool::release $pool
        }
    }

    return $results
}

#############################################################################
# Single map/reduce stage.
#
# Options:
#   -pool       pool of threads to use
#   -reduce     the stage is a reduce stage (only aggregates results,
#               does not generate new keys)
#
# Arguments:
#   data        input data as a dict
#   proc        procedure to run - must return a list of (key,value) pairs
#                                  (flat, 2-stride list)
#                                  or a result list if -reduce is given
#############################################################################
proc ::tmr::mrstage {args} {
    # Usage text and options
    set usage "::tmr::mrstage ?options? data proc"
    set options {
        {pool.arg   ""  "pool of threads to use"}
        {reduce         "the stage is a reduce stage (only aggregates results, does not generate new keys)"}
    }

    # Parse the options
    array set params [::cmdline::getoptions args $options "$usage\noptions:"]
    # Check the options
    if {$params(pool) ne "" && [lsearch -exact [::tpool::names] $params(pool)] eq -1} {
        return -code error "invalid pool: $params(pool)"
    }
    # Check the number of arguments
    if {[llength $args] != 2} {
        return -code error "wrong number of arguments: should be \"$usage\""
    }
    # Get the arguments
    lassign $args data proc
    # Check the arguments
    if {![string is dict $data]} {
        return -code error "data argument is not a dict"
    }
    if {![info pcexists -proc $proc]} {
        return -code error "procedure does not exist: $proc"
    }

    # Run the stage
    set results [dict create]
    if {$params(pool) eq ""} {
        # Process each input (key,value) pair in the main thread
        dict for {k1 v1} $data {
            # Run the stage in the current thread
            try {
                if {$params(reduce)} {
                    # Simply store the results for a reduce stage
                    dict set results $k1 [$proc $k1 $v1]
                } else {
                    # Store the new (key,value) pairs for a map stage
                    foreach {k2 v2} [$proc $k1 $v1] {
                        dict lappend results $k2 $v2
                    }
                }
            } on error {message options} {
                # Failed stage: bail out
                return -code error "error in stage $proc (key: $k1): $message"
            }
        }
    } else {
        # Reserve the thread pool
        ::tpool::preserve $params(pool)
        try {
            # Post jobs to process each input (key,value) pair
            set jobs [dict create]
            dict for {k1 v1} $data {
                # Store the job ID
                dict set jobs [::tpool::post $params(pool) [list $proc $k1 $v1]] [list $k1 $v1]
            }
            # Wait for all jobs to finish
            set pending [dict keys $jobs]
            while {[llength $pending]} {
                ::tpool::wait $params(pool) $pending pending
            }
            # Process the results of each job
            dict for {job args} $jobs {
                lassign $args k1 v1
                try {
                    if {$params(reduce)} {
                        # Simply store the results for a reduce stage
                        dict set results $k1 [::tpool::get $params(pool) $job]
                    } else {
                        # Store the new (key,value) pairs for a map stage
                        foreach {k2 v2} [::tpool::get $params(pool) $job] {
                            dict lappend results $k2 $v2
                        }
                    }
                } on error {message options} {
                    # Failed stage: bail out
                    return -code error "error in stage $proc (key: $k1): $message"
                }
            }
        } finally {
            # Release the thread pool
            ::tpool::release $params(pool)
        }
    }

    # Return the results
    return $results
}


################################ End of file ################################
