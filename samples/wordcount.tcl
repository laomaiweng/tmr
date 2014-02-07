#############################################################################
#   wordcount - Word-count example for TMR                                  #
#                                                                           #
# Author: quentin <quentin AT minster DOT io>                               #
#                                                                           #
# Basic Tcl implementation of the word-count map-reduce algorithm.          #
#                                                                           #
# Sample usage:                                                             #
#   # Split the input text in blocks of 100 lines                           #
#   set text [::wc::slab $text 100]                                         #
#   # Execute the word-count map-reduce algorithm on 4 threads              #
#   set words [::tmr::mapreduce -threads 4 $text ::wc::map ::wc::reduce]    #
#   # Sort the words from most to least occuring                            #
#   set words [lsort -integer -decreasing -stride 2 -index 1 $words]        #
#   # Dump the Top-5 words to stdout                                        #
#   foreach {w c} [lrange $words 0 9] {                                     #
#       puts "$w\t$c"                                                       #
#   }                                                                       #
#                                                                           #
# History:                                                                  #
# * v0.1    Initial version                                                 #
#############################################################################

package provide tmr-wc 0.1


# Package dependencies
package require tmr 0.1


# Define the wc namespace
namespace eval ::wc {}


#############################################################################
#############################################################################
#
# Procedures
#
#############################################################################
#############################################################################

#############################################################################
# Map stage for the word-count algorithm.
#
# Arguments:
#   name        text block name (unused)
#   text        text block
#
# Return:
#   flat, 2-stride list of ($word,1) pairs for each occurence of any word
#   in the input text
#############################################################################
proc ::wc::map {name text} {
    set res [list]
    # Split the words in the text
    foreach word [split $text] {
        # Remove non-alphabetic characters and empty strings
        set w [string tolower [regsub -all {[^[:alpha:]]} $word ""]]
        if {$w ne ""} {
            # Append the ($word,1) pair to the results list
            lappend res $w 1
        }
    }
    return $res
}

#############################################################################
# Reduce stage for the word-count algorithm.
#
# Arguments:
#   word        word from the initial input text
#   counts      occurence counts for this word
#
# Return:
#   aggregated number of occurences for the given word
#############################################################################
proc ::wc::reduce {word counts} {
    set res 0
    # Aggregate all counts (due to the current implementation of [::wc::map],
    # all items have value 1)
    foreach c $counts {
        incr res $c
    }
    return [list $res]
}

#############################################################################
# Slab a text into blocks that can be fed to the word-count map-reduce
# algorithm.
#
# Arguments:
#   text        input text
#   blocksize   size of resulting blocks (in number of items from the
#               delimiter-based split)
#   delimiter   delimiter for splitting the text before assembling blocks
#
# Return:
#   dictionary of blocks from the text
#############################################################################
proc ::wc::slab {text blocksize {delimiter \n}} {
    # Split the text
    set items [split $text $delimiter]
    # Assemble blocks
    set blocks [dict create]
    for {set i 0} {$i < [llength $items]} {incr i $blocksize} {
        dict set blocks $i [join [lrange $items $i [expr {$i+$blocksize-1}]]]
    }
    return $blocks
}


################################ End of file ################################
