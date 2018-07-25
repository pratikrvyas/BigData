#!/bin/bash
INPUTDIR=$1;
hadoop fs -test -e ${INPUTDIR}
testFilesExist=$?
if  [ "$testFilesExist" -ne 0 ]; then
        echo ISPATHEXIST=FALSE
else
                echo ISPATHEXIST=TRUE
fi
