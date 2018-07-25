#!/bin/bash -e

hadoop fs -ls $1
flag=$?
if [ $flag -ne 0 ] 
then
echo "Previous run of Edgs Step 1 perquisite is failed ! Please rerun the step 1 process first and then rerun the step 2"
exit 1
fi