#!/bin/bash

PATH_DATE=`echo $1 | rev | cut -d '/' -f1 | rev`
echo PATH_DATE=$PATH_DATE
