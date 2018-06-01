#!/usr/bin/env bash

## Script information
# author: Florian Hucke, florian(dot)hucke(at)hhu(dot)de
# date: 02. Feb 2018
# version: 0.1
# license: GPL V3

#ant variables
ANTPATH=""
ANTBUILDFILE="$PWD/scripts/ant/dxram-memory-build.xml"
ARGUMENTS="-f $ANTBUILDFILE"


# Check if the given executeable file is a ant version
check_if_ant()
{
    OUTPUT=$($1 -version 2>&1)
    echo $OUTPUT | grep -E "^Apache Ant" 2>&1 > /dev/null
    if [ $? -eq 0 ]; then
        echo $1
    else
        echo ""
    fi
}

# Get the path of a ant executeable file
get_path()
{
    if [ $# -gt 0 ] && [ -x $1 ]; then
        CHECKPATH=$1
    else
        CHECKPATH=$(which ant 2> /dev/null)
    fi

    echo $(check_if_ant $CHECKPATH)
}

#read the arguments
for i in $@
do
    if [ ${i,,} = debug ]; then
        ARGUMENTS="-debug $ARGUMENTS"
    elif [ ${i,,} = verbose ]; then
        ARGUMENTS="-verbose $ARGUMENTS"
    elif [ ${i,,} = diagnostics ]; then
        ARGUMENTS="-diagnostics $ARGUMENTS"
    elif [ -x $i ]; then
        ANTPATH=$i
    fi
done

ANTPATH=$(get_path $ANTPATH)

if [ $ANTPATH ]; then
    $ANTPATH $ARGUMENTS
    echo $?
else
    echo "ant not found install it or run"
    echo "$0 /path/to/ant_binary"
    echo
    exit 1
fi

