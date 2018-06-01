#!/usr/bin/env bash

## Script information
# author: Florian Hucke, florian(dot)hucke(at)hhu(dot)de
# date: 02. Feb 2018
# version: 0.5
# license: GPL V3

LIB_DIR=$PWD/lib
BIN_DIR=$PWD/bin
CONF_DIR=$PWD/config

BINARY=${BIN_DIR}/dxram-memory-develop.jar

if [ $# -lt 1 ]; then
    echo RUN: $0 MAINCLASS [ARGUMENTS]
fi

if [ ! -e ${BINARY} ]; then
    echo "No jar found at $BINARY"
    echo "Build DXRAM-Memory"
    $PWD/build-dxram-memory.sh
    BINARY=${BIN_DIR}/dxram-memory-develop.jar
fi

create_lib_path() {
    TMP=""
    for i in $@; do
        if [ -z ${TMP} ]; then
            TMP=${LIB_DIR}/${i}
        else
            TMP=${TMP}:${LIB_DIR}/${i}
        fi
    done

    echo ${TMP}
}

LIBS=$(create_lib_path \
            slf4j-api-1.6.1.jar \
            gson-2.7.jar \
            log4j-api-2.7.jar \
            log4j-core-2.7.jar \
            jline-2.15.jar \
            perf-timer.jar)

CLASSPATH=${LIBS}:${BINARY}

java -ea\
    -Dlog4j.configurationFile=${CONF_DIR}/log4j.xml\
    -Ddxram.config=${CONF_DIR}/dxram.json\
    -cp ${CLASSPATH} $@ #> /dev/null 2>&1
