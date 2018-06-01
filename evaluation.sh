#!/usr/bin/env bash

## Script information
# author: Florian Hucke, florian(dot)hucke(at)hhu(dot)de
# date: 02. Feb 2018
# version: 0.2
# license: GPL V3

if [ $# -ne 5 ]; then 
    echo "RUN: $0 HEAPSIZE INITCHUNKS THREADS OPERATIONS ROUNDS"
    exit 1
fi

GIT=$(which git)
[ $? -eq 1 ] && exit 1

CUR_BRANCH=$($GIT rev-parse --abbrev-ref HEAD)
MAIN_CLASS=de.hhu.bsinfo.DXMemoryEvaluation
START_EVAL="$PWD/start-dxram-memory.sh $MAIN_CLASS"

nodeID=0
heapSize=$1
initChunks=$2
initMin=16
initMax=48

threads=$3
operations=$4
rounds=$5

get_branch_names() {
    branches=$(${GIT} branch)
    printf "%s\n" "$branches"|sed -e "s#\*##g"
}

run_script() {
    $START_EVAL ${nodeID} ${heapSize} $1 ${initChunks} ${initMin} ${initMax} \
        $threads $operations $rounds
}

for branch in "master" "old_logic"; do
    ${GIT} checkout ${branch} &> /dev/null
    $PWD/build-dxram-memory.sh &> /dev/null
    [ $? -ne 0 ] && exit 2

    run_script $branch
done

${GIT} checkout ${CUR_BRANCH} &> /dev/null
