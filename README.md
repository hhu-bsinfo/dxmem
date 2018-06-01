# DXRAM-Memory
**DXRAM** is a distributed in-memory key-value store developed by the operating systems group of the department of computer science of the Heinrich-Heine-University Düsseldorf.

*DXRAM-Memory* is about the development and optimization for the memory management of DXRAM. So this repository consists of all the classes which are needed to build a standalone *Java Archive* of the DXRAM memory management.

For more information about the DXRAM project check the [project site](https://dxram.io) or the [GitHub site](https://github.com/hhu-bsinfo/dxram).

## Building DXRAM-Memory
To build the project you need [Apache Ant(TM)](http://ant.apache.org/).

There is a **BASH** script to build *DXRAM-Memory*. To build simply run
```
bash build-dxram-memory.sh
```
You can find the script in the project root. 

## Starting DXRAM-Memory
*DXRAM-Memory* requires *JAVA 8*. Furthermore it depends on a few extra libaries, which can be found in the `lib/` folder.

To start *DXRAM-Memory* testing util you have to run the **BASH** script, with:
```
bash start-dxram-memory.sh de.hhu.bsinfo.DXMemoryTesting 
```
Like the build script you can find this script in the root folder of the project.

If you want to run a evaluation use the evaluation **BASH** script. Run
something like:
```
nohup ./evaluation.sh $((2**34)) 1000 4 $((10**7)) 5 > ~/${PWD##*/}.output &
```
This evaluation allocates 16GiB RAM and create 1000 inital chunks.  run 10
million operations with 4 threads. and do it 5 round. 

Use nohup because this evaluation maybe take a few hours.

This script will build and run the evaluation on master and the old\_logic branch.

The result are saved as csv files in the ~/eval folder.

