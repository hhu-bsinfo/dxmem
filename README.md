# DXMem: Memory allocator and management for billions of very small objects

[![Build Status master](https://travis-ci.org/hhu-bsinfo/dxmem.svg?branch=master)](https://travis-ci.org/hhu-bsinfo/dxmem)
[![Build Status development](https://travis-ci.org/hhu-bsinfo/dxmem.svg?branch=development)](https://travis-ci.org/hhu-bsinfo/dxmem)

DXMem implements a custom memory allocator and stores allocated data outside of the Java heap using Unsafe. The
allocator is optimized for very small objects (< 64 byte) with low per object metadata overhead. Objects stored are
referred to as "chunks". Chunks are identified using a unique chunk ID which is translated by the memory management
to a corresponding address when getting/putting data.

DXMem was developed for the distributed key-value storage [DXRAM](https://github.com/hhu-bsinfo/dxram) but can also be
used as a library by other projects.

# How to Build and Run

## Requirements
DXMem requires Java 1.8 to run on a Linux distribution of your choice (MacOSX might work as well but is not supported,
officially).

## Building
The script build.sh bootstraps our build system which is using gradle to build DXMem. The build output is located in
build/dist either as directory (dxmem) or zip-package (dxmem.zip).

## DXMemMain: Benchmark, Development and Debugging Tool
The dxmem jar-file contains a built in benchmark similar to the Yahoo! Cloud Service Benchmark (YCSB) to evaluate the
performance of DXMem. Furthermore it includes tools for development and debugging.

Deploy the build output to your machine/cluster and run DXMem by executing the script *dxmem* in the *bin* subfolder:
```
./bin/dxmem
```

### Benchmark
To run the benchmark tool, simply run dxmem with:
```
./bin/dxmem benchmark
```

The benchmark tool includes a set of predefined benchmarks which test typical data access patterns, e.g. the YCSB
workload A with 50% get and 50% put operations:
```
/bin/dxmem benchmark 128-mb ycsb-a false false 1 1000 1 10000
```

Further parameters determine the heap size used, check data integrity (expensive operation!), the number of threads to
use or objects to load. Please refer to the help yielded by the tools when omitting the required parameters.

### Analyzer
Development tool to analyze a heap dumped to a file for errors. This is used for development when allocator errors
occur and need to be analyzed.

### Debugger
The debugger tool is an interactive shell which allows you to test or debug DXMem. All operations available by DXMem
are available as terminal commands. Please refer to the instructions given by the debugger tool.

Sandbox example:
Create a new heap with 128 MB size:
```
> new 0 128-mb
```

Create a 64 byte chunk:
```
> create 64
```

Write data to the chunk created (CID 0):
```
> put 0 0 str test
```

Get the data from the chunk:
```
> get 0 str
```

Delete the chunk:
```
remove 0
```

# License

Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf,
Institute of Computer Science, Department Operating Systems. 
Licensed under the [GNU General Public License](LICENSE.md).
