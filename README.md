# adbf_rs
![build status](https://travis-ci.org/NattapongSiri/adbf_rs.svg?branch=master) ![test coverage](https://codecov.io/gh/NattapongSiri/adbf_rs/branch/master/graph/badge.svg)
## WIP
The long term goal of this project is to provide an ability to read/write dbf file in asynchronous fashion using Rust.

adbf is an acronym for async dbf lib.

## TODO
1. Read FoxPro/VFP DBF file
1. Basic query using callback closure
1. Join multiple tables using closure
1. TBD
1. Write FoxPro/VFP dbf file
1. Write all updated FoxPro/VFP dbf file
1. TBD
1. Support more DBF file type such as DBase3+

## Why making this ?
Because in Python, there's [DBF](https://pypi.org/project/dbf/) 
library that's very powerful.
In Rust, there's not even FoxPro compatible DBF library.
There's dbase crate. However, it cannot read FoxPro table.
I need to access FoxPro DBF so either I've to use Python 
or I need to do this.

Why not enhance dbase crate ? It's because the crate name is 
dbase. I think it'll just make some confusion or I may forget
that why would I need this dbase crate in the first place.
