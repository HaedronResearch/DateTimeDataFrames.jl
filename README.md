# DateTimeDataFrames.jl
This package is no longer being actively developed.

## Purpose
Extends [`DataFrames.jl`](https://github.com/JuliaData/DataFrames.jl) with basic time series utilities, for `DateTime` indexed `DataFrame`s.
Intended to be small, intuitive, and [suckless](https://suckless.org/philosophy).

## Install
Install this package to your Julia project environment as you would any other package from a Git repo.

From the Julia REPL:
```
julia> ]
(MyProject) pkg> add https://github.com/HaedronResearch/DateTimeDataFrames.jl
```

## Overview
Contains simple extensions for time series functionality including:
* `subset()`
* `shift()`
* `groupby()`

### Notes
* Ranges follow Julia convention (inclusive at both ends).
* All operations work at arbitrary frequencies (time-based or "irregular").
* By default the index column is assumed to be called `:datetime`, setting the `index` keyword can change this behavior.
* The index column is assumed to be sorted in ascending order when relevant. Sort options for `DataFrames.jl` calls are set off for performance.

## TODO
* unit tests
* docs
* examples
