# DateTimeDataFrames.jl

Extends `DataFrames.jl` with basic time series utilities, for `DateTime` indexed `DataFrame`s.
Intended to be small, intuitive, and [suckless](https://suckless.org/philosophy/`). This package is a work in progress.

Contains simple extensions for time series functionality including:
* `subset()`
* `shift()`
* `groupby()`

## Notes
* Ranges follow Julia convention (inclusive at both ends).
* All operations work at arbitrary frequencies (time-based or "irregular").
* By default the index column is assumed to be called `:datetime`, setting the `index` keyword can change this behavior.
* The index column is assumed to be sorted in ascending order when relevant. Sort options for `DataFrames.jl` calls are set off for performance.

## TODO before ver 1.**
* unit tests
* docs

