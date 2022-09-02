# DateTimeDataFrames.jl

Extends `DataFrames.jl` with basic time series utilities, for `DateTime` indexed DataFrames.
Intended to be small and [suckless](https://suckless.org/), exporting as few new keywords as possible.

Contains simple verbs/extensions for time series functionality including:
* `subset()`
* `shift()`
* `groupby()`

## Notes
* Ranges follow Julia convention (inclusive at both ends).
* All operations work at arbitrary frequencies (time-based or "irregular").
* By default the index column is assumed to be called `:datetime`, setting the `index` keyword can change this behavior.
* The index column is assumed to be sorted in ascending order when relevant. Sort options for DataFrames calls are set off for performance.

## TODO
* unit tests

