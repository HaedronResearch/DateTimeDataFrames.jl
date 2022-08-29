# DateTimeDataFrames.jl

Simple utilities for working with DateTime indexed (time series) DataFrames.
Intended to be small and [suckless](https://suckless.org/).

Contains simple verbs for time series functionality including:
* `sub()`{set, range}
* `shift()`
* `groupby()` (extensions for time series)

## Notes
* Ranges follow Julia convention (inclusive at both ends).
* All operations work at arbitrary frequencies (time-based or "irregular").
* By default the index column is assumed to be called `:datetime`, setting the `index` keyword can change this behavior.
* The index column is assumed to be sorted in ascending order when relevant. Sort options for DataFrames calls are set off for performance.

## TODO
* unit tests

