# DateTimeDataFrames.jl

Simple verbs for working with DateTime indexed (time series) DataFrames.
Intended to be small and [suckless](https://suckless.org/).

Contains simple verbs for time series functionality including:
* `sub()`{set, range}
* `agg()`regate
* `shift!()`
* `groupby()` (extensions)
* cleaning functions: `cleave()`

## Notes
* Ranges follow Julia convention (inclusive at both ends).
* All operations work at arbitrary frequencies (time-based or "irregular").
* By default the index column is called `:datetime`.
* The index column is assumed to be sorted in ascending order.

