# DateTimeDataFrames.jl

Simple verbs for working with DateTime indexed (time series) DataFrames.

Contains simple verbs for time series functionality including:
* `sub()`{set, range}
* `agg()`regate
* `shift!()`
* `groupby()` (extensions)
* cleaning functions: `cleave()`

All operations work at arbitrary frequencies (time-based or "irregular").
The `DateTime` column is called `:datetime` by default, and is assumed to be sorted in ascending order.
