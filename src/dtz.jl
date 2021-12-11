
using DataFrames, Dates, TimeZones

"""
Time Series (Zoned)
"""
const DTZ_INDEX = :datetimez

"""
ZonedDateTime range
"""
function dtzr(start::ZonedDateTime, stop::ZonedDateTime, s::Dates.Period=Dates.Hour(1))
	start:s:stop+s
end

function dtzr(start::DateTime, stop::DateTime, s::Dates.Period=Dates.Hour(1); tz=tz"UTC")
	dtzr(ZonedDateTime(start, tz), ZonedDateTime(stop, tz), s)
end

function dtzr(df::AbstractDataFrame, s::Dates.Period=Dates.Hour(1); index::Symbol=DTZ_INDEX)
	dtzr(df[begin, index], df[end, index], s)
end

"""
sub(set)
Select a DataFrame subinterval by start and stop points.
The start and stop arguments can be any type that can be converted to a ZonedDateTime (UInt, Dates.Date, etc)
"""
function sub(df::AbstractDataFrame, start, stop;
	index::Symbol=DTZ_INDEX, tz=tz"UTC")
	df[ZonedDateTime(start, tz) .< df[:, index] .< ZonedDateTime(stop, tz), :]
end

"""
sub(set)
Select DataFrame subrange by ZonedDateTime `index` column values in `s` Period.

Simple boolean indexing (like df[minute.(df[:, index]) .== 0, :]) may be faster.
"""
function sub(df::AbstractDataFrame, s::Dates.Period=Dates.Hour(1);
	index::Symbol=DTZ_INDEX)
	sub(df, dtzr(df, s; index=index); index=index)
end

"""
agg(regate)
Aggregate over subrange groups by ZonedDateTime `index` column values in `s` Period.
Can be used to aggregate abritrary time bars.
"""
function agg(df::AbstractDataFrame, s::Dates.Period=Dates.Hour(1);
	index::Symbol=DTZ_INDEX)
	agg(df, dtzr(df, s; index=index); index=index)
end

"""
Return a random DataFrame indexed by a ZonedDateTime range.
"""
function getdf_rand(start::DateTime, stop::DateTime, step::Dates.Period=Dates.Hour(1);
	ncol::Integer=4, tz=tz"UTC", index::Symbol=DTZ_INDEX, randfn=rand)
	dtzRange = collect(dtzr(start, stop, step; tz=tz))
	getdf_rand(dtzRange, ncol; index=index, randfn=randfn)
end
