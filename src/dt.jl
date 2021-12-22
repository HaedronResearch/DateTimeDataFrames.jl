using DataFrames, Dates

"""
Time Series
"""
const DT_INDEX = :datetime

"""
DateTime range
"""
function dtr(start::DateTime, stop::DateTime, s::Dates.Period=Dates.Hour(1))
	start:s:stop+s
end

function dtr(df::AbstractDataFrame, s::Dates.Period=Dates.Hour(1);
	index::Symbol=DT_INDEX)
	dtr(df[begin, index], df[end, index], s)
end

"""
sub(set)
Select a DataFrame subinterval by start and stop points.
"""
function sub(df::AbstractDataFrame, start, stop;
	index::Symbol=DT_INDEX)
	df[DateTime(start) .< df[:, index] .< DateTime(stop), :]
end

"""
sub(set)
Select DataFrame subrange by DateTime `index` column values in `s` Period.

Simple boolean indexing (like df[minute.(df[:, index]) .== 0, :]) may be faster.
"""
function sub(df::AbstractDataFrame, s::Dates.Period=Dates.Hour(1);
	index::Symbol=DT_INDEX)
	sub(df, dtr(df, s; index=index); index=index)
end

"""
agg(regate)
Aggregate over subrange groups by DateTime `index` column values in `s` Period.
Can be used to aggregate abritrary time bars.
"""
function agg(df::AbstractDataFrame, s::Dates.Period=Dates.Hour(1);
	index::Symbol=DT_INDEX)
	agg(df, dtr(df, s; index=index); index=index)
end

"""
Shift DataFrame by moving index up or down.
Use first or last seen observation to fill adjacent slots that have been shifted off.
"""
function shift!(df::AbstractDataFrame, s::Integer; index::Symbol=DT_INDEX)
	if s > 0
		df[!, index] = lead!(df[:, index], s)
		df[begin:end+s, :]
	elseif s < 0
		df[!, index] = lag!(df[:, index], s)
		df[begin+abs(s):end, :]
	end
end

"""
Return a random DataFrame indexed by a DateTime range.
"""
function getdf_rand(start::DateTime, stop::DateTime, s::Dates.Period=Dates.Hour(1);
	ncol::Integer=4, index::Symbol=DT_INDEX, randfn=rand)
	getdf_rand(collect(dtr(start, stop, s)), ncol; index=index, randfn=randfn)
end
