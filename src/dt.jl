using DataFrames, Dates

"""
Time Series
"""
const DT_INDEX = :datetime
const DT_PERIOD = Dates.Hour(1)

"""
DateTime range
"""
function dtr(start::DateTime, stop::DateTime, τ::Dates.Period=DT_PERIOD)
	start:τ:stop+τ
end

function dtr(df::AbstractDataFrame, τ::Dates.Period=DT_PERIOD;
	index::Symbol=DT_INDEX)
	dtr(df[begin, index], df[end, index], τ)
end

"""
sub(set)
Select a DataFrame subinterval by start and stop points.
"""
function sub(df::AbstractDataFrame, start, stop; index::Symbol=DT_INDEX)
	df[DateTime(start) .<= df[:, index] .< DateTime(stop), :]
end

"""
sub(set)
Select DataFrame subrange by DateTime `index` column values in `τ` Period.

Simple boolean indexing (like df[minute.(df[:, index]) .== 0, :]) may be faster.
"""
function sub(df::AbstractDataFrame, τ::Dates.Period; index::Symbol=DT_INDEX)
	sub(df, dtr(df, τ; index=index); index=index)
end

"""
agg(regate)
Aggregate over subrange groups by DateTime `index` column values in `τ` Period.
Can be used to aggregate abritrary time bars.
"""
function agg(df::AbstractDataFrame, τ::Dates.Period; index::Symbol=DT_INDEX, col::Symbol=AGG_COL)
	df = copy(df; copycols=true)
	df[!, col] = floor.(df[!, index], τ)
	groupby(df, col)
end

"""
Shift DataFrame by moving index up or down `abs(s)` steps.
Use first or last seen observation to fill adjacent slots that have been shifted off.
"""
function shift!(df::AbstractDataFrame, s::Integer; index::Symbol=DT_INDEX)
	if s > 0
		df[!, index] = lead!(df[:, index], s)
		df[begin:end-s, :]
	elseif s < 0
		df[!, index] = lag!(df[:, index], s)
		df[begin+abs(s):end, :]
	end
	df
end

shift(df::AbstractDataFrame, s::Integer; index::Symbol=DT_INDEX) = shift!(copy(df), s; index=index)

"""
Return a random DataFrame indexed by a DateTime range.
"""
function getdf_rand(start::DateTime, stop::DateTime, τ::Dates.Period=DT_PERIOD;
	ncol::Integer=4, index::Symbol=DT_INDEX, randfn=rand)
	getdf_rand(collect(dtr(start, stop, τ)), ncol; index=index, randfn=randfn)
end
