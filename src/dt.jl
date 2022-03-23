using DataFrames, Dates
import DataFrames: groupby

"""
Time Series
"""
const DT_INDEX = :datetime
const DT_PERIOD = Dates.Hour(1)

@inline function DateTime(c::Dates.CompoundPeriod)
	p = Dates.periods(c)
	Dates.DateTime(p[1]) + sum(p[2:end])
end

"""
DateTime range
"""
@inline dtr(start::Dates.DateTime, stop::Dates.DateTime, τ::Dates.Period=DT_PERIOD) = start:τ:stop+τ

@inline dtr(df::AbstractDataFrame, τ::Dates.Period=DT_PERIOD; index::Symbol=DT_INDEX) = dtr(df[begin, index], df[end, index], τ)

"""
sub(set)
Select a DataFrame subinterval by start and stop points.
"""
@inline sub(df::AbstractDataFrame, start::Dates.DateTime, stop::Dates.DateTime; index::Symbol=DT_INDEX) = df[start .<= df[:, index] .< stop, :]

"""
sub(set)
Select a DataFrame subinterval by start and stop points.
"""
@inline sub(df::AbstractDataFrame, start, stop; index::Symbol=DT_INDEX) = sub(df, Dates.DateTime(start), Dates.DateTime(stop); index=index)

"""
sub(set)
Select DataFrame subrange by DateTime `index` column values in `τ` Period.

Simple boolean indexing (like df[minute.(df[:, index]) .== 0, :]) may be faster.
"""
@inline sub(df::AbstractDataFrame, τ::Dates.Period; index::Symbol=DT_INDEX) = sub(df, dtr(df, τ; index=index); index=index)

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
end

function shift(df::AbstractDataFrame, s::Integer; index::Symbol=DT_INDEX)
	shift!(copy(df), s; index=index)
end

"""
Group a DataFrame a constructor mapped to the index.
For example `groupby(df, Year)` groups into years.
"""
groupby(df::AbstractDataFrame, by::DataType; index=DT_INDEX) = groupby(df, [by]; index=index)

"""
Group a DataFrame using constructors mapped to the index.
For example `groupby(df, [Year, Quarter])` groups into year quarter combinations.
"""
function groupby(df::AbstractDataFrame, by::Vector{DataType}; index=DT_INDEX)
	gcols = ["$(b)($(index))" for b in by]
	for i in 1:length(by)
		df[:, gcols[i]] = by[i].(df[:, index])
	end
	groupby(df, gcols)
end

"""
Start the DataFrame from the start of the first full day to the end of the last full day, cleave off the rest.
"""
function cleave(df::AbstractDataFrame; index=DT_INDEX)
	dti = df[:, index]
	firstidx = findfirst(dt->Time(dt)==Time(0), dti)
	lastidx = findlast(dt->Time(dt)==Time(0), dti)
	firstday = df[firstidx, index]
	lastday = df[lastidx, index]
	sub(df, firstday, lastday)
end

"""
Return a random DataFrame indexed by a DateTime range.
"""
function randdf(start::Dates.DateTime, stop::Dates.DateTime, τ::Dates.Period=DT_PERIOD;
	ncol::Integer=4, index::Symbol=DT_INDEX, randfn=rand)
	randdf(collect(dtr(start, stop, τ)), ncol; index=index, randfn=randfn)
end

function randdf(offset::Dates.Period, τ::Dates.Period=DT_PERIOD;
	ncol::Integer=4, index::Symbol=DT_INDEX, randfn=rand)
	stop = now()
	randdf(stop-offset, stop, τ; ncol=ncol, index=index, randfn=randfn)
end

