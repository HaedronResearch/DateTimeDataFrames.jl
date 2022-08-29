using DataFrames, Dates
import DataFrames: groupby

"""
Time Series
"""
const INDEX_DT = :datetime
const AGG_DT = AGG_DF

@inline function DateTime(c::Dates.CompoundPeriod)
	p = Dates.periods(c)
	Dates.DateTime(p[1]) + sum(p[2:end])
end

"""
DateTime range
"""
@inline dtr(start::Dates.DateTime, stop::Dates.DateTime, τ::Dates.Period) = start:τ:stop

@inline dtr(df::AbstractDataFrame, τ::Dates.Period; index::C=INDEX_DT) = dtr(df[begin, index], df[end, index], τ)

"""
sub(set)
Select a DataFrame subinterval by start point.
"""
@inline sub(df::AbstractDataFrame, start::Dates.DateTime; index::C=INDEX_DT) = df[start .<= df[:, index], :]

"""
sub(set)
Select a DataFrame subinterval by start and stop points.
"""
@inline sub(df::AbstractDataFrame, start::Dates.DateTime, stop::Dates.DateTime; index::C=INDEX_DT) = df[start .<= df[:, index] .<= stop, :]

"""
sub(set)
Select a DataFrame subinterval by start and stop points.
"""
@inline sub(df::AbstractDataFrame, start, stop; index::C=INDEX_DT) = sub(df, Dates.DateTime(start), Dates.DateTime(stop); index=index)

"""
sub(set)
Select DataFrame subrange by DateTime `index` column values in `τ` Period.

Simple boolean indexing (like df[minute.(df[:, index]) .== 0, :]) may be faster.
"""
@inline sub(df::AbstractDataFrame, τ::Dates.Period; index::C=INDEX_DT) = sub(df, dtr(df, τ; index=index); index=index)

"""
agg(regate)
Aggregate over subrange groups by DateTime `index` column values in `τ` Period.
Can be used to aggregate abritrary time bars.
"""
function agg(df::AbstractDataFrame, τ::Dates.Period; index::C=INDEX_DT, col::CN=AGG_DT)
	df = copy(df; copycols=true)
	df[!, col] = floor.(df[!, index], τ)
	groupby(df, col)
end

"""
Shift DataFrame by moving index up or down `abs(s)` steps.
"""
function shift!(df::AbstractDataFrame, s::Integer; index::C=INDEX_DT)
	df[!, index] = shift!(df[:, index], s)
	if s > 0
		df[begin:end-s, :]
	elseif s < 0
		df[begin+abs(s):end, :]
	end
end

@inline shift(df::AbstractDataFrame, s::Integer; index::C=INDEX_DT) = shift!(copy(df), s; index=index)

"""
Group a DataFrame a constructor mapped to the index.
For example `groupby(df, Year)` groups into years.
"""
groupby(df::AbstractDataFrame, by::DataType; index::C=INDEX_DT) = groupby(df, [by]; index=index)

"""
Group by a DataFrame using constructors mapped to the index.
For example `groupby(df, [Year, Quarter])` groups into year quarter combinations.
"""
function groupby(df::AbstractDataFrame, by::Vector{DataType}; index::C=INDEX_DT)
	gcols = ["$(b)($(index))" for b in by]
	for i in 1:length(by)
		df[:, gcols[i]] = by[i].(df[:, index])
	end
	groupby(df, gcols)
end

"""
Start the DataFrame from the first day with time `t₀` to the end of the last day with time `t₁`, cleave off the rest.
"""
function cleave(df::AbstractDataFrame, t₀::Time, t₁::Time; index::C=INDEX_DT)
	dti = df[!, index]
	day₀ = df[findfirst(dt->Time(dt)>=t₀, dti), index]
	day₁ = df[findlast(dt->Time(dt)<=t₁, dti), index]
	sub(df, day₀, day₁)
end

"""
Cleave from the first and last day with time `t`.
"""
@inline cleave(df::AbstractDataFrame, t::Time=Time(0); index::C=INDEX_DT) = cleave(df, t, t; index=index)

"""
Return a random DataFrame indexed by a DateTime range.
"""
function randdf(start::Dates.DateTime, stop::Dates.DateTime, τ::Dates.Period;
	ncol::Integer=5, index::CN=INDEX_DT, randfn=randn)
	randdf(collect(dtr(start, stop, τ)), ncol; index=index, randfn=randfn)
end

"""
Return a random DataFrame indexed by a DateTime range; `offset` sets the lookback window from now.
Zero arg method provided for convenience.
"""
function randdf(offset::Dates.Period=Month(1), τ::Dates.Period=Hour(1);
	ncol::Integer=5, index::CN=INDEX_DT, randfn=randn)
	stop = now()
	randdf(stop-offset, stop, τ; ncol=ncol, index=index, randfn=randfn)
end

