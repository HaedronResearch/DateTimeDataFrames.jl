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
@inline dtr(dt₀::Dates.DateTime, dt₁::Dates.DateTime, τ::Dates.Period) = dt₀:τ:dt₁

@inline dtr(df::AbstractDataFrame, τ::Dates.Period; index::C=INDEX_DT) = dtr(df[begin, index], df[end, index], τ)

"""
sub(set)
Select a DataFrame subinterval by time type.
"""
@inline sub(df::AbstractDataFrame, tt::Dates.TimeType; index::C=INDEX_DT, after::Bool=true) = after ? df[tt .<= df[:, index], :] : df[tt .>= df[:, index], :]

"""
sub(set)
Select a DataFrame subinterval within [tt₀, tt₁].
"""
@inline sub(df::AbstractDataFrame, tt₀::Dates.TimeType, tt₁::Dates.TimeType; index::C=INDEX_DT) = df[tt₀ .<= df[:, index] .<= tt₁, :]

"""
sub(set)
Select DataFrame subintervals by start and stop time within aggregation period `τ`.
"""
@inline sub(df::AbstractDataFrame, t₀::Dates.Time, t₁::Dates.Time, τ::Dates.Period=Day(1); index::C=INDEX_DT, col::CN=AGG_DT) = subset(
	groupby(df, τ; index=index, col=col),
	index => dt -> t₀ .<= Time.(dt) .<= t₁,
	ungroup=true)

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
Group by time period `τ` of the `index` column.
"""
function groupby(df::AbstractDataFrame, τ::Dates.Period; index::C=INDEX_DT, col::CN=AGG_DT, sort::Union{Bool, Nothing}=false, skipmissing::Bool=false)
	df = copy(df; copycols=true)
	df[!, col] = floor.(df[!, index], τ)
	groupby(df, col; sort=sort, skipmissing=skipmissing)
end

"""
Group by a constructor mapped to the `index` column.
For example `groupby(df, Year)` groups into years.
"""
groupby(df::AbstractDataFrame, by::DataType; index::C=INDEX_DT, sort::Union{Bool, Nothing}=false, skipmissing::Bool=false) = groupby(df, [by]; index=index, sort=sort, skipmissing=skipmissing)

"""
Group by a DataFrame using constructors mapped to the `index` column.
For example `groupby(df, [Year, Quarter])` groups into year quarter combinations.
"""
function groupby(df::AbstractDataFrame, by::Vector{DataType}; index::C=INDEX_DT, sort::Union{Bool, Nothing}=false, skipmissing::Bool=false)
	gcols = ["$(b)($(index))" for b in by]
	for i in 1:length(by)
		df[:, gcols[i]] = by[i].(df[:, index])
	end
	groupby(df, gcols, sort=sort, skipmissing=skipmissing)
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

# """
# Start the DataFrame from the first day with time `t₀` to the end of the last day with time `t₁`, cleave off the rest.
# XXX Deprecated
# """
# function cleave(df::AbstractDataFrame, t₀::Time, t₁::Time; index::C=INDEX_DT)
# 	dti = df[!, index]
# 	day₀ = df[findfirst(dt->Time(dt)>=t₀, dti), index]
# 	day₁ = df[findlast(dt->Time(dt)<=t₁, dti), index]
# 	sub(df, day₀, day₁)
# end

# """
# Cleave from the first and last day with time `t`.
# XXX Deprecated
# """
# @inline cleave(df::AbstractDataFrame, t::Time=Time(0); index::C=INDEX_DT) = cleave(df, t, t; index=index)

"""
Return a random DataFrame indexed by a DateTime range.
"""
function randdf(tt₀::Dates.TimeType, tt₁::Dates.TimeType, τ::Dates.Period;
	ncol::Integer=5, index::CN=INDEX_DT, randfn=randn)
	randdf(collect(dtr(tt₀, tt₁, τ)), ncol; index=index, randfn=randfn)
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

