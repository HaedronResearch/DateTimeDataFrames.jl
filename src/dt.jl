"""
Time Series
"""
const INDEX_DT = :datetime
const AGG_DT = :bar

"""
$(TYPEDSIGNATURES)
"""
@inline function DateTime(c::Dates.CompoundPeriod)
	p = Dates.periods(c)
	Dates.DateTime(p[1]) + sum(p[2:end])
end

"""
$(TYPEDSIGNATURES)
DateTime range
"""
@inline dtr(dt₀::Dates.AbstractDateTime, dt₁::Dates.AbstractDateTime, τ::Dates.Period) = dt₀:τ:dt₁

"""
$(TYPEDSIGNATURES)
"""
@inline dtr(df::AbstractDataFrame, τ::Dates.Period; index::C=INDEX_DT) = dtr(df[begin, index], df[end, index], τ)

"""
$(TYPEDSIGNATURES)
sub(interval)
Select a DataFrame subinterval by time type.
"""
@inline subset(df::AbstractDataFrame, tt::Dates.TimeType; index::C=INDEX_DT, after::Bool=true) = after ? df[tt .<= df[:, index], :] : df[tt .>= df[:, index], :]

"""
$(TYPEDSIGNATURES)
sub(interval)
Select a DataFrame subinterval [`tt₀`, `tt₁`].
"""
@inline subset(df::AbstractDataFrame, tt₀::Dates.TimeType, tt₁::Dates.TimeType; index::C=INDEX_DT) = df[tt₀ .<= df[:, index] .<= tt₁, :]

"""
$(TYPEDSIGNATURES)
sub(interval)
Select DataFrame subintervals in [`t₀`, `t₁`], within all `τ`aggregation periods.
"""
function subset(df::AbstractDataFrame, t₀::Dates.Time, t₁::Dates.Time, τ::Dates.Period; index::C=INDEX_DT, col::CN=AGG_DT, skipmissing::Bool=false, view::Bool=false, ungroup::Bool=true)
	select!(
		subset(groupby(df, τ; index=index, col=col),
			index => dt -> t₀ .<= Time.(dt) .<= t₁,
			skipmissing=skipmissing, view=view, ungroup=ungroup),
		Not(col)
	)
end

"""
$(TYPEDSIGNATURES)
sub(range)
Select DataFrame subrange by DateTime `index` column values in `τ` Period.
Simple boolean indexing (like df[minute.(df[:, index]) .== 0, :]) may be faster.
"""
@inline subset(df::AbstractDataFrame, τ::Dates.Period; index::C=INDEX_DT) = subset(df, dtr(df, τ; index=index); index=index)

"""
$(TYPEDSIGNATURES)
Group by time period `τ` of the `index` column.
"""
function groupby(df::AbstractDataFrame, τ::Dates.Period; index::C=INDEX_DT, col::CN=AGG_DT, sort::Union{Bool, Nothing}=false, skipmissing::Bool=false)
	df = copy(df; copycols=true)
	df[!, col] = floor.(df[!, index], τ)
	groupby(df, col; sort=sort, skipmissing=skipmissing)
end

"""
$(TYPEDSIGNATURES)
Group by a constructor mapped to the `index` column.
For example `groupby(df, Year)` groups into years.
"""
groupby(df::AbstractDataFrame, by::DataType; index::C=INDEX_DT, sort::Union{Bool, Nothing}=false, skipmissing::Bool=false) = groupby(df, [by]; index=index, sort=sort, skipmissing=skipmissing)

"""
$(TYPEDSIGNATURES)
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
$(TYPEDSIGNATURES)
Shift DataFrame in place by moving index up or down `abs(s)` steps.
"""
function shift!(df::AbstractDataFrame, s::Integer; index::C=INDEX_DT)
	df[!, index] = shift!(df[:, index], s)
	if s > 0
		df[begin:end-s, :]
	elseif s < 0
		df[begin+abs(s):end, :]
	end
end

"""
$(TYPEDSIGNATURES)
Shift DataFrame by moving index up or down `abs(s)` steps.
"""
@inline shift(df::AbstractDataFrame, s::Integer; index::C=INDEX_DT) = shift!(copy(df), s; index=index)

"""
$(TYPEDSIGNATURES)
Return a random DataFrame indexed by a DateTime range.
"""
function randdf(tt₀::Dates.TimeType, tt₁::Dates.TimeType, τ::Dates.Period;
	ncol::Integer=5, index::CN=INDEX_DT, randfn=randn)
	randdf(collect(dtr(tt₀, tt₁, τ)), ncol; index=index, randfn=randfn)
end

"""
$(TYPEDSIGNATURES)
Return a random DataFrame indexed by a DateTime range; `offset` sets the lookback window from now.
Zero arg method provided for convenience.
"""
function randdf(offset::Dates.Period=Month(1), τ::Dates.Period=Hour(1);
	ncol::Integer=5, index::CN=INDEX_DT, randfn=randn)
	stop = now()
	randdf(stop-offset, stop, τ; ncol=ncol, index=index, randfn=randfn)
end

