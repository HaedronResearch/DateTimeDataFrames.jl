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
First row of the given TimeType.
"""
@inline first(df::AbstractDataFrame, tt::T; index::C=INDEX_DT) where T<:Dates.TimeType = df[findfirst(dt->T(dt)==tt, df[:, index]), :]

"""
$(TYPEDSIGNATURES)
Last row of the given TimeType.
"""
@inline last(df::AbstractDataFrame, tt::T; index::C=INDEX_DT) where T<:Dates.TimeType = df[findlast(dt->T(dt)==tt, df[:, index]), :]

"""
$(TYPEDSIGNATURES)
sub(interval)
Select a DataFrame subinterval by time type condition.
The `op` argument must be one of (`:∈`, :∉, `:<`, `:≤`, `:≥`, `:>`).
"""
function subset(df::AbstractDataFrame, op::Symbol, tt::Dates.TimeType; index::C=INDEX_DT)
	r₀, r₁ = 1, nrow(df)
	if op == :∈
		r₀ = first(df, tt; index=index)
		r₁ = last(df, tt; index=index)
	elseif op == ∉
		throw("unimplemented")
	elseif op == :<
		r₁ = first(df, tt; index=index)
	elseif op == :≤
		r₁ = last(df, tt; index=index)
	elseif op == :≥
		r₀ = first(df, tt; index=index)
	elseif op == :>
		r₀ = last(df, tt; index=index)
	end
	subset(df, r₀, r₁)
end

"""
$(TYPEDSIGNATURES)
sub(interval)
Select a DataFrame subinterval.
"""
@inline subset(df::AbstractDataFrame, interval::Pair{TimeType, TimeType}; index::C=INDEX_DT) = subset(df, first(df, interval[1]; index=index), last(df, interval[2]; index=index))

"""
$(TYPEDSIGNATURES)
sub(interval)
Select DataFrame subintervals within all `τ`aggregation periods.
"""
function subset(df::AbstractDataFrame, τ::Period, interval::Pair{Time, Time}; index::C=INDEX_DT, col::CN=AGG_DT, skipmissing::Bool=false, view::Bool=false, ungroup::Bool=true)
	select!(
		subset(groupby(df, τ; index=index, col=col),
			index => dt -> interval[1] .<= Time.(dt) .<= interval[2],
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
@inline subset(df::AbstractDataFrame, τ::Period; index::C=INDEX_DT) = subset(df, df[1, index]: τ:df[end, index]; index=index)

"""
$(TYPEDSIGNATURES)
Group by time period `τ` of the `index` column.
"""
function groupby(df::AbstractDataFrame, τ::Period; index::C=INDEX_DT, col::CN=AGG_DT, sort::Union{Bool, Nothing}=false, skipmissing::Bool=false)
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
Last unique row.
# TODO refactor with DataFrames.nonunique
"""
function lastunique(df::AbstractDataFrame; index::C=INDEX_DT)
	start = last(select(df, Not(index)))
	values = select(df[1:end-1, :], Not(index))
	for row in reverse(eachrow(values))
		if row != start
			return df[rownumber(row)+1, :]
		end
	end
	df[1, :] # they're all the same
end

# """
# $(TYPEDSIGNATURES)
# Repeat last row up to t₁ as a DataFrame
# """
# function repeatlast(df::AbstractDataFrame, τ::Period, t₁::Time; index::C=INDEX_DT)
# 	dᵢ, tᵢ = Date(df[end, index]), Time(df[end, index])
# 	if (lenᵢ₁ = Int((t₁ - tᵢ)/τ)) > 0
# 		rl = repeat(df[[end], :]; inner=lenᵢ₁)
# 		rl[:, index] = Dates.DateTime.(dᵢ, tᵢ+τ:τ:t₁)
# 		rl
# 	else
# 		nothing
# 	end
# end

# """
# $(TYPEDSIGNATURES)
# Repeat last row up to t₁ as a DataFrame
# """
# function repeatlast(df::AbstractDataFrame, τ::Period, tt₁::T; index::C=INDEX_DT) where T<:TimeType
# 	ttᵢ = T(df[end, index])
# 	if (lenᵢ₁ = Int((tt₁ - ttᵢ)/τ)) > 0
# 		rl = repeat(df[[end], :]; inner=lenᵢ₁)
# 		rl[:, index] = Dates.DateTime.(tᵢ+τ:τ:t₁)
# 		rl
# 	else
# 		nothing
# 	end
# end

"""
$(TYPEDSIGNATURES)
Expand index.
"""
@inline function expandindex(df::AbstractDataFrame, idx::AbstractVector{T}; index::C=INDEX_DT) where T<:TimeType
	nrow(df) < size(idx, 1) ? sort!(outerjoin(DataFrame(index=>idx), df; on=index), index) : df
end

"""
$(TYPEDSIGNATURES)
Expand within index to sampling period `τ`.
"""
function expandindex(df::AbstractDataFrame, τ::Period; index::C=INDEX_DT)
	idx = df[1, index]:τ:df[end, index]
	expandindex(df, idx; index=index)
end

"""
$(TYPEDSIGNATURES)
Expand index.
"""
function expandindex(df::AbstractDataFrame, τ::Period, interval::Pair{Time, Time}; index::C=INDEX_DT)
	d₁, d₂ = Date(df[1, index]), Date(df[end, index])
	idx = Dates.DateTime(d₁, interval[1]):τ:Dates.DateTime(d₂, interval[2])
	expandindex(df, idx; index=index)
end

"""
$(TYPEDSIGNATURES)
Expand index.
"""
function expandindex(df::AbstractDataFrame, τ::Period, interval::Pair{TimeType, TimeType}; index::C=INDEX_DT)
	expandindex(df, interval[1]:τ:interval[2]; index=index)
end

"""
$(TYPEDSIGNATURES)
Forward fill / forward coalesce values
"""
function ffill(df::AbstractDataFrame; index::C=INDEX_DT)
	select(df, index, Not(index).=>ffill, renamecols=false)
end

"""
$(TYPEDSIGNATURES)
Forward fill / forward coalesce values
"""
function ffill!(df::AbstractDataFrame; index::C=INDEX_DT)
	select!(df, index, Not(index).=>ffill, renamecols=false)
end

"""
$(TYPEDSIGNATURES)
Expand index and ffill non-index Missing values.
"""
function expand(df::AbstractDataFrame, idx::AbstractVector{T}; index::C=INDEX_DT) where T<:TimeType
	ffill!(expandindex(df, idx; index=index); index=index)
end

"""
$(TYPEDSIGNATURES)
Expand within index by sampling period `τ` and ffill non-index Missing values.
"""
function expand(df::AbstractDataFrame, τ::Period; index::C=INDEX_DT)
	ffill!(expandindex(df, τ; index=index); index=index)
end

"""
$(TYPEDSIGNATURES)
Expand index and ffill non-index Missing values.
"""
function expand(df::AbstractDataFrame, τ::Period, interval::Pair{TimeType, TimeType}; index::C=INDEX_DT)
	ffill!(expandindex(df, τ, interval; index=index); index=index)
end

"""
$(TYPEDSIGNATURES)
Return a Matrix of simulated OHLC prices, sampled from random walk with constant drift.
This isn't meant to be a realistic model for prices, just for quick testing.
"""
function randohlc(l::Integer; resolution::Integer=10, p₀::Real=100.)
	n = l*resolution
	x = fill(1/(n-1), n-1) .+ randn(n-1)
	insert!(x, 1, p₀)
	p = cumsum(x)
	ohlc = [[w[1] maximum(w) minimum(w) w[end]] for  w in Iterators.partition(p, resolution)]
	reduce(vcat, ohlc)
end

"""
$(TYPEDSIGNATURES)
Return a random TimeType indexed DataFrame.
By default returns random OHLC data.
"""
function randdf(idx::AbstractVector{T}, randmat::Function=randohlc; index::CN=INDEX_DT, names=[:open, :high, :low, :close]) where T<:Union{TimeType, Period}
	hcat(DataFrame(index=>idx), DataFrame(randmat(length(idx)), names))
end

"""
$(TYPEDSIGNATURES)
Return a random TimeType indexed DataFrame.
By default returns random OHLC data.
Zero arg method provided for convenience.
"""
function randdf(offset::Period=Month(1), τ::Period=Hour(1), randmat::Function=randohlc; index::CN=INDEX_DT, names=[:open, :high, :low, :close])
	stop = now()
	randdf(stop-offset:τ:stop, randmat; index=index, names=names)
end

