using DataFrames
import DataFrames: groupby

"""
General
"""
const INDEX_DF = :index
const AGG_DF = :bar
const CN = Union{Symbol, AbstractString}   # valid column name types
const C = Union{CN, Integer}               # valid column getindex() identifier types

@inline Vector{T}(gd::GroupedDataFrame) where T<:AbstractDataFrame = [convert(T, g) for g in gd]

"""
inr(ange)
Decently fast StepRange boolean indexer.
"""
@inline inr(df::AbstractDataFrame, r::StepRange; index::C=INDEX_DF) = ∈(r).(df[:, index])

"""
in(range)
Alternate method, returns integer indices (https://dm13450.github.io/2021/04/21/Accidentally-Quadratic.html).
About the same speed as `inr`, although the difference on memory and speed may vary by the sparseness of the selection.
"""
@inline inr2(df::AbstractDataFrame, r::StepRange; index::C=INDEX_DF) = findall(∈(r), df[:, index])

"""
sub(set)
Select DataFrame subset by boolean indexing.
"""
@inline sub(df::AbstractDataFrame, set::BitVector) = df[set, :]

"""
sub(set)
Select DataFrame subrange (subset in range) by `index` column values in `r`.
"""
@inline sub(df::AbstractDataFrame, r::StepRange; index::C=INDEX_DF) = sub(df, inr(df, r; index=index))

"""
Group by sequential subsets demarcated by true values.
Can be used to create custom bars.
"""
function groupby(df::AbstractDataFrame, set::BitVector; index::C=INDEX_DF, col::CN=AGG_DF, sort::Union{Bool, Nothing}=false, view::Bool=false, skipmissing::Bool=false)
	df = copy(df; copycols=true)
	df[!, col] = cumsum(set)
	groupby(df, col; sort=sort, view=view, skipmissing=skipmissing)
end

"""
Group by subrange groups.
"""
@inline groupby(df::AbstractDataFrame, r::StepRange; index::C=INDEX_DF, col::CN=AGG_DF, sort::Union{Bool, Nothing}=false, view::Bool=false, skipmissing::Bool=false) = groupby(df, inr(df, r; index=index); index=index, col=col, sort=sort, view=view, skipmissing=skipmissing)

"""
Shift vector and use first/last seen observation to fill adjacent slots that have been shifted off.

If s > 0 -> shift vector to next sth slot, forward filling end slots.
If s < 0 -> shift vector to previous sth slot, back filling beginning slots.
"""
function shift!(vec::AbstractVector{T}, s::Integer) where T
	if s > 0
		append!(vec[begin+s:end], fill(vec[end], s))
	elseif s < 0
		prepend!(vec[begin:end+s], fill(vec[begin], abs(s)))
	end
end

"""
Return a random DataFrame indexed by `idx`.
"""
function randdf(idx::AbstractVector{T}, ncol::Integer=5; index::CN=INDEX_DF, randfn=randn) where T
	val = randfn(size(idx)[1], ncol-1)
	hcat(DataFrame(index=>idx), DataFrame(val, :auto))
end

"""
Sample `n` rows of a DataFrame.
"""
@inline sampledf(df::AbstractDataFrame, n::Integer) = df[rand(1:nrow(df), n), :]

