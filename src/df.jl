using DataFrames

"""
General
"""
const INDEX_DF = :index
const AGG_DF = :bar
const C = Union{Symbol, AbstractString, Integer} # single column getindex() identifier types

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
agg(regate)
Aggregate over sequential subsets demarcated by true values.
Can be used to aggregate custom bars.
"""
function agg(df::AbstractDataFrame, set::BitVector; index::C=INDEX_DF, col::C=AGG_DF)
	df = copy(df; copycols=true)
	df[!, col] = cumsum(set)
	groupby(df, col)
end

"""
agg(regate)
Aggregate over subrange groups.
"""
function agg(df::AbstractDataFrame, r::StepRange; index::C=INDEX_DF, col::C=AGG_DF)
	agg(df, inr(df, r; index=index); index=index, col=col)
end

"""
Shift vector to next sth slot.
The end slots are filled with the last observation.
`s` must be a positive integer
"""
function lead!(vec::AbstractVector{T}, s::Integer) where T
	@assert s > 0
	append!(vec[begin+s:end], fill(vec[end], s))
end

"""
Shift vector to the previous sth slot.
The beginning slots are filled with the first observation.
`s` must be a negative integer
"""
function lag!(vec::AbstractVector{T}, s::Integer) where T
	@assert s < 0
	prepend!(vec[begin:end+s], fill(vec[begin], abs(s)))
end

"""
Shift vector.
Use first or last seen observation to fill adjacent slots that have been shifted off.
"""
function shift!(vec::AbstractVector{T}, s::Integer) where T
	if s > 0
		lead!(vec, s)
	elseif s < 0
		lag!(vec, s)
	end
end

"""
Return a random DataFrame indexed by `idx`.
"""
function randdf(idx::AbstractVector{T}, ncol::Integer=4; index::C=INDEX_DF, randfn=randn) where T
	val = randfn(size(idx)[1], ncol-1)
	hcat(DataFrame(index=>idx), DataFrame(val, :auto))
end

"""
Sample `n` rows of a DataFrame.
"""
@inline sampledf(df::AbstractDataFrame, n::Integer) = df[rand(1:nrow(df), n), :]

