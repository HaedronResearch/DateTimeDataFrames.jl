"""
General
"""
const INDEX_DF = :index
const AGG_DF = :bar

"""
$(TYPEDSIGNATURES)
"""
@inline Vector{T}(gd::GroupedDataFrame) where T<:AbstractDataFrame = [convert(T, g) for g in gd]

"""
$(TYPEDSIGNATURES)
inr(ange)
Decently fast StepRange boolean indexer.
"""
@inline inr(df::AbstractDataFrame, r::StepRange; index::C=INDEX_DF) = ∈(r).(df[:, index])

"""
$(TYPEDSIGNATURES)
in(range)
Alternate method, returns integer indices (https://dm13450.github.io/2021/04/21/Accidentally-Quadratic.html).
About the same speed as `inr`, although the difference on memory and speed may vary by the sparseness of the selection.
"""
@inline inr2(df::AbstractDataFrame, r::StepRange; index::C=INDEX_DF) = findall(∈(r), df[:, index])

"""
$(TYPEDSIGNATURES)
sub(set)
Select DataFrame subset by boolean indexing.
"""
@inline subset(df::AbstractDataFrame, set::BitVector) = df[set, :]

"""
$(TYPEDSIGNATURES)
sub(range)
Select DataFrame subrange (subset in range) by `index` column values in `r`.
"""
@inline subset(df::AbstractDataFrame, r::StepRange; index::C=INDEX_DF) = subset(df, inr(df, r; index=index))

"""
$(TYPEDSIGNATURES)
Group by sequential subsets demarcated by true values.
Can be used to create custom bars.
"""
function groupby(df::AbstractDataFrame, set::BitVector; index::C=INDEX_DF, col::CN=AGG_DF, sort::Union{Bool, Nothing}=false, skipmissing::Bool=false)
	df = copy(df; copycols=true)
	df[!, col] = cumsum(set)
	groupby(df, col; sort=sort, skipmissing=skipmissing)
end

"""
$(TYPEDSIGNATURES)
Group by subrange groups.
"""
@inline groupby(df::AbstractDataFrame, r::StepRange; index::C=INDEX_DF, col::CN=AGG_DF, sort::Union{Bool, Nothing}=false, skipmissing::Bool=false) = groupby(df, inr(df, r; index=index); index=index, col=col, sort=sort, skipmissing=skipmissing)

"""
$(TYPEDSIGNATURES)
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
$(TYPEDSIGNATURES)
Return a random DataFrame indexed by `idx`.
"""
function randdf(idx::AbstractVector{T}, ncol::Integer=5; index::CN=INDEX_DF, randfn=randn) where T
	val = randfn(size(idx)[1], ncol-1)
	hcat(DataFrame(index=>idx), DataFrame(val, :auto))
end

"""
$(TYPEDSIGNATURES)
Sample `n` rows of a DataFrame.
"""
@inline sampledf(df::AbstractDataFrame, n::Integer) = df[rand(1:nrow(df), n), :]

