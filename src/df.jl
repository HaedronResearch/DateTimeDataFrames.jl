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
Decently fast set boolean indexer.
"""
@inline inr(df::AbstractDataFrame, r::Union{StepRange, AbstractVector}; index::C=INDEX_DF) = ∈(r).(df[:, index])

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
Select DataFrame subset.
"""
@inline subset(df::AbstractDataFrame, set::Union{BitVector, UnitRange}) = df[set, :]

"""
$(TYPEDSIGNATURES)
sub(set)
Select DataFrame subset by row or row number.
TODO change input to Pair{Union{DataFrameRow, Integer}, Union{DataFrameRow, Integer}}
"""
function subset(df::AbstractDataFrame, r₀::Union{DataFrameRow, Integer}, r₁::Union{DataFrameRow, Integer})
	i₀ = r₀ isa DataFrameRow ? rownumber(r₀) : r₀
	i₁ = r₁ isa DataFrameRow ? rownumber(r₁) : r₁
	subset(df, i₀:i₁)
end

"""
$(TYPEDSIGNATURES)
sub(range)
Select DataFrame subrange by `index` column values in `r`.
"""
@inline subset(df::AbstractDataFrame, r::StepRange; index::C=INDEX_DF) = subset(df, inr(df, r; index=index))

# """
# $(TYPEDSIGNATURES)
# Get subset of df rows where df[!, key[1]] ∈ key[2]. Both vectors must be sorted.
# The typical way to do this can be very slow if key[2] is large (O(m*n)).
# This method takes advantage of both the column and key vectors being sorted to enable
# a single pass through the column.. TODO
# """
# function subset(df::AbstractDataFrame, key::Pair{Symbol, AbstractVector{T}}) where T
# 	col = df[!, key[1]]
# 	indices = []
# 	i = 1
# 	for r in eachrow(col)
# 	end
# end

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
Forward fill / forward coalesce values
"""
function ffill(xₜ::AbstractVector; default=missing)
	prev = default
	for (i, xᵢ) in enumerate(xₜ)
		prev = xₜ[i] = coalesce(xᵢ, prev)
	end
	xₜ
end

"""
$(TYPEDSIGNATURES)
Back fill / backward coalesce values
"""
function bfill(xₜ::AbstractVector; default=missing)
	reverse(ffill(reverse(xₜ); default=default))
end

"""
$(TYPEDSIGNATURES)
Return a random `idx` indexed DataFrame of `randmat(length(idx))` data.
By default returns four data columns of unit Normal data.
"""
function randdf(idx::AbstractVector{T}, randmat::Function=l->randn(l, 4); index::CN=INDEX_DF, names=:auto) where T
	hcat(DataFrame(index=>idx), DataFrame(randmat(length(idx)), names))
end

"""
$(TYPEDSIGNATURES)
Sample `n` rows of a DataFrame.
"""
@inline sampledf(df::AbstractDataFrame, n::Integer) = df[rand(1:nrow(df), n), :]

