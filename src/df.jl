using DataFrames

"""
General
"""
const DEF_INDEX = :index

"""
inr(ange)
Decently fast StepRange boolean indexer.
"""
function inr(df::AbstractDataFrame, r::StepRange; index::Symbol=DEF_INDEX)
	∈(r).(df[:, index])
end

"""
sub(set)
Select DataFrame subset by boolean indexing.
"""
function sub(df::AbstractDataFrame, set::BitVector)
	df[set, :]
end

"""
sub(set)
Select DataFrame subrange (subset in range) by `index` column values in `r`.
"""
function sub(df::AbstractDataFrame, r::StepRange; index::Symbol=DEF_INDEX)
	sub(df, inr(df, r; index=index))
end

"""
agg(regate)
Aggregate over sequential subsets demarcated by true values.
Can be used to aggregate custom bars.
"""
function agg(df::AbstractDataFrame, set::BitVector; index::Symbol=DEF_INDEX)
	df = copy(df; copycols=true)
	df[!, :bar] = cumsum(set)
	groupby(df, :bar)
end

"""
agg(regate)
Aggregate over subrange groups.
"""
function agg(df::AbstractDataFrame, r::StepRange; index::Symbol=DEF_INDEX)
	agg(df, inr(df, r; index=index); index=index)
end

"""
Shift vector to next sth slot.
The end slots are filled with the last observation.
`s` must be a positive integer
"""
function lead!(vec::Vector{T}, s::Integer) where T
	push!(vec[begin+s:end], fill(vec[end], s))
end

"""
Shift vector to the previous -sth slot.
The beginning slots are filled with the first observation.
`s` must be a negative integer
"""
function lag!(vec::Vector{T}, s::Integer) where T
	insert!(vec[begin:end+s], fill(vec[begin], abs(s)))
end

"""
Shift vector.
Use first or last seen observation to fill adjacent slots that have been shifted off.
"""
function shift!(vec::Vector{T}, s::Integer) where T
	if s > 0
		lead!(vec, s)
	elseif s < 0
		lag!(vec, s)
	end
end

"""
Return a random DataFrame indexed by `idx`.
"""
function getdf_rand(idx::Vector{T}, ncol::Integer=4; index::Symbol=DEF_INDEX, randfn=rand) where T
	val = randfn(size(idx)[1], ncol-1)
	df = DataFrame(hcat(idx, val), :auto)
	rename!(df, 1=>index)
end
