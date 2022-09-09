module DateTimeDataFrames

using Dates
using DataFrames
import DataFrames: first, last, subset, groupby
using DocStringExtensions

const CN = Union{Symbol, AbstractString}   # valid column name types
const C = Union{CN, Integer}               # valid column getindex() identifier types

export shift, shift!, lastunique, sampledf, randdf
export first, last, subset, groupby

include("df.jl")
include("dt.jl")

end
