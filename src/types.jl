const NO_ID = -1
const PathComponent = Union{Number,Symbol,Function}
const JsonType = Union{JSON3.Object,JSON3.Array,String,Float64,Int64,Bool,Nothing}

"""
    Var

A variable:
    id: the variable's unique ID, used in the protocol
    name: the variable's human-readable name, used in UIs and diagnostics
"""
@kwdef mutable struct Var{T}
    parent::Int = NO_ID
    id::Int = NO_ID
    name::Symbol
    value::Any = nothing
    metadata::Dict{Symbol,AbstractString} = Dict()
    children::Dict{Symbol,Var} = Dict()
    properties::Dict{Symbol,Any} = Dict() # misc properties of any type
    active = true # monitors refreshing
    internal_value = nothing
    readable::Bool = true
    writeable::Bool = true
    action::Bool = false
    path::Vector{PathComponent} = []
    json_value = nothing
    ref = false
    refresh_exception = nothing
    error_count = 0
    level = 1
    value_conversion = identity
end

@kwdef mutable struct VarEnv{T}
    vars::Dict{Int,Var{T}} = Dict{Int,Var{T}}()
    roots::Dict{Symbol,Any} = Dict{Symbol,Any}()
    oids::Dict{Int,WeakRef} = Dict{Int,WeakRef}()
    objoids::WeakKeyDict{Any,Int} = WeakKeyDict{Any,Int}()
    curoid::Int = 0
    curvid::Int = 0
    default_level = 1
    changed::Set{Int} = Set{Int}()
    errors::Dict{Int,Exception} = Dict{Int,Exception}()
    verbose_oids::Bool = false
    data::Union{Nothing,T} = nothing
end

struct VarCtx{T}
    env::VarEnv{T}
    var::Var{T}
end

struct NoCause <: Exception end

"""
    VarException

Error while interacting with a variable

- type: Symbol for the type of exception:
  - path: error using a path
  - not_writeable: variable is not writeable
  - not_readable: variable is not readable
  - refresh: error while refreshing
  - program: error in program
- cmd: the command that caused the problem
- msg: description of the problem
- cause: cause of the problem (if any)
"""
struct VarException{T} <: Exception
    type::Symbol
    env::Union{VarEnv,Nothing}
    var::Union{Var,Nothing}
    msg::AbstractString
    cause::Exception
    VarException(type, env, var, msg, cause = NoCause()) =
        new{type}(type, env, var, msg, cause)
    VarException(type, ctx::VarCtx, msg, cause = NoCause()) =
        new{type}(type, ctx.env, ctx.var, msg, cause)
end

@kwdef mutable struct MonitorData
    name::Symbol
    rootpath::String
    data::Dict{Symbol,Any}
    data_keys::Vector{Pair{Symbol,Symbol}} = Pair{Symbol,Symbol}[]
    vars::Dict{Symbol,Var} = Dict{Symbol,Var}()
    update::Float64
    root::Var
    disabled::Bool = false
end

"""
Connection{DataType}

DataType is the type of the `data` field
"""
@kwdef mutable struct Connection{DataType}
    data::DataType
    channel::Channel{Function} = Channel{Function}()
    running::Bool = true
    pending_update::Bool = false
    env::VarEnv{Connection{DataType}} = VarEnv{Connection{DataType}}()
    monitors::Dict{Symbol,MonitorData} = Dict{Symbol,MonitorData}()
    changed::Set{Var} = Set{Var}()
    outgoing::Dict{Symbol,JsonType} = Dict{Symbol,JsonType}() # data to be sent out
    lastcheck::Float64 = 0
    default_update::Float64 = 0.25
    verbosity::Int64 = 0
    indicate_start::Bool = true
end

function Connection(data::T; kw...) where {T}
    local con = Connection{T}(; data, kw...)
    con.env.data = con
    con
end

const CURRENT_CONNECTION = ScopedValue{Union{Nothing,Connection}}(nothing)
