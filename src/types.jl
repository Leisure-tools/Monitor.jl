const NO_ID = -1
const PathComponent = Union{Number, Symbol, Function, Tuple{Symbol, Symbol}}
const JsonType = Union{JSON3.Object, JSON3.Array, String, Float64, Int64, Bool, Nothing}
const DEFAULT_UPDATE_PERIOD = 0.1

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
    fullname::String
    value::Any = nothing
    metadata::Dict{Symbol, AbstractString} = Dict()
    children::Dict{Symbol, Var} = Dict()
    properties::Dict{Symbol, Any} = Dict() # misc properties of any type
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

"""
    vars: variables keyed by their ID
    varnames: variables keyed by their names
    varfullnames: variables keyed by their full names
    changed: the variables which were changed during a refresh
"""
@kwdef mutable struct VarEnv{T}
    vars::Dict{Int, Var{T}} = Dict{Int, Var{T}}()
    varnames::Dict{Symbol, Var{T}} = Dict{String, Var{T}}()
    varfullnames::Dict{String, Var{T}} = Dict{String, Var{T}}()
    roots::Dict{Symbol, Any} = Dict{Symbol, Any}()
    oids::Dict{Int, WeakRef} = Dict{Int, WeakRef}()
    objoids::WeakKeyDict{Any, Int} = WeakKeyDict{Any, Int}()
    curoid::Int = 0
    curvid::Int = 0
    default_level = 1
    changed::Set{Int} = Set{Int}()
    errors::Dict{Int, Exception} = Dict{Int, Exception}()
    verbose_oids::Bool = false
    data::T
end

VarEnv(data::T) where {T} = VarEnv{T}(; data)

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
    env::Union{VarEnv, Nothing}
    var::Union{Var, Nothing}
    msg::AbstractString
    cause::Exception
    VarException(type, env, var, msg, cause=NoCause()) =
        new{type}(type, env, var, msg, cause)
    VarException(type, ctx::VarCtx, msg, cause=NoCause()) =
        new{type}(type, ctx.env, ctx.var, msg, cause)
end

@kwdef mutable struct MonitorData
    name::Symbol
    root::Var
    rootpath::String
    update::Float64
    quiet::Bool = false
    topics::Vector{String} = String[]
    update_topics::Vector{String} = String[]
    data::Dict{Symbol, Any}
    data_keys::Vector{Pair{Symbol, String}} = Pair{Symbol, String}[]
    vars::Dict{Symbol, Var} = Dict{Symbol, Var}()
    disabled::Bool = false
    original::JSON3.Object
end

@kwdef mutable struct ConStats
    incoming_updates::Int = 0
    incoming_update_attempts::Int = 0
    outgoing_updates::Int = 0
    refreshes::Int = 0
end

"""
Connection{DataType}

DataType is the type of the `data` field
"""
mutable struct Connection{DataType}
    name::String
    data::DataType
    com_channel::Channel{Function}
    refresh_channel::Channel{Function}
    running::Bool
    pending_update::Bool
    env::VarEnv{Connection{DataType}}
    monitors::Dict{Symbol, MonitorData}
    changed::Set{Var}
    outgoing::OrderedDict{Symbol, JsonType} # data to be sent out
    lastcheck::Float64
    default_update::Float64
    verbosity::Int64
    indicate_start::Bool
    data_blocks::Dict{Symbol, Any}
    incoming_blocks::OrderedDict{Symbol, Any}
    stats::ConStats
    accounting::Dict{Int64, Tuple{Float64, Channel, Function, Tuple}}
    accounting_channel::Channel{Function}
    channel_names::Dict{Channel, String}
    exec_num::Int64
    griped::Dict{Int64, Tuple{Float64, Channel, Function, Tuple}}
    use_accounting::Bool
    account::Set{Channel}
    update_input::Channel{Function}
    update_output::Channel{Function}
    Connection(::Type{T}) where {T} = new{T}()
end

function Connection(
    name::String,
    data::T;
    channel::Channel{Function}=Channel{Function}(10),
    refresh_channel::Channel{Function}=Channel{Function}(10),
    running::Bool=true,
    pending_update::Bool=false,
    monitors::Dict{Symbol, MonitorData}=Dict{Symbol, MonitorData}(),
    changed::Set{Var}=Set{Var}(),
    outgoing::OrderedDict{Symbol, JsonType}=OrderedDict{Symbol, JsonType}(), # data to be sent out
    lastcheck::Float64=0.0,
    default_update::Float64=DEFAULT_UPDATE_PERIOD,
    verbosity::Int64=0,
    indicate_start::Bool=true,
    data_blocks::Dict{Symbol, Any}=Dict{Symbol, Any}(),
    incoming_blocks::OrderedDict{Symbol, Any}=OrderedDict{Symbol, Any}(),
    use_accounting::Bool=false,
) where {T}
    local con = Connection(T)
    con.name = name
    con.data = data
    con.com_channel = channel
    con.refresh_channel = refresh_channel
    con.running = running
    con.pending_update = pending_update
    con.monitors = monitors
    con.changed = changed
    con.outgoing = outgoing
    con.lastcheck = lastcheck
    con.default_update = default_update
    con.verbosity = verbosity
    con.indicate_start = indicate_start
    con.data_blocks = data_blocks
    con.incoming_blocks = incoming_blocks
    con.env = VarEnv{Connection{T}}(; data=con)
    con.stats = ConStats()
    con.accounting = Dict{Int64, Tuple{Float64, Channel, Function}}()
    con.accounting_channel = Channel{Function}(10)
    con.channel_names = Dict{Channel, String}()
    con.exec_num = 0
    con.griped = Dict{Int64, Tuple{Float64, Channel, Function}}()
    con.use_accounting = use_accounting
    con.account = Set{Channel}()
    con.update_input = Channel{Function}(10)
    con.update_output = Channel{Function}(10)
    con
end

const current_connection = ScopedValue{Union{Nothing, Connection}}(nothing)
const current_runner = ScopedValue{Union{Nothing, Channel}}(nothing)
const current_runner_name = ScopedValue{Union{Nothing, String}}(nothing)
const LONG_TIME = 10
