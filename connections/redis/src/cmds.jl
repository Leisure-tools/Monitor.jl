using Redis: @redisfunction, RedisConnection

# see https://redis.io/docs/latest/commands/xadd/
# see https://github.com/JuliaDatabases/Redis.jl/blob/master/src/commands.jl

millis(seconds=time()) = round(Int, seconds * 1000)

"""
XADD key [NOMKSTREAM] [<MAXLEN | MINID> [= | ~] threshold
  [LIMIT count]] <* | id> field value [field value ...]
"""
function xadd(
    con::RedisConnection,
    key::AbstractString,
    id,
    field_values...;
    nomkstream=false,
    maxlen=nothing,
    minid=nothing,
    limit=nothing,
    appx=true,
)
    local args = [key]
    nomkstream && push!(args, "nomkstream")
    if !isnothing(maxlen) || !isnothing(minid)
        if !isnothing(maxlen)
            push!(args, "maxlen", (appx ? "~" : "="), maxlen)
        elseif ~isnothing(minid)
            push!(args, "minid", (appx ? "~" : "="), minid)
        end
        opt(args; limit)
    end
    _xadd(con, args..., id, field_values...)
end

"""
XRANGE key start end [COUNT count]

Returns:
  [
    id => (; key = value ...)
    ...
  ]
"""

xrange(con::RedisConnection, key::AbstractString, start, finish; count=nothing) =
    _xrange(con, key, start, finish, opt(; count)...)

"""
XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id
  [id ...]

Returns:
```
  Dict(
    STREAM => [
      id => (; key = value ...)
      ...
    ]
    ...
  )
```
"""
function xread(con::RedisConnection, streams; count=nothing, block=nothing)
    local args = opt(; count)

    if block isa Number
        block = millis(block)
    end
    opt(args; block)
    push!(args, "streams")
    values = []
    for (k, v) in streams
        push!(args, k)
        push!(values, v)
    end
    push!(args, values...)
    #verbose(3, con, "XREAD ", join(args, " "))
    res = _xread(con, args...)
    #verbose(3, con, "XREAD: ", res)
    return res
end

"""
XTRIM key <MAXLEN | MINID> [= | ~] threshold [LIMIT count]
"""
function xtrim(con::RedisConnection, key; maxlen=nothing, minid=nothing, limit=nothing, appx=true)
    local args = [key]
    if !isnothing(maxlen)
        push!(args, "maxlen", (appx ? "~" : "="), maxlen)
    elseif ~isnothing(minid)
        push!(args, "minid", (appx ? "~" : "="), minid)
    end
    opt(args; limit)
    return _xtrim(con, args...)
end

"""
Trim items before the given time in seconds
"""
trim_before(con::RedisConnection, key::AbstractString, time::Float64; appx=true) =
    xtrim(con, key; minid="$(millis(time))-0", appx)

function opt(result::Vector=[]; kw...)
    for (k, v) in kw
        !isnothing(v) && push!(result, string(k), v)
    end
    return result
end

@redisfunction "_xadd" Union{Nothing, AbstractString} key args...;

Redis.convert_response(::Type{Any}, response) = response


@redisfunction "_xread" Any key args...;
#@redisfunction "_xread" Union{Nothing,Vector{Pair{String,Vector}}} key args...;
#@redisfunction "_xread" Union{Nothing,Vector{Pair{String,Vector{Pair{String,NamedTuple}}}}} key args...;
#@redisfunction "_xread" Union{Nothing,Dict{String,Vector{Pair{String,NamedTuple}}}} key args...;
#@redisfunction "_xread" Union{Nothing,Dict{String,Vector}} key args...;

@redisfunction "_xrange" Union{Nothing, Vector} key args...;
#@redisfunction "_xrange" Union{Nothing,Vector{Pair{String,Vector}}} key args...;
#@redisfunction "_xrange" Union{Nothing,Vector{Pair{String,NamedTuple}}} key args...;
#@redisfunction "_xrange" Union{Nothing,Vector} key args...;

@redisfunction "_xtrim" Int key args...;

Redis.convert_response(::Type{T}, v::U) where {T, U <: T} = v

Redis.convert_response(::Type{Union{Nothing, Vector{Pair{A, B}}}}, v::Vector) where {A, B} =
    Redis.convert_response(Vector{Pair{A, B}}, v)

Redis.convert_response(::Type{Union{Nothing, Dict{A, B}}}, v::Vector) where {A, B} =
    Redis.convert_response(Dict{A, B}, v)

function Redis.convert_response(::Type{Vector{Pair{T, U}}}, v::Vector) where {T, U}
    local result = Pair{T, U}[]
    for (a, b) in v
        push!(result, Redis.convert_response(T, a) => Redis.convert_response(U, b))
    end
    return result
end

function Redis.convert_response(::Type{NamedTuple}, v::Vector{String})
    return (; (Symbol(v[i]) => v[i + 1] for i = 1:2:length(v))...)
end

function Redis.convert_response(::Type{Dict{String, T}}, v::Vector) where {T}
    return Dict{String, T}(key => Redis.convert_response(T, value) for (key, value) in v)
end
