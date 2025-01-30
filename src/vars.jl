using JSON3: JSON3, StructTypes

const REF = r"^\?([0-9]+)$"
const LOCAL_ID = r"^@/([0-9]+)$"
const UNKNOWN = "?"
#const PATH_COMPONENT = r"^([-[:alnum:]]+|@)/([0-9]+)$"
const VAR_NAME =
    r"^([0-9]+|\pL\p{Xan}*)(?:\?((\pL\p{Xan}*)(?:=((?:[^,]|\\,)*))?(?:,(\pL\p{Xan}*)(?:=((?:[^,]|\\,)*))?)*))?$"
const METAPROP = r"(\pL\p{Xan}*)(?:=((?:[^,]|\\,)*))?(,|$)"
const ARRAY_INDEX = r"[1-9][0-9]*"

const PATH_COMPONENT =
    r"(@?[\p{LC}\p{Lm}\p{Lo}\p{Nl}\p{No}\p{Sc}\p{So}_][\p{LC}\p{Lm}\p{Lo}\p{Nl}\p{No}\p{Sc}\p{So}_!\p{Nd}\p{No}\p{Mn}\p{Mc}\p{Me}\p{Sk}\p{Pc}]*|\[[1-9][0-9]*\]|\(\)|\.)\s*"

const JPATH_COMPONENT =
    r"(@?[\p{LC}\p{Lm}\p{Lo}\p{Nl}\p{Sc}\p{So}_][\p{LC}\p{Lm}\p{Lo}\p{Nl}\p{Sc}\p{So}_!\p{Nd}\p{No}\p{Mn}\p{Mc}\p{Me}\p{Sk}\p{Pc}]*|[1-9][0-9]*)(?:((?:\.[\p{LC}\p{Lm}\p{Lo}\p{Nl}\p{Sc}\p{So}_][\p{LC}\p{Lm}\p{Lo}\p{Nl}\p{Sc}\p{So}_!\p{Nd}\p{No}\p{Mn}\p{Mc}\p{Me}\p{Sk}\p{Pc}]*)*)(?:\(\))?)?"

verbosity(::VarEnv) = 0

verbose(::VarEnv) = verbosity(env) > 0

Base.show(io::IO, var::Var) =
    print(io, "Var($(metaname(var)), $(var.parent)->$(var.id), $(var.path))")

Base.show(io::IO, e::VarException) = print(io, e.msg)

function metaname(var::Var)
    isempty(var.metadata) && return var.name
    return "$(var.name)?$(join(("$k=$v" for (k,v) in var.metadata), "&"))"
end

function oid(env::VarEnv, obj)
    get!(env.objoids, obj) do
        oid = env.curoid += 1
        env.oids[oid] = WeakRef(obj)
        oid
    end
end

ref(env::VarEnv, obj) =
    env.verbose_oids ? (; ref = oid(env, obj), repr = repr(obj)) : (; ref = oid(env, obj))

function walk(env::VarEnv, data, level)
    if data isa AbstractString || data isa Symbol
        return string(data)
    elseif data isa Number || data isa Bool || isnothing(data)
        return data
    elseif ismissing(data)
        return nothing
    elseif ismutable(data)
        return ref(env, data)
    elseif data isa AbstractArray || data isa AbstractSet
        return map(d -> walk(env, d, level + 1), data)
    elseif data isa AbstractDict
        local isobj = true
        for (k, _) in data
            if !(k isa Symbol || k isa AbstractString)
                isobj = false
                break
            end
        end
        return if isobj
            Dict(string(k) => walk(env, v, level + 1) for (k, v) in data)
        else
            [[k, walk(env, v, level + 1)] for (k, v) in data]
        end
    end
    # "object"
    local result = Dict()
    function add(i, name, type, value; kw...)
        result[name] = walk(env, value, level + 1)
    end
    StructTypes.foreachfield(add, data)
    return result
end

isref(json) = json isa NamedTuple && haskey(json, :ref)

function hasrefs(json)
    isref(json) && return true
    json isa AbstractDict && return refsin(values(json))
    (json isa AbstractSet || json isa AbstractArray) && return refsin(json)
    return false
end

function refsin(iter)
    for i in iter
        hasrefs(i) && return true
    end
    return false
end

function deref(env::VarEnv, obj)
    hasrefs(obj) && return basic_deref(env, obj)
    return obj
end

function basic_deref(env::VarEnv, obj)
    isref(obj) && return get(env.oids, obj.ref, nothing)
    obj isa AbstractArray && return [basic_deref(env, v) for v in obj]
    obj isa AbstractDict && return [k => basic_deref(env, v) for (k, v) in obj]
    return obj
end

function parsemetadata(meta::AbstractString, original_meta = Dict{Symbol,String}())
    metadata = Dict{Symbol,String}(original_meta)
    while meta !== ""
        (m = match(METAPROP, meta)) === nothing && throw("Bad metaproperty format: $(meta)")
        metadata[Symbol(m[1])] = m[2] === nothing ? "" : string(m[2])
        length(meta) == length(m.match) && break
        meta[length(m.match)] != ',' && throw("Bad metaproperty format: $(meta)")
        meta = meta[length(m.match)+1:end]
    end
    @debug("USING METADATA: $(repr(metadata))")
    metadata
end

function parsepath(pathstr::AbstractString)
    pathstr = strip(pathstr)
    local path = PathComponent[]
    local matches = [eachmatch(JPATH_COMPONENT, pathstr)...]
    #println("MATCHES: ", matches)
    isempty(matches) && return path
    local parent = String[]
    local start = 1
    while matches[1].match == "."
        push!(parent, ".")
        start += 1
    end
    local cur = 1
    if !isempty(parent)
        push!(path, Symbol(join(parent)))
        cur += length(parent)
    end
    local rest = pathstr
    for m in @views matches[start:end]
        local frag = @views(rest[1:length(m.match)])
        frag != m.match && error("Bad match, '$frag' != '$(m.match)'")
        rest = strip(@views rest[length(m.match)+1:end])
        if length(frag) > 2 && frag[end-1:end] == "()"
            local modpath = m[1] * m[2]
            try
                # this eval finds the function with name modpath
                #cmd.config.approval(:find_function, nothing, modpath)
                push!(path, Main.eval(Meta.parse(modpath)))
            catch e
                check_sigint(e)
                rethrow(
                    VarException(
                        :path,
                        nothing,
                        nothing,
                        "Bad path element: '$modpath' in path '$pathstr'",
                    ),
                )
            end
        elseif !isnothing(match(ARRAY_INDEX, frag))
            push!(path, parse(Int, frag))
        else
            push!(path, Symbol(m.match))
        end
    end
    return path
end

"""
    Create a variable. Name can be suffixed with "?" and a metadata string.

To customize, override this with a type specialization.

potential metadata: create, access, path
"""
Var(
    env::VarEnv,
    name::Union{Symbol,AbstractString} = "";
    id::Int = NO_ID,
    level = 0,
    value = nothing,
    kw...,
) = basicvar(env, id, name; level, value, kw...)

"Basic variable creation"
function basicvar(
    env::VarEnv{T},
    id::Int = NO_ID,
    name::Union{Symbol,AbstractString} = "";
    level = 0,
    value = nothing,
    json_value = nothing,
    metadata::Dict{Symbol,String} = Dict{Symbol,String}(),
    kw...,
) where {T}
    args = (; value, kw...)
    if !isnothing(value) && !haskey(args, :internal_value)
        args = (; args..., internal_value = args.value)
    end
    if string(name) != ""
        m = match(VAR_NAME, string(name))
        if m !== nothing && m[2] !== nothing
            name = m[1]
            metadata = parsemetadata(m[2], metadata)
            #println("METADATA: ", metadata)
            if haskey(metadata, :path) && !haskey(args, :path)
                args = (; args..., path = parsepath(metadata[:path]))
            end
            if level == 0 && haskey(metadata, :level)
                level = parse(Int, metadata[:level])
            end
            args = (; args..., metadata, name)
        end
    end
    if level == 0
        level = env.default_level
    end
    args = (; args..., level)
    if name isa AbstractString
        name = Symbol(name)
    end
    if id == NO_ID
        id = env.curvid += 1
    end
    v = Var{T}(; args..., id, name = name isa Number ? Symbol("") : name)
    !isnothing(v.internal_value) && use_value(env, v, v.internal_value)
    v.metadata[:type] = typename(v.internal_value)
    if isnothing(json_value)
        v.json_value = json_value
    end
    env.vars[id] = v
    push!(env.changed, v.id)
    if v.parent !== NO_ID && name != Symbol("")
        env.vars[v.parent].children[name] = v
        @debug("ADDED CHILD OF $(v.parent) NAMED $(name) = $v")
    end
    !isempty(v.metadata) && @debug("VAR $(v.name) metadata: $(v.metadata)")
    return v
end

function remove(env::VarEnv, var::Var)
    if var.parent != NO_ID
        local parent = env.vars[var.parent]

        delete!(parent.children, var.name)
    end
    delete!(env.vars, var.id)
end

typename(value) = typename(typeof(value))

typename(type::Type) = "$(Base.parentmodule(type)).$(split(string(type), ".")[end])"

set_metadata(ctx::VarCtx, name::Symbol, value) = ctx.var.metadata[name] = value

function set_type(ctx::VarCtx)
    local var = ctx.var
    name = typename(var.internal_value)
    if name != get(var.metadata, :type, "")
        set_metadata(ctx, :type, name)
    end
end

dicty(::Union{AbstractDict,NamedTuple}) = true
dicty(::T) where {T} = hasmethod(haskey, Tuple{T,Symbol}) && hasmethod(get, Tuple{T,Symbol})

get_path(ctx::VarCtx, path) = get_path(ctx.env, ctx.var, path)

get_path(env::VarEnv, var::Var, path) = invokelatest(basic_get_path, env, var, path)

function basic_get_path(env::VarEnv{T}, var::Var, path) where {T}
    exc(type, msg, err) = throw(VarException(type, env, var, msg, err))
    if var.parent == NO_ID && isempty(var.path)
        isnothing(var.internal_value) &&
            throw(VarException(:path, env, var, "no parent and no value"))
        return var.internal_value
    end
    cur = var.parent == NO_ID ? nothing : env.vars[var.parent].internal_value
    for el in path
        local elstr = string(el)
        cur = if !isnothing(match(r"^\.\.+$", elstr))
            for _ = 2:length(string(el))
                var = env.vars[var.parent]
                var.parent == NO_ID && throw(
                    VarException(:path, env, var, "error going up in path with no parent"),
                )
            end
            env.vars[var.parent].internal_value
        elseif el isa Symbol && startswith(elstr, "@")
            try
                env.roots[Symbol(elstr[2:end])]
            catch err
                check_sigint(err)
                exc(:path, "error getting $var root $el in path $path", err)
            end
        elseif cur === nothing
            throw(VarException(:path, env, var, "error getting $var field $el in path $path"))
        elseif el isa Symbol
            try
                if dicty(cur) && haskey(cur, el)
                    cur[el]
                elseif cur isa AbstractDict && haskey(cur, string(el))
                    cur[string(el)]
                else
                    getproperty(cur, el)
                end
            catch err
                check_sigint(err)
                @error "error getting $var field $el in path $path" exception =
                    (err, catch_backtrace())
                exc(:path, "error getting $var field $el in path $path", err)
            end
        elseif el isa Number
            try
                getindex(cur, el)
            catch err
                check_sigint(err)
                exc(:path, "error getting $var field $el in path $path", err)
            end
        elseif hasmethod(el, (VarCtx{T}, typeof(cur)))
            try
                el(VarCtx(env, var), cur)
            catch err
                check_sigint(err)
                exc(:program, "error calling $var getter function $el in path $path", err)
            end
        elseif hasmethod(el, typeof.((cur,)))
            try
                el(cur)
            catch err
                check_sigint(err)
                exc(:program, "error calling $var getter function $el in path $path", err)
            end
        else
            exc(
                :program,
                "No $var getter method $el for $(typeof.((cur,))) in path $path",
                NoCause(),
            )
        end
    end
    cur
end

set_value(env::VarEnv, var::Var, value; creating = false) =
    set_value(VarCtx(env, var), value; creating)

set_value(ctx::VarCtx, value; creating = false) =
    invokelatest(basic_set_value, ctx, value; creating)

function basic_set_value(ctx::VarCtx, value; creating = false)
    local env = ctx.env
    local var = ctx.var
    exc(type, msg, err = NoCause) = throw(VarException(type, ctx, msg, err))
    creating &&
        (haskey(var.metadata, :create) || var.action || !isempty(var.path)) &&
        return
    !var.writeable &&
        throw(VarException(:writeable_error, env, var, "variable $var is not writeable"))
    cur = get_path(env, var, var.path[1:end-1])
    isnothing(cur) && return
    el = var.path[end]
    value = var.value_conversion(value)
    if cur === nothing
        exc(:path, ctx, "error setting field $(el) in path $(var.path) for $var")
    elseif el isa Symbol
        try
            local ft = fieldtype(typeof(cur), el)
            if !(typeof(value) <: ft)
                try
                    value = StructTypes.constructfrom(ft, value)
                catch err
                    check_sigint(err)
                    exc(
                        :path,
                        "error setting field $(el) in path $(var.path) for $var: could not convert value <$(value)> to type $ft",
                        err,
                    )
                end
            end
            setproperty!(cur, el, value)
        catch err
            check_sigint(err)
            exc(:path, "error setting $var field $el", err)
        end
    elseif el isa Number
        if el == length(cur) + 1
            push!(cur, value)
        else
            setindex!(cur, el, value)
        end
    elseif var.action
        parent = env.vars[var.parent].internal_value
        if :.. in var.path && hasmethod(el, typeof.((ctx, cur, parent)))
            try
                el(ctx, cur, parent)
            catch err
                check_sigint(err)
                exc(
                    :program,
                    "error calling $var action function $el for $(typeof.((env, var, cur, parent))): $err",
                    err,
                )
            end
        elseif hasmethod(el, typeof.((ctx, cur)))
            try
                el(ctx, cur)
            catch err
                check_sigint(err)
                exc(
                    :program,
                    "error calling $var action function $el for $(typeof.((env, var, cur))): $err",
                    err,
                )
            end
        elseif :.. in var.path && hasmethod(el, typeof.((cur, parent)))
            try
                el(cur, parent)
            catch err
                check_sigint(err)
                exc(
                    :program,
                    "error calling $var action function $el for $(typeof.((cur, parent))): $err",
                    err,
                )
            end
        elseif hasmethod(el, typeof.((cur,)))
            try
                el(cur)
            catch err
                check_sigint(err)
                exc(
                    :program,
                    "error calling $var action function $el for $(typeof.((cur,))): $err",
                    err,
                )
            end
        else
            exc(:path, "no $var action function $el for $(typeof.((cur,)))")
        end
    elseif hasmethod(el, typeof.((ctx, cur, value)))
        try
            el(ctx, cur, value)
        catch err
            check_sigint(err)
            exc(:program, "error calling $var setter function $el: $err", err)
        end
    elseif hasmethod(el, typeof.((cur, value)))
        try
            el(cur, value)
        catch err
            check_sigint(err)
            exc(:program, "error calling $var setter function $el: $err", err)
        end
    else
        exc(:path, "no $var setter function $el for $(typeof.((cur, value)))")
    end
end

use_value(env::VarEnv, var::Var, value) = use_value(VarCtx(env, var), value)

function use_value(ctx::VarCtx, value)
    local env = ctx.env
    local var = ctx.var
    var.value = var.internal_value = value
    json_value = walk(env, var.value, 0)
    var.json_value = json_value
    var.ref = isref(json_value)
    set_type(ctx)
end

function is_same(a, b, seen = Set())
    ismutable(a) && a ∈ seen && return true
    local atype = typeof(a)
    local btype = typeof(b)
    if atype <: Ref && btype <: Ref
        isassigned(a) && return isassigned(b) && is_same(a[], b[], seen)
        return !isassigned(b)
    end
    ismutable(a) && push!(seen, a)
    if atype <: AbstractDict && btype <: AbstractDict
        length(a) != length(b) && return false
        for (k, v) in a
            (!haskey(b, k) || !is_same(v, b[k], seen))
            return false
        end
        return true
    elseif atype <: AbstractSet && btype <: AbstractSet
        length(a) != length(b) && return false
        for v in a
            !v in b && return false
        end
        return true
    elseif atype <: AbstractArray && btype <: AbstractArray
        length(a) != length(b) && return false
        for (i, v) in enumerate(a)
            !is_same(v, b[i], seen) && return false
        end
        return true
    elseif isstructtype(atype) && atype == btype
        # recursively compare immutable structs because refs don't compare for equality
        for i = 1:fieldcount(atype)
            !is_same(getfield(a, i), getfield(b, i), seen) && return false
        end
        fieldcount(atype) == 0 && return a == b
        return true
    else
        return a == b
    end
end

"""
    Compute the value of a variable and return whether it changed
"""
compute_value(env::VarEnv, var::Var) = compute_value(VarCtx(env, var))

function compute_value(ctx::VarCtx)
    local var = ctx.var
    isempty(var.path) && return false
    local old = var.internal_value
    !var.readable &&
        throw(VarException(:readable_error, ctx, "variable $var is not readable"))
    use_value(ctx, get_path(ctx, var.path))
    return !is_same(old, var.internal_value)
end

function refresh(env::VarEnv, vars = values(env.vars); throw = false, track = true)
    track && empty!(env.changed)
    track && empty!(env.errors)
    for var in vars
        refresh(env, var; throw, track)
    end
end

function refresh(env::VarEnv, var::Var; throw = false, track = true)
    var.id ∈ env.changed && return
    try
        if compute_value(env, var) && track
            #println("CHANGED VAR $(var.name): ", var.json_value)
            push!(env.changed, var.id)
        end
    catch err
        check_sigint(err)
        if throw
            rethrow()
        else
            env.errors[var.id] = err
        end
    end
end
