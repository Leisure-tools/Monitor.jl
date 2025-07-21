using JSON3: JSON3, StructTypes

const REF = r"^\?([0-9]+)$"
const LOCAL_ID = r"^@/([0-9]+)$"
const UNKNOWN = "?"
#const PATH_COMPONENT = r"^([-[:alnum:]]+|@)/([0-9]+)$"
# groups 1: var name, 2: metadata, 3: optional parens (if no metadata)
const VAR_NAME = r"^([0-9]+|\pL[\p{Xan}_!.]*)(?:\?((?:\pL[\p{Xan}_!]*)(?:=(?:(?:[^,]|\\,)*))?(?:,(?:\pL[\p{Xan}_!]*)(?:=(?:(?:[^,]|\\,)*))?)*)|(\(\)))?$"
const METAPROP = r"(\pL[\p{Xan}_!]*)(?:=((?:[^,]|\\,)*))?(,|$)"
const ARRAY_INDEX = r"[1-9][0-9]*"
const QUALIFIED_NAME = r"^[^.]+\.[^.]+$"

const PATH_COMPONENT = r"(@?[\p{LC}\p{Lm}\p{Lo}\p{Nl}\p{No}\p{Sc}\p{So}_][\p{LC}\p{Lm}\p{Lo}\p{Nl}\p{No}\p{Sc}\p{So}_!\p{Nd}\p{No}\p{Mn}\p{Mc}\p{Me}\p{Sk}\p{Pc}]*|\[[1-9][0-9]*\]|\(\)|\.)\s*"

const JPATH_COMPONENT = r"(@?[\p{LC}\p{Lm}\p{Lo}\p{Nl}\p{Sc}\p{So}_][\p{LC}\p{Lm}\p{Lo}\p{Nl}\p{Sc}\p{So}_!\p{Nd}\p{No}\p{Mn}\p{Mc}\p{Me}\p{Sk}\p{Pc}]*|[1-9][0-9]*)(?:((?:\.[\p{LC}\p{Lm}\p{Lo}\p{Nl}\p{Sc}\p{So}_][\p{LC}\p{Lm}\p{Lo}\p{Nl}\p{Sc}\p{So}_!\p{Nd}\p{No}\p{Mn}\p{Mc}\p{Me}\p{Sk}\p{Pc}]*)*)(?:\(\))?)?"

verbosity(::VarEnv) = 0

verbose(env::VarEnv) = verbosity(env) > 0

Base.show(io::IO, var::Var) =
    print(io, "Var($(metaname(var)), $(var.id)<-$(var.parent), $(var.path))")

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
    env.verbose_oids ? (; ref=oid(env, obj), repr=repr(obj)) : (; ref=oid(env, obj))

function parent(ctx::VarCtx)
    v = parent(ctx.env, ctx.var)
    isnothing(v) &&
        return nothing
    return VarCtx(ctx.env, v)
end

function parent(env::VarEnv, var::Var)
    var.parent == NO_ID &&
        return nothing
    return env.vars[var.parent]
end

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

function parsemetadata(meta::AbstractString, original_meta=Dict{Symbol, String}())
    metadata = Dict{Symbol, String}(original_meta)
    while meta !== ""
        (m = match(METAPROP, meta)) === nothing && throw("Bad metaproperty format: $(meta)")
        metadata[Symbol(m[1])] = m[2] === nothing ? "" : string(m[2])
        length(meta) == length(m.match) && break
        meta[length(m.match)] != ',' && throw("Bad metaproperty format: $(meta)")
        meta = meta[(length(m.match) + 1):end]
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
        rest = strip(@views rest[(length(m.match) + 1):end])
        if length(frag) > 2 && frag[(end - 1):end] == "()"
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
        elseif !isnothing(match(QUALIFIED_NAME, frag))
            push!(path, (Symbol.(split(frag, "."))...,))
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
    name::Union{Symbol, AbstractString}="";
    id::Int=NO_ID,
    level=0,
    value=nothing,
    fullname=string(name),
    json_value=nothing,
    metadata::Dict{Symbol, String}=Dict{Symbol, String}(),
    kw...,
) = basicvar(env, id, name; level, value, fullname, json_value, metadata, kw...)

"Basic variable creation"
function basicvar(
    env::VarEnv{T},
    id::Int=NO_ID,
    name::Union{Symbol, AbstractString}="";
    level=0,
    value=nothing,
    json_value=nothing,
    metadata::Dict{Symbol, String}=Dict{Symbol, String}(),
    fullname=string(name),
    kw...,
) where {T}
    args = (; value, kw...)
    if !isnothing(value) && !haskey(args, :internal_value)
        args = (; args..., internal_value=args.value)
    end
    if fullname != ""
        m = match(VAR_NAME, fullname)
        if m !== nothing && m[2] !== nothing
            name = m[1]
            metadata = parsemetadata(m[2], metadata)
            #println("METADATA: ", metadata)
            if haskey(metadata, :path) && !haskey(args, :path)
                args = (; args..., path=parsepath(metadata[:path]))
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
    v = Var{T}(; args..., id, name=name isa Number ? Symbol("") : name, fullname)
    !isnothing(v.internal_value) && use_value(env, v, v.internal_value)
    v.metadata[:type] = typename(v.internal_value)
    if isnothing(json_value)
        v.json_value = json_value
    end
    env.vars[id] = v
    env.varnames[v.name] = v
    env.varfullnames[fullname] = v
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

dicty(::Union{AbstractDict, NamedTuple}) = true
dicty(::T) where {T} = hasmethod(haskey, Tuple{T, Symbol}) && hasmethod(get, Tuple{T, Symbol})

get_path(ctx::VarCtx, path) = get_path(ctx.env, ctx.var, path)

get_path(env::VarEnv, var::Var, path) = invokelatest(basic_get_path, env, var, path)

function basic_get_path(env::VarEnv{T}, var::Var, path) where {T}
    function exc(type, msg, err=NoCause())
        throw(VarException(type, env, var, msg, err))
    end
    if var.parent == NO_ID && isempty(var.path)
        isnothing(var.internal_value) &&
            exc(:path, "no parent and no value")
        return var.internal_value
    end
    cur = var.parent == NO_ID ? nothing : env.vars[var.parent].internal_value
    for el in path
        local elstr = string(el)
        cur = if !isnothing(match(r"^\.\.+$", elstr))
            for _ = 2:length(string(el))
                var = env.vars[var.parent]
                var.parent == NO_ID &&
                    exc(:path, "error going up in path with no parent")
            end
            env.vars[var.parent].internal_value
        elseif el isa Symbol && startswith(elstr, "@")
            try
                env.roots[Symbol(elstr[2:end])]
            catch err
                check_sigint(err)
                exc(:path, "error getting $var root $el in path $path", err)
            end
        elseif el isa Tuple
            try
                getfield(getfield(Main, el[1]), el[2])
            catch err
                check_sigint(err)
                exc(:path, "error getting $var root $el in path $path", err)
            end
        elseif cur === nothing
            exc(:path, "error getting $var field $el in path $path")
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
                @error "error getting $var field $el in path $path" exception = (
                    err, catch_backtrace()
                )
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
                @error "Exception executing method in variable path" cur path exception = (
                    err, catch_backtrace()
                )
                exc(:program, "error calling $var getter function $el in path $path", err)
            end
        elseif hasmethod(el, typeof.((cur,)))
            try
                el(cur)
            catch err
                check_sigint(err)
                @error "Exception executing method in variable path" cur path exception = (
                    err, catch_backtrace()
                )
                exc(:program, "error calling $var getter function $el in path $path", err)
            end
        else
            @error "Exception getting variable value" path exception = (err, catch_backtrace())
            exc(
                :program,
                "No $var getter method $el for $(typeof.((cur,))) in path $path",
                NoCause(),
            )
        end
    end
    cur
end

set_value(env::VarEnv, var::Var, value; creating=false) =
    set_value(VarCtx(env, var), value; creating)

set_value(ctx::VarCtx, value; creating=false) =
    invokelatest(basic_set_value, ctx, value; creating)

function basic_set_value(ctx::VarCtx, value; creating=false)
    local env = ctx.env
    local var = ctx.var
    exc(type, msg, err=NoCause) = throw(VarException(type, ctx, msg, err))
    creating &&
        (haskey(var.metadata, :create) || var.action || !isempty(var.path)) &&
        return nothing
    !var.writeable &&
        throw(VarException(:writeable_error, env, var, "variable $var is not writeable"))
    cur = get_path(env, var, var.path[1:(end - 1)])
    println("CUR VALUE: $cur")
    isnothing(cur) && verbose(env, "No holder for variable ", var)
    isnothing(cur) && return nothing
    el = var.path[end]
    value = var.value_conversion(value)
    verbose(env, "Setting ", cur, ".", el, " to ", value)
    if el isa Tuple
        try
            local mod = getfield(Main, el[1])
            local var = el[2]
            local ft = Core.get_binding_type(mod, var)
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
            mod.eval(:(v -> $var = v))(value)
        catch err
            check_sigint(err)
            exc(:program, "error setting field $el in path $(var.path) for $var")
        end
    elseif cur === nothing
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
            verbose(env, "Setting value of ", var, " (", cur, ") to ", value)
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

function is_same(a, b, seen=Set())
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
    #if !is_same(old, var.internal_value)
    #    println("VAR CHANGED: $(var.name), ", repr(old), " -> ", repr(var.internal_value))
    #end
    return !is_same(old, var.internal_value)
end

"Refresh variables that have not changed, marking them changed if they have new values"
function refresh(env::VarEnv, vars=values(env.vars); throw=false, track=true)
    v = [v for v in vars]
    seen = Set{Int}()
    i = 0
    # find parents
    while i < length(v)
        i += 1
        var = v[i]
        if var.id ∉ seen
            p = parent(env, var)
            !isnothing(p) &&
                push!(v, p)
        end
    end
    # refresh parents and then children
    for i in length(v):-1:1
        var = v[i]
        refresh(env, var; throw, track)
    end
end

"Refresh variable if it has not changed, marking it changed if it has a new value"
function refresh(env::VarEnv, var::Var; throw=false, track=true)
    var.id ∈ env.changed && return nothing
    try
        if compute_value(env, var) && track
            push!(env.changed, var.id)
        end
    catch err
        check_sigint(err)
        if throw
            rethrow()
        else
            verbose(env)
            env.errors[var.id] = err
        end
    end
end
