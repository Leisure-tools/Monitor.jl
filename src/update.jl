function opt(mon::JSON3.Object, keysym::Symbol)
    local opts = mon.Options
    local key = ":$keysym"
    for (i, v) in enumerate(opts)
        v != key && continue
        i < length(opts) && !startswith(opts[i + 1], ":") && return opts[i + 1]
        break
    end
    return nothing
end

function prop(obj::JSON3.Object, key::Symbol, default)
    v = something(get(obj, key, ""), "")
    isempty(v) ? default : v
end

function prop_list(obj::JSON3.Object, key::Symbol, default::Vector{T}) where {T}
    v = get(obj, key, nothing)
    isnothing(v) &&
        return default
    v isa Vector && isempty(v) &&
        return default
    v isa JSON3.Array{T} &&
        return v
    if v isa JSON3.Array
        T == String &&
            return string.(v)
        return T[convert(T, item) for item in v]
    end
    v isa T &&
        return [v]
    T == String &&
        return [string(v)]
    return [convert(T, v)]
end

function parse_prop(t::Type{T}, obj::JSON3.Object, key::Symbol, default::T) where {T}
    p = prop(obj, key, default)
    return if T == Bool && get(obj, key, nothing) == ""
        true
    elseif p isa T
        p
    elseif p isa String
        parse(T, p)
    else
        convert(T, p)
    end
end

function monitor_from(con::Connection, name::Symbol, mon::JSON3.Object)
    local cur = get(con.monitors, name, nothing)
    local new = isnothing(cur)
    local rootpath = prop(mon, :root, "Main")
    local update = parse_prop(Float64, mon, :update, con.default_update)
    local quiet = parse_prop(Bool, mon, :quiet, false)
    local topics = prop_list(mon, :topics, String[])
    local update_topics = prop_list(mon, :updatetopics, String[])

    if update isa String
        update = try
            parse(Float64, update)
        catch
            con.default_update
        end
    end
    if new
        verbose(2, con, "NEW MONITOR: $name")
        root = Var(con.env, "root?path=$rootpath")
        cur = MonitorData(;
            rootpath, name, data=mon, update, root, original=mon, quiet, update_topics, topics
        )
        con.monitors[name] = cur
    else
        verbose(con, "OLD MONITOR: $name")
        cur.update = update
        cur.quiet = quiet
        cur.update_topics = update_topics
        root = cur.root
        if '&' ∈ rootpath || cur.root.metadata[:path] != rootpath
            local newmeta = parsemetadata("path=$rootpath")
            if cur.root.metadata != newmeta
                root = Var(
                    con.env,
                    "root?path=$rootpath";
                    cur.root.id,
                    cur.root.level,
                    cur.root.value,
                )
            end
        end
    end
    return new, root, cur
end

find_monitor_vars(::Connection, name::Symbol, ::MonitorData, val) =
    error("Bad monitor object named $name: $val")

find_monitor_vars(::Connection, ::Symbol, ::MonitorData, ::Nothing) = Pair{String, Symbol}[]

function find_monitor_vars(
    con::Connection,
    new::Bool,
    name::Symbol,
    cur::MonitorData,
    mon::JSON3.Object,
)
    local val = mon.value
    local new_data_keys = Pair{Symbol, String}[]
    local old_vars = Set{Symbol}()
    local disabled = get(mon, :disabled, false)
    local rename_result = ()

    cur.disabled = disabled
    if !disabled
        rename = get(mon, :rename, nothing)
        if !isnothing(rename)
            rename_result = find_monitor_var(con, cur, name, rename, old_vars)
            ((varname,),) = rename_result
            cur.rename = cur.vars[varname]
        end
        for (ksym, _) in val
            pair = find_monitor_var(con, cur, name, string(ksym), old_vars)[1]
            push!(new_data_keys, pair)
        end
        for (name, var) in cur.vars
            (name ∈ old_vars || var.parent == NO_ID) && continue
            local parent = con.env.vars[var.parent]
            haskey(parent.children, name) && delete!(parent.children, name)
        end
    elseif !new
        # disabled -- remove vars from cur
        for (name, var) in cur.vars
            remove(con.env, var)
        end
        empty!(cur.vars)
    end
    return rename_result, new_data_keys
end

function find_monitor_var(
    con::Connection, cur::MonitorData, mon_name::Symbol, name::String, old_vars
)
    m = match(VAR_NAME, name)
    isnothing(m) && error("Bad monitor rename variable in $mon_name: $name")
    #local var_name = Symbol(m[1])
    local var_name = Symbol(name)
    push!(old_vars, var_name)
    local metastr = something(m[2], "path=$(m[1])$(something(m[3], ""))")
    local meta = parsemetadata(metastr)
    if !haskey(meta, :path)
        meta[:path] = var_name
    end
    if haskey(cur.vars, var_name)
        local var = cur.vars[var_name]
        var.metadata == meta && return (var_name => name,)
        # metadata changed
        if var.metadata[:path] != meta[:path]
            var.path = parsepath(meta[:path])
        end
        var.metadata = meta
    else
        local var = Var(con.env, var_name; id=NO_ID, parent=cur.root.id, fullname=name)
        var.metadata = meta
        var.path = parsepath(meta[:path])
        verbose(2, con, "MADE NEW VAR $var_name($(var.id))")
        cur.vars[var.name] = var
    end
    return (var_name => name,)
end

function handle_incoming_block(
    con::Connection,
    _,
    name::Symbol,
    update::JSON3.Object,
    updated::Set{Symbol},
)
    if update.type ∈ ("monitor", "data") && update == get(con.data_blocks, name, nothing)
        delete!(updated, name)
        verbose(2, con, "IGNORING REDUNDANT MONITOR '$name'", update)
    elseif update.type == "monitor"
        verbose(2, con, "RECEIVED MONITOR '$name'", update)
        handle_monitor(con, name, update)
    elseif update.type == "code"
        handle_code(con, name, update)
    elseif update.type == "data"
        handle_data(con, name, update)
    end
end

# Receive a data block, by default just calls base_handle_data(con, name, data)
handle_data(con::Connection, name::Symbol, data::JSON3.Object) =
    base_handle_data(con, name, data)

function base_handle_data(con::Connection, name::Symbol, data::JSON3.Object)
    con.data_blocks[name] = data
end

# Delete data blocks, by default just calls base_handle_delete(con, del)
handle_delete(con::Connection, del::JSON3.Object) = base_handle_delete(con, del)

function base_handle_delete(con::Connection, del::JSON3.Object)
    local names = if del.value isa String
        [del.value]
    elseif del.value isa JSON3.Array
        del.value
    else
        error("Bad value for delete block, expecting string or array but got $(del.value)")
    end
    for name in names
        delete!(con.data_blocks, name)
    end
end

function handle_monitor(kw::NamedTuple)
    if isnothing(current_connection[])
        println("NO CURRENT CONNECTION!")
    else
        verbose(current_connection[], "HANDLING ", json(kw))
        handle_monitor(current_connection[], kw.name, json(kw))
    end
end

# Receive a monitor block, by default just calls base_handle_monitor(con, name, mon)
handle_monitor(con::Connection, name::Symbol, mon::JSON3.Object) =
    base_handle_monitor(con, name, mon)

function base_handle_monitor(con::Connection, name::Symbol, mon::JSON3.Object)
    try
        verbose(2, con, "GOT MONITOR: ", mon)
        local missing = filter(∉(keys(mon)), (:root, :value))
        if !isempty(missing)
            @warn "Bad monitor object, missing these keys: $missing" mon
            return false
        end
        local new, root, cur = monitor_from(con, name, mon)
        local val = mon.value
        local rename, new_data_keys = find_monitor_vars(con, new, name, cur, mon)
        cur.rootpath = mon.root
        cur.data = JSON3.read(JSON3.write(val), Dict{Symbol, Any})
        cur.data_keys = new_data_keys
        cur.root = root
        cur.original = mon
        con.env.vars[cur.root.id] = root
        # set data from incoming monitor
        if new
            if !cur.disabled
                # add monitor variable changes from env so they get sent out
                for (_, var) in cur.vars
                    haskey(con.env.vars, var.id) &&
                        push!(con.env.changed, var.id)
                end
            else
            end
        elseif !cur.disabled && !(isempty(new_data_keys) && isempty(rename))
            # remove monitor variable changes from env so they don't get sent out
            for (jsym, vfullname) in Iterators.flatten((new_data_keys, rename))
                json = get(cur.data, jsym, nothing)
                var = con.env.varfullnames[vfullname]
                if !is_same(json, var.json_value)
                    safe_set(con, cur, var, json)
                end
                delete!(con.env.changed, var.id)
            end
            verbose(
                con,
                "MONITOR ROOT: ",
                mon.root,
                "\n  ROOT:",
                root,
                "\n  VALUE: ",
                root.internal_value,
            )
        end
    catch err
        @error "Error handling incoming monitor" exception = (err, catch_backtrace())
    end
end

# Receive a code block, by default just calls base_handle_code(con, name, ev)
handle_code(con::Connection, name::Symbol, ev::JSON3.Object) =
    base_handle_code(con, name, ev)

function base_handle_code(con::Connection, name::Symbol, ev::JSON3.Object)
    local str = get(ev, :value, "")
    if !(str isa String)
        @error "Bad value type for code, expecting string but got: $(typeof(str))"
        return nothing
    end
    return if !isempty(str)
        try
            Main.eval(Meta.parse("begin " * str * " end"))
        catch err
            check_sigint(err)
            @error "Error evaluating block: $err" exception = (err, catch_backtrace())
        end
    end
end

function safe_set(con::Connection, mon::MonitorData, var::Var, value)
    try
        verbose(con, "SETTING VAR ", var.name, " PATH ", var.path, " TO ", value)
        set_value(con.env, var, deref(con.env, value))
    catch err
        check_sigint(err)
        if !(err isa VarException && err.type == :path && con.verbosity < 2)
            @error "Error setting value in monitor $(mon.name) for variable $(var.name)" exception = (
                err, catch_backtrace()
            )
        end
    end
end

function find_outgoing_updates(con::Connection; force=:none)
    local t = time()
    local check = Set{Int}()
    local monitors = Set{Symbol}()
    local force_update = if force == :all
        (_) -> true
    elseif force isa Set
        (mon) -> mon.name ∈ force
    else
        (_) -> false
    end
    for (_, mon::MonitorData) in con.monitors
        !force_update(mon) &&
            t - (con.lastcheck ÷ mon.update) * mon.update < mon.update &&
            continue
        !isempty(mon.vars) && push!(check, (v.id for (_, v) in mon.vars)...)
        push!(check, mon.root.id)
        # don't publish quiet monitors, but keep refreshing their variables
        !mon.quiet &&
            push!(monitors, mon.name)
    end
    con.lastcheck = t
    local checked = Set{Int}()
    # refresh parents first
    for v in check
        v ∈ checked && continue
        local p = con.env.vars[v]
        local parents = [p]
        while p.parent != NO_ID
            p.parent ∈ checked && break
            p = con.env.vars[p.parent]
            push!(parents, p)
        end
        for a in reverse!(parents)
            refresh(con.env, a)
            push!(checked, a.id)
        end
    end
    #send_changes(con, (con.monitors[name] for name in monitors))
    send_changes(con)
end

function send_changes(con::Connection, monitors=values(con.monitors))
    mons = Set()
    for mon in monitors
        if haschanges(con, mon)
            push!(mons, mon)
            #@info "MONITOR CHANGED: $(mon.name)"
        end
        if !mon.disabled && haschanges(con, mon)
            data = compute_data(con, mon)
            con.data_blocks[mon.name] = data
            mon.original = json(data)
            !mon.quiet && verbose(
                1, con, "Queuing monitor update for ", mon.name, ": ", compute_data(con, mon)
            )
            !mon.quiet && send(con, mon.name, data)
        end
    end
    #if !isempty(con.env.changed)
    #    @info "CHANGES" vars = [
    #        id => con.env.vars[id] for id in con.env.changed
    #    ] monitors = [
    #        mon.name for mon in mons
    #    ]
    #end
    empty!(con.env.changed)
    empty!(con.env.errors)
end

function outgoing_update_period(con::Connection)
    local mintime = typemax(Float64)
    for (_, mon) in con.monitors
        mintime = min(mintime, mon.update)
    end
    return mintime == typemax(Float64) ? con.default_update : mintime
end

function haschanges(con::Connection, mon::MonitorData)
    for (_, v) in mon.vars
        v.id ∈ con.env.changed &&
            return true
    end
    return false
end

function namefor(var::Var, name::String)
    local vn, meta = match(VAR_NAME, name)
    if something(meta, "") == ""
        meta = "path=$name"
    end
    local vm = Dict(var.metadata)
    delete!(vm, :type)
    return Symbol(vm == parsemetadata(meta) ? vn : clean(name))
end

clean(name) = endswith(name, "()") ? name[1:(end - 2)] : name

# compute data for changed variables
function compute_data(con::Connection, mon::MonitorData)
    for (key, fullname) in mon.data_keys
        local var = con.env.varfullnames[fullname]
        mon.data[key] = var.json_value
    end
    exclude = MONITOR_KEYS
    if !isnothing(mon.rename)
        exclude = Set(MONITOR_KEYS)
        push!(exclude, :rename)
    end
    # maintain order
    return OrderedDict{Symbol, Any}(
        (Symbol(k) => v
         for (k, v) in mon.original if k ∉ exclude)...,
        :root => mon.rootpath,
        (!isnothing(mon.rename) ? (:name => mon.rename.json_value,) : ())...,
        (mon.update != con.default_update ? (:update => mon.update,) : ())...,
        (mon.quiet ? (:quiet => mon.quiet,) : (;))...,
        (!isempty(mon.update_topics) ? (:updatetopics => mon.update_topics,) : ())...,
        :value => OrderedDict(name => mon.data[name] for (name,) in mon.data_keys),
    )
end
