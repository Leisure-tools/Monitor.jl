function opt(mon::JSON3.Object, keysym::Symbol)
    local opts = mon.Options
    local key = ":$keysym"
    for (i, v) in enumerate(opts)
        v != key && continue
        i < length(opts) && !startswith(opts[i+1], ":") && return opts[i+1]
        break
    end
    return nothing
end

function monitor_from(con::Connection, name::Symbol, mon::JSON3.Object)
    local cur = get(con.monitors, name, nothing)
    local new = isnothing(cur)
    local rootpath = mon.root
    local update = get(mon, :update, con.default_update)

    if new
        root = Var(con.env, "root?path=$rootpath")
        cur = MonitorData(; rootpath, name, data = mon, update, root)
        con.monitors[name] = cur
    else
        cur.update = update
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

find_monitor_vars(::Connection, ::Symbol, ::MonitorData, ::Nothing) = Pair{String,Symbol}[]

function find_monitor_vars(
    con::Connection,
    new::Bool,
    name::Symbol,
    cur::MonitorData,
    mon::JSON3.Object,
)
    local val = mon.value
    local new_data_keys = Pair{Symbol,Symbol}[]
    local old_vars = Set{Symbol}()
    local disabled = get(mon, :disabled, false)

    cur.disabled = disabled
    if !disabled
        for (ksym, _) in val
            local k = string(ksym)
            local m = match(VAR_NAME, k)
            isnothing(m) && error("Bad monitor variable in $name: $k")
            local name = Symbol(m[1])
            push!(old_vars, name)
            push!(new_data_keys, ksym => name)
            local metastr = something(m[2], "path=$name")
            local meta = parsemetadata(metastr)
            if !haskey(meta, :path)
                meta[:path] = name
            end
            if haskey(cur.vars, name)
                local var = cur.vars[name]
                var.metadata == meta && continue
                # metadata changed
                if var.metadata[:path] != meta[:path]
                    var.path = parsepath(meta[:path])
                end
                var.metadata = meta
                push!(con.env.changed, var.id)
            else
                local var = Var(con.env, name; id = NO_ID, parent = cur.root.id)
                var.metadata = meta
                var.path = parsepath(meta[:path])
                verbose(con, "MADE NEW VAR $name")
                cur.vars[var.name] = var
                push!(con.env.changed, var.id)
            end
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
    return new_data_keys
end

function handle_update(
    con::Connection,
    name::Symbol,
    update::JSON3.Object,
    updated::Set{Symbol},
)
    if update.type == "monitor"
        verbose(con, "RECEIVED MONITOR '$name'", update)
        if handle_monitor_update(con, name, update)
            push!(updated, name)
        end
    elseif update.type == "eval"
        handle_eval(con, name, update)
    end
end

function handle_monitor_update(con::Connection, name::Symbol, mon::JSON3.Object)
    local missing = filter(∉(keys(mon)), (:root, :value))
    if !isempty(missing)
        @warn "Bad monitor object, missing these keys: $missing"
        return false
    end
    local new, root, cur = monitor_from(con, name, mon)
    local val = mon.value
    local new_data_keys = find_monitor_vars(con, new, name, cur, mon)

    cur.rootpath = mon.root
    cur.data = JSON3.read(JSON3.write(val), Dict{Symbol,Any})
    cur.data_keys = new_data_keys
    cur.root = root
    con.env.vars[cur.root.id] = root
    # set data from incoming monitor
    if !new && !isempty(new_data_keys) && !cur.disabled
        refresh(con.env, (root,); track = false)
        #for v in Set(con.env.changed)
        #    local var = con.env.vars[v]
        #    if is_same(get(cur.data, var.name, nothing), var.json_value)
        #        delete!(con.env.changed, v)
        #    end
        #end
        verbose(
            con,
            "MONITOR ROOT: ",
            mon.root,
            "\n  ROOT:",
            root,
            "\n  VALUE: ",
            root.internal_value,
        )
        #println("VALUE: $(cur.data)")
        # plug values from data into Julia objects
        for (str, name) in new_data_keys
            local var = cur.vars[name]
            if !is_same(get(cur.data, var.name, nothing), var.json_value)
                safe_set(con, cur, var, cur.data[str])
                delete!(con.env.changed, var.id)
            end
        end
    end
    return true
end

function handle_eval(con::Connection, name::Symbol, mon::MonitorData)
    local str = get(mon, :value, "")
    if !(str isa String)
        @error "Bad value type for eval: $(typeof(str))"
        return
    end
    if !isempty(str)
        try
            Main.eval(Meta.parse(str))
        catch err
            check_sigint(err)
            @error "Error evaluationg block: $err" exception = (err, catch_backtrace())
        end
    end
end

function safe_set(con::Connection, mon::MonitorData, var::Var, value)
    try
        verbose(con, "SETTING VAR $(var.name) PATH $(var.path)")
        set_value(con.env, var, deref(con.env, value))
    catch err
        check_sigint(err)
        @error "Error setting value in monitor $(mon.name) for variable $(var.name)" exception =
            (err, catch_backtrace())
    end
end

function find_outgoing_updates(con::Connection; force = :none)
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
    for (_, mon) in con.monitors
        !force_update(mon) &&
            t - (con.lastcheck ÷ mon.update) * mon.update < mon.update &&
            continue
        !isempty(mon.vars) && push!(check, (v.id for (_, v) in mon.vars)...)
        push!(check, mon.root.id)
        push!(monitors, mon.name)
    end
    con.lastcheck = t
    local checked = Set{Int}()
    empty!(con.env.changed)
    empty!(con.env.errors)
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
    for (_, mon) in con.monitors
        if haschanges(con, mon)
            verbose(con, "Queuing monitor update for ", mon.name, ": ", compute_data(mon))
            send(con, mon.name, compute_data(mon))
        end
    end
    empty!(con.env.changed)
end

function outgoing_update_period(con::Connection)
    local mintime = typemax(Float64)
    for (_, mon) in con.monitors
        mintime = min(mintime, mon.update)
    end
    return mintime == typemax(Float64) ? con.default_update : mintime
end

function haschanges(con::Connection, mon::MonitorData)
    return !isnothing(findfirst(mon.vars) do v
        return v.id ∈ con.env.changed
    end)
end

function namefor(var::Var, name::String)
    local vn, meta = match(VAR_NAME, name)
    if something(meta, "") == ""
        meta = "path=$name"
    end
    local vm = Dict(var.metadata)
    delete!(vm, :type)
    return Symbol(vm == parsemetadata(meta) ? vn : name)
end

# compute data for changed variables
function compute_data(mon::MonitorData)
    for (_, varname) in mon.data_keys
        local var = mon.vars[varname]
        mon.data[varname] = var.json_value
    end
    # maintain order
    return (;
        type = "monitor",
        root = mon.rootpath,
        value = (;
            (
                namefor(mon.vars[name], string(str)) => mon.data[name] for
                (str, name) in mon.data_keys
            )...
        ),
    )
end
