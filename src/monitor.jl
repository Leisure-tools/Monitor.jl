function opt(ctrl::JSON3.Object, keysym::Symbol)
    local opts = ctrl.Options
    local key = ":$keysym"
    for (i, v) in enumerate(opts)
        v != key && continue
        i < length(opts) && !startswith(opts[i+1], ":") && return opts[i+1]
        break
    end
    return nothing
end

function control_from(con::Connection, name::Symbol, ctrl::JSON3.Object)
    local cur = get(con.controls, name, nothing)
    local new = isnothing(cur)
    local rootpath = ctrl.root
    local update = get(ctrl, :update, con.default_update)

    if new
        root = Var(con.env, "root?path=$rootpath")
        cur = ControlData(; name, data = ctrl, update, root)
        con.controls[name] = cur
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

find_ctrl_vars(::Connection, name::Symbol, ::ControlData, val) =
    error("Bad control object named $name: $val")

find_ctrl_vars(::Connection, ::Symbol, ::ControlData, ::Nothing) = Pair{String,Symbol}[]

function find_ctrl_vars(con::Connection, name::Symbol, cur::ControlData, val::JSON3.Object)
    local new_data_keys = Pair{Symbol,Symbol}[]
    local old_vars = Set{Symbol}()

    for (ksym, _) in val
        local k = string(ksym)
        local m = match(VAR_NAME, k)
        isnothing(m) && error("Bad control variable in $name: $k")
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
            if var.metadata == meta
                continue
            end
            if var.metadata[:path] != meta[:path]
                var.path = parsepath(meta[:path])
            end
            var.metadata = meta
        else
            local var = Var(con.env, name; id = NO_ID, parent = cur.root.id)
            var.metadata = meta
            var.path = parsepath(meta[:path])
            verbose(con, "MADE NEW VAR $name")
            cur.vars[var.name] = var
        end
    end
    for (name, var) in cur.vars
        (name ∈ old_vars || var.parent == NO_ID) &&
            continue
        local parent = con.env.vars[var.parent]
        haskey(parent.children, name) && delete!(parent.children, name)
    end
    return new_data_keys
end

function handle_control_update(con::Connection, name::Symbol, ctrl::JSON3.Object)
    local missing = filter(∉(keys(ctrl)), (:root, :value))
    if !isempty(missing)
        @warn "Bad control object, missing these keys: $missing"
        return false
    end
    local new, root, cur = control_from(con, name, ctrl)
    local val = ctrl.value
    local new_data_keys = find_ctrl_vars(con, name, cur, val)

    cur.data = JSON3.read(JSON3.write(val), Dict{Symbol,Any})
    cur.data_keys = new_data_keys
    cur.root = root
    con.env.vars[cur.root.id] = root
    if !new && !isempty(new_data_keys)
        refresh(con.env, (root,); track = false)
        for v in Set(con.env.changed)
            local var = con.env.vars[v]
            if is_same(get(cur.data, var.name, nothing), var.json_value)
                delete!(con.env.changed, v)
            end
        end
        verbose(con, "CTRL ROOT: ", ctrl.root, "\n  ROOT:", root, "\n  VALUE: ", root.internal_value)
        println("VALUE: $(cur.data)")
        # plug values from data into Julia objects
        for (str, name) in new_data_keys
            local var = cur.vars[name]
            safe_set(con, cur, var, cur.data[str])
            delete!(con.env.changed, var.id)
        end
    end
    return true
end

function safe_set(con::Connection, ctrl::ControlData, var::Var, value)
    try
        verbose(con, "SETTING VAR $(var.name) PATH $(var.path)")
        set_value(con.env, var, deref(con.env, value))
    catch err
        check_sigint(err)
        @error "Error setting value in control $(ctrl.name) for variable $(var.name)" exception =
            (err, catch_backtrace())
    end
end

iscontrol(obj::JSON3.Object) = obj.type == "control"

function find_outgoing_updates(con::Connection; force = :none)
    local t = time()
    local check = Set{Int}()
    local controls = Set{Symbol}()
    local force_update = if force == :all
        (_)-> true
    elseif force isa Set
        (ctrl)-> ctrl.name ∈ force
    else
        (_)-> false
    end
    for (_, ctrl) in con.controls
        !force_update(ctrl) && t - (con.lastcheck ÷ ctrl.update) * ctrl.update < ctrl.update &&
            continue
        !isempty(ctrl.vars) && push!(check, (v.id for (_, v) in ctrl.vars)...)
        push!(check, ctrl.root.id)
        push!(controls, ctrl.name)
    end
    con.lastcheck = t
    local checked = Set{Int}()
    empty!(con.env.changed)
    empty!(con.env.errors)
    # refresh parents first
    for v in check
        v ∈ checked && continue
        local p = con.env.vars[v]
        local parents = []
        while p.parent != NO_ID
            p.parent ∈ checked && break
            p = con.env.vars[p.parent]
            push!(checked, p.id)
            push!(parents, p)
        end
        for a in reverse!(parents)
            refresh(con.env, a)
        end
    end
    for v in check
        v ∈ checked && continue
        refresh(con.env, con.env.vars[v])
    end
    isempty(con.env.changed) && return ()
    for (_, ctrl) in con.controls
        haschanges(con, ctrl) && refresh(con.env, (ctrl.root,); track = false)
    end
    Dict(
        k => compute_data(ctrl) for
        (k, ctrl) in con.controls if ctrl.name ∈ controls && haschanges(con, ctrl)
    )
end

function outgoing_update_period(con::Connection)
    local mintime = typemax(Float64)
    for (_, ctrl) in con.controls
        mintime = min(mintime, ctrl.update)
    end
    return mintime == typemax(Float64) ? con.default_update : mintime
end

function haschanges(con::Connection, ctrl::ControlData)
    return !isnothing(findfirst(ctrl.vars) do v
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
function compute_data(ctrl::ControlData)
    for (_, varname) in ctrl.data_keys
        local var = ctrl.vars[varname]
        ctrl.data[varname] = var.json_value
    end
    # maintain order
    return (;
        (
            namefor(ctrl.vars[name], string(str)) => ctrl.data[name] for
            (str, name) in ctrl.data_keys
        )...
    )
end
