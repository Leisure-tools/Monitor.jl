"""
Listens to and updates controls on Julia.

You must define handlers in order for this to work:

- send_updates()
- get_updates()

Optional handlers:

- init(::Connection)
- incoming_update_period(::Connection)
- outgoing_update_period(::Connection)

"""
module Control

using UUIDs
import Base.Threads.@spawn

const astr = AbstractString

include("types.jl")
include("vars.jl")
include("monitor.jl")

verbose(con::Connection, args...) = verbose(1, con, args...)

verbosity(env::VarEnv{Connection}) = env.data.verbosity

function verbose(level::Int64, con::Connection, args...)
    if con.verbosity >= level
        println(args...)
    end
end

check_sigint(err) = err isa InterruptException && exit(1)

"Initialize a new connection right after it is created"
init(::Connection) = nothing

"""
Creat a connection and start it by default

The `spawn` keyword can be
- `true`: run the connection in a spawned thread
- `false`: immediately use this thread to run the connection -- does not return until closed
- `nothing`: do not run the connection. The caller will arrange to run it
"""
function start(
    data::T;
    roots::Dict{Symbol} = Dict{Symbol}(),
    spawn::Union{Bool, Nothing} = true,
    checkincoming = true,
    checkoutgoing = true,
) where {T}
    con = Connection(data)
    con.env.roots = roots
    init(con)
    checkincoming && @spawn check_incoming_updates(con)
    checkoutgoing && @spawn check_outgoing_updates(con)
    if !isnothing(spawn)
        if spawn
            verbose(con, "SPAWNING CONNECTION RUNNER")
            @spawn run_connection(con)
        else
            verbose(con, "RUNNING CONNECTION")
            run_connection(con)
        end
    end
    return con
end

"Run code in the connection thread synchronously"
function sync(f::Function, con::Connection)
    local result = Channel{Any}()
    async(con) do
        put!(result, f())
    end
    return take!(result)
end

"Run code in the connection thread asynchronously"
async(f::Function, con::Connection) = put!(con.channel, f)

"""
Process commands in the connection thread

This is used for getting and setting data values.
"""
function run_connection(con::Connection)
    local failures = 0
    while isopen(con.channel)
        try
            take!(con.channel)()
            failures = 0
        catch err
            check_sigint(err)
            @error "Error processing command: $err" exception = (err, catch_backtrace())
            failures += 1
            if failures > 3
                @error "Exiting because there were more than three failures in a row processing commands"
                shutdown(con)
                break
            end
        end
    end
end

incoming_update_period(::Connection) = 60.0 * 2

"Hook: retrieve current updates"
function get_updates(::Connection, wait_period::Float64)::Union{Nothing,JSON3.Object}
    error("UNDEFINED HOOK: get_updates")
end

function check_incoming_updates(con::Connection)
    local count = 0
    while isopen(con.channel)
        count += 1
        try
            local updates = get_updates(con, incoming_update_period(con))
            if !isnothing(updates) && !isempty(updates)
                verbose(con, "UPDATE: $updates \n  $(typeof(updates))")
                incoming_updates(con, updates)
            end
        catch err
            check_sigint(err)
            @error "Error updating" exception = (err, catch_backtrace())
        end
    end
end

function incoming_updates(con::Connection, updates::JSON3.Object)
    local updated = Set{Symbol}()
    sync(con) do
        for (name, change) in updates
            !iscontrol(change) && continue
            @info "RECEIVED CONTROL '$name'" control = change
            if handle_control_update(con, name, change)
                push!(updated, name)
            end
        end
        refresh(con; force=updated)
    end
end

function refresh(con::Connection; force=:none)
    local changes = find_outgoing_updates(con; force)
    if !isempty(changes)
        try
            verbose(con, "CALLING SEND UPDATES")
            send_updates(con, changes)
        catch e
            check_sigint(e)
            err = e
        end
    end
end

function check_outgoing_updates(con::Connection)
    local failures = 0
    local sleepcount = 0
    verbose(con, "\nCHECKING UPODATES, SLEEP PERIOD: $(outgoing_update_period(con))\n")
    try
        while isopen(con.channel)
            try
                local period = outgoing_update_period(con)
                if con.lastcheck + period - time() > 0
                    sleepcount += 1
                    (sleepcount == 11 || sleepcount == 101) &&
                        verbose(con, "SLEPT MORE THAN $(sleepcount - 1) TIMES")
                    sleep(period / 10)
                    continue
                end
                #verbose(con, "\nFINISHED WAITING\n")
                sleepcount = 0
                local err = nothing
                # sync here so it doesn't spin out of control
                sync(con) do
                    refresh(con)
                end
                !isnothing(err) && throw(err)
                failures = 0
            catch err
                check_sigint(err)
                @error "Error updating" exception = (err, catch_backtrace())
                failures += 1
                # three failures in a row disables the connection
                if failures > 3
                    shutdown(con)
                end
            end
        end
    finally
        verbose(con, "\nDONE CHECKING UPODATES\n")
    end
end

shutdown(con::Connection) = close(con.channel)

"HOOK"
function send_updates(::Connection, changes::Dict{String,Any})
    error("UNDEFINED HOOK: send_updates")
end

end
