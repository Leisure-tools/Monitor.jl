"""
# Monitor.jl
Listen to and update monitors on Julia.

Monitor.jl is a pubsub system that communicates with "blocks" (JSON objects) which allow you to
monitor and change data values in subscribing Julia programs.

![arch](arch.png)

![screencap](screencap.gif)

# Capabilities

Monitor.jl communicates with simple JSON structures called “blocks” to

* Dynamically monitor Juila data, automatically detect when it changes, and report the new values  
  * You use the connection to make and remove monitors in running programs  
  * Polls for value changes – optionally specify how often to check values  
  * Polling means you do **not** need to alter your code to inform the system when a value changes  
* Modify mutable values  
* Evaluate code  
* Exchange data between programs  
* Treat programs like ”database shards”  
* Optionally target a subset of subscribers  
* Communicate via  
  * Pubsub with REDIS streams  
  * Named pipes  
  * Easy to extent with custom transports

Monitor.jl can publish to different topics to

* Evaluate code and select data to monitor  
* Send updates to monitored values  
* Control programs for rollups or MapReduce

Connected UIs, like notebooks, can present data blocks in a meaningful way

* Blocks can update in-place  
* Eval blocks can be sent with a button click  
* Blocks can appear in sections based on tags  
* Views can create more monitors and change monitored values

Monitor.jl supports 4 types of blocks with some of these common properties:

* **`type:`** how a subscriber should interpret the block. Supported types are "monitor", "code", "data", and "delete".  
* **`name:`** This block’s name; receivers overwrite the previous values for duplicates  
* **`origin:`** ID of the subscriber that produced this block  
* **`topics:`** optional topic(s) to publish the block to – transports should route these  
* **`tags:`** identifies sets of blocks this block “belongs to”. Can be a string or an array of strings; monitor and data blocks can use this to categorize results and also for cleanup  
* **`targets:`** optional subscriber or list of subscribers that should receive the block (others ignore it)  

Block types:  

* Monitor blocks -- monitor Julia values  
  * **`type:`** "monitor"
  * **`topic:`** controls which program(s) install the monitor
  * **`targets:`** controls which program(s) install the monitor
  * **`updateTopics:`** controls which programs receive updates  
      A UI sending a monitor with this property should automatically subscribe to the topics.  
  * **`updateTargets:`** controls which programs receive updates  
  * **`tags:`** identifyies a set of blocks. Can be a string or an array of strings  
  * **`root:`** root value for the variables  
  * **`quiet:`** while true, monitor but don't publish updates  
  * **`value:`** variables that monitor values  
      Initial values are not placed into Julia but incoming changes are not placed into Julia data  

* Code blocks -- run Julia code  
  * **`type:`** "code"  
  * **`topic:`** optional topic to publish the block to, when it’s not the default  
  * **`targets:`** optional list of subscribers that should receive the block (others ignore it)  
  * **`tags:`** identifyies a set of blocks. Can be a string or an array of strings  
  * **`language:`** language in which to evaluate code  
  * **`return:`** true if the code should return a block to be published.  
  * **`value:`** code to evaluate  

* Data Blocks -- hold data, can be used for responses, 
  * **`type:`** "data"  
  * **`topic:`** optional topic to publish the block to, when it’s not the default  
  * **`targets:`** optional list of subscribers that should receive the block (others ignore it)  
  * **`tags:`** identifyies a set of blocks. Can be a string or an array of strings  
  * **`code:`** optional name of the code block that produced this block, if was one  
  * **`value:`** value of data  

* Delete Blocks
  * **`type:`** "delete"  
  * **`topic:`** optional topic to publish the block to, when it’s not the default  
  * **`targets:`** optional list of subscribers that should receive the block (others ignore it)  
  * **`value:`** NAME, [NAME, ...], {"tagged": TAG}, or {"tagged": [TAG, ...]}  

# API:

```julia
start(con::Connection; roots::Dict{Symbol,Any}=Dict(), verbosity=0)

send(data) -- queue an update to send out

shutdown() -- close the connection
```

ADDING YOUR OWN TRANSPORTS

You can make your own transport by implementing two required handlers:

```julia
# returns updates, an iterator of Symbol=>JSON3.Object, this is allowed to block
get_updates(con::Connection{T}, wait_time::Float64)

# send updates out, this is allowed to block
send_updates(con::Connection{T}, changes::Dict{Symbol})
```

Optional handlers:

```julia
# initialize a newly created connection
init(::Connection)

# returns the time to wait between refreshes
incoming_update_period(::Connection)

# returns the time to wait before sending out pending publishes
outgoing_update_period(::Connection)

# returns whether there are pending updates
has_updates(::Connection, ::UpdateType)

# Receive a monitor block, by default just calls base_handle_monitor(con, name, mon)
handle_monitor(con::Connection, name::Symbol, mon::JSON3.Object) =

# Receive an eval block, by default just calls base_handle_eval(con, name, ev)
handle_eval(con::Connection, name::Symbol, ev::JSON3.Object)

# Receive a data block, by default just calls base_handle_data(con, name, data)
handle_data(con::Connection, name::Symbol, data::JSON3.Object) =

# Delete data blocks, by default just calls base_handle_delete(con, data)
handle_delete(con::Connection, del::JSON3.Object) =
```
"""
module Monitor

using UUIDs, JSON3
import Base.Threads.@spawn
using Base.ScopedValues

const astr = AbstractString

include("types.jl")
include("vars.jl")
include("update.jl")

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
    spawn::Union{Bool,Nothing} = true,
    checkincoming = true,
    checkoutgoing = true,
    verbosity = 0,
) where {T}
    con = Connection(data; verbosity)
    con.env.roots = roots
    init(con)
    with(CURRENT_CONNECTION => con) do
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
    end
    return con
end

"Run code in the connection thread synchronously"
function sync(f::Function, con::Connection)
    local result = Channel{Any}()
    local exception = nothing
    async(con) do
        try
            put!(result, f())
        catch err
            exception = err
            put!(result, nothing)
            rethrow()
        end
    end
    local value = take!(result)
    !isnothing(exception) &&
        throw(exception)
    return value
end

"Run code in the connection thread asynchronously"
async(f::Function, con::Connection) = put!(con.channel, f)

"""
Process commands in the connection thread

This is used for getting and setting data values.
"""
function run_connection(con::Connection)
    local failures = 0
    local failurebase = 1
    while isopen(con.channel)
        try
            take!(con.channel)()
            failures = 0
            failurebase = 1
        catch err
            check_sigint(err)
            @error "Error processing command: $err" exception = (err, catch_backtrace())
            failures += 1
            if failures == 10 ^ failurebase
                @error "$failures errors in a row"
                failurebase += 1
            elseif failures > 3
                @error "Muting errors because there were more than 3 in a row"
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
            if has_updates(con, updates)
                verbose(con, "UPDATE: $updates \n  $(typeof(updates))")
                local updated = Set{Symbol}()
                sync(con) do
                    handle_updates(con, updates, updated)
                    refresh(con; force = updated)
                end
            end
        catch err
            check_sigint(err)
            @error "Error updating" exception = (err, catch_backtrace())
        end
    end
end

has_updates(::Connection, updates) = !isnothing(updates) && !isempty(updates)

function handle_updates(con::Connection, updates::JSON3.Object, updated::Set{Symbol})
    for (name, change) in updates
        handle_update(con, name, change, updated)
    end
end

send(name::Symbol, data) = send(CURRENT_CONNECTION[], name, data)

send(::Nothing, ::Symbol, ::Any) =
    try
        error("Attempt to queue to a shut down connection")
    catch err
        @info err exception = (err, catch_backtrace())
    end

function send(con::Connection, name::Symbol, data::Any)
    verbose(con, "Adding update MONITOR ", name, " VALUE ", json(data))
    con.outgoing[name] = json(data)
end

json(value) = JSON3.read(JSON3.write(value))

function refresh(con::Connection; force = :none)
    verbose(2, con, "checking for changes")
    find_outgoing_updates(con; force)
    if !isempty(con.outgoing)
        local updates = Dict(con.outgoing)
        empty!(con.outgoing)
        try
            verbose(con, "CALLING SEND UPDATES")
            send_updates(con, updates)
        catch e
            check_sigint(e)
            err = e
            @error "Error sending update: $err" exception = (err, catch_backtrace())
        end
    end
end

function check_outgoing_updates(con::Connection)
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
                    if con.indicate_start
                        con.indicate_start = false
                        println("READY")
                    end
                end
                !isnothing(err) && throw(err)
            catch err
                check_sigint(err)
                @error "Error updating" exception = (err, catch_backtrace())
            end
        end
    finally
        verbose(con, "\nDONE CHECKING UPODATES\n")
    end
end

shutdown() = shutdown(CURRENT_CONNECTION[])

shutdown(::Nothing) = @info("Attempt to shut down a connection that is already shut down")

function shutdown(con::Connection)
    close(con.channel)
    CURRENT_CONNECTION[] = nothing
end

"HOOK"
function send_updates(::Connection, ::Dict)
    error("UNDEFINED HOOK: send_updates")
end

end
