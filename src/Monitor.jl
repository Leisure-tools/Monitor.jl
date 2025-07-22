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
  * Easy to extend with custom transports

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
* **`targets:`** optional subscriber or list of subscribers that should act on the block (others track it)  

Block types:  

* Monitor blocks -- monitor Julia values  
  * **`type:`** "monitor"  
  * **`rename`** rename this monitor to the variable's value before publishing
  * **`topics:`** controls which program(s) install the monitor  
  * **`targets:`** controls which program(s) install the monitor  
  * **`updateTopics:`** specifies the topics to publish this monitor to when it changes  
      A UI sending a monitor with this property should automatically subscribe to the topics  
      unless the topics are reduce topics, in which case only the reduction peers should subscribe  
  * **`updateTargets:`** controls which programs receive updates  
  * **`tags:`** identifyies a set of blocks. Can be a string or an array of strings  
  * **`root:`** root value for the variables  
  * **`quiet:`** while true, monitor but do not publish updates
      Code can depend on the values in the monitor and potentially turn off "quiet"
  * **`value:`** variables that monitor values  
      Initial values are not placed into Julia but incoming changes are not placed into Julia data  

* Code blocks -- run Julia code  
  * **`type:`** "code"  
  * **`topics:`** optional topic to publish the block to, when it’s not the default  
  * **`reduce:`** optional list of topics to listen on that most other peers should ignore  
  * **`targets:`** optional list of subscribers that should receive the block (others should ignore it)  
  * **`tags:`** identifyies a set of blocks. Can be a string or an array of strings  
  * **`language:`** language in which to evaluate code  
  * **`return:`** true if the code should return a block to be published.  
  * **`value:`** code to evaluate  
  * **`updateTopics:`** specifies the topics to publish the return value to
      A UI sending a monitor with this property should automatically subscribe to the topics.  
  * **`updateTargets:`** controls which programs receive updates

* Data Blocks -- hold data, can be used for responses, 
  * **`type:`** "data"  
  * **`topics:`** optional topic to publish the block to, when it’s not the default  
  * **`targets:`** optional list of subscribers that should receive the block (others ignore it)  
  * **`tags:`** identifyies a set of blocks. Can be a string or an array of strings  
  * **`code:`** optional name of the code block that produced this block, if was one  
  * **`value:`** value of data  

* Delete Blocks
  * **`type:`** "delete"  
  * **`topics:`** optional topic to publish the block to, when it’s not the default  
  * **`targets:`** optional list of subscribers that should receive the block (others ignore it)  
  * **`value:`** NAME, [NAME, ...], {"tagged": TAG}, or {"tagged": [TAG, ...]}  

# API:

```julia
start(con::Connection; roots::Dict{Symbol,Any}=Dict(), verbosity=0, init_func=Monitor.init)

send(data) -- queue an update to send out

shutdown() -- close the connection

current_connection[]::Connection -- get the current connection, useful for adding streams, etc.

sync(func) -- run a function synchronusly within the monitor thread, returning the result

async(func) -- run a function asynchronously within the monitor thread
```

ADDING YOUR OWN TRANSPORTS

You can make your own transport by implementing two required handlers:

```julia
# returns updates, an iterator of Symbol=>JSON3.Object, this is allowed to block
get_updates(con::Connection{T}, wait_time::Float64)

# send updates out, this is allowed to block
send_updates(con::Connection{T}, changes::OrderedDict{Symbol})
```

Optional handlers:

```julia
# initialize a newly created connection
init(::Connection)

# returns the time to wait between refreshes
incoming_update_period(con::Connection{T}, data::T)

# returns the time to wait before sending out pending publishes
outgoing_update_period(con::Connection{T}, data::T)

# returns whether there are pending updates
has_updates(con::Connection{T}, data::T, ::UpdateType)

# Receive a monitor block, by default just calls base_handle_monitor(con, name, mon)
handle_monitor(con::Connection{T}, data::T, name::Symbol, mon::JSON3.Object) =

# Receive an code block, by default just calls base_handle_code(con, name, ev)
handle_code(con::Connection{T}, data::T, name::Symbol, ev::JSON3.Object)

# Receive a data block, by default just calls base_handle_data(con, name, data)
handle_data(con::Connection{T}, data::T, name::Symbol, data::JSON3.Object) =

# Delete data blocks, by default just calls base_handle_delete(con, data)
handle_delete(con::Connection{T}, data::T, del::JSON3.Object) =
```
"""
module Monitor

using UUIDs, JSON3
import Base.Threads.@spawn
using Base.ScopedValues: ScopedValue, with
using OrderedCollections: OrderedDict
using Dates

const astr = AbstractString
const MONITOR_KEYS = Set((:root, :update, :quiet, :updatetopics))

# hooks
function incoming_update_period end
function get_updates end
function send_updates end
function init end

include("types.jl")
include("vars.jl")
include("update.jl")
include("reducer.jl")

verbosity(env::VarEnv{Connection}) = env.data.verbosity

verbose(con, args...) = verbose(1, con, args...)

verbose(level::Int64, env::VarEnv{Connection{T}}, args...) where {T} =
    verbose(level, env.data, args...)

function verbose(level::Int64, con::Connection, args...)
    if con.verbosity >= level
        println(args...)
    end
end

check_sigint(err) = err isa InterruptException && exit(1)

"Initialize a new connection right after it is created"
init(con::Connection) = init(con, con.data)

init(::Connection, _) = nothing

"""
Creat a connection and start it by default

The `spawn` keyword can be
- `true`: run the connection in a spawned thread
- `false`: immediately use this thread to run the connection -- does not return until closed
- `nothing`: do not run the connection. The caller will arrange to run it
"""
function start(
    name::String,
    data::T;
    init_func::Function=init,
    roots::Dict{Symbol}=Dict{Symbol}(),
    spawn::Union{Bool, Nothing}=true,
    verbosity=0,
    use_accounting=false,
) where {T}
    con = Connection(name, data; verbosity, use_accounting)
    con.env.roots = roots
    init_func(con)
    run_accounting(con)
    with(current_connection => con) do
        # put incoming updates into con.incoming_blocks
        Monitor.spawn() do
            check_incoming_updates(con)
        end
        # 
        Monitor.spawn() do
            process_updates(con)
        end
        run_connection("INPUT", con, con.update_input; spawn=true)
        run_connection("OUTPUT", con, con.update_output; spawn=true)
        if !isnothing(spawn)
            if spawn
                verbose(
                    2, con, "SPAWNING CONNECTION RUNNER, CURRENT CONNECTION: ", current_connection[]
                )
                run_connection("CONNECTION", con; spawn=true)
                run_connection("REFRESH", con, con.refresh_channel; spawn=true)
            else
                verbose(con, "RUNNING CONNECTION")
                @async run_connection("CONNECTION", con)
                run_connection("REFRESH", con, con.refresh_channel)
            end
        end
    end
    return con
end

function run_accounting(con::Connection)
    spawn() do
        run_connection("ACCOUNTING", con, con.accounting_channel; use_accounting=false)
    end
    Timer(2; interval=2) do t
        local old = Set{Int64}()
        local cur = time()
        async(con, con.accounting_channel) do
            for (i, tup) in con.accounting
                t, _ = tup
                if cur - t > LONG_TIME && !haskey(con.griped, i)
                    con.griped[i] = tup
                    push!(old, i)
                end
            end
            filter!(con.griped) do (i, (t, chan, f, tracking))
                if !haskey(con.accounting, i)
                    tim = Dates.format(unix2datetime(t), "H:M:S.s")
                    name = con.channel_names[chan]
                    println("FINISHED $name #$i ($tim): $f")
                    return false
                elseif i ∈ old
                    t, chan, f = con.griped[i]
                    tim = Dates.format(unix2datetime(t), "H:M:S.s")
                    name = con.channel_names[chan]
                    @warn "LONG RUNNING TIME $name #$i ($tim): $f" exception = tracking
                end
                return true
            end
        end
    end
end

function accounting_start(con::Connection, chan::Channel, func::Function, tracking::Tuple)
    num = Ref{Int64}()
    async(con, con.accounting_channel) do
        num[] = con.exec_num
        con.accounting[con.exec_num] = (time(), chan, func, tracking)
        con.exec_num += 1
    end
    return num
end

function accounting_finish(con, num)
    async(con, con.accounting_channel) do
        delete!(con.accounting, num[])
    end
end

function in_connection(f::Function; wrap=:die)
    async(f, current_connection[], current_connection[].refresh_channel; wrap)
end

# this could be called in the main loop instead of spawning check_outgoing_updates
function update(con::Connection=current_connection[])
    updates = Dict{Symbol, Any}()
    updated = Set{Symbol}()
    sync(con, con.com_channel) do
        merge!(updates, con.incoming_blocks)
        empty!(con.incoming_blocks)
    end
    sync(con, con.refresh_channel) do
        # refresh first
        refresh(con; force=updated)
        # next handle incoming blocks, which removes monitor var changes from con.env
        handle_incoming_blocks(con, updates, updated)
    end
    # send out monitors with changed vars
    send_changes(con)
end

"Run code in the connection thread synchronously"
sync(f::Function, con::Connection=current_connection[]) = sync(f, con, con.refresh_channel)
function sync(f::Function, con::Connection, chan::Channel{Function})
    current_runner[] == chan &&
        return f()
    local result = Channel{Any}()
    local exception = nothing
    local exec_num

    async(con, chan; wrap=:none) do
        try
            put!(result, invokelatest(f))
        catch err
            exception = err
            put!(result, nothing)
            rethrow()
        end
    end
    local value = take!(result)
    !isnothing(exception) && throw(exception)
    return value
end

"Run code in the connection thread asynchronously"
async(f::Function) = async(f, current_connection[])
async(f::Function, con::Connection) = async(f, con, con.refresh_channel)
function async(
    f::Function, con::Connection, chan::Channel{Function}; wrap::Symbol=:die
)
    f2 = f
    if wrap != :none
        f2 = function ()
            try
                f()
            catch err
                @error "Error in async call, shutting down" exception = (err, catch_backtrace())
                if wrap == :die
                    exit(1)
                end
            end
        end
    end
    f3 = if chan ∈ con.account
        tracking = num = try
            error("backtrace")
        catch err
            (err, catch_backtrace())
        end
        () -> begin
            num = accounting_start(con, chan, f, tracking)
            try
                f2()
            finally
                accounting_finish(con, num)
            end
        end
    else
        f2
    end
    put!(chan, f3)
end

"""
Process commands in the connection thread

This is used for getting and mutating shared state in the connection
"""
function run_connection(
    name::String, con::Connection, chan::Channel=con.com_channel; use_accounting=true, spawn=false
)
    if spawn
        return Monitor.spawn() do
            run_connection(name, con, chan; use_accounting, spawn=false)
        end
    end
    use_accounting && con.use_accounting &&
        push!(con.account, chan)
    cur = current_connection[]
    curname = isnothing(cur) ? "none" : cur.name
    verbose(
        con, "SPAWNED CONNECTION RUNNER $name, CURRENT CONNECTION ", curname
    )
    local failures = 0
    local failurebase = 1
    local total_failures = 0
    local count = 0
    async(con, con.accounting_channel) do
        con.channel_names[chan] = name
    end
    while isopen(chan)
        count += 1
        try
            with(current_connection => con, current_runner => chan) do
                f = take!(chan)
                invokelatest(f)
            end
            failures = 0
        catch err
            check_sigint(err)
            failures += 1
            total_failures += 1
            if failures < 4
                @error "Error processing command: $err" exception = (err, catch_backtrace())
            elseif failures == 4
                @error "Muting errors because there were more than 3 in a row"
            end
            if total_failures == 10^failurebase
                @error "$failures errors in total"
                failurebase += 1
            end
        end
    end
end

#incoming_update_period(::Connection) = 60.0 * 2
"Hook"
incoming_update_period(con::Connection) =
    incoming_update_period(con, con.data)

incoming_update_period(con::Connection, _) = 2

"Base hook that delegates to the monitor's data"
function get_updates(con::Connection, wait_period::Float64)
    get_updates(
        con, con.data, wait_period::Float64
    )::Union{Nothing, OrderedDict{Symbol, JSON3.Object}}
end

"Hook: retrieve current updates"
function get_updates(
    ::Connection, _, wait_period::Float64
)::Union{Nothing, OrderedDict{Symbol, JSON3.Object}}
    error("UNDEFINED HOOK: get_updates")
end

# put incoming updates into con.incoming_blocks
function check_incoming_updates(con::Connection)
    count = 0
    try
        while isopen(con.com_channel)
            count % 10 == 0 &&
                verbose(con, "UPDATE CHECK $count")
            sync(con, con.update_input) do
                process_incoming_update(con)
            end
            count += 1
        end
        @info "con channel closed"
    catch err
        @error "Error getting updates" exception = (err, catch_backtrace())
    end
end

# put incoming updates into con.incoming_blocks
function process_incoming_update(con::Connection)
    try
        con.stats.incoming_update_attempts += 1
        verbose(2, con, "GETTING UPDATES")
        local updates = get_updates(
            con, incoming_update_period(con, con.data)
        )::Union{OrderedDict{Symbol, JSON3.Object}, Nothing}
        if has_updates(con, updates)
            con.stats.incoming_updates += 1
            verbose(2, con, "UPDATE: $updates \n  $(typeof(updates))")
            sync(con, con.com_channel) do
                merge!(con.incoming_blocks, updates)
            end
        else
            verbose(2, con, "NO UPDATES")
        end
    catch err
        check_sigint(err)
        @error "Error updating" exception = (err, catch_backtrace())
        exit(1)
    end
end

has_updates(::Connection, ::Nothing) = false
has_updates(::Connection, updates) = !isempty(updates)

function handle_incoming_blocks(con::Connection, updates::AbstractDict, updated::Set{Symbol})
    updatekeys = sort!([k for k in keys(updates)]; by=string)
    for name in updatekeys
        change = updates[name]
        handle_incoming_block(con, name, change, updated)
    end
end

function send(data::NamedTuple)
    con = current_connection[]
    sync(con, con.refresh_channel) do
        send(con, data.name, data)
    end
end

function send(::Nothing, ::Symbol, ::Any)
    try
        error("Attempt to queue to a shut down connection")
    catch err
        @info err exception = (err, catch_backtrace())
    end
end

function send(con::Connection, name::Symbol, data::Any)
    verbose(2, con, "Adding update MONITOR ", name, " VALUE ", json(data))
    con.outgoing[name] = json(data)
end

json(value) = JSON3.read(JSON3.write(value))

function refresh(con::Connection; force=:none)
    con.stats.refreshes += 1
    #verbose(2, con, "checking for changes")
    find_outgoing_updates(con; force)
    if !isempty(con.outgoing)
        local updates = OrderedDict(con.outgoing)
        empty!(con.outgoing)
        try
            verbose(2, con, "CALLING SEND UPDATES")
            send_updates(con, updates)
        catch e
            check_sigint(e)
            err = e
            @error "Error sending update: $err" exception = (err, catch_backtrace())
        end
    end
end

function process_updates(con::Connection)
    local sleepcount = 0
    verbose(con, "\nCHECKING UPODATES, SLEEP PERIOD: $(outgoing_update_period(con))\n")
    try
        while isopen(con.com_channel)
            sync(con, con.update_output) do
                sleepcount = invokelatest(process_outgoing_update, con, sleepcount)
            end
        end
    catch err
        @error "Error processing update: $err" exception = (err, catch_backtrace())
    finally
        verbose(con, "\nDONE CHECKING UPODATES\n")
    end
end

#
function process_outgoing_update(con::Connection, sleepcount::Int64)
    try
        local period = outgoing_update_period(con)
        if con.lastcheck + period > time()
            sleepcount += 1
            (sleepcount == 11 || sleepcount == 101) &&
                verbose(con, "SLEPT MORE THAN $(sleepcount - 1) TIMES")
            sleep(period / 10)
            return sleepcount
        end
        #verbose(con, "\nFINISHED WAITING\n")
        sleepcount = 0
        local err = nothing
        update(con)
        if con.indicate_start
            con.indicate_start = false
            println("READY")
        end
        !isnothing(err) && throw(err)
    catch err
        check_sigint(err)
        @error "Error updating" exception = (err, catch_backtrace())
    end
    return sleepcount
end

shutdown() = shutdown(current_connection[])

shutdown(::Nothing) = @info("Attempt to shut down a connection that is already shut down")

function shutdown(con::Connection)
    close(con.com_channel)
end

"Base hook that delegates to data"
function send_updates(con::Connection, updates::OrderedDict)
    send_updates(con, con.data, updates)
end

"HOOK"
function send_updates(::Connection, _, ::OrderedDict)
    error("UNDEFINED HOOK: send_updates")
end

function spawn(f::Function, name="")
    @spawn try
        f()
    catch err
        @error "Error $(name != "" ? "in thread $name" : "")" exception = (err, catch_backtrace())
    end
end

end
