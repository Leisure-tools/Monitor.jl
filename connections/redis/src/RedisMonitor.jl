"""
Data is encoded as key-json pairs in each streaming record
"""
module RedisMonitor

using Monitor: Monitor, Connection, JSON3, verbose, json, sync, async, current_connection,
    process_incoming_update, current_runner, spawn
using Monitor.UUIDs
using Base.ScopedValues
using Redis
using Dates
using OrderedCollections: OrderedDict

const MONITOR_PREFIX = "MONITOR-"

include("cmds.jl")

mutable struct RedisCon
    redis::Redis.RedisConnection
    peerid::String
    original_input_streams::Vector{String}
    input_streams::Dict{String, String}
    output_stream::String
    last_topics::Set{String} # track by topic name (without "MONITOR-" prefix)
    watch_count::Int64
    output_channel::Channel{Union{Nothing, OrderedDict}}
    service::Channel{Function}
end

function RedisCon(;
    redis=Redis.RedisConnection,
    peerid=string(uuid4()),
    output_stream::String,
    input_streams::Vector{String}=[output_stream],
)
    RedisCon(
        redis,
        peerid,
        ["$MONITOR_PREFIX$s" for s in input_streams],
        Dict(
            "$MONITOR_PREFIX$s" => "0" for s in input_streams
        ),
        "$MONITOR_PREFIX$output_stream",
        Set{String}(),
        0,
        Channel{Union{Nothing, OrderedDict}}(10),
        Channel{Function}(10),
    )
end

function listen(con::Connection{RedisCon}, stream::AbstractString)
    con.data.input_streams[stream] = "0"
end

function stop_listening(con::Connection{RedisCon}, stream::AbstractString)
    delete!(con.data.input_streams, stream)
end

Monitor.incoming_update_period(::Connection{RedisCon}) = 5.0

function require_control(con::Connection{RedisCon}, has=true)
    (current_connection[] == con) != has &&
        error("Connection $(has ? "not" : "should not be") in control of task: ", con.name)
end

function require_control(chan::Channel, has=true)
    (current_runner[] == chan) != has &&
        error("Runner $(has ? "not" : "should not be") in control of task: ", con.name)
end

function all_topics(con::Connection{RedisCon})
    require_control(con)
    return Set(
        topic
        for (_, mon) in con.monitors
        for topic in Iterators.flatten((mon.topics, mon.update_topics))
    )
end

function has_topic(con::Connection{RedisCon}, block, topic)
    if topic == ""
        topic = con.data.output_stream
    end
    #@info "CHECKING BLOCK" block
    btop = get(block, :topics, nothing)
    if isnothing(btop) || btop == ""
        return topic == con.data.output_stream
    elseif btop isa String
        return btop == topic
    end
    return topic ∈ btop
end

function check_topics(con::Connection{RedisCon}; update=true)
    async(con, con.com_channel) do
        topics = all_topics(con)
        async(con, con.data) do
            if topics != con.data.last_topics
                prefix = con.data.original_input_streams[1]
                verbose(con, "TOPICS CHANGED FROM ", con.data.last_topics, " TO ", topics)
                con.data.last_topics = topics # track by topic name (without "MONITOR-" prefix)
                streams = Set("$prefix-$topic" for topic in topics)
                old = setdiff(keys(con.data.input_streams), con.data.original_input_streams)
                setdiff!(old, streams)
                !isempty(old) &&
                    verbose(con, "ABANDONING STREAMS: $(join(old, " "))")
                for topic in old
                    delete!(con.data.input_streams, topic)
                end
                setdiff!(streams, keys(con.data.input_streams))
                !isempty(streams) &&
                    verbose(con, "ADDING STREAMS: $(join(streams, " "))")
                for stream in streams
                    con.data.input_streams[stream] = "0"
                end
                # invalidate the current watch and start another
                con.data.watch_count += 1
                update &&
                    spawn() do
                        process_incoming_update(con)
                    end
            end
        end
    end
end

# note if we need to group by stream, we can override handle_updates
function Monitor.get_updates(con::Connection{RedisCon}, timeout::Float64)
    async(con, con.data) do
        topics = con.data.last_topics
        count = con.data.watch_count
        spawn() do
            strs = []
            ids = []
            for (str, id) in con.data.input_streams
                push!(strs, str)
                push!(ids, id)
            end
            push!(strs, ids...)
            verbose(
                2,
                con,
                "GETTING UPDATES BLOCK STREAMS $timeout ($(join(strs, " ")))",
            )
            t = time()
            updates = nothing
            errs = 0
            while isnothing(updates) && time() - t < timeout
                try
                    updates = xread(con.data.redis, con.data.input_streams; block=0.002)
                    errs = 0
                catch err
                    errs += 1
                    if errs > 3
                        @error "$errs ERRORS IN A ROW DURING XREAD"
                        exit(1)
                    end
                end
                sleep(0.1)
            end
            if count != con.data.watch_count || isnothing(updates)
                # if watch_count has changed, discard the results, do not update the stream counters, etc.
                count != con.data.watch_count &&
                    @info "DISCARDING DATA BECAUSE WATCH_COUNT INCREASED" updates
                put!(con.data.output_channel, nothing)
            else
                #@info "HANDLING DATA" updates
                put!(con.data.output_channel, handle_updates(con, updates))
            end
        end
    end
    try
        return take!(con.data.output_channel)
    catch err
        @error "ERROR GETTING REDIS DATA" exception = (err, catch_backtrace())
        return nothing
    end
end

function handle_updates(con::Connection{RedisCon}, updates)
    (updates isa AbstractString || isnothing(updates)) &&
        return nothing
    local dict = OrderedDict{Symbol, JSON3.Object}()
    local streams = Dict{String, String}()
    #@info "GOT UPDATES" updates
    for (stream, result) in updates
        isnothing(result) &&
            continue
        for (id, update) in result
            #con.data.input_streams[stream] = id
            streams[stream] = id
            if length(update) != 4
                error(
                    """Illegal stream format, items must be "batch", [{name:block...}...], "sender", ID"""
                )
            end
            local info = Dict(Iterators.partition(update, 2))
            if !haskey(info, "batch") && !haskey(info, "sender")
                error(
                    """Illegal stream format, items must be "batch", [{name:block...}...], "sender", ID"""
                )
            end
            info["sender"] == con.data.peerid &&
                continue
            if info["batch"] != "null"
                local batch = try
                    JSON3.read(info["batch"])
                catch err
                    error("Error decoding batch: $(info["batch"]): $err")
                end
                for item in batch
                    if haskey(item, :target)
                        t = item.target
                        id = con.data.peerid
                        t != id && !(t isa Vector && id ∈ t) &&
                            continue
                    end
                    dict[Symbol(item.name)] = item
                end
            end
        end
    end
    async(con, con.data) do
        merge!(con.data.input_streams, streams)
    end
    check_topics(con)
    return dict
end

function Monitor.send_updates(con::Connection{RedisCon}, outgoing::OrderedDict)
    isempty(outgoing) &&
        verbose(2, con, "NO UPDATE TO SEND")
    isempty(outgoing) &&
        return nothing
    check_topics(con)
    verbose(2, con, "SENDING UPDATE")
    output = con.data.output_stream
    topics = Set(
        str
        for (name, data) in outgoing
        for str in Monitor.prop_list(data, :topics, [con.data.output_stream])
    )
    verbose(2, con, "SENDING UPDATES: ", topics)
    for (name, value) in outgoing
        verbose(2, con, "   ITEM name: ", name, " value: ", value)
    end
    push!(topics, "")
    for topic in topics
        #@info "CHECKING BLOCKS" outgoing
        blocks = [
            block
            for (name, block) in outgoing
            if has_topic(con, block, topic)
        ]
        if !isempty(blocks)
            if topic == ""
                topic = con.data.output_stream
            elseif topic != con.data.output_stream
                topic = "$(con.data.output_stream)-$topic"
                verbose(2, con, "SENDING ON $topic: ", blocks)
            end
            spawn() do
                xadd(
                    con.data.redis,
                    topic,
                    "*",
                    "batch",
                    JSON3.write(blocks),
                    "sender",
                    con.data.peerid,
                )
            end
        end
    end
end

Monitor.sync(f::Function, con::Connection, rcon::RedisCon) =
    Monitor.sync(f, con, rcon.service)

Monitor.async(f::Function, con::Connection, rcon::RedisCon; wrap::Symbol=:die) =
    Monitor.async(f, con, rcon.service; wrap)

function init(con::Connection{RedisCon})
    @info "SPAWNING REDIS CONNECTION"
    spawn() do
        try
            @info "STARTING REDIS CONNECTION"
            Monitor.run_connection("REDIS", con, con.data.service)
        catch err
            @error "Error running connection" exception = (err, catch_backtrace())
        end
    end
    check_topics(con; update=false)
end

function start(name::String, output_stream::String, first_input_stream::String=output_stream,
    more_inputs::String...
    ; verbosity=0, roots=Dict{Symbol, Any}())
    rcon = RedisCon(;
        redis=Redis.RedisConnection(),
        output_stream="$MONITOR_PREFIX$output_stream",
        input_streams=Dict(
            "$MONITOR_PREFIX$s" => "0" for s in [first_input_stream, more_inputs...]
        ),
    )
    @info "STARTING MONITOR"
    Monitor.start(init, name, rcon; roots, verbosity)
end

end
