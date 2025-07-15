"""
Data is encoded as key-json pairs in each streaming record
"""
module RedisMonitor

using Monitor: Monitor, Connection, JSON3, verbose, json
using Monitor.UUIDs
using Base.ScopedValues
using Redis, Dates
using OrderedCollections: OrderedDict

const MONITOR_PREFIX = "MONITOR-"

include("cmds.jl")

mutable struct RedisCon
    redis::Redis.RedisConnection
    peerid::String
    input_streams::Dict{String, String}
    output_stream::String
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
        Dict(
            "$MONITOR_PREFIX$s" => "0" for s in input_streams
        ),
        "$MONITOR_PREFIX$output_stream",
    )
end

function listen(con::Connection{RedisCon}, stream::AbstractString)
    con.data.input_streams[stream] = "0"
end

function stop_listening(con::Connection{RedisCon}, stream::AbstractString)
    delete!(con.data.input_streams, stream)
end

Monitor.incoming_update_period(::Connection{RedisCon}) = 5.0

# note if we need to group by stream, we can override handle_updates
function Monitor.get_updates(con::Connection{RedisCon}, timeout::Float64)
    #@info "GETTING UPDATES" con.data.input_streams timeout block=timeout
    local updates = xread(con.data.redis, con.data.input_streams; block=timeout)

    (updates isa AbstractString || isnothing(updates)) &&
        return nothing
    local dict = OrderedDict{Symbol, JSON3.Object}()
    @info "GOT UPDATES" updates
    for (stream, result) in updates
        for (id, update) in result
            con.data.input_streams[stream] = id
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
    return dict
end

function Monitor.send_updates(con::Connection{RedisCon}, outgoing::OrderedDict)
    isempty(outgoing) &&
        return nothing
    output = con.data.output_stream
    topics = Set(
        str
        for (name, data) in outgoing
        for mon in (con.monitors[name],)
        for str in mon.update_topics
    )
    @info "SENDING UPDATES" topics
    for (name, value) in outgoing
        @info "   ITEM" name value
    end
    push!(topics, "")
    for topic in topics
        blocks = [
            merge!(Dict{Symbol, Any}(:name => name), block)
            for (name, block) in outgoing
            for mon in (con.monitors[name],)
            if (topic == "" && isempty(mon.update_topics)) || topic ∈ mon.update_topics
        ]
        if !isempty(blocks)
            if topic == ""
                topic = con.data.output_stream
            else
                topic = "$(con.data.output_stream)-$topic"
                @info "SENDING ON $topic" blocks
            end
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

function start(name::String, output_stream::String, first_input_stream::String=output_stream,
    more_inputs::String...
    ; verbosity=0, roots=Dict{Symbol, Any}())
    Monitor.start(
        name,
        RedisCon(;
            redis=Redis.RedisConnection(),
            output_stream="$MONITOR_PREFIX$output_stream",
            input_streams=Dict(
                "$MONITOR_PREFIX$s" => "0" for s in [first_input_stream, more_inputs...]
            ),
        );
        roots,
        verbosity,
    )
end

end
