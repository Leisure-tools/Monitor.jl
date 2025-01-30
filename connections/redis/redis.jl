"""
Data is encoded as key-json pairs in each streaming record
"""
module RedisMonitor

using Monitor: Monitor, Connection, JSON3, verbose, json
using Base.ScopedValues
using Redis, Dates

include("cmds.jl")

@kwdef mutable struct RedisCon
    redis::Redis.RedisConnection
    input_streams::Vector{String}
    output_stream::String
end

# note if we need to group by stream, we can override handle_updates
function Monitor.get_updates(con::Connection{RedisCon}, timeout::Float64)
    local update = xread(con.data; block = millis(timeout), con.data.input_streams...)
    local dict = Dict{String,Any}()

    for (stream, result) in updates
        for (_, update) in result
            if length(update) != 2 || update[1] != "batch"
                error("""Illegal stream format, items must be "batch"=> [item...]""")
            end
            for (name, item) in JSON3.read(update[2], Dict)
                dict[name] = item
                item["stream"] = stream
            end
        end
    end
    return dict
end

function Monitor.send_updates(con::Connection{RedisCon}, outgoing::Dict)
    xadd(con.data.redis, con.data.output_stream, "*", "batch", JSON3.write(outgoing))
end

end
