Base.exit_on_sigint(false)

using MonitorRedis
using MonitorRedis.Monitor: Monitor, start, check_sigint, current_connection
using Redis: Redis

include("src/MonitorRedis.jl")

using .RedisMonitor: RedisMonitor, RedisMonitor.MONITOR_PREFIX

@kwdef mutable struct Person
    name::String
    number::String
end

current_person = Person("Herman", "1313")

function test(
    output_stream::String, first_input_stream::String, input_streams::String...
    ; verbosity=1,
)
    try
        RedisMonitor.start("test", output_stream, first_input_stream, input_streams...; verbosity)
    catch err
        check_sigint(err)
        @error err exception = (err, catch_backtrace())
    end
end
