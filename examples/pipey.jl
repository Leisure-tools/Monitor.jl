module Pipey

using Control: Control, Connection, JSON3, verbose
import Control: get_updates, send_updates
import Base.Threads.@spawn

@kwdef mutable struct PipeCon
    input_pipe_name::String
    output_pipe_name::String
    input::Union{Nothing,IO} = nothing
end

function get_updates(con::Connection{PipeCon}, ::Float64)
    verbose(con, "WAITING FOR UPDATE")
    local changes = Dict()
    #local input = read(con.data.input_pipe_name, String)
    local input = read(`cat $(con.data.input_pipe_name)`, String)
    verbose(con, "GOT INPUT: ", input)
    for line in split(input, "\n")
        isempty(line) &&
            continue
        verbose(con, "JSON: $line")
        local change = JSON3.read(line, Dict)
        merge!(changes, change)
    end
    return JSON3.read(JSON3.write(changes))
end

function send_updates(con::Connection{PipeCon}, changes::Dict{Symbol})
    @spawn begin
        verbose(con, "PIPEY SENDING UPDATES")
        #local output = open(con.data.output_pipe_name, "w")
        local output = open(pipeline(`cat`, stdout=con.data.output_pipe_name), "w")
        write(output, JSON3.write(changes))
        close(output)
    end
end

end
