Base.exit_on_sigint(false)

using Monitor: start, check_sigint, current_connection, Monitor

include("pipey.jl")

@kwdef mutable struct Person
    name::String
    number::String
end

current_person = Person("Herman", "1313")

function test(
    input_pipe_name::String = ARGS[1],
    output_pipe_name::String = ARGS[2];
    verbosity = 1,
)
    try
        #println("listening...")
        start(
            Pipey.PipeCon(; input_pipe_name, output_pipe_name);
            #roots = Dict(:person => Person("Herman", "1313")),
            roots = Dict{Symbol,Any}(),
            verbosity,
        )
        #println("started...")
    catch err
        check_sigint(err)
        @error err exception = (err, catch_backtrace())
    end
end
