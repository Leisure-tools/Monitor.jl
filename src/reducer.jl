module Reducers
using JSON3
using OrderedCollections
using ..Monitor: Monitor, Connection, json, verbose, is_target

@kwdef struct Reducer
    data::JSON3.Object
    func::Function
    reduce_topics::Set{String} = Set{String}()
end

@kwdef struct ReducingPeer{T}
    data::T
    reducers::Dict{Symbol, Reducer} = Dict{Symbol, Function}()
end

Monitor.incoming_update_period(con::Connection, data::ReducingPeer) =
    Monitor.incoming_update_period(con, data.data)

Monitor.get_updates(con::Connection, data::ReducingPeer, wait_period::Float64) =
    Monitor.get_updates(
        con, data.data, wait_period
    )::Union{Nothing, OrderedDict{Symbol, JSON3.Object}}

Monitor.send_updates(con::Connection, data::ReducingPeer, outgoing::OrderedDict) =
    Monitor.send_updates(con, data.data, outgoing)

Monitor.init(con::Connection, data::ReducingPeer; kw...) =
    Monitor.init(con, data.data; kw...)

Monitor.is_target(con::Connection, data::ReducingPeer, block::JSON3.Object) =
    Monitor.is_target(con, data.data, block)

function Monitor.handle_monitor(con::Connection{ReducingPeer}, name::Symbol, mon::JSON3.Object)
    Monitor.is_target(con, mon) &&
        verbose(1, con, "RECEIVING UNTRANSFORMED MONITOR BLOCK: ", mon)
    if !Monitor.is_target(con, mon)
        verbose(1, con, "GOT MONITOR BLOCK: ", mon)
        block = OrderedDict(
            :name => mon.name,
            :type => "data",
            :value => mon.value,
        )
        mon = Monitor.json(block)
        verbose(1, con, "TRANSFORMED MONITOR BLOCK INTO DATA: ", mon)
    end
end

function Monitor.handle_code(con::Connection{ReducingPeer}, name::Symbol, code::JSON3.Object)
    #if code.target == con.
    func = Monitor.base_handle_code(con, name, code)
    if func isa Function
        verbose(1, con, "INSTALLING REDUCER: ", func)
        con.data.reducers[name] = func

    end
end

end
