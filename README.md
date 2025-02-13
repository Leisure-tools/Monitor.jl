# Monitor.jl
Listen to and update monitors on Julia.

Monitor.jl is a pubsub system that communicates with "blocks" (JSON objects) which allow you to
monitor and change data values in subscribing Julia programs.

![screencap](screencap.gif)

Block types:

  Monitor blocks -- monitor Julia values
    type: "monitor"
    origin: ID of the subscriber that produced this block
    topic: optional topic to publish the block to, when it’s not the default
    targets: optional list of subscribers that should receive the block (others ignore it)
    tags: identifyies a set of blocks. Can be a string or an array of strings
    root: root value for the variables
    value: variables that monitor values
        Initial values are not placed into Julia but incoming changes are

  Code blocks -- run Julia code
    type: "code"
    origin: ID of the subscriber that produced this block
    topic: optional topic to publish the block to, when it’s not the default
    targets: optional list of subscribers that should receive the block (others ignore it)
    tags: identifyies a set of blocks. Can be a string or an array of strings
    language: language in which to evaluate code
    return: true if the code should return a block to be published.
    value: code to evaluate

  Data Blocks -- hold data, can be used for responses, 
    type: "data"
    origin: ID of the subscriber that produced this block
    topic: optional topic to publish the block to, when it’s not the default
    targets: optional list of subscribers that should receive the block (others ignore it)
    tags: identifyies a set of blocks. Can be a string or an array of strings
    code: optional name of the code block that produced this block, if was one
    value: value of data

  Delete Blocks
    type: "delete"
    origin: ID of the subscriber that produced this block
    topic: optional topic to publish the block to, when it’s not the default
    targets: optional list of subscribers that should receive the block (others ignore it)
    value: NAME, [NAME, ...], {"tagged": TAG}, or {"tagged": [TAG, ...]}


API:

```julia
start(con::Connection; roots::Dict{Symbol,Any}=Dict(), verbosity=0)

queue_update(name, data) -- queue an update to send out

shutdown() -- close the connection
```

ADDING YOUR OWN TRANSPORTS

You can make your own transport by implementing two required handlers:

```julia
get_updates(con::Connection{T}, wait_time::Float64)

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
```

