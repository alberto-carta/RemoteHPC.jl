mutable struct Job
    id::Int
    state::JobState
end

Base.@kwdef mutable struct QueueInfo
    full_queue::Dict{String,Job} = Dict{String,Job}()
    current_queue::Dict{String,Job} = Dict{String,Job}()
    submit_queue::PriorityQueue{String, Tuple{Int, Float64}} = PriorityQueue{String, Tuple{Int, Float64}}(Base.Order.Reverse)
end
StructTypes.StructType(::Type{QueueInfo}) = StructTypes.Mutable()

struct Queue
    lock::ReentrantLock
    info::QueueInfo
end
Queue() = Queue(ReentrantLock(), QueueInfo())

function Base.lock(f::Function, q::Queue)
    lock(q.lock)
    try
        f(q.info)
    catch e
        log_error(e, logtype=RuntimeLog)
    finally
        unlock(q.lock)
    end
end

"""

Read queue from queue.json stored in RemoteHPC config. Useful for when restarting server.
"""
function read_queue()
    qfile = config_path("jobs", "queue.json")
    tq = JSON3.read(read(qfile, String))
    full_queue = StructTypes.constructfrom(Dict{String,Job}, tq[:full_queue])
    current_queue = StructTypes.constructfrom(Dict{String, Job}, tq[:current_queue])
    # submit_queue = StructTypes.constructfrom(Dict{String, Job}, tq[:submit_queue])
    return (;full_queue, current_queue)
end
    
function Base.fill!(qu::Queue, scheduler::Scheduler, init)
    qfile = config_path("jobs", "queue.json")
    if init
        if ispath(qfile)
            t = read(qfile)
            if !isempty(t)
                # TODO: should be deprecated
                tq = JSON3.read(t)
                lock(qu) do q
                    q.full_queue = StructTypes.constructfrom(Dict{String,Job}, tq[:full_queue])
                    q.current_queue = StructTypes.constructfrom(Dict{String, Job}, tq[:current_queue])
                    if tq[:submit_queue] isa AbstractArray
                        for jdir in tq[:submit_queue]
                            q.submit_queue[jdir] = DEFAULT_PRIORITY
                            q.full_queue[jdir].state = Submitted
                        end
                    else
                        for (jdir, priority) in tq[:submit_queue]
                            if length(priority) > 1
                                q.submit_queue[string(jdir)] = (priority...,)
                            else
                                q.submit_queue[string(jdir)] = (priority, -time())
                            end
                        end
                    end
                end
            end
        end
    end
    lock(qu) do q
        for q_ in (q.full_queue, q.current_queue)
            for (dir, info) in q_
                if !ispath(joinpath(dir, "job.sh"))
                    delete!(q_, dir)
                end
            end
        end
    end
    # Here we check whether the scheduler died while the server was running and try to restart and resubmit   
    if maybe_scheduler_restart(scheduler)
        lock(qu) do q
            for (d, i) in q.current_queue
                if ispath(joinpath(d, "job.sh"))
                    q.full_queue[d] = Job(-1, Saved)
                    q.submit_queue[d] = (DEFAULT_PRIORITY, -time())
                end
                pop!(q.current_queue, d)
            end
        end
    else
    # updating queue
        squeue = queue(scheduler)
        lock(qu) do q
            for (d, i) in q.current_queue
                if haskey(squeue, d)
                    state = pop!(squeue, d)[2]
                else
                    state = jobstate(scheduler, i.id)
                end
                if in_queue(state)
                    delete!(q.full_queue, d)
                    q.current_queue[d] = Job(i.id, state)
                else
                    delete!(q.current_queue, d)
                    q.full_queue[d] = Job(i.id, state)
                end
            end
            for (k, v) in squeue
                q.current_queue[k] = Job(v...)
            end
        end
    end
    return qu
end

Base.@kwdef mutable struct ServerData
    server::Server
    total_requests::Int = 0
    current_requests::Int = 0
    t::Float64 = time()
    requests_per_second::Float64 = 0.0
    total_job_submissions::Int = 0
    submit_channel::Channel{Pair{String, Tuple{Int, Float64}}} = Channel{Pair{String, Tuple{Int, Float64}}}(Inf)
    queue::Queue = Queue()
    sleep_time::Float64 = 5.0
    connections::Dict{String, Bool} = Dict{String, Bool}()
    stop::Bool = false
    lock::ReentrantLock = ReentrantLock()
end


const SLEEP_TIME = Ref(5.0)

function main_loop(s::ServerData)
    fill!(s.queue, s.server.scheduler, true)
    
    # Queue update task
    t_writequeue = Threads.@spawn while !s.stop
        try
            @debugv 3 "Running queue update task" logtype=RuntimeLog
            fill!(s.queue, s.server.scheduler, false)
            JSON3.write(config_path("jobs", "queue.json"), s.queue.info)
        catch e
            log_error(e, logtype = RuntimeLog)
        end
        sleep(s.sleep_time)
    end
    
    # Job submission handling task
    t_submit = Threads.@spawn while !s.stop
        try
            @debugv 3 "Running job submission task" logtype=RuntimeLog
            handle_job_submission!(s)
        catch e
            log_error(e, logtype = RuntimeLog)
        end
        sleep(s.sleep_time)
    end
    
    while !s.stop
        try
            log_info(s)
        catch e
            log_error(e, logtype = RuntimeLog)
        end
        if ispath(config_path("self_destruct"))
            @debug "self_destruct found, self destructing..." logtype=RuntimeLog
            exit()
        end
        sleep(s.sleep_time)
    end
    
    fetch(t_writequeue)
    fetch(t_submit)  # Make sure we wait for the submission handler to finish
    return JSON3.write(config_path("jobs", "queue.json"), s.queue.info)
end

function log_info(s::ServerData)
    dt = time() - s.t
    lock(s.lock)
    curreq = s.current_requests
    s.current_requests = 0
    s.t = time()
    unlock(s.lock)
    s.requests_per_second = curreq / dt
    s.total_requests += curreq
    
    # @debugv 0 "current_queue: $(length(s.queue.info.current_queue)) - submit_queue: $(length(s.queue.info.submit_queue))" logtype=RuntimeLog
    @debugv 0 "total requests: $(s.total_requests) - r/s: $(s.requests_per_second)" logtype=RESTLog 
end


function monitor_issues(log_mtimes)
    # new_mtimes = mtime.(joinpath.((config_path("logs/runtimes"),),
    #                               readdir(config_path("logs/runtimes"))))
    # if length(new_mtimes) != length(log_mtimes)
    #     @error "More Server logs got created signalling a server was started while a previous was running." logtype=RuntimeLog
    #     touch(config_path("self_destruct"))
    # end
    # ndiff = length(filter(x -> log_mtimes[x] != new_mtimes[x], 1:length(log_mtimes)))
    # if ndiff > 1
    #     @error "More Server logs modification times differed than 1." logtype=RuntimeLog
    #     touch(config_path("self_destruct"))
    # end
end

function handle_job_submission!(s::ServerData)
    @debugv 2 "Handling job submissions" logtype=RuntimeLog
    
    # Process channel items
    while !isempty(s.submit_channel)
        jobdir, priority = take!(s.submit_channel)
        @debugv 0 "Moving from channel to submit_queue: $jobdir" logtype=RuntimeLog
        s.queue.info.submit_queue[jobdir] = priority
    end
    
    # Try to submit jobs
    n_submit = min(s.server.max_concurrent_jobs - length(s.queue.info.current_queue), 
                  length(s.queue.info.submit_queue))
    
    submitted = 0
    for i in 1:n_submit
        job_dir, priority = dequeue_pair!(s.queue.info.submit_queue)
        @debugv 0 "Attempting to submit: $job_dir" logtype=RuntimeLog
        
        if !ispath(job_dir)
            @warnv 0 "Directory not found: $job_dir" logtype=RuntimeLog
            continue
        end
        
        try
            id = submit(s.server.scheduler, job_dir)
            lock(s.queue) do q
                q.current_queue[job_dir] = Job(id, Pending)
                delete!(q.full_queue, job_dir)
            end
            @debugv 0 "Successfully submitted: $job_dir (ID: $id)" logtype=RuntimeLog
            submitted += 1
        catch e
            @warnv 0 "Submission failed for $job_dir: $(sprint(showerror, e))" logtype=RuntimeLog
            s.queue.info.submit_queue[job_dir] = (priority[1] - 1, priority[2])
        end
    end
    @debugv 2 "Submitted $submitted jobs" logtype=RuntimeLog
end

function requestHandler(handler, s::ServerData)
    return function f(req)
        start = Dates.now()
        @debugv 2 "BEGIN - $(req.method) - $(req.target)" logtype=RESTLog
        resp = HTTP.Response(404)
        try
            obj = handler(req)
            if obj === nothing
                resp = HTTP.Response(204)
            elseif obj isa HTTP.Response
                return obj
            elseif obj isa Exception
                resp = HTTP.Response(500, log_error(obj))
            else
                resp = HTTP.Response(200, JSON3.write(obj))
            end
        catch e
            resp = HTTP.Response(500, log_error(e))
        end
        stop = Dates.now()
        @debugv 2 "END - $(req.method) - $(req.target) - $(resp.status) - $(Dates.value(stop - start)) - $(length(resp.body))" logtype=RESTLog
        lock(s.lock)
        s.current_requests += 1
        unlock(s.lock)
        return resp
    end
end

function AuthHandler(handler, user_uuid::UUID)
    return function f(req)
        if HTTP.hasheader(req, "USER-UUID")
            uuid = HTTP.header(req, "USER-UUID")

            @debug "uuid of request: $uuid, uuid of server: $user_uuid"
            if UUID(uuid) == user_uuid
                t = ThreadPools.spawnbg() do 
                    return handler(req)
                end
                while !istaskdone(t)
                    yield()
                end
                return fetch(t)
            end
        end
        return HTTP.Response(401, "unauthorized")
    end
end

function check_connections!(connections, verify_tunnels; names=keys(connections))
    @debug "Checking connections..." logtype=RuntimeLog
    
    for n in names
        if !exists(Server(name=n))
            pop!(connections, n)
            continue
        end
        
        s = load(Server(n))
        s.domain == "localhost" && continue
        
        try
            connections[n] = @timeout 30 begin
                return HTTP.get(s, URI(path="/isalive")) !== nothing
            end false
        catch
            connections[n] = false
        end
    end
    
    if verify_tunnels
        @debugv 1 "Verifying tunnels" logtype=RuntimeLog
        for n in names
            
            connections[n] && continue
            
            s = load(Server(n))
            s.domain == "localhost" && continue
            
            @debugv 0 "Connection to $n: $(connections[n])" logtype=RuntimeLog
            
            connections[n] = @timeout 30 begin 
                destroy_tunnel(s)
                try
                    remote_server = load_config(s.username, s.domain, config_path(s))
                    remote_server === nothing && return false
                    s.port = construct_tunnel(s, remote_server.port)
                    sleep(5)
                    s.uuid = remote_server.uuid
                    try
                        
                        HTTP.get(s, URI(path="/isalive")) !== nothing
                        save(s)
                        @debugv 1 "Connected to $n" logtype=RuntimeLog
                        return true
                    catch
                        destroy_tunnel(s)
                        return false
                    end
                catch err
                    log_error(err, logtype=RuntimeLog)
                    destroy_tunnel(s)
                    return false
                end
            end false
        end
    end
    
    return connections
end

function check_connections!(server_data::ServerData, args...; kwargs...)
    all_servers = load(Server(""))
    
    # delete unconfigured servers
    for k in filter(x-> !(x in all_servers), keys(server_data.connections))
        delete!(server_data.connections, k)
    end
    
    for n in all_servers
        n == server_data.server.name && continue
        server_data.connections[n] = get(server_data.connections, n, false)
    end
    
    conn = check_connections!(server_data.connections, args...; kwargs...)
    
    @debugv 1 "Connections: $(server_data.connections)" logtype=RuntimeLog
    
    return conn
end


"""
    $(SIGNATURES)
Check if the ssh port forwarding tunnel to server s is still alive using command `nc`.
"""
function check_tunnel(s::Server)
    check_tunnel(s.port)
end

function check_tunnel(port::Int64)
    try
        r = run(Cmd(`nc -z localhost $port`))
        return iszero(r.exitcode)
    catch
        return false
    end
end

"""
Check the status of ssh tunnel forwarding to all configured servers
"""
function check_tunnels()
    all_servers = load(Server(""))
    host = gethostname()
   
    status = Dict{String, Any}()
    for sn in all_servers
        # skip local server
        if sn == host
            continue
        end
    
        server = Server(sn)
        res = check_tunnel(server.port)
        status[sn] = res
    end
    return status
end

"""
Rebuild dead ssh tunnels based on local ports stored in the config and remote ports stored
on servers. The status of the tunnels are decieded by `check_tunnels()`
This is necessary since ssh tunnels are frequently destroyed. 
"""
function maybe_revive_tunnels()
    status = check_tunnels()
    # TODO will maintain ssh tunnel for all configured servers
    # this can block the port even when the server is not running on the remote
    # machine, alternatively we can connect ssh tunnel to only the "intended"
    # servers, which may need additional configurations.
    # TODO check autossh https://github.com/Autossh/autossh
    for sn in keys(status)
        # skip established tunnels
        if status[sn]
            continue
        end
        @debug "Attempt to rebuild tunnel to server $sn." logtype=RuntimeLog
        s = Server(sn)
        config = load_config(s)
        cmd = Cmd(`ssh -o ExitOnForwardFailure=yes -o ServerAliveInterval=60 -N -L $(s.port):localhost:$(config.port) $(ssh_string(s))`)
        try
            process = run(cmd; wait=false)
        catch e
            log_error(e)
        end
    end
end

function julia_main(;verbose=0, kwargs...)::Cint
    logger = TimestampLogger(TeeLogger(HTTPLogger(),
                                   NotHTTPLogger(TeeLogger(RESTLogger(),
                                                 RuntimeLogger(),
                                                 GenericLogger()))))
    with_logger(logger) do
        LoggingExtras.withlevel(LoggingExtras.Debug; verbosity=verbose) do
            try
                s = local_server()
                port, server = listenany(ip"0.0.0.0", 8080)
                s.port = port

                server_data = ServerData(server=s; kwargs...)

                @debug "Setting up Router" logtype=RuntimeLog

                setup_core_api!(server_data)
                setup_job_api!(server_data)
                setup_database_api!()

                # ssh portforwarding may crash
                # this periodically checks the ssh tunnels and revive them
                connection_task = Threads.@spawn @stoppable server_data.stop begin
                    @debug "Checking Tunnels" logtype=RuntimeLog
                    try
                        t = time()
                        while true
                            if time() - t > server_data.sleep_time
                                t = time()
                                maybe_revive_tunnels()
                            else
                                sleep(10)
                            end
                        end
                    catch e
                        log_error(e)
                    end
                end

                
                @debug "Starting main loop" logtype=RuntimeLog

                t = Threads.@spawn try
                    main_loop(server_data)
                catch e
                    log_error(e, logtype=RuntimeLog)
                end
                @debug "Starting RESTAPI - HOST $(gethostname()) - USER $(get(ENV, "USER", "unknown_user"))" logtype=RuntimeLog 
                save(s)
                @async serve(middleware = [x -> requestHandler(x, server_data), x -> AuthHandler(x, UUID(s.uuid))],
                                  host="0.0.0.0", port=Int(port), server = server, access_log=nothing, serialize=false)
                while !server_data.stop
                    sleep(1)
                end
                @debug "Shutting down server"
                terminate()
                fetch(t)
                fetch(connection_task)
                return 0
            catch e
                log_error(e)
                rethrow(e)
            end
        end
    end
end
