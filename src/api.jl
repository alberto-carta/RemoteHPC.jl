function execute_function(req::HTTP.Request)
    funcstr = Meta.parse(queryparams(req)["path"])
    func = eval(funcstr)
    args = []
    for (t, a) in JSON3.read(req.body, Vector)
        typ = Symbol(t)
        eval(:(arg = JSON3.read($a, $typ)))
        push!(args, arg)
    end
    return func(args...)
end

function get_version()
    return PACKAGE_VERSION
end

function server_config!(req, s::ServerData)
    splt = splitpath(req.target)
    if splt[4] == "sleep_time"
        return s.sleep_time = parse(Float64, splt[5])
    end 
    field = Symbol(splt[4])
    server = s.server
    setfield!(server, field, parse(fieldtype(server, field), splt[5]))
    save(server)
    return server
end
function server_config(req, s::ServerData)
    server = s.server
    splt = splitpath(req.target)
    if length(splt) == 3
        return server
    else
        return getfield(server, Symbol(splt[4]))
    end
end


"""
    $(SIGNATURES)

check if remote server is pingable. This checks the tunnel and server status at the same time.
isalive = tunnel && islistening
"""
function _isalive(sn::AbstractString)
    s  = Server(sn)
    try
        resp = HTTP.get(s, URI(path="/isalive/"); connect_timeout=2, retries=2)
        return resp.status == Int16(200)
    catch
        return false
    end
end

function setup_core_api!(s::ServerData)
    # POSIX-like commands
    @get  "/ispath/"     req -> (p = queryparams(req)["path"]; ispath(p))
    @get  "/read/"       req -> (p = queryparams(req)["path"]; read(p))
    @post "/write/"      req -> (p = queryparams(req)["path"]; write(p, req.body))
    @post "/rm/"         req -> (p = queryparams(req)["path"]; rm(p; recursive = true))
    @post "/mv/"         req -> (p = queryparams(req); mv(p["src"], p["dst"], force=p["force"]))
    @get  "/readdir/"    req -> (p = queryparams(req)["path"]; readdir(p))
    @get  "/mtime/"      req -> (p = queryparams(req)["path"]; mtime(p))
    @get  "/filesize/"   req -> (p = queryparams(req)["path"]; filesize(p))
    @get  "/realpath/"   req -> (p = queryparams(req)["path"]; realpath(p))
    @post "/mkpath/"     req -> (p = queryparams(req)["path"]; mkpath(p))
    @post "/symlink/"    req -> symlink(JSON3.read(req.body, Vector{String})...)
    @post "/cp/"         req -> cp(JSON3.read(req.body, Tuple{String, String})...; force=true)
    
    # server main loop will run while !s.stop
    # see main_loop in runtime.jl
    # client side function kill(server)
    @put  "/server/kill"  req -> (s.stop = true)
    # server version, client side function version(server)
    @get  "/info/version" req -> get_version()
    # is the local server alive
    @get  "/isalive/"     req -> true
    # is some other server alive
    @get  "/isalive/*"    req -> (n = splitpath(req.target)[end]; _isalive(n))
    # return local server by name
    # server is local if server.name == hostname
    @get  "/server/config"    req -> local_server()
    # check the connections of servers through /isalive/
    @put  "/server/check_connections"   req -> check_connections!(s, get(queryparams(req), "verify_tunnels", false))
    # same as above but will rebuild the ssh tunnel first
    @put  "/server/check_connections/*" req -> check_connections!(s, get(queryparams(req), "verify_tunnels", true); names=[splitpath(req.target)[end]])

    # i don't think the following are used anywhere...
    @get  "/api/**"       execute_function
    # set config
    @post "/server/config/*"  req -> server_config!(req, s)
    # get config
    @get  "/server/config/**" req -> server_config(req, s)
end

function submit_job(req, queue::Queue, channel)
    p = queryparams(req)
    jdir = p["path"]
    @debugv 0 "Submitting job: $jdir" logtype=RuntimeLog
    
    lock(queue) do q
        if !haskey(q.full_queue, jdir)
            error("No Job is present at $jdir.")
        else
            @debugv 0 "Found in full_queue, state: $(q.full_queue[jdir].state)" logtype=RuntimeLog
            q.full_queue[jdir].state = Submitted
        end
    end
    
    priority = haskey(p, "priority") ? parse(Int, p["priority"]) : DEFAULT_PRIORITY
    put!(channel, jdir => (priority, -time()))
    @debugv 0 "Added to submit_channel" logtype=RuntimeLog
end 
        
function get_job(req::HTTP.Request, queue::Queue)
    p = queryparams(req)
    job_dir = p["path"]
    @debugv 0 "Getting job info for: $job_dir" logtype=RuntimeLog
    
    # Check queue state
    info = get(queue.info.current_queue, job_dir, nothing)
    if info === nothing
        info = get(queue.info.full_queue, job_dir, nothing)
    end
    
    @debugv 0 "Queue state for job: $(info === nothing ? "not found" : info.state)" logtype=RuntimeLog
    
    info = info === nothing ? Job(-1, Unknown) : info
    
    # Log queue sizes
    @debugv 0 "Queue sizes - full: $(length(queue.info.full_queue)), current: $(length(queue.info.current_queue))" logtype=RuntimeLog
    
    tquery = HTTP.queryparams(URI(req.target))
    if isempty(tquery) || !haskey(tquery, "data")
        return [info,
                JSON3.read(read(joinpath(job_dir, ".remotehpc_info")),
                           Tuple{String,Environment,Vector{Calculation}})...]
    else
        dat = tquery["data"] isa Vector ? tquery["data"] : [tquery["data"]]
        out = []
        jinfo = any(x -> x in ("name", "environment", "calculations"), dat) ? JSON3.read(read(joinpath(job_dir, ".remotehpc_info")),
                           Tuple{String,Environment,Vector{Calculation}}) : nothing
        for d in dat
            if d == "id"
                push!(out, info.id)
            elseif d == "state"
                push!(out, info.state)
            elseif d == "name"
                if jinfo !== nothing
                    push!(out, jinfo[1])
                end
           elseif d == "environment"
                if jinfo !== nothing
                    push!(out, jinfo[2])
                end
            elseif d == "calculations"
                if jinfo !== nothing
                    push!(out, jinfo[3])
                end
            end
        end
        return out
    end
end

function get_jobs(state::JobState, queue::Queue)
    jobs = String[]
    for q in (queue.info.full_queue, queue.info.current_queue)
        for (d, j) in q
            if j.state == state
                push!(jobs, d)
            end
        end
    end
    return jobs
end

function get_jobs(dirfuzzy::AbstractString, queue::Queue)
    jobs = String[]
    for q in (queue.info.full_queue, queue.info.current_queue)
        for (d, _) in q
            if occursin(dirfuzzy, d)
                push!(jobs, d)
            end
        end
    end
    return jobs
end


function save_job(req::HTTP.Request, args...)
    return save_job(queryparams(req)["path"],
                    JSON3.read(req.body, Tuple{String,Environment,Vector{Calculation}}),
                    args...)
end

function save_job(dir::AbstractString, job_info::Tuple, queue::Queue, sched::Scheduler)
    @debugv 0 "Saving job to directory: $dir" logtype=RuntimeLog
    mkpath(dir)
    
    # Write job.sh
    open(joinpath(dir, "job.sh"), "w") do f
        write(f, job_info, sched)
    end
    
    # Write job info
    JSON3.write(joinpath(dir, ".remotehpc_info"), job_info)
    
    # Update queue state
    lock(queue) do q
        @debugv 0 "Adding job to full_queue with state Saved" logtype=RuntimeLog
        q.full_queue[dir] = Job(-1, Saved)
        @debugv 0 "Current full_queue size: $(length(q.full_queue))" logtype=RuntimeLog
    end
    
    # Force write queue state
    try
        JSON3.write(config_path("jobs", "queue.json"), queue.info)
        @debugv 0 "Successfully wrote queue.json" logtype=RuntimeLog
    catch e
        @debugv 0 "Failed to write queue.json: $(sprint(showerror, e))" logtype=RuntimeLog
        rethrow(e)
    end
end

function abort(req::HTTP.Request, queue::Queue, sched::Scheduler)
    jdir = queryparams(req)["path"]
    j = get(queue.info.current_queue, jdir, nothing)
    if j === nothing
        if haskey(queue.info.submit_queue, jdir)
            lock(queue) do q
                delete!(q.submit_queue, jdir)
                q.full_queue[jdir].state = Cancelled
            end
            return 0
        else
            error("No Job is running or submitted at $jdir.")
        end
    else
        abort(sched, j.id)
        lock(queue) do q
            j = pop!(q.current_queue, jdir)
            j.state = Cancelled
            q.full_queue[jdir] = j
        end

        return j.id
    end
end

function priority!(req::HTTP.Request, queue::Queue)
    p = queryparams(req)
    jdir = p["path"]
    if haskey(queue.info.submit_queue, jdir) 
        priority = haskey(p, "priority") ? parse(Int, p["priority"]) : DEFAULT_PRIORITY
        lock(queue) do q
            q.submit_queue[jdir] = (priority, q.submit_queue[jdir][2])
        end
        return priority
    else
        error("Job at $jdir not in submission queue.")
    end
end

function get_queue(queue::Queue, scheduler::Scheduler)
    return  (
        string(length(queue.info.full_queue)),
        string(length(queue.info.current_queue)),
        string(length(queue.info.submit_queue)),
        scheduler.type
    )
end

function setup_job_api!(s::ServerData)
    @debugv 0 "Setting up job API endpoints" logtype=RuntimeLog
    
    # write job.sh and .remotehpcinfo
    @post "/job/" req -> begin
        @debugv 0 "POST /job/ endpoint called" logtype=RuntimeLog
        save_job(req, s.queue, s.server.scheduler)
    end
    
    # submit job only if the job is in queue
    @put "/job/" req -> begin
        @debugv 0 "PUT /job/ endpoint called" logtype=RuntimeLog
        try
            submit_job(req, s.queue, s.submit_channel)
            @debugv 0 "submit_job completed successfully" logtype=RuntimeLog
        catch e
            @debugv 0 "submit_job failed: $(sprint(showerror, e))" logtype=RuntimeLog
            rethrow(e)
        end
    end
    
    # update job priority
    @put  "/job/priority" req -> priority!(req, s.queue)
    # query job based on job directory
    # see load(s, dir) in client.jl
    @get  "/job/"         req -> get_job(req, s.queue)
    # query jobs based on JobState
    @get  "/jobs/state"   req -> get_jobs(JSON3.read(req.body, JobState), s.queue)
    # query jobs based on Job directory
    @get  "/jobs/fuzzy"   req -> get_jobs(JSON3.read(req.body, String), s.queue)
    @get  "/jobs/queue"  req -> get_queue(s.queue, s.server.scheduler)
    # abort job, will cancel job from internal queue and also the external scheduler eg slurm
    @post "/abort/"       req -> abort(req, s.queue, s.server.scheduler)
end

function load(req::HTTP.Request)
    p = config_path("storage", queryparams(req)["path"])
    if isdir(p)
        return map(x -> splitext(x)[1], readdir(p))
    else
        p *= ".json"
        if ispath(p)
            return read(p, String)
        end
    end
end

function save(req::HTTP.Request)
    p = config_path("storage", queryparams(req)["path"])
    mkpath(splitdir(p)[1])
    write(p * ".json", req.body)
end

function database_rm(req)
    p = config_path("storage", queryparams(req)["path"]) * ".json"
    ispath(p)
    return rm(p)
end

function setup_database_api!()
    # manage central database stored as json files in .julia/config/RemoteHPC
    # see database.jl
    @get  "/storage/" req -> load(req)
    @post "/storage/" req -> save(req)
    @put  "/storage/" req -> database_rm(req)
end
