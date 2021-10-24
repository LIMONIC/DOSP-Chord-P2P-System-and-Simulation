#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit"
#r "nuget: Akka.Remote" 

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit

let system = ActorSystem.Create("ChordModel", Configuration.defaultConfig())
// Use Actor system for naming
// let system = System.create "my-system" (Configuration.load())

// let measureTime f = 
//     let proc = Process.GetCurrentProcess()
//     let cpu_time_stamp = proc.TotalProcessorTime
//     let timer = new Stopwatch()
//     timer.Start()
//     try
//         f()
//         timer.Stop()
//     finally
//         let cpu_time = (proc.TotalProcessorTime-cpu_time_stamp).TotalMilliseconds
//         printfn "CPU time = %dms" (int64 cpu_time)
//         printfn "Absolute time = %dms" timer.ElapsedMilliseconds

type Information = 
    | Input of (int64*int64)
    | NetDone of (string)
    // | Key of (string)
    | Request of (int*int*int) // target id, origin id, num of jump
    | Response of (string) // target node response its address to origin
    | Report of (int) // target node report num of jumps to boss



    // | Output of (list<string * string>)
    // | Done of (string)

(*/ Print results and send them to server /*)
let printer (mailbox:Actor<_>) =
    let mutable res = []
    let rec loop () = actor {
        let! message = mailbox.Receive()
        // printfn "worker acotr receive msg: %A" message
        let printRes resList = 
            printfn "-------------RESULT-------------" 
            resList |> List.iter(fun (str, sha256) -> printfn $"{str}\t{sha256}")
            res <- []
            printfn "--------------------------------" 
        match message with
        | Output(resList) -> 
            if res.Length >= 100
                then 
                    res |> printRes
                else
                    res <- res @ resList
        | Done(completeMsg) -> 
            printfn $"[INFO][DONE]: {completeMsg}"
            if res.Length > 0 then printRes res
        | _ -> ()
        return! loop()
    }
    loop()
let printerRef = spawn system "printer" printer

(*/ Worker Actors
    Takes input from remoteActor, calculate results and pass the result to PostMan Actor
 /*)
let worker (mailbox:Actor<_>) =
    let rec loop () = actor {
        let! message = mailbox.Receive()
        let outBox = mailbox.Sender()
        let tid = Threading.Thread.CurrentThread.ManagedThreadId
        match message with
        | Input(start, k, zeros) -> 
            printfn $"input: {message}"
        | _ -> ()
        return! loop()
    }
    loop()

let localActor (mailbox:Actor<_>) = 
    let actcount = System.Environment.ProcessorCount |> int64
    let totalWorkers = actcount*125L

    printfn "ProcessorCount: %d" actcount
    printfn "totalWorker: %d" totalWorkers

    let workersPool = 
            [1L .. totalWorkers]
            |> List.map(fun id -> spawn system (sprintf "Local_%d" id) worker)

    let workerenum = [|for i = 1 to workersPool.Length do (sprintf "/user/Local_%d" i)|]
    let workerSystem = system.ActorOf(Props.Empty.WithRouter(Akka.Routing.RoundRobinGroup(workerenum)))
    let mutable completedLocalWorkerNum = 0L
    let mutable localActorNum = totalWorkers
    let mutable taskSize = 1E6 |> int64

// Assign tasks to worker
    let rec loop () = actor {
        let! message = mailbox.Receive()
        // printfn $"[DEBUG]: Boss received {message}"
        match message with 
        | TaskSize(size) -> taskSize <- size
        | Input(n,k,t) -> 
            // task init
            let totalTasks = k - n
            let requiredActorNum = 
                if totalTasks % taskSize = 0L then totalTasks / taskSize else totalTasks / taskSize + 1L
            let assignTasks (size, actors) = 
                printfn $"[DEBUG]: Task size: {size}"
                [1L..actors] |> List.iteri(fun i x -> 
                    printfn $"- Initialize actor [{i + 1}/{actors}]: \t{int64 i * size + n} - {(int64 i + 1L)* size + n - 1L}"
                    workerSystem <! Input(int64 i * size + n, size, t)
                )
            // assign tasks based on actor number
            match requiredActorNum with
            | _ when requiredActorNum > localActorNum ->
                // resize taskSize to match actor number
                if (totalTasks % localActorNum = 0L) then taskSize <- totalTasks / localActorNum else taskSize <- totalTasks / localActorNum + 1L
                assignTasks(taskSize, localActorNum)
            | _ when requiredActorNum = localActorNum -> 
                assignTasks(taskSize, localActorNum)
            | _ when requiredActorNum < localActorNum -> 
                // reduce actor numbers
                localActorNum <- requiredActorNum
                if totalTasks < taskSize then assignTasks(totalTasks, requiredActorNum) else assignTasks(taskSize, requiredActorNum)
            | _ -> failwith "[ERROR] wrong taskNum"
            // printfn "End Input"
        | Output (res) -> 
            printerRef <! Output(res)
        | Done(completeMsg) ->
            completedLocalWorkerNum <- completedLocalWorkerNum + 1L
            printfn $"> {completeMsg} \tcompleted:{completedLocalWorkerNum} \ttotal:{localActorNum}" 
            if completedLocalWorkerNum = localActorNum then
                printerRef <! Done($"All tasks completed! local: {completedLocalWorkerNum}")
                mailbox.Context.System.Terminate() |> ignore
        // | _ -> ()
        return! loop()
    }
    loop()

let client = spawn system "localActor" localActor
// Input from Command Line
let N = fsi.CommandLineArgs.[1] |> int64
let K = fsi.CommandLineArgs.[2] |> int64
let T = fsi.CommandLineArgs.[3] |> int64
// client <! TaskSize(int64 1E6)
client <! Input(N, K, T)
// Wait until all the actors has finished processing
system.WhenTerminated.Wait()