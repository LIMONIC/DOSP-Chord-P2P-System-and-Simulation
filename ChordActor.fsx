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
    | Init of (string) // choose to create a chord network or join to a exist network.
    | NetDone of (string)
    // | Key of (string)
    | Request of (int64*int64*int64) // target id, origin id, num of jump
    | Response of (string) // target node response its address to origin
    | Report of (int64) // target node report num of jumps to boss



    // | Output of (list<string * string>)
    // | Done of (string)

(*/ Print results and send them to server /*)
// let printer (mailbox:Actor<_>) =
//     let mutable res = []
//     let rec loop () = actor {
//         let! message = mailbox.Receive()
//         // printfn "worker acotr receive msg: %A" message
//         let printRes resList = 
//             printfn "-------------RESULT-------------" 
//             resList |> List.iter(fun (str, sha256) -> printfn $"{str}\t{sha256}")
//             res <- []
//             printfn "--------------------------------" 
//         match message with
//         | Output(resList) -> 
//             if res.Length >= 100
//                 then 
//                     res |> printRes
//                 else
//                     res <- res @ resList
//         | Done(completeMsg) -> 
//             printfn $"[INFO][DONE]: {completeMsg}"
//             if res.Length > 0 then printRes res
//         | _ -> ()
//         return! loop()
//     }
//     loop()
// let printerRef = spawn system "printer" printer


(*/ Worker Actors
    Takes input from remoteActor, calculate results and pass the result to PostMan Actor
 /*)
let worker (mailbox:Actor<_>) =
    let rec loop () = actor {
        let! message = mailbox.Receive()
        let outBox = mailbox.Sender()
        let tid = Threading.Thread.CurrentThread.ManagedThreadId
        printfn $"{message}"
        // match message with
        // | Init(nodeNum, reqNum) -> 
        //     printfn $"input: {message}"
        // | _ -> ()
        outBox <! Report(10L)
        return! loop()
    }
    loop()

let localActor (mailbox:Actor<_>) = 
    // Chrod ring initialization

    // let workersPool = 
    //         [1L .. totalWorkers]
    //         |> List.map(fun id -> spawn system (sprintf "Local_%d" id) worker)

    // let workerenum = [|for i = 1 to workersPool.Length do (sprintf "/user/Local_%d" i)|]
    // let workerSystem = system.ActorOf(Props.Empty.WithRouter(Akka.Routing.RoundRobinGroup(workerenum)))
    let actorZero = spawn system "0" worker
    let mutable completedLocalWorkerNum = 0L
    let mutable localActorNum = 0L
    let mutable totJumpNum = 0L

// Assign tasks to worker
    let rec loop () = actor {
        let! message = mailbox.Receive()
        // printfn $"[DEBUG]: Boss received {message}"
        match message with 
        | Input(n,r) -> 
            printfn $"[INFO]: Num of Nodes: {n}\t Num of Request: {r}"
            localActorNum <- n
            // generate a list with random actor ids
            let rnd = System.Random ()
            let idList = [1L..(n - 1L)] |> List.sortBy(fun _ -> rnd.Next(1, int (n - 1L)) |> int64 )
            let workersPool = idList |> List.map(fun id -> spawn system (sprintf "%d" id) worker)
            let workerenum = [|for i = 1 to workersPool.Length do (sprintf "/user/%d" i)|]
            let workerSystem = system.ActorOf(Props.Empty.WithRouter(Akka.Routing.RoundRobinGroup(workerenum)))
            // system.ActorOf()
            // create chord network
            // let url = "akka.tcp://Project2@localhost:8777/user/"
            actorZero <! Init("create")
            // join actors
            [1L..(n - 1L)] |> List.iter(fun _ -> (workerSystem <! Init("join")))
            printfn "[INFO]: All nodes have joined into the Chord network."
            // Send finish msg
            actorZero <! NetDone("Done");
            [1L..(n - 1L)] |> List.iter(fun _ -> (workerSystem <! Init("Done")))
            printfn "[INFO]: Start request."
        | Report(numOfJumps) ->
            completedLocalWorkerNum <- completedLocalWorkerNum + 1L
            totJumpNum <- totJumpNum + numOfJumps
            printfn $"[INFO]: \tcompleted:{completedLocalWorkerNum} \ttotal:{localActorNum} \tjump num: {numOfJumps}" 
            if completedLocalWorkerNum = localActorNum then
                printfn $"All tasks completed! local: {completedLocalWorkerNum}"
                mailbox.Context.System.Terminate() |> ignore
        | _ -> ()
        return! loop()
    }
    loop()

let coordinator = spawn system "localActor" localActor
// Input from Command Line
let N = fsi.CommandLineArgs.[1] |> int64 // numNodes  
let R = fsi.CommandLineArgs.[2] |> int64 // numRequests
// client <! TaskSize(int64 1E6)
coordinator <! Input(N, R)
// Wait until all the actors has finished processing
system.WhenTerminated.Wait()