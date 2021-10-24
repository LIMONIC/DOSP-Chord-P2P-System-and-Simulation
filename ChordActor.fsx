#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit"
#r "nuget: Akka.Remote" 

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit

let system = ActorSystem.Create("ChordModel", Configuration.defaultConfig())

type Information = 
    | Input of (int*int)
    | NetDone of (string)
    | Request of (int*int*int) // target id, origin id, num of jump
    | Response of (string) // target node response its address to origin
    | Report of (int) // target node report num of jumps to boss
    | Init of (string)

// Input from Command Line
let numNodes = fsi.CommandLineArgs.[1] |> int
let numRequests = fsi.CommandLineArgs.[2] |> int

(*/ Worker Actors
    * stabilize(): it asks its successor for the successor��s predecessor p, and decides whether p should be n��s successor instead.
    * fix fingers(): to make sure its finger table entries are correct
    * check predecessor(): return current node's predecessor.
    * sendRequest(): send request
    * getResponse(): request succeed or not

    ### Variables
    * predecessor -> used in stabilize()
    * successor -> used in stabilize()
    * requestSuccess -> count the num of successful request
    * List fingerTable -> finger table
    * selfcheck -> when netDone turn selfcheck to false and stop stabilize, fix fingers, check predecessor and start request sending
 /*)

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
            let workersPool = idList |> List.map(fun id -> (createWorker id))
            let workerenum = [|for i = 1 to workersPool.Length do (sprintf "/user/worker%d" i)|]
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

let boss = spawn system "localActor" localActor

let getWorkerById id =
    let actorPath = @"akka://ChordModel/user/worker" + string id
    select actorPath system

let stabilize id =
    printfn $"worker{id} self stabilize"
    -1

let checkPredecessor id = 
    printfn $"worker{id} check its predecessor"
    -1

let fixFinger id fingertable= 
    let newFing = fingertable
    printfn $"worker{id} fix its fingertable"
    newFing

let createWorker id = 
    spawn system ("worker" + id.ToString())
        (fun mailbox ->
            let rec loop() =
                actor {
                    let! message = mailbox.Receive()
                    let outBox = mailbox.Sender()
                    let mutable predecessor = -1
                    let mutable successor = id;
                    let mutable requestSuccess = 0
                    let mutable fingerTable = List.Empty
                    let mutable selfcheck = true;
                    while selfcheck do 
                        successor <- stabilize id
                        predecessor <- checkPredecessor id 
                        fingerTable <- fixFinger id fingerTable
                    match message with
                    | Init(msg) -> ()
                    | NetDone(msg) -> 
                        printfn $"net done. End stabilize fix check and start sending request"
                        selfcheck <- false; 
                        for i in 1 .. numRequests do
                            let key = 0; //need to generate a random key within the chord
                            let self = getWorkerById id
                            self <! Request(key, id, 0) 
                    | Request(targetId, originId, jumpNum) ->
                        //check whether require next jump(targetid exceed the largest range of fingertable)
                        if targetId > (id + 32) then 
                            let nextWorker = getWorkerById fingerTable.Tail
                            nextWorker <! Request(targetId, originId, jumpNum + 1)
                        else //find range and the node it belong to
                            if targetId = id then 
                                let origin = getWorkerById originId
                                origin <! Response("succeed")
                                boss <! Report(jumpNum)
                            else 
                                let mutable plus = 2
                                for i in fingerTable do
                                    let startrange = (id + plus / 2)
                                    let endrange = (id + plus)
                                    if targetId >= startrange && targetId < endrange then 
                                        let origin = getWorkerById originId
                                        origin <! Response("succeed")
                                        boss <! Report(jumpNum + 1)
                                    plus <- plus * 2
                    | Response(msg) ->
                        if msg = "succeed" then requestSuccess <- requestSuccess + 1
                    | _ -> ()
                    return! loop()
                }
            loop()
        )

boss <! Input(numNodes, numRequests)
let coordinator = spawn system "localActor" localActor
// Input from Command Line
let N = fsi.CommandLineArgs.[1] |> int64 // numNodes  
let R = fsi.CommandLineArgs.[2] |> int64 // numRequests
// client <! TaskSize(int64 1E6)
coordinator <! Input(N, R)
// Wait until all the actors has finished processing
system.WhenTerminated.Wait()