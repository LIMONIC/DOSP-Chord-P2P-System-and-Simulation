#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit"
#r "nuget: Akka.Remote" 

open System
open System.Security.Cryptography
open System.Text
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit

let system = ActorSystem.Create("ChordModel", Configuration.defaultConfig())

type Information = 
    | Input of (int*int)
    | NetDone of (string)
    | Request of (string*string*int) // target id, origin id, num of jump
    | Response of (string) // target node response its address to origin
    | Report of (int) // target node report num of jumps to boss
    | Init of (string)

let CHORD_RING_SIZE = 2.**64.

// Input from Command Line
let numNodes = fsi.CommandLineArgs.[1] |> int
let numRequests = fsi.CommandLineArgs.[2] |> int
let rnd = System.Random ()

/// Generate SHA1
// let removeChar (stripChars:string) (text:string) =
//     text.Split(stripChars.ToCharArray(), StringSplitOptions.RemoveEmptyEntries) |> String.Concat
// let getSHA1Str (input:string) = 
//     input
//     |> Encoding.ASCII.GetBytes
//     |> (new SHA1Managed()).ComputeHash
//     |> System.BitConverter.ToString
//     |> removeChar "-"
// let getSHA1Arr (input:string) = input |> getSHA1Str |> Seq.toList



(*/ Worker Actors
    * find_successor(id)
    * notify()
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

let generateRandom lo hi =
    rnd.Next(lo, hi)

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
    spawn system $"{id}"
        (fun mailbox ->
            let rec loop() =
                actor {
                    let! message = mailbox.Receive()
                    let outBox = mailbox.Sender()
                    let mutable predecessor = -1
                    let mutable successor = id;
                    let mutable requestSuccess = 0
                    let mutable fingerTable = List.Empty
                    let mutable selfcheck = false;

                    let timer = new Timers.Timer(500.) // 500ms
                    let waitTime = Async.AwaitEvent (timer.Elapsed) |> Async.Ignore
                    timer.Start()

                    let findSuccessor id = 
                        0
                    // Methods for join and stablize 
                    while selfcheck do 
                        Async.RunSynchronously waitTime // Wait some tiem before run following codes
                        successor <- stabilize id
                        predecessor <- checkPredecessor id 
                        fingerTable <- fixFinger id fingerTable
                    match message with
                    | Init(msg) -> 
                        match msg with
                        | "create" -> 
                            predecessor <- -1
                            successor <- id
                        | "join" ->
                            predecessor <- -1
                            // successor <- actor <? FindSuccessor(id)
                        | _ -> failwith $"[ERROR]: Wrong init message!"
                    | NetDone(msg) -> 
                        printfn $"net done. End stabilize fix check and start sending request"
                        selfcheck <- false; // Stop periodical method 
                        for i in 1 .. numRequests do
                            let key = 0; //need to generate a random key within the chord
                            let self = getWorkerById id
                            self <! Request(key, id, 0) 
                    | Request(targetId, originId, jumpNum) ->
                        //check whether require next jump(targetid exceed the largest range of fingertable)
                        if targetId > (id + 160) then 
                            let nextWorker = getWorkerById fingerTable.Tail
                            nextWorker <! Request(targetId, originId, jumpNum + 1)
                        else //find range and the node it belong to
                            let myboss = select @"akka://ChordModel/user/localActor" system
                            if targetId = id then 
                                let origin = getWorkerById originId
                                origin <! Response("succeed")
                                myboss <! Report(jumpNum)
                            else 
                                let mutable plus = 2
                                for i in fingerTable do
                                    let startrange = (id + plus / 2)
                                    let endrange = (id + plus)
                                    if targetId >= startrange && targetId < endrange then 
                                        let origin = getWorkerById originId
                                        origin <! Response("succeed")
                                        myboss <! Report(jumpNum + 1)
                                    plus <- plus * 2
                    | Response(msg) ->
                        if msg = "succeed" then requestSuccess <- requestSuccess + 1
                    | _ -> ()
                    return! loop()
                }
            loop()
        )

let localActor (mailbox:Actor<_>) = 
    let actorZero = createWorker 0
    let mutable completedLocalWorkerNum = 0
    let mutable localActorNum = 0
    let mutable totJumpNum = 0

// Assign tasks to worker
    let rec loop () = actor {
        let! message = mailbox.Receive()
        // printfn $"[DEBUG]: Boss received {message}"
        match message with 
        | Input(n,r) -> 
            printfn $"[INFO]: Num of Nodes: {n}\t Num of Request: {r}"
            localActorNum <- n
            // generate a list with random actor ids
            let idList = [1..(n - 1)] |> List.sortBy(fun _ -> rnd.Next(1, int (n - 1)) |> int )
            let workersPool = idList |> List.map(fun id -> (createWorker id))
            let workerenum = [|for i = 1 to workersPool.Length do (sprintf "/user/%d" i)|]
            let workerSystem = system.ActorOf(Props.Empty.WithRouter(Akka.Routing.RoundRobinGroup(workerenum)))
            // system.ActorOf()
            // create chord network
            // let url = "akka.tcp://Project2@localhost:8777/user/"
            actorZero <! Init("create")
            // join actors
            [1..(n - 1)] |> List.iter(fun _ -> (workerSystem <! Init("join")))
            printfn "[INFO]: All nodes have joined into the Chord network."
            // Send finish msg
            actorZero <! NetDone("Done");
            [1..(n - 1)] |> List.iter(fun _ -> (workerSystem <! NetDone("Done")))
            printfn "[INFO]: Start request."
        | Report(numOfJumps) ->
            completedLocalWorkerNum <- completedLocalWorkerNum + 1
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
boss <! Input(numNodes, numRequests)

system.WhenTerminated.Wait()