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
    | UpdateSuccessor of (int) //update the successor of current node
    | Create of (int)
    | Join of (int)
    | Notify of (int)
    | CheckPredecessor of (int)
    | FindSuccessor of (int)


let F_TABLE_SIZE = 32
let CHORD_RING_SIZE = 2.**32. |> int64


// Input from Command Line
let numNodes = fsi.CommandLineArgs.[1] |> int
let numRequests = fsi.CommandLineArgs.[2] |> int
let rnd = System.Random ()

(*/ Worker Actors
    * find_successor(id)
    * notify()
    * stabilize(): it asks its successor for the successor's predecessor p, and decides whether p should be n's successor instead.
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

let fixFinger id fingertable= 
    let newFing = fingertable
    printfn $"worker{id} fix its fingertable"
    newFing

let checkWithin targetid startid endid =
    let searchid = if startid < endid then targetid else targetid + 64
    if searchid > startid && searchid <= endid then true else false
        

let createWorker id = 
    spawn system ("worker" + string id)
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
                    
                    let timer = new Timers.Timer(500.) // 500ms
                    let waitTime = Async.AwaitEvent (timer.Elapsed) |> Async.Ignore
                    // Generate default finger table. [(1, 0); (2, 0); (4, 0); ... ]
                    let initFingerTab id size = 
                        let key = [for i in 0..size - 1-> (id + int (2. ** (float (i - 1))))]
                        let value = [for _ in 0..size - 1  -> id]
                        List.zip key value

                    let stabilize =
                        printfn $"worker{id} self stabilize"
                        // Ask currerent node's successor for the successor¡¯s predecessor p
                        let response = (Async.RunSynchronously ((getWorkerById successor) <? CheckPredecessor(id)))
                        // check if its successor¡¯s predecessor is still itself
                        // if not: update its successor; Notify its new successor to update predecessor
                        if id <> response then 
                            successor <- response
                            (getWorkerById successor) <! Notify(id)

                    let findSuccessor id targetId fingerTable =
                        let mutable plus = 2.0 ** 63.0
                        let revFing = List.rev fingerTable
                        let mutable endrange = float id + plus
                        let mutable i = 0;
                        let mutable searchId = targetId
                        if endrange < 2.0 ** 64.0 || (searchId > id && float searchId < endrange) then 
                            searchId <- targetId
                        else searchId <- targetId + 64
                        while (float searchId < endrange) do
                            plus <- plus / 2.0
                            endrange <- endrange + plus
                            i <- i + 1
                        getWorkerById revFing.[i]

                    let fixFingerTable succ fingertable = 
                        let succWorker = getWorkerById succ
                        //random pick an worker to find successor
                        let range = fst (List.unzip fingerTable)
                        let rangeSuss = [for i in range -> (succWorker <? FindSuccessor(i))]
                        List.zip range rangeSuss

                    timer.Start()

                    // Methods for join and stablize 
                    while selfcheck do 
                        Async.RunSynchronously waitTime // Wait some tiem before run following codes
                        stabilize
                        // TODO: fixTable(); (Optional)check if predecessor is failed
                        fingerTable <- fixFinger successor fingerTable

                    match message with
                    | Create(currId) ->
                        predecessor <- -1
                        successor <- currId
                        fingerTable <- initFingerTab currId F_TABLE_SIZE
                    | Join(target) ->
                        predecessor <- -1
                        successor <- (Async.RunSynchronously ((getWorkerById target) <? FindSuccessor(id)))
                        (getWorkerById successor) <! Notify(id)
                        outBox <! "" // TODO: Recursively return the successor id
                    | Notify(predId) -> 
                        predecessor <- predId
                    | CheckPredecessor(currId) -> 
                        outBox <! predecessor
                    | NetDone(msg) -> 
                        printfn $"net done. End stabilize fix check and start sending request"
                        selfcheck <- false; // Stop periodical method 
                        for i in 1 .. numRequests do
                            let key = 0; //need to generate a random key within the chord
                            let self = getWorkerById id
                            self <! Request(key, id, 0) 
                    | UpdateSuccessor(id) ->
                        successor <- id
                    | FindSuccessor(targetId) ->
                        let x = 2.0 ** 63.0
                        if float targetId > (float id + x) then  //if have to jump the end node of fingertable
                            let nextWorker = getWorkerById fingerTable.Tail
                            nextWorker <! FindSuccessor(targetId)
                        else //find range and the node it belong to
                            let myboss = select @"akka://ChordModel/user/localActor" system
                            let lists = List.unzip fingerTable
                            let range = fst lists
                            let rangeSuss = snd lists
                            if checkWithin targetId id successor then 
                                outBox <! id
                            else 
                                let nextWorker = findSuccessor id targetId fingerTable 
                                nextWorker <! FindSuccessor(targetId)
                    | Request(targetId, originId, jumpNum) ->
                        //check whether require next jump(targetid exceed the largest range of fingertable)
                        let x = 2.0 ** 63.0
                        if float targetId > (float id + x) then 
                            let nextWorker = getWorkerById fingerTable.Tail
                            nextWorker <! Request(targetId, originId, jumpNum + 1)
                        else //find range and the node it belong to
                            let myboss = select @"akka://ChordModel/user/localActor" system
                            let lists = List.unzip fingerTable
                            let range = fst lists
                            let rangeSuss = snd lists
                            if checkWithin targetId id successor then 
                                let origin = getWorkerById originId
                                origin <! Response("succeed")
                                myboss <! Report(jumpNum) 
                            else 
                                let nextWorker = findSuccessor id targetId fingerTable 
                                nextWorker <! Request(targetId, originId, jumpNum + 1)
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
            actorZero <! Create(0)
            // join actors
            [1..(n - 1)] |> List.iter(fun _ -> (workerSystem <! Join(0)))
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