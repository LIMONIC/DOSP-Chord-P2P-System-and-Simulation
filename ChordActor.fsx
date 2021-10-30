#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit"
#r "nuget: Akka.Remote" 

open System
open Akka.Actor
open System.Collections.Generic
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit

let system = ActorSystem.Create("ChordModel", Configuration.defaultConfig())

type Information = 
    | Input of (int*int)
    | NetDone of (string)
    | Request of (int64*int64*int) // target id, origin id, num of jump
    | Response of (int64) // target node response its address to origin
    | Report of (int) // target node report num of jumps to boss
    // | Create of (int64)
    | Join of (int64)
    | Notify of (int64)
    | FindSuccessor of (int64)
    | CheckPredecessor of (int64)
    | Update of (int64)
    | Print of (string)
    | Create of (int64*int64)
    | RepProp of (string)
    | Alive of (string)

let F_TABLE_SIZE = 32
let CHORD_RING_SIZE = 2.**32. |> int64

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

let printer (mailbox:Actor<_>) =
    let rec loop () = actor {
        let! message = mailbox.Receive()
        // printfn "worker acotr receive msg: %A" message
        match message with
        | Print(msg) -> 
            printfn "%s" msg
        | _ -> ()
        return! loop()
    }
    loop()
let printerRef = spawn system "printer" printer


let createWorker id = 
    spawn system ("worker" + string id)
        (fun mailbox ->
            let DEBUG = false
            // if DEBUG then printfn $"[DEBUG]: id: {id}"

            let myboss = select @"akka://ChordModel/user/boss" system
            let mutable predecessor = -1L
            let mutable successor = id;
            let mutable fingerTable = []
            let mutable requestSuccess = 0
            let mutable selfcheck = false; 
            let timer = new Timers.Timer(50.) // 50ms
            let waitTime = Async.AwaitEvent (timer.Elapsed) |> Async.Ignore
            let checkWithin targetid startid endid = 
                if startid > endid then
                    (targetid > startid && targetid <= CHORD_RING_SIZE) || (targetid >= 0L && targetid <= endid)
                else if startid < endid then
                    targetid > startid && targetid <= endid
                else 
                    true
            let reportProp msg = 
                printerRef <! Print($"[***][{msg}]ID: {id}, predecessor: {predecessor}, successor: {successor}, fTable: {fingerTable}")
            let checkAlive msg = 
                printerRef <! Print($"[{msg}]{id} is Alive!")
            let stabilize _ =
                // printerRef <! Print($"worker{id} self stabilize")
                // Ask currerent node's successor for the successor’s predecessor p
                // if DEBUG then printfn $"[DEBUG][stabilize]: id {id}, successor {successor}"
                // Async.RunSynchronously 
                // (((getWorkerById successor) <! CheckPredecessor(id)))
                // if successor <> id then 
                printerRef <! Print($"BEFORE STAB id:{id} successor:{successor} predecessor:{predecessor}")
                let mutable response = predecessor
                // process first node
                if id <> successor then 
                // printerRef <! Print($"worker:{id} successor:{successor} predecessor:{predecessor}")
                    printerRef <! Print($"&&&&&&&&&&&&& id:{id} successor:{successor} predecessor:{predecessor}")
                    response <- (Async.RunSynchronously((getWorkerById successor) <? CheckPredecessor(id)))
                    printerRef <! Print($"response:{response}; id {id}")
                    // printerRef <! Print($"{id}  response  {response}")
                    // if response = -1L then (response <- successor)
                    // if DEBUG then printfn $"[DEBUG][stabilize]: response {response}"
                    // let response = predecessor
                    // if response = -1L then response
                    // printerRef <! Print($"!!!Response {response}")

                    // check if its successor’s predecessor is still itself
                    // if not: update its successor; Notify its new successor to update predecessor
                    if response <> id then
                        if response <> -1L then successor <- response
                        (getWorkerById successor) <! Notify(id)
                if id = successor && predecessor <> -1L then 
                    successor <- predecessor
                    reportProp("-!!-STB")
                    (getWorkerById successor) <! Notify(id)
                // printerRef <! Print($"[DEBUG]: id {id}, successor {successor}, predecessor {predecessor}")
            let getClosestPrecedingNode currId =
                let mutable res = id //0
                for i = F_TABLE_SIZE - 1 downto 0 do
                    let (_, succ) = fingerTable.[i]
                    if (checkWithin succ id currId) then res <- succ
                res
            let findSuccessor currId = 
                reportProp($"!!!!!!!findSuccessor  currId={currId} id={id} successor={successor} ")
                // if target within the range of current node and its successor return successer
                // if not, find the closest preceding node of the target. The successor of preceding node should be same with target
                // printfn $"currId:{currId}\tid:{id}\tsuccessor:{successor}\t in range:{checkWithin currId id successor}"
                if (checkWithin currId id successor) then 
                    successor
                else 
                    let closestPrecedingNode = (currId |> getClosestPrecedingNode |> getWorkerById) 
                    reportProp($"^^^^^^^^findSuccessor currID={currId} closestPrecedingNode={closestPrecedingNode}")
                    Async.RunSynchronously (closestPrecedingNode <? FindSuccessor(currId),500)
            let findSuccessorAndCnt tId oId jNum = 
                if (checkWithin tId id successor) then 
                    successor |> getWorkerById <! Request(tId, oId, jNum + 1)
                else 
                    let closestPrecedingNode = (tId |> getClosestPrecedingNode |> getWorkerById) 
                    closestPrecedingNode <! Request(tId, oId, jNum + 1)
            let fixFingerTable _ = 
                let mutable next = 0
                let mutable finger = []
                // update each finger
                let fixFinger _ =
                    // printfn $"next:{next}"
                    next <- next + 1
                    // printfn $"next:{next}"
                    if next > F_TABLE_SIZE then next <- 1
                    findSuccessor (id + int64 (2. ** (float (next - 1))))
                    // Async.RunSynchronously(getWorkerById successor <? FindSuccessor((id + int64 (2. ** (float (next - 1))))))
                reportProp($"----fixFingerTable")
                for i in 1..F_TABLE_SIZE do 
                    reportProp($"~~~~~~fixFingerTable i={i} next={next} fixFinger()={fixFinger()}")
                    finger <- finger@[(next, fixFinger())]
                reportProp($"######fixFingerTable {finger}")
                finger
            fingerTable <- fixFingerTable()
            
            timer.Start()
            // Methods for join and stablize 
            let rec check _ =
                    printfn $"!!!{id} do CHECK!!!"
                // while true do 
                    async {
                        stabilize()
                        reportProp("check_after_stabilize")
                        fingerTable <- fixFingerTable()
                        reportProp("check_after_fix_ftable")
                        // System.Threading.Thread.Sleep(generateRandom 1000 5000)
                        // getWorkerById id <! Update(id)
                        check ()
                    } |> Async.Start
                    // // Async.RunSynchronously waitTime // Wait some tiem before run following codes
                    // stabilize()
                    // // TODO: fixTable(); (Optional)check if predecessor is failed
                    // // predecessor <- checkPredecessor id 
                    // fingerTable <- fixFingerTable()
            // async {check()} |> ignore
            check ()
            let rec loop() =
                actor {
                    let! message = mailbox.Receive()
                    let outBox = mailbox.Sender()
                    match message with
                    | Create(succ, pred) ->
                        successor <- succ
                        predecessor <- pred
                    | Join(target) ->
                        if true then printerRef <! Print($"[DEBUG][Join]: joining {id} to {target}") 
                        // predecessor <- -1L
                        // if id <> 0L then
                        successor <- (Async.RunSynchronously ((getWorkerById target) <? FindSuccessor(id)))
                        printfn $"[DEBUG][Join]:successor={successor}"
                        (getWorkerById successor) <! Notify(id)
                        fingerTable <- fixFingerTable()
                        reportProp("--AFTER JOIN")
                        // (getWorkerById id) <! Update(id)
                        // check()
                        // async {check()} |> ignore 
                    | FindSuccessor(currId) ->
                        if DEBUG then printerRef <! Print($"[DEBUG][FindSuccessor]: currId {currId}") 
                        reportProp($"vvvvvvvvvFindSuccessor currId={currId}")
                        outBox <! findSuccessor currId
                    | Notify(predId) -> 
                        if true then printerRef <! Print($"[DEBUG][Notify]: change {id}'s pred to {predId}")
                        if predecessor = -1L || checkWithin predId predecessor id then 
                            predecessor <- predId
                        reportProp("Notify")
                            // printfn $"!!!Update predecessor!!! id={id}, predecessor={predecessor}"
                    | CheckPredecessor(currId) -> 
                        // if DEBUG then printfn $"[DEBUG][CheckPredecessor]: currId {currId}"
                        reportProp("CHECK PRED")
                        currId |> ignore
                        outBox <! predecessor
                    // | NetDone(msg) -> 
                    //     printfn $"net done. End stabilize fix check and start sending request"
                    //     selfcheck <- false; // Stop periodical method 
                    //     for i in 1 .. numRequests do
                    //         let key = 0; //need to generate a random key within the chord
                    //         let self = getWorkerById id
                    //         self <! Request(key, id, 0) 
                    // | Update(_) ->
                    //     // async {
                    //     // Async.RunSynchronously waitTime
                    //     // Async.StartAsTask(Threading.Tasks.TaskCreationOptions(stabilize())
                        
                    //     reportProp("Update")
                    //     // stabilize()
                    //     // printerRef <! Print($"[DEBUG]: id {id}, successor {successor}, predecessor {predecessor}")

                    //     // fingerTable <- fixFingerTable()
                        
                    //     // Async.StartAsTask(getWorkerById id <? Update(id)) |> ignore
                    //     // printerRef <! Print("UPDATE")
                    //     // getWorkerById id <! Update(id)
                    //     async {
                    //         stabilize()
                    //         fingerTable <- fixFingerTable()
                    //         System.Threading.Thread.Sleep(5000)
                    //         getWorkerById id <! Update(id)
                    //     } |> Async.Start
                        // } |> ignore
                        (*
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
                        *)
                    | Request(targetId, originId, jumpNum) ->
                        if DEBUG then printfn $"[DEBUG][Request]: targetId {targetId}, originId {originId}, jumpNum {jumpNum}"
                        // resources in the range of (predecessor, id] are maintained in
                        if checkWithin targetId predecessor id then 
                            // report id to the node that made request
                            originId |> getWorkerById <! Response(id)
                            // report jumpNum to boss 
                            myboss <! Report(jumpNum)
                        findSuccessorAndCnt targetId originId jumpNum
                        // //check whether require next jump(targetid exceed the largest range of fingertable)
                        // let x = 2.0 ** 63.0
                        // if float targetId > (float id + x) then 
                        //     let nextWorker = getWorkerById fingerTable.Tail
                        //     nextWorker <! Request(targetId, originId, jumpNum + 1)
                        // else //find range and the node it belong to
                            
                        //     let lists = List.unzip fingerTable
                        //     let range = fst lists
                        //     let rangeSuss = snd lists
                        //     if checkWithin targetId id successor then 
                        //         let origin = getWorkerById originId
                        //         origin <! Response("succeed")
                        //         myboss <! Report(jumpNum) 
                        //     else 
                        //         let nextWorker = findSuccessor id targetId fingerTable 
                        //         nextWorker <! Request(targetId, originId, jumpNum + 1)
                    | Response(currId) ->
                        if DEBUG then printfn $"[DEBUG][Response]: currId {currId}"
                        printfn $"[INFO]: Found requested resource' successor: {currId} for {id}"
                    | RepProp(msg) -> reportProp(msg)
                    | Alive(msg) -> 
                        async {
                            checkAlive(msg)
                            System.Threading.Thread.Sleep(1000)
                            getWorkerById id <! Alive(msg)
                        } |> Async.Start
                    | _ -> ()
                    
                    return! loop()
                }
            loop()
        )


// type Node() = 
//     member val id = -1 with get, set
//     member val succ = -1 with get, set
//     member val pred = -1 with get, set
let localActor (mailbox:Actor<_>) = 
    // let actorZero = createWorker 0

    let mutable completedLocalWorkerNum = 0
    let mutable localActorNum = 0
    let mutable totJumpNum = 0
    // let mutable map = Map.empty // (id:(pred, succ))

// Assign tasks to worker
    let rec loop () = actor {
        let! message = mailbox.Receive()
        // printfn $"[DEBUG]: Boss received {message}"
        match message with 
        | Input(n,r) -> 
            printfn $"[INFO]: Num of Nodes: {n}\t Num of Request: {r}"
            localActorNum <- n
            // create actors
            let zero = createWorker (0 |> int64) 
            // zero <! Create(1L, 1L)
            let one = createWorker (1 |> int64) 
            // one <! Create(0L, 0L)
            one <! Join(0L)
            one <! RepProp("After join 1 to 0")
            zero <! RepProp("After join 1 to 0")
            // Async.Sleep(2000) |> ignore
            zero <! Alive("@@@")
            one <! Alive("@@@")
            // zero <! Update(0L)
            // one <! Update(0L)
            one <! RepProp("After update 0")
            zero <! RepProp("After update 0")
            // [2..(n - 1)] |> List.iter(fun id -> 
            //     createWorker (id |> int64) <! Join(0L))
                    // actor <! Update(0L))
            // Group actors by router
            // let workerenum = [|for i = 1 to workersPool.Length - 1 do (sprintf "/user/worker%d" i)|] |> Array.sortBy(fun _ -> rnd.Next(1, int (n - 1)))
            // let workerSystem = system.ActorOf(Props.Empty.WithRouter(Akka.Routing.RoundRobinGroup(workerenum)))
            // system.ActorOf()
            // create chord network
            // let url = "akka.tcp://Project2@localhost:8777/user/"
            // (0 |> getWorkerById) <! Create(0L)
            // join actors
            // [1..(n - 1)] |> List.iter(fun _ -> (workerSystem <! Join(0L)))
            printfn "[INFO]: All nodes have joined into the Chord network."
            // Send finish msg
            // actorZero <! NetDone("Done");
            // [1..(n - 1)] |> List.iter(fun _ -> (workerSystem <! NetDone("Done")))
            // printfn "[INFO]: Start request."
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

let boss = spawn system "boss" localActor
boss <! Input(numNodes, numRequests)

system.WhenTerminated.Wait()