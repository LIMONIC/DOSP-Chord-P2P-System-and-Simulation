#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit"
#r "nuget: Akka.Remote" 

open System
open System.Security.Cryptography
open System.Text
open Akka.Actor
open System.Collections.Generic
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit

let system = ActorSystem.Create("ChordModel", Configuration.defaultConfig())

type Information = 
    | Input of (int*int)
    | NetDone of (int)
    | Request of (int64*int64*int) // target id, origin id, num of jump
    | Response of (int64) // target node response its address to origin
    | Report of (int) // target node report num of jumps to boss
    | Create of (int64*int64*list<int * int64>)
    | Join of (int64)
    | Notify of (int64)
    | FindSuccessor of (int64)
    | CheckPredecessor of (int64)
    | Print of (string)
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

// Method for printing finger table
let printTab (tab:list<int * int64>) =
    let mutable str = ""
    for i in 0..tab.Length - 1 do
        str <- str + ", " + $"{tab.[i]}"
    str

// generate random int64
let generateRandom min max =
    let buf: byte[] = Array.zeroCreate 8 
    rnd.NextBytes(buf)
    let longRand = BitConverter.ToInt64(buf, 0);
    (abs (longRand % (max - min)) + min)

let getWorkerById id =
    let actorPath = @"akka://ChordModel/user/worker" + string id
    select actorPath system

let printer (mailbox:Actor<_>) =
    let rec loop () = actor {
        let! message = mailbox.Receive()
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
            let debug = false
            let detail = false
            let myboss = select @"akka://ChordModel/user/boss" system

            let mutable predecessor = -1L
            let mutable successor = id;
            let mutable fingerTable = []
            let timer = new Timers.Timer(1000.) // 50ms
            let timerForCheck = new Timers.Timer(10.)
            let waitTime = Async.AwaitEvent (timer.Elapsed) |> Async.Ignore
            let waitForCheck = Async.AwaitEvent (timer.Elapsed) |> Async.Ignore
            let checkWithin targetid startid endid = 
                if startid > endid then
                    (targetid > startid && targetid <= CHORD_RING_SIZE - 1L) || (targetid >= 0L && targetid <= endid)
                else if startid < endid then
                    targetid > startid && targetid <= endid
                else 
                    true

            let checkRange targetid startid endid = 
                if startid > endid then
                    (targetid > startid && targetid < CHORD_RING_SIZE - 1L) || (targetid >= 0L && targetid < endid)
                else if startid < endid then
                    targetid > startid && targetid < endid
                else 
                    false
                    
            let reportProp msg = 
                printerRef <! Print($"[***][{msg}]ID: {id}, predecessor: {predecessor}, successor: {successor}, fTable: {printTab fingerTable}")
            let checkAlive msg = 
                printerRef <! Print($"[{msg}]{id} is Alive!")
            let stabilize _ =
                // Ask currerent node's successor for the successor’s predecessor p
                if debug then printfn $"[DEBUG][BEFORE STAB]: id {id}, predecessor {predecessor}, successor {successor}"
                // Async.RunSynchronously 
                let mutable response = predecessor
                if id <> successor then 
                    response <- (Async.RunSynchronously((getWorkerById successor) <? CheckPredecessor(id)))
                    if response <> id then
                        if response <> -1L then successor <- response
                        (getWorkerById successor) <! Notify(id)
                // if id = successor && predecessor <> -1L then 
                //     successor <- predecessor
                //     (getWorkerById successor) <! Notify(id)
                if debug then printerRef <! Print($"[DEBUG]: id {id}, successor {successor}, predecessor {predecessor}")
            let getClosestPrecedingNode currId =
                let mutable res = id //0
                for i = F_TABLE_SIZE - 1 downto 0 do
                    let (_, succ) = fingerTable.[i]
                    if (checkRange succ id currId) then res <- succ
                res
            let findSuccessor currId = 
                // if target within the range of current node and its successor return successer
                // if not, find the closest preceding node of the target. The successor of preceding node should be same with target
                if (checkWithin currId id successor) then 
                    successor
                else 
                    let closestPrecedingNode = (currId |> getClosestPrecedingNode |> getWorkerById) 
                    if id = (currId |> getClosestPrecedingNode) then successor else
                        Async.RunSynchronously (closestPrecedingNode <? FindSuccessor(currId))

            let findSuccessorAndCnt tId oId jNum = 
                if (checkWithin tId id successor) then 
                    successor |> getWorkerById <! Request(tId, oId, jNum + 1)
                else 
                    let closestPrecedingNode = (tId |> getClosestPrecedingNode |> getWorkerById) 
                    if id = (tId |> getClosestPrecedingNode) then 
                        successor |> getWorkerById <! Request(tId, oId, jNum + 1)
                    else
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
                for _ in 1..F_TABLE_SIZE do 
                    finger <- finger@[(next, fixFinger())]
                finger
            
            timer.Start()
            timerForCheck.Start()
            // Periodically run stablize and fixFingerTable
            let rec check _ =
                    if debug then printerRef <! Print($"!!!{id} do CHECK!!!")
                    async {
                        // reportProp("check")
                        stabilize()
                        // reportProp("check_after_stabilize")
                        fingerTable <- fixFingerTable()
                        // reportProp("check_after_fix_ftable")
                        // // Async.RunSynchronously waitForCheck 
                        System.Threading.Thread.Sleep(50)
                        check ()
                    } |> Async.Start

            let rec loop() =
                actor {
                    let! message = mailbox.Receive()
                    let outBox = mailbox.Sender()
                    match message with
                    | Create(succ, pred, fTable) ->
                        successor <- succ
                        predecessor <- pred
                        fingerTable <- fTable
                        check ()
                    | Join(target) ->
                        if debug then printerRef <! Print($"[DEBUG][Join]: joining {id} to {target}") 
                        fingerTable <- fixFingerTable()
                        successor <- (Async.RunSynchronously ((getWorkerById target) <? FindSuccessor(id)))
                        if debug then printerRef <! $"[DEBUG][Join]:successor={successor}"
                        (getWorkerById successor) <! Notify(id)
                        fingerTable <- fixFingerTable()
                        check()
                    | FindSuccessor(currId) ->
                        if debug then reportProp($"[DEBUG][FindSuccessor]: currId {currId}") 
                        outBox <! findSuccessor currId
                    | Notify(predId) -> 
                        if debug then printerRef <! Print($"[DEBUG][Notify]: change {id}'s pred to {predId}")
                        if predecessor = -1L || checkWithin predId predecessor id then 
                            predecessor <- predId
                        if debug then reportProp("Notify")
                    | CheckPredecessor(currId) -> 
                        if debug then reportProp("CHECK PRED: currId {currId}")
                        currId |> ignore
                        outBox <! predecessor
                    | NetDone(reqNum) -> 
                        // Start sending request
                        for _ in 1 .. reqNum do
                            let key = generateRandom 0L (CHORD_RING_SIZE - 1L)  //need to generate a random key within the chord size
                            if detail then printerRef <! Print($"[Detail][Request] Node {id} is looking for node {key}..")
                            let self = getWorkerById id
                            self <! Request(key, id, 0) 
                            System.Threading.Thread.Sleep(1000)
                    | Request(targetId, originId, jumpNum) ->
                        if debug then printerRef <! Print($"[DEBUG][Request]: myId {id}, targetId {targetId}, originId {originId}, jumpNum {jumpNum}")
                        if checkWithin targetId predecessor id then 
                            // report id to the node that made request
                            originId |> getWorkerById <! Response(id)
                            // report jumpNum to boss 
                            myboss <! Report(jumpNum)
                        else 
                            findSuccessorAndCnt targetId originId jumpNum
                    | Response(currId) ->
                        if debug then printerRef <! Print($"[DEBUG][Response]: currId {currId}")
                        if detail then printerRef <! Print($"[Detail]: Found requested resource' successor: {currId} for {id}")
                    | Alive(msg) -> // For debug: report if the node is still alive.
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


(* Boss Actor *)
let localActor (mailbox:Actor<_>) = 
    let mutable completedRequest = 0
    let mutable localActorNum = 0
    let mutable totJumpNum = 0
    let mutable totRequest = 0
    // A set for nodes that will join to the network
    let mutable set = Set.empty // (id:(pred, succ))

    let timer = new Timers.Timer(500.) 
    let waitTime = Async.AwaitEvent (timer.Elapsed) |> Async.Ignore
    let timer2 = new Timers.Timer(5000.) 
    let waitForStabilize = Async.AwaitEvent (timer2.Elapsed) |> Async.Ignore
    
    // Initilize finger table for node zero in two-node-network. This table should be correct.
    let initFTableZero thisNode otherNode = 
                let mutable next = 0
                let mutable finger = []
                for _ in 1..F_TABLE_SIZE do 
                    next <- next + 1
                    if (thisNode + int64 (2. ** (float (next - 1)))) <= otherNode then 
                        finger <- finger@[(next, otherNode)]
                    else
                        finger <- finger@[(next, thisNode)]
                finger
    
    // Initilize finger table for the second node. Not necesary to be correct. The table will be fixed by fixFinger methord later
    let initFingerTable num = 
        let mutable next = 0
        let mutable finger = []
        for _ in 1..F_TABLE_SIZE do 
            next <- next + 1
            finger <- finger@[(next, num)]
        finger

    let rec loop () = actor {
        let! message = mailbox.Receive()
        // printfn $"[DEBUG]: Boss received {message}"
        match message with 
        | Input(n,r) -> 
            printfn $"[INFO]: Num of Nodes: {n}\t Num of Request: {r}"
            totRequest <- n * r
            // Randomly generate actors
            if n > 2 then 
                localActorNum <- n - 2
                while set.Count < localActorNum do
                    let radNum = generateRandom 1L (CHORD_RING_SIZE - 1L)
                    if radNum <> 100L then  
                        set <- set.Add(radNum)

            // Initialize the Chord network with two nodes
            let zero = createWorker (0 |> int64) 
            let hundred = createWorker (100 |> int64)
            zero <! Create(100L,100L,(initFTableZero 0L 100L))
            hundred <! Create(0L,0L,(initFingerTable 100L))

            let mutable str = "0, 100"
            set |> Set.toSeq |> Seq.iteri (fun i x -> str <- str + $", {x}")
            printerRef <! Print($"[INFO]: Nodes ids: {str}")

            timer.Start()
            timer2.Start()

            // Join nodes to the network
            set |> Set.toSeq |> Seq.iteri (fun i x -> 
                Async.RunSynchronously waitTime // Add waiting time for the network to stabilize
                // System.Threading.Thread.Sleep(500)
                printerRef <! Print($"[INFO]:Joining node {x}..")
                let tem = createWorker (x |> int64)
                tem <! Join(0L)
             )

            Console.ForegroundColor <- ConsoleColor.Yellow
            printerRef <! Print($"[INFO]: All nodes have joined to the network.")
            printerRef <! Print($"[INFO]: Waiting for the network to stabilize... ")
            Console.ForegroundColor <- ConsoleColor.White

            Async.RunSynchronously waitForStabilize

            // Send done message. Start sending request
            set |> Set.toSeq |> Seq.iteri (fun i x -> 
                //  printfn "%A" x
                getWorkerById (x |> int64) <! NetDone(numRequests)
             )
            zero <! NetDone(numRequests)
            hundred <! NetDone(numRequests)

        | Report(numOfJumps) ->
            completedRequest <- completedRequest + 1
            totJumpNum <- totJumpNum + numOfJumps
            printerRef <! Print($"[INFO]: completed:{completedRequest} \ttotal:{totRequest} \tjump num: {numOfJumps}")
            if completedRequest = totRequest then
                printerRef <! Print($"{completedRequest} requests finished. \nAverage jumps: {totJumpNum / completedRequest} \tTotal jumps: {totJumpNum} \tTotal Requests: {totRequest}")
                mailbox.Context.System.Terminate() |> ignore
        | _ -> ()
        return! loop()
    }
    loop()

let boss = spawn system "boss" localActor
boss <! Input(numNodes, numRequests)

system.WhenTerminated.Wait()