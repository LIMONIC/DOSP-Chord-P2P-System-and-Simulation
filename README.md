# DOSP-Chord-P2P-System-and-Simulation
# Introduction

A common issue in a P2P system is how to efficiently locate nodes, that is, how a node can efficiently know which node in the network contains the data it is looking for. Chord is a protocol that solves this problem by maintaining a cycle of network and jumping through multiple nodes. It is commonly used to build Distributed Hash Tables (DHT) for structured P2P.

The main idea of DHT is that first, each file index is represented as a (K, V) pair, where K is an identical key related to the file and V is the IP address of the node that actually stores the file. All the file index entries (i.e., all the (K, V) pairs) form a large file index hash table, from which all the addresses of the nodes storing the file can be found by simply entering the K value of the target file. Then, the above large file hash table is divided into many small local blocks, and the local hash tables of these small blocks are distributed to all participating nodes in the system according to specific rules, making each node responsible for maintaining one of them. In this way, when a node queries a file, it simply routes the query message to the corresponding node (which maintains a hash table chunk containing the (K,V) pair to be found).

This project implements Chord protocol with AKKA[.](http://akka.net/)net's Actor model. It first dynamically joins new nodes into the network, then makes each node randomly search for a resource. The number of jumps are counted and calculated for an average value.

# Usage**:**

```
dotnet fsi --langversion:preview ChordActor.fsx <numNodes> <numRequests>
```

- numNodes: number of nodes try to join in the network
- numRequests: number of requests each node needs to send

# **Output**

The program prints the average number of hops (node connections) that have to be traversed to deliver a message.

# Implementation

### Network Initialization

1. First, we create a boss actor who is responsible for two tasks. One is to create the required number of nodes (worker actor) and join them into the chord network. The second task is to monitor the number of jumps reported by the workers. After all actors have sent the specified number of requests and reported the number of jumps, the average number of hops will be counted and printed by the boss actor.
2. The boss actor creates the chord network dynamically by creating and joining n nodes to the chord circle of size 2^32.

### Join Operation

1. Each node is a worker actor. When an actor is joined to the chord network, it will randomly send a `findSuccessor` request to an existing node in the network to help it find its successor node. After it obtains and points to the successor node successfully, it will notify the successor node of its existence. When the successor node receives the notification, it will mark the node as its predecessor.
2. After being created, each actor will periodically run the stabilize and `fixFingertable` methods until the entire network is created.
3. Calling the Stabilize method can maintain the successor node of the current node and the predecessor node of the successor node. First, the current node will ask whether the predecessor node of its successor node is itself. If not (indicating that there is a new node joining between the current node and the successor node), the current node will set the predecessor node of its successor node as its successor, and notify the new successor node. When the new successor receives the notification, it will set the current node as its predecessor. In this way, the position changes caused by the addition of new nodes can be maintained correctly.
4. The `fix_fingertable` method will call the `findSuccessor` method to check the correctness of each finger[i] in the `fingertable` and update. Fig 2. is a sequence diagram for joining operations.
    
    [Sequence diagram of adding a new node](https://lh5.googleusercontent.com/D1-4u7w4qPgxXD4FMtpJEyiYmpbOE43zBZyiuqRgSvPfys0s9ytq24qKOrjmUSKJiX--kkdKYgCrQHuN1rj0SZcp4OeKfz64FvWf4JWQEsWCGYOG3lAtHi8-MU7DRQjNM6ajWcAS)
    
    Fig2. Sequence diagram of adding a new node N to an existing Chord network consisting of two nodes A and B.
    
### Search for a Node

1. After the network is created, the boss actor sends a `netDone` notification to each node. After receiving the notification, the node will randomly generate `KeyI` and send a specified number of lookup requests.
2. When the node receives the lookup request, it will call the find successor method to find the node where the resource is located. A request will keep track of the count of hops traversed and send the count to the boss once the key is found.
3. Boss actor will keep track of total requests processed and shut down when all requests are processed.

### APIs of Node

Create: Initialize a node's predecessor, successor and finger table. This is only used for initializing a stabilized small size Chord network in the beginning. 

- `Join(n):` Join current node to a node n within the Chord network.
- `FindSuccessor(n):` Find node n's successor by current node.
- `Notify(n):` Notify node n to change its predecessor to current node.
- `CheckPredecessor:` return current node's predecessor
- `NetDone(requstNum):` Generate random resource id and send `requstNum` of requests for searching the successor node of that id.
- `Request:` Send a  request of searching a specific node once per second.

## Chrod ring initialization
1. create the first node of the ring. Each node has a predecessor = null, successor = n;
2. Join n nodes with the first node to increase the ring size -> method in boss
3. Send message to each nodes indication the network is established
4. Calculate average jumps; if all node finished its job -> exit


## For each node, there are 3 method runs periodically:
* stabilize(): it asks its successor for the successor’s predecessor p, and decides whether p should be n’s successor instead.
* fix fingers(): to make sure its finger table entries are correct
* check predecessor(): return current node's predecessor.

