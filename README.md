# DOSP-Chord-P2P-System-and-Simulation

## Actor initiation
Use numbered ID for each actor for now. The ID can be changed to SHA-1 in the future. SHA-1 is good for clients that runs in different remote machines.

## Chrod ring initialization
1. create the first node of the ring. Each node has a predecessor = null, successor = n;
2. Join n nodes with the first node to increase the ring size -> method in boss
3. Send message to each nodes indication the network is established
4. Calculate average jumps; if all node finished its job -> exit

## For each node, there are 3 method runs periodically:
* stabilize(): it asks its successor for the successor’s predecessor p, and decides whether p should be n’s successor instead.
* fix fingers(): to make sure its finger table entries are correct
* check predecessor(): return current node's predecessor.
### Variables
* predecessor
* number of jumps -> notify boss if reach the end
* List -> finger table

* Request(): send request to find certain node 
