# Distributed Systems Pset 

## Distributed Mutual Exclusion 

### Lamport's Shared Priority Queue 

**Structs Required: **

1. **Node** 
   1. ID 
   2. Channel 
   3. LogicalClock 
2. **Priority Queue** 
   1. Use a heap (?)
   2. Push and pop 
3. **Messages** 
   1. Type 
      1. Enum (Request, Reply, Release)
   2. From 
   3. To 

**Algorithm:** 

1. Create nodes for everyone 

2. Create channels for everyone 

3. Tell everyone of each others' channel 

4. Start sending messages  

   