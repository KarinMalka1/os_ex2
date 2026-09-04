# Multi-threaded MapReduce Framework

This repository contains a C++ implementation of a multi-threaded MapReduce framework, developed as an assignment for the Operating Systems course.

The framework allows clients to run MapReduce jobs concurrently. It handles thread creation, workload distribution, and synchronization behind the scenes, so the user only needs to implement the specific `Map` and `Reduce` logic.

## The Pipeline

The framework executes jobs in three distinct phases:
1. **Map Phase:** Multiple worker threads read input elements and generate intermediate (Key, Value) pairs concurrently.
2. **Sort/Shuffle Phase:** A dedicated thread (usually thread 0) takes the intermediate pairs, sorts them, and groups them by their keys.
3. **Reduce Phase:** The worker threads pick up the grouped data and process them to produce the final output vectors.

## Under the Hood (Concurrency & Synchronization)

To ensure thread safety and efficiency, the framework implements several OS-level concepts:
* **Pthreads:** Used for creating and managing the worker threads.
* **Mutexes:** Applied to protect shared data structures (like the intermediate vectors and output queues) from race conditions.
* **Atomic Variables:** Utilized for lock-free state tracking (e.g., updating the percentage of completion for the current job phase).

## Main Structure

* `MapReduceJob.cpp` / `.h` - The core logic managing the thread pool, contexts, and the transition between the Map, Shuffle, and Reduce phases.
* `MapContext.cpp` / `ReduceContext.cpp` - Wrappers for the data contexts passed to the threads during their execution.
* `MapReduceClient.h` - The interface containing the virtual `Map` and `Reduce` functions that a user needs to implement.
* `sample_client/SampleClient.cpp` - A basic example showing how to initialize and run a job using the framework.
