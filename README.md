# pandadb-bench

This benchmark suits for PandaDB and other neo4j-compatible graph databases.

## Usage
1. Clone this repo, and replace the string "bolt://db:8687" to your db bolt address, also the authentication.
2. Set the sampleSize as your wishes!
3. `mvn package`

(It will run performance tests when packaging.)

## Introduction
This project consists of 5 atomic benchmarks: 'creationBench', 'modifyBench', 'doubleFilterBench', 'tripleFilterBench' and 'regexpFilterBench', as well as several mixed benchmarks by them.
All of them use random chosen sample nodes from current running db to simulate their create/set/where cypher clauses. The query submission takes a very simple policy: a thread poll (size = 8) sends queries one after another to server side every 200 ms.

(Certainly, you can change those default parameters).