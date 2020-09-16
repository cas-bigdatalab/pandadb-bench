# pandadb-bench

The benchmark suit for pandadb and other neo4j-compatible graph databases.

## Usage
1. Clone this repo, and replace the string "bolt://db:8687" to your db bolt address, also the authentication.
2. Set the sampleSize as your wishes!
3. `mvn package`
(It will run performance tests when packaging.)

## Introduction
This bench consists of 5 atomic benchmark 'creationBench', 'modifyBench', 'doubleFilterBench', 'tripleFilterBench' and 'regexpFilterBench', and several mixed benchmark by them.benchmark
All these benchmark use random choose sample nodes from current running db to simulate their create/set/where cypher clauses. The query submission takes a simple policy: a thread poll (size = 8) send async queries to server every 200 ms.
(You can change those default parameters).