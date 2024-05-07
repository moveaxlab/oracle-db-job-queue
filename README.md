# Oracle DB Job Queue

This repo contains an example implementation of a transactional job queue
using Oracle DB.

## Running against Oracle Cloud

1. Retrieve the suggested connection string from Oracle Cloud
2. Forward a proxy to the Oracle DB instance with something like `kubectl port-forward adb-proxy 1521 --address 0.0.0.0`
3. Set the `connectString` inside the `NewOracleQueue` function to the one suggested by Oracle Cloud,
   changing the host to your machine IP (if you ran the forward from outside the dev container)
4. Update the `user` and `password` values inside the `NewOracleQueue` function
