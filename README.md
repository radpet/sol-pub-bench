# sol-pub-bench
Just a simple direct publisher for Solace

# Basic Usage

```
./sol-pub-bench -d 10 -c 80 
```

This runs a benchmark for 10 seconds, using 80 go routines (connections)
Output:

```
^Cstopping...
Direct Publisher Terminated?  true
Messaging Service Disconnected?  true
Publisher id:0
Number of Errors:       0
Msg seq 128559

Direct Publisher Terminated?  true
Messaging Service Disconnected?  true
Publisher id:1
Number of Errors:       0
Msg seq 143904
```
