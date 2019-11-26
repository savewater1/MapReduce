The master implemets map-reduce functionalities as services that could be consumed by other applications.
The current implementation is for local machine which will be updated to support remote execution on cloud.
master provides 3 functions to consume its services:
init_cluster(): This method is used to connect to key-val store, partition input data and store it as chunks on key-val store and in future,
to provision gcloud instances (will be added later).
runmapred(): This method calls map and reduce tasks, each as a seperate process, to execute the task in hand.
destroy(): This method is used to close connection to key-val store and , in future, destroy gcloud instances
provisioned to start workers executing map-reduce tasks.
The config.json file stores the configuration used by master for its working. It contains the following information:
1) The address of key-val store
2) The address of node where main file is executing
3) The address of node where master is to be executed
4) The keys for input_data, intermediate_data and output_data
5) The name of scripts implementing map and reduce tasks
6) The address of worker nodes.

For the current implementation, make sure main.py, master.py, reducer.py, mapper.py, config.json and input_data.pickle
are in the same directory. There are two sets of mapper.py, reducer.py and input_data.pickle for each
task, word count and inverted index, stored in seperate folders. Copy the required files to parent directory
to run the task of interest.

First start the server.py that implements the key-val store and then start the main.py with name of 
configuration file(config.json) as argument.

The main program automatically launches the master and then consumes its services using RPC. The master launches
and coordinates worker nodes to implement the map reduce task. Both intermediate and output data are stored
to the key-val store directly by worker nodes.

The main.py script produces a log file, named mapreduce.log, that logs all the major events. The logger named 
root logs events of master and logger named main logs events of driver program.

Use dos2unix on files.