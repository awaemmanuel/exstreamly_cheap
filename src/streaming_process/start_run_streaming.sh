#!/bin/bash

## Start the user interaction process.
## 1. Generate user profiles
## 2. Subscribe these users to different categories 
##       a. create a purchase pattern. 
##       b. Process is driven by a randomized simulation
##          that generates a users name and then randomly 
##          samples deals categories and uniquely assigning it to user 
## 3. Produce to streaming topic for spark processing


## Tmux session variables.
SESSION_NAME='Streaming-Process'
ID_1=1
ID_2=2

## Start a session and then windows for user simulation and spark streaming processes.
tmux new-session -s $SESSION_NAME -n bash -d

# Start user simulation process
tmux new-window -t $ID_1
tmux send-keys -t $SESSION_NAME:$ID_1 'python ../engineered_data/simulate_user_interaction.py' C-m

# Tmux window for the spark process
### Run the bash process using the pyspark-cassandra connector on the spark master 
tmux new-window -t $ID_2
tmux send-keys -t $SESSION_NAME:$ID_2 'spark-submit --master spark://ip-172-31-2-36:7077  --conf spark.cassandra.connection.host=172.31.2.36 --jars ../libs/sample-assembly-1.0.jar --packages TargetHolding/pyspark-cassandra:0.2.4 stream_user_interaction.py > extract.txt' C-m

