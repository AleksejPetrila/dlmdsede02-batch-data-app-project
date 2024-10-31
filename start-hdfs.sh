#!/bin/bash

# Checking whether the NameNode directory does not exist and formats it if necessary
if [ ! -d "/opt/hadoop/data/nameNode/current" ]; then
    echo "Formatting NameNode..."
    # formatting the directory and -force is used to bypass the confirmation prompt
    hdfs namenode -format -force
fi

# Starting the NameNode
echo "Starting NameNode."
hdfs namenode &

# The following part of the script is added, because it was an issue with HDFS safemode. Polling option is chosen,
# to avoid manual adjustment of sleep. It makes sure that leave safemode command would be run only after HDFS started.
# Was tested well during personal testing, so hope it runs well :D
# However, sometimes the first start gets error, while the second and consecutive work fine.

# Polling to check if HDFS is in safe mode
echo "Waiting for HDFS to leave safe mode."
while true; do
    # Check if safe mode is ON
    safemode_status=$(hdfs dfsadmin -safemode get | grep -i "Safe mode is ON")

    if [ -z "$safemode_status" ]; then
        echo "HDFS is now out of safe mode."
        break
    else
        echo "Safe mode is still ON. Waiting..."
        sleep 5
    fi
done

# Attempt to leave safe mode
echo "Attempting to leave safe mode."
hdfs dfsadmin -safemode leave

# Keeping the script running and waits for background jobs to finish before finishing the script.
wait
