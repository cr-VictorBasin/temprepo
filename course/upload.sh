#!/bin/bash
#################
# Calculate and adjust elastic heap size
#################
/bin/echo "Lets Calculate and adjust elastic heap size"
TOTAL_MEM_GB=$(cat /proc/meminfo |grep MemTotal |awk '{print $2 / 1024 / 1024}')
TOTAL_MEM_GB=$( printf "%.0f" $TOTAL_MEM_GB )
if [ $TOTAL_MEM_GB -ge 7 -a $TOTAL_MEM_GB -lt 15 ]; then
    ELASTIC_HEAP=4
elif [ $TOTAL_MEM_GB -ge 15 -a $TOTAL_MEM_GB -lt 31 ]; then
    ELASTIC_HEAP=8
elif [ $TOTAL_MEM_GB -ge 31 -a $TOTAL_MEM_GB -lt 63 ]; then
    ELASTIC_HEAP=16
elif [ $TOTAL_MEM_GB -ge 63 ]; then
    ELASTIC_HEAP=30
else
    ELASTIC_HEAP=1
fi
echo "Calculated elastic heap size is: "$ELASTIC_HEAP
ELASTIC_HEAP=$ELASTIC_HEAP"g"
echo "about ot replace with new value: "$ELASTIC_HEAP
sed -i "s/-Xms.*/-Xms$ELASTIC_HEAP/g" /etc/elasticsearch/jvm.options
sed -i "s/-Xmx.*/-Xmx$ELASTIC_HEAP/g" /etc/elasticsearch/jvm.options