#!/bin/bash
for i in `grep -v '#' us-east-1b-nodes`
do
   echo "Welcome $i"
   STATE=$(curl -s -k -u cruser:U87P^wK@ev -XGET "https://cr-prod-hp1-us-east1-b-420-master1.prod.cybereason.net:9200/_cluster/stats" |jq -r '.status')
   echo "Got cluster status = $STATE"
   if [ $STATE == "green" ]; then
    echo "found that cluster is green!"
   elif [ $STATE == "yellow" ]; then
     echo "found that cluster is yellow!"
   else
     echo "elastic is RED!"
   fi
   while [ $STATE != "green" ]
   do
     echo "cluster is not green. sleeping for 60 seconds and retry..."
     sleep 60
     STATE=$(curl -s -k -u cruser:U87P^wK@ev -XGET "https://cr-prod-hp1-us-east1-b-420-master1.prod.cybereason.net:9200/_cluster/stats" |jq -r '.status')
   done
   #ssh -o StrictHostKeyChecking=no -i /home/centos/.ssh/CR-Production.pem centos@$i 'hostname;sudo service elasticsearch-cr-prod-eu-hp2 stop;sudo sleep 5; sudo service elasticsearch-cr-prod-eu-hp2 start'
   echo "cluster is green, start working on the node"
   ssh -o StrictHostKeyChecking=no -i  centos@$i 'hostname'
   ssh -o StrictHostKeyChecking=no -i /home/centos/.ssh/CR-Production.pem centos@$i 'sudo /bin/puppet agent -t'
   echo -e "finished working on the node. moving to next node\n\n"
   sleep 2
done


