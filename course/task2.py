nodes_rm_ips = []
nodes_rm_ips.append('10.244.0.51')
nodes_rm_ips.append('10.244.0.52')   
nodes_rm_ips.append('10.244.0.53')   
nodes_rm_ips.append('10.244.0.54')  
null = None
nodes_rm_ips_str = null 
print(nodes_rm_ips_str)
#nodes_rm_ips = ', '.join(map(str, nodes_rm_ips))
#for ip in nodes_rm_ips:
exclude_ips = "curl -s -k -u " + 'elastic_user' + ":" + 'elastic_pass' + \
                         " -XPUT \"https://127.0.0.1:" + str(9200) + \
                         "/_cluster/settings\" -H " "'Content-Type: application/json' -d' {\"persistent\":" \
                         ' { \"cluster.routing.allocation.exclude._ip\": ' + str(null) + '}}\''
print(exclude_ips)
