""" 
Unit makes an inventory of a cluster which is prompted by the user. Additionaly, the user must prompt the path of the .json file which contains the inventory of the cluster. The clusterInfo.json file is the required format for making a full inventory of the cluster.

NOTE: localhost:9092 is put as a bootstrap server default so you must put a specific bootstrap_server. Additionaly, it is highly recommended to get at least one topic created for getting the full results of the cluster inventory.

 Examples: 
     Registering a cluster with the mininmum requirements:
         python inv_main.py -f cluster_localhost.json
  
     Registering a cluster with a specified dns location of the cluster:
         python inv_main.py -b mycluster:9092 -f cluster_localhost.json
""" 

import json
import os
import modules.cluster_options, modules.consumer_generator
import logging
import argparse

log = logging.getLogger(os.path.splitext(__file__)[0]) 
logfile = 'operations.log'
version = "1.0" 

def extract_topic(bootstrap_server):
    """
    It checks if there is a topic for just getting the name of an existing topic in the cluster for dealing the inventory of the cluster configuration.
    """
    import confluent_kafka
    p = confluent_kafka.Producer({'bootstrap.servers': '192.0.2.11:9092'})
    clusterMetadata = p.list_topics()
    topics = clusterMetadata.topics
    topic_name = ""
    for topic_line in list(topics.items()):
        if topic_line!="":
            topic_name=str(topic_line[1])
            break
    return topic_name
    
def main(args, loglevel):
    """
    It fuses both, the .json file from the path_file prompted and the results of both modules, consumer_generator and cluster_options.
    """
    if args.logging: 
         logging.basicConfig(filename=logfile, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', level=loglevel) 
    logging.info('Started check_hardware')
    fname='cluster_inventory.json'
    with open(args.file_path) as file:
        cluster_main = json.load(file)
    cluster_inventory = {}
    sizing_values = cluster_main['clusters'][0]['metadata']['sizingValues']
    cluster_options = modules.cluster_options.cluster_config(args.bootstrap_server, extract_topic(args.bootstrap_server))
    producers = cluster_main['clusters'][0]['metadata']['producers']
    consumers = modules.consumer_generator.consumer_listing(args.bootstrap_server)
    cluster_inventory.update({"clusters":{"clusterName": "name", "metadata": {"sizing_values": sizing_values, "cluster_options":  cluster_options, "producers": producers, "consumers": consumers}}})
    with open('cluster_inventory.json', 'w') as file:
        json.dump(cluster_inventory, file, indent=4)    

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-V', '--version', action='version', version='%(prog)s ' + version)
    parser.add_argument('-b', action='store',
                    dest='bootstrap_server',
                    default='localhost:9092',
                    help='Introduces the IP of a bootstrap server')
    parser.add_argument('-f', '-fp', action='store',
                    dest='file_path',
                    help='Adds the path of the file which contains the cluster Info in .json file')
    parser.add_argument('-l', '--logging', help='create log output in current directory', action='store_const', const=True, default=False)
    verbosity = parser.add_mutually_exclusive_group() 
    verbosity.add_argument('-v', '--verbose', help='increase output verbosity', action='store_const', const=logging.DEBUG, default=logging.INFO) 
    verbosity.add_argument('-q', '--quiet', help='hide any debug exit', dest='verbose', action='store_const', const=logging.WARNING) 
    return parser.parse_args()
    
if __name__ == '__main__': 
    args = parse_args()
    main(args, args.verbose)