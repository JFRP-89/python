""" 
Unit executes commands of a typical confluent-kafka server. 

Possible options: 
 - create_topic 
 - cluster_config
 - describe_topics
 - list_topics
 - produce_message

NOTE: localhost:9092 is put as a bootstrap server default so you must put a specific bootstrap_server.

 Examples: 
     Create a topic:
         python kafkaGob.py -o create_topic -b 192.0.2.11:9092 -t test -p 1 -r 1
  
     Showing the configuration of a cluster:
         python kafkaGob.py -o cluster_config -b 192.0.2.11:9092 -t juanfran
  
     Describe topic: 
         python kafkaGob.py -o describe_topics -b 192.0.2.11:9092 
""" 


import kafka
from kafka import KafkaProducer
import argparse
import confluent_kafka
import concurrent.futures
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource
import logging
import os

log = logging.getLogger(os.path.splitext(__file__)[0]) 
logfile = 'operations.log' 
version = "1.0" 

def create_topic(bootstrap_server, new_topic, partitions, replication_factors):
    """
    Creates a topic. It requires a bootstrap_server ip, a topic name, partitions (1 by default) and replication_factors (1 by default)    
    """
    admin_client = kafka.admin.KafkaAdminClient(bootstrap_servers=bootstrap_server)
    topic_list = []
    topic_list.append(kafka.admin.NewTopic(name=new_topic, num_partitions=partitions, replication_factor=replication_factors))
    return admin_client.create_topics(new_topics=topic_list, validate_only=False)
def cluster_config(bootstrap_server, topic):
    """
    Shows the cluster configuration. It requires a bootstrap_server ip and the name of a existing topic
    """
    conf = {'bootstrap.servers': bootstrap_server}
    adminClient = AdminClient(conf)
    topic_configResource = adminClient.describe_configs([ConfigResource(confluent_kafka.admin.RESOURCE_TOPIC, topic)])
    for j in concurrent.futures.as_completed(iter(topic_configResource.values())):
        config_response = j.result(timeout=1)        
    print config_response
def describe_topics(bootstrap_server, group_id):
    """
    Describe the topics of a bootstrap server. Although the group_id is put by default, it requires a specific bootstrap_server ip for working this command.
    """
    c = confluent_kafka.Consumer({'bootstrap.servers': bootstrap_server, 'group.id': group_id})
    clusterMetadata = c.list_topics()
    topics = clusterMetadata.topics
    for linea_topic in list(topics.items()):
        print('Topic name: {}'.format(linea_topic[1]))
        particiones = linea_topic[1].partitions
        for linea_particion in list(particiones.items()):
            print('Partition number: {}'.format(linea_particion[1]))
            describe_data = linea_particion[1]
            print('ID: {}'.format(describe_data.id))
            print('Leader: {}'.format(describe_data.leader))
            print('Replicas: {}'.format(describe_data.replicas))
            print('ISRS: {}'.format(describe_data.isrs))
            
def list_topics(bootstrap_server, group_id):
    """
    List the topics of a bootstrap server. It requires the same arguments than describe_topics.
    """
    consumer = confluent_kafka.Consumer({'bootstrap.servers': bootstrap_server, 'group.id': group_id})
    clusterMetadata = consumer.list_topics()
    topics = clusterMetadata.topics
    for linea_topic in list(topics.items()):
         print(linea_topic[0])
            
def produce_message (bootstrap_server, topic, key, message):
    """
    Posts a message in a topic of a bootstrap server. It requires the bootstrap_server IP, the name of an existing topic and the content of a message. Key is optional.
    """
    producer = KafkaProducer(bootstrap_servers=bootstrap_server)
    producer.send(topic, key=key, value=message)
    print ('The message has been posted in the topic {}'.format(topic))
    
def main(args, loglevel):
    if args.logging: 
         logging.basicConfig(filename=logfile, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', level=loglevel) 
    logging.info('Started check_hardware') 
    # if option is put, then it will switch to the right command
    if args.order == 'create_topic':
        create_topic(args.bootstrap_server, args.topic, args.partitions, args.replication_factor)
    elif args.order == 'cluster_config':
        cluster_config(args.bootstrap_server, args.topic)
    elif args.order == 'describe_topics':
        describe_topics(args.bootstrap_server, args.group_id)
    elif args.order == 'list_topics':
        list_topics(args.bootstrap_server, args.group_id)
    elif args.order == 'produce_message':
        produce_message(args.bootstrap_server, args.topic, args.key, args.message)
        
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-V', '--version', action='version', version='%(prog)s ' + version)
    parser.add_argument('-o', action='store',
                    dest='order',
                    help='Puts the command to execute')
    parser.add_argument('-b', action='store',
                    dest='bootstrap_server',
                    default='localhost:9092',
                    help='Introduces the IP of a bootstrap server')
    parser.add_argument('-t', action='store',
                    dest='topic',
                    help='Adds the name of a topic in certain commands')
    parser.add_argument('-k', action='store',
                    dest='key',
                    help='Adds the key of a producer message')
    parser.add_argument('-m', action='store',
                    dest='message',
                    help='Writes the content of a producer message')
    parser.add_argument('-g', action='store',
                    dest='group_id',
                    default='1',
                    help='Puts a specific group.id')
    parser.add_argument('-p', action='store',
                    dest='partitions',
                    default='1',
                    help='Adds a specific ammount of partitions',
                    type=int)
    parser.add_argument('-r', action='store',
                    dest='replication_factor',
                    default='1',
                    help='Adds a specific ammount of replication factors',
                    type=int)
    parser.add_argument('-l', '--logging', help='create log output in current directory', action='store_const', const=True, default=False)
    verbosity = parser.add_mutually_exclusive_group() 
    verbosity.add_argument('-v', '--verbose', help='increase output verbosity', action='store_const', const=logging.DEBUG, default=logging.INFO) 
    verbosity.add_argument('-q', '--quiet', help='hide any debug exit', dest='verbose', action='store_const', const=logging.WARNING) 
    return parser.parse_args()

if __name__ == '__main__': 
    args = parse_args()
    main(args, args.verbose)