from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource
import confluent_kafka
import concurrent.futures

""" 
This is a module from inv_main.py with the purpose of sending the configuration of the cluster.
""" 
def cluster_config(bootstrap_server, topic):
    conf = {'bootstrap.servers': bootstrap_server}
    adminClient = AdminClient(conf)
    topic_configResource = adminClient.describe_configs([ConfigResource(confluent_kafka.admin.RESOURCE_TOPIC, topic)])
    for j in concurrent.futures.as_completed(iter(topic_configResource.values())):
        config_response = j.result(timeout=1)
    dict_config = {}
    for key in config_response:
        dict_config[key]=str(config_response[key])
    return dict_config