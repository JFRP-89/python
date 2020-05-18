from kafka.admin import KafkaAdminClient

""" 
This is a module from inv_main.py with the purpose of sending the current consumers in the cluster.
""" 
def consumer_listing(bootstrap_server):
    admin_client = KafkaAdminClient(bootstrap_servers='192.0.2.11:9092')
    list_consumers = []
    for num in range(len(admin_client.list_consumer_groups())):
        list_consumers.append(admin_client.list_consumer_groups()[num][0])
    consumer_dict = {}
    for num_c in range(len(list_consumers)):
        consumer_dict['consumer {}'.format(num_c+1)]={}
        consumer_dict['consumer {}'.format(num_c+1)]['group_id']=list_consumers[num_c]
        if admin_client.describe_consumer_groups(list_consumers)[num_c][2] != 'Empty':
            topic_name = admin_client.describe_consumer_groups(list_consumers)[num_c][5][0][4].decode('utf-8','ignore')
            consumer_dict['consumer {}'.format(num_c+1)]['topic']=topic_name
            #the pointer is made for identifying the domain and the subdomain of a topic, assuming the topic names are made with the structure of [domain].[subdomain].[schema].[version]
            pointer="."
            deleting_pointers=topic_name.split(pointer)
            consumer_dict['consumer {}'.format(num_c+1)]['consumer_domain']=deleting_pointers[0]
            consumer_dict['consumer {}'.format(num_c+1)]['consumer_subdomain']=deleting_pointers[1]
            consumer_dict['consumer {}'.format(num_c+1)]['client-id']=admin_client.describe_consumer_groups(list_consumers)[num_c][5][0][1]
        else:
            consumer_dict['consumer {}'.format(num_c+1)]['topic']=""
            consumer_dict['consumer {}'.format(num_c+1)]['client-id']=""
        consumer_dict['consumer {}'.format(num_c+1)]['max_consumers']=3
    return consumer_dict