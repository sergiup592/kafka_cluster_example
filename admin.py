from confluent_kafka.admin import AdminClient, NewTopic

class Admin:
    def __init__(self, bootstrap_server):
        #Admin client will create the topic.
        #it needs the bootstrap server since admin client is also
        #an external client and it needs to be connected
        #to the cluster since the topic is created in the
        #cluster.
        self.bootstrap_server = bootstrap_server
        self.admin = AdminClient({"bootstrap.servers": self.bootstrap_server})
    

    def topic_exists(self, topic):
        #We dont want to create the same topic repeatedly so we have to check
        #if it already exists.
        all_topics = self.admin.list_topics()
        return topic in all_topics.topics.keys()

    def create_topic(self, topic):
        if not self.topic_exists(topic):
            new_topics = NewTopic(topic)
            self.admin.create_topics([new_topics])
            print(f"Topic: {topic} has been created")
        else:
            print(f"Topic: {topic} already exists")



        