from confluent_kafka import Producer
from admin import Admin


class ProducerClass:
    def __init__(self, bootstrap_server, topic):
        #Create an instance of the producer and initialize it with
        #the url of the kafka cluster whihc in this case is the
        #bootstrap_server and a topic to which the message will be
        #produced.
        self.bootstrap_server = bootstrap_server
        self.topic = topic
        self.producer = Producer({"bootstrap.servers": self.bootstrap_server})

    def send_message(self, message):
        try:
            self.producer.produce(self.topic, message)
        except Exception as e:
            print(e)
    
    def commit(self):
        #Flush: wait for all messages in the Producer queue to be delivered.
        #We need this method because the producer will continiously produce
        #some message and in order to know if all the messages were delivered 
        #we wil flush which will wait for all the messages to be delivered.
        self.producer.flush()

if __name__ == "__main__":
    bootstrap_server = "localhost:19092"
    topic = "test-topic"
    a = Admin(bootstrap_server)
    a.create_topic(topic)
    p = ProducerClass(bootstrap_server, topic)
    #Produce a meaasage
    try:
        while True:
            message = input("Enter your message: ")
            p.send_message(message)
    #This will continiously ask for new message, so will try
    #to listen for a keyboard interrupt to stop.
    except KeyboardInterrupt:
        pass
    #Once all the messages have been send we can flush
    p.commit()





