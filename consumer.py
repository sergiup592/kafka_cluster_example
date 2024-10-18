from confluent_kafka import Consumer
import json

class ConsumerClass:
    def __init__(self, bootstrap_server, topic, group_id):
        self.bootstrap_server = bootstrap_server
        self.group_id = group_id
        #bootstrap server is the enpoint url of kafka cluster.
        #grop id is the id of the group to which the consumer 
        #belongs too. Consumer groups all consume from the same
        #topic partition.
        self.consumer = Consumer({
                            "bootstrap.servers": self.bootstrap_server,
                            "group.id": self.group_id
                        })
        self.topic = topic
        
    def consumer_messages(self):
        #Meessage wont be removed from the topic when consumed.
        #Before consuming a message we need to subscribe
        #to the topic.
        self.consumer.subscribe([self.topic])
        #Consumer should be always up and running since it keeps
        #checking if there are any new messages. We will use the
        #poll method which will consume a single message at a time.
        try:
            while True:
                #read the message and if there is no message
                #to be read timeout after 1 sec.
                msg = self.consumer.poll(1.0)
                #if the consumer keeps polling and there is
                #message we will keep polling
                if msg is None:
                    continue
                #If there is an error while we are consuming
                #a message, we will handel it here.
                if msg.error():
                    print(f"Error while consuming message: {msg.error}")
                    #The consumer should not stop if we encounter an error
                    continue
                #If the message is consumed without any errors,
                #read the message value. Also decode the message
                #from byte to string since messages are atomatically
                #serilzed to bytes by the producer before being 
                #send to the kafka cluster. Usually we dont pass
                #messages in form of a string. The messages are
                #usually passed in form of avro, protobuf or json.
                #the format and structure of the message is stored 
                #in the schema registry.
                print(f"Message Consumed {msg.value().decode("utf-8")}")
                #Get the message in json form
                dict_message = json.loads(msg.value().decode("utf-8"))
                print(dict_message, type(dict_message))
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()

if __name__ == "__main__":
    bootstrap_server = "localhost:19092"
    topic = "test-topic"
    group_id = "my-group-id"

    consumer = ConsumerClass(bootstrap_server, topic, group_id)
    consumer.consumer_messages()




            
            


        
