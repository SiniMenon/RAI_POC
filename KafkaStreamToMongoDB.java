import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Properties;
import com.mongodb.*;
import com.mongodb.MongoClient;
public class KafkaStreamToMongoDB {


    public static void main(String[] args) throws UnknownHostException {
        Properties myconsumerprop = new Properties();

        myconsumerprop.put("bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094");
        myconsumerprop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        myconsumerprop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        myconsumerprop.put("group.id", "sampleTest1");
        myconsumerprop.put("auto.offset.reset", "earliest");
        myconsumerprop.put("client.id", "testclient");
        KafkaConsumer myconsumer = new KafkaConsumer(myconsumerprop);
        MongoClient mongoClient;
        mongoClient = new MongoClient("localhost");
        DB db = mongoClient.getDB("myMongoDB");
        DBCollection collection = db.getCollection("sample");
        BasicDBObject document = new BasicDBObject();



        ArrayList<String> mytopicList = new ArrayList<String>();
        mytopicList.add("sample");
        mytopicList.add("test");
        myconsumer.subscribe(mytopicList);
        do {
            ConsumerRecords<String, String> myConsumerRecords = myconsumer.poll(10);
            for (ConsumerRecord<String, String> myConsumerRecord : myConsumerRecords) {
                    /*System.out.println(String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s",
                            myConsumerRecord.topic(), myConsumerRecord.partition()
                            , myConsumerRecord.offset(), myConsumerRecord.key(), myConsumerRecord.value()));*/
                document.put("Key", myConsumerRecord.key());
                document.put("Value", myConsumerRecord.value());
                WriteResult insert = collection.insert(document);

            }

        } while (true);
        //mongoClient.close();
        //myconsumer.close();

    }
}



