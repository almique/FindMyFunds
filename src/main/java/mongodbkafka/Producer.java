package mongodbkafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import com.mongodb.client.MongoCollection;

import java.util.Iterator; 
import com.mongodb.client.FindIterable;
import com.mongodb.MongoClientURI;

import java.util.Properties;
import java.util.Scanner;

public class Producer {
  private static Scanner in;
  /*
  final String uri = "mongodb://admin0:anuranjan5@kafkamd-shard-00-00-pvk6g.mongodb.net:27017,kafkamd-shard-00-01-pvk6g.mongodb.net:27017,kafkamd-shard-00-02-pvk6g.mongodb.net:27017/sample_analytics?ssl=true&replicaSet=kafkamd-shard-0&authSource=admin&retryWrites=true&w=majority";
  final MongoClientURI clientURI = new MongoClientURI(uri);
  final MongoClient mongo = new MongoClient(clientURI);

  final MongoDatabase database = mongo.getDatabase("sample_analytics");
  final MongoCollection<Document> collection = database.getCollection("customers");
  */
  public static void main(final String args[]) {
    /*
    Properties configProperties = new Properties();
    configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
    configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
    configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

    final org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);
    */

    in = new Scanner(System.in);
    System.out.println("Enter Number: {1}Find By Name, {2}Find By Id , {3}Find by birthdate");
    int x = in.nextInt();
    switch(x){
        case 1:
          System.out.println("Find By Name");
          FindbyName();
          break;
        case 2:
          System.out.println("Find By Id");
          FindbyId();
          break;
      }
        //TODO: Make sure to use the ProducerRecord constructor that does not take parition Id
    }

  private static void FindbyId() {
    final String uri = "mongodb://admin0:anuranjan5@kafkamd-shard-00-00-pvk6g.mongodb.net:27017,kafkamd-shard-00-01-pvk6g.mongodb.net:27017,kafkamd-shard-00-02-pvk6g.mongodb.net:27017/sample_analytics?ssl=true&replicaSet=kafkamd-shard-0&authSource=admin&retryWrites=true&w=majority";
    final MongoClientURI clientURI = new MongoClientURI(uri);
    final MongoClient mongo = new MongoClient(clientURI);
    final MongoDatabase database = mongo.getDatabase("sample_analytics");
    final MongoCollection<Document> collection = database.getCollection("customers");
    System.out.println("Database Connected");

    Properties configProperties = new Properties();
    configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
    configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
    configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
    final org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);

    String iid = in.nextLine();
    BasicDBObject whereQuery = new BasicDBObject();
    whereQuery.put("_id", iid);
    final FindIterable<Document> cursor = collection.find(whereQuery);
    final Iterator it = cursor.iterator();
    while(it.hasNext()) {
        System.out.println(it.next());
        final ProducerRecord<String , Object> rec = new ProducerRecord<String, Object>("test1", it.next());
        producer.send(rec);
    }
    in.close();
    producer.close();
    mongo.close();
  }

  private static void FindbyName() {
    final String uri = "mongodb://admin0:anuranjan5@kafkamd-shard-00-00-pvk6g.mongodb.net:27017,kafkamd-shard-00-01-pvk6g.mongodb.net:27017,kafkamd-shard-00-02-pvk6g.mongodb.net:27017/sample_analytics?ssl=true&replicaSet=kafkamd-shard-0&authSource=admin&retryWrites=true&w=majority";
    final MongoClientURI clientURI = new MongoClientURI(uri);
    final MongoClient mongo = new MongoClient(clientURI);
    final MongoDatabase database = mongo.getDatabase("sample_analytics");
    final MongoCollection<Document> collection = database.getCollection("customers");

    Properties configProperties = new Properties();
    configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
    configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
    configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
    final org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);

    System.out.println("Database Connected");

    String iname = in.nextLine();
    BasicDBObject whereQuery = new BasicDBObject();
    whereQuery.put("name", iname);
    final FindIterable<Document> cursor = collection.find(whereQuery);
    final Iterator it = cursor.iterator();
    while(it.hasNext()) {
      System.out.println(it.next());
      final ProducerRecord<String , Object> rec = new ProducerRecord<String, Object>("test1", it.next());
      producer.send(rec);
    }
    in.close();
    producer.close();
    mongo.close();
  }
}