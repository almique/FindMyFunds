package mongodbkafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

import com.mongodb.MongoClientURI;

import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import javax.lang.model.element.Element;

public class Producer {
  private static Scanner in;

  public static void main(final String args[]) {
    in = new Scanner(System.in);
    System.out.println("Enter Number: {1}Find By Name, {2}Find By Id , {3}Find by birthdate");
    int x = in.nextInt();
    switch (x) {
      case 1:
        System.out.println("Find By Name");
        FindbyName();
        break;
      case 2:
        System.out.println("Find By Id");
        FindbyId();
        break;
    }
  }

  private static void FindbyId() {

    String iid = in.next();
    BasicDBObject whereQuery = new BasicDBObject();
    whereQuery.put("_id", iid);

    final String uri = "mongodb://admin0:anuranjan5@kafkamd-shard-00-00-pvk6g.mongodb.net:27017,kafkamd-shard-00-01-pvk6g.mongodb.net:27017,kafkamd-shard-00-02-pvk6g.mongodb.net:27017/sample_analytics?ssl=true&replicaSet=kafkamd-shard-0&authSource=admin&retryWrites=true&w=majority";
    final MongoClientURI clientURI = new MongoClientURI(uri);
    final MongoClient mongoclient = new MongoClient(clientURI);
    DB database = mongoclient.getDB("sample_analytics");
    DBCollection collection = database.getCollection("accounts");

    System.out.println("Database Connected");

    Properties configProperties = new Properties();
    configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    final org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);

    DBCursor cursor = collection.find(whereQuery);
    while (cursor.hasNext()) {
      System.out.println(cursor.next());
      System.out.println("\n");

      final ProducerRecord<String , Object> rec = new ProducerRecord<String, Object>("test1", cursor.next());
      producer.send(rec);
    }
    in.close();
    producer.close();
    mongoclient.close();
  }

  private static void FindbyName() {
    String iname = in.next();
    BasicDBObject whereQuery = new BasicDBObject();
    whereQuery.put("name", iname);

    final String uri = "mongodb://admin0:anuranjan5@kafkamd-shard-00-00-pvk6g.mongodb.net:27017,kafkamd-shard-00-01-pvk6g.mongodb.net:27017,kafkamd-shard-00-02-pvk6g.mongodb.net:27017/sample_analytics?ssl=true&replicaSet=kafkamd-shard-0&authSource=admin&retryWrites=true&w=majority";
    final MongoClientURI clientURI = new MongoClientURI(uri);
    final MongoClient mongoclient = new MongoClient(clientURI);
    DB database = mongoclient.getDB("sample_analytics");
    DBCollection collection = database.getCollection("accounts");

    System.out.println("Database Connected");

    Properties configProperties = new Properties();
    configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
    configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
    final org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);

    DBCursor cursor = collection.find(whereQuery);
    while(cursor.hasNext()) {
      System.out.println(cursor.next());
      final ProducerRecord<String , Object> rec = new ProducerRecord<String, Object>("test1", cursor.next());
      producer.send(rec);
    }
    in.close();
  }
}