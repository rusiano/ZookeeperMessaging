package websocket;

import com.company.ZooHelper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.zookeeper.*;
import org.java_websocket.WebSocket;

import java.io.IOException;
import java.util.*;

public class KafkaSocketConnectedWorker extends SocketConnectedWorker implements Runnable{

    private String id, topicName, groupId;
    private String enrollUserPath, quitUserPath, onlineUserPath;
    private ZooKeeper zoo;
    private ZooHelper zooHelper;
    private String kafkaHost;


    private boolean logged;
    private KafkaConsumer<String,String> kafkaConsumer;
    private Properties configPropertiesProducer, configPropertiesConsumer;
    private org.apache.kafka.clients.producer.Producer producer;



    public KafkaSocketConnectedWorker(String id, WebSocket client, String keeperhost, String kafkahost) throws IOException, InterruptedException {
        super(id, client, keeperhost);
        this.id = id;
        this.kafkaHost = kafkahost;

        this.logged = false;
        this.topicName = id;
        this.groupId = id;

        //Configure the Producer
        this.configPropertiesProducer = new Properties();
        this.configPropertiesProducer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        this.configPropertiesProducer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        this.configPropertiesProducer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer(configPropertiesProducer);

        this.configPropertiesConsumer = new Properties();
        this.configPropertiesConsumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        this.configPropertiesConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        this.configPropertiesConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        this.configPropertiesConsumer.put(ConsumerConfig.GROUP_ID_CONFIG, this.id);
        this.configPropertiesConsumer.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");

    }

    @Override
    public void write(String idReceiver, String message) {
        ProducerRecord<String, String> rec = new ProducerRecord<String, String>(idReceiver,this.id+">"+message);
        producer.send(rec);
    }

    @Override
    public boolean writeWithAnswer(String idReceiver, String message) {
        try {
            this.write(idReceiver, message);
        } catch (Exception e) {
            return false;
        }
        return true;
    }


    @Override
    public boolean login() {
        System.out.println("try to login");
        if(!logged)
            logged = super.login();
        else
            System.out.println("Already logged");
        return logged;
    }


    @Override
    public boolean disconnect(){
        producer.close();
        return super.disconnect();
    }

    public void run() {
        //Figure out where to start processing messages from
        kafkaConsumer = new KafkaConsumer<String, String>(configPropertiesConsumer);
        kafkaConsumer.subscribe(Arrays.asList(topicName));
        //Start processing messages
        while(!logged){
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) { e.printStackTrace(); }
        }
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records)
                    super.read(record.topic(), record.value());
            }
        }catch(WakeupException ex){
            System.out.println("Exception caught " + ex.getMessage());
        }finally{
            kafkaConsumer.close();
            System.out.println("After closing KafkaConsumer");
        }
    }


    @Override
    public void process(WatchedEvent ev){
        System.out.println("-------------\n\n-------------- evaluating");
        super.process(ev);
    }
}
