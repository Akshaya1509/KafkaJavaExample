package com.nvexample.kafka;
import java.util.*;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import java.io.*;

public class KafkaConsumer {
	private ConsumerConnector consumerConnector = null;
    private final String topic = "second_topic";
    
    public void initialize() {
          Properties props = new Properties();
          props.put("zookeeper.connect", "localhost:2181");
          props.put("group.id", "testgroup");
          props.put("zookeeper.session.timeout.ms", "400");
          props.put("zookeeper.sync.time.ms", "300");
          props.put("auto.commit.interval.ms", "1000");
          ConsumerConfig conConfig = new ConsumerConfig(props);
          consumerConnector = Consumer.createJavaConsumerConnector(conConfig);
    }

    public void consume() throws Exception {
          //Key = topic name, Value = No. of threads for topic
          Map<String, Integer> topicCount = new HashMap<String, Integer>();       
          topicCount.put(topic, new Integer(1));
          
          
          
          
          
          

         
          //ConsumerConnector creates the message stream for each topic
          Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams =
                consumerConnector.createMessageStreams(topicCount);         
         
          // Get Kafka stream for topic 'random_num'
          List<KafkaStream<byte[], byte[]>> kStreamList =
                                               consumerStreams.get(topic);
          // Iterate stream using ConsumerIterator
          for (final KafkaStream<byte[], byte[]> kStreams : kStreamList) {
                 ConsumerIterator<byte[], byte[]> consumerIte = kStreams.iterator();
                 
                while (consumerIte.hasNext()){
                       //System.out.println("Message consumed from topic[" + topic + "] : "       +
                                       //new String(consumerIte.next().message())); 
                       try{
                       FileWriter writer = new FileWriter("kafka_testing.txt",true); 
                       BufferedWriter bufferedWriter = new BufferedWriter(writer);
                       /*bufferedWriter.write("hiii");
                       bufferedWriter.newLine();
                       bufferedWriter.write("this is me");*/
                      
                       bufferedWriter.write("Message consumed from topic[" + topic + "] : "       +
                                       new String(consumerIte.next().message())+"\n");
                       
                       bufferedWriter.newLine();
                       
                      
                       bufferedWriter.close();
                       } catch (IOException e) {
                           e.printStackTrace();
                       }
                      	 
                }
                
                 
          }
          //Shutdown the consumer connector
          if (consumerConnector != null)   consumerConnector.shutdown();          
    }


	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		KafkaConsumer kafkaConsumer = new KafkaConsumer();
        // Configure Kafka consumer
        kafkaConsumer.initialize();
        // Start consumption
        kafkaConsumer.consume();

	}

}
