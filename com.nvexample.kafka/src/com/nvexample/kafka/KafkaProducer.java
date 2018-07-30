package com.nvexample.kafka;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

import javax.print.DocFlavor.READER;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.*;

public class KafkaProducer {
	private static Producer<Integer, String> producer;
    private static final String topic= "second_topic";

    public void initialize() {
          Properties producerProps = new Properties();
          producerProps.put("metadata.broker.list", "localhost:9092");
          producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
          producerProps.put("request.required.acks", "1");
          ProducerConfig producerConfig = new ProducerConfig(producerProps);
          producer = new Producer<Integer, String>(producerConfig);
    }
    public void publishMesssage() throws Exception{            
          //BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));   
          Random rand = new Random();
      //while (true){
          //System.out.print("Enter message to send to kafka broker(Press 'Y' to close producer): ");
          //String msg = null;
          //msg = reader.readLine(); // Read message from console
          
          for(int i=0;i<10;i++){
          int  n = rand.nextInt(50) + 1;
    	  
    	  
        //Define topic name and message
        //KeyedMessage<Integer, String> keyedMsg =
                    // new KeyedMessage<Integer, String>(topic, msg);
    	KeyedMessage<Integer, String> keyedMsg =
    	                new KeyedMessage<Integer, String>(topic, String.valueOf(n));
    	    	  
          producer.send(keyedMsg); 
          }
          //System.out.print("Enter Y to close producer: ");
         // msg=reader.readLine();
          // This publishes message on given topic 
        //if("Y".equals(msg)){ break; }
         //System.out.println("--> Message [" + msg + "] sent.Check message on Consumer's program console");
                    
      //}
      return;
    }


	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		KafkaProducer kafkaProducer = new KafkaProducer();
        // Initialize producer
        kafkaProducer.initialize();            
        // Publish message
        
        kafkaProducer.publishMesssage();
        
        //Close the producer
        producer.close();

	}

}
