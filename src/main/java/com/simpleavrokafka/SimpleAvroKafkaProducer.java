package com.simpleavrokafka;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

public class SimpleAvroKafkaProducer {

	public static void main(String[] args) throws InterruptedException, IOException {
		
		
		// CHECK VALIDATIONS HERE. ADD EXCEPTION
		File file = new File(args[0]);
		
		// CODE A CONSTRUCTOR HERE
		Properties props = new Properties();
		props.put("bootstrap.servers", args[1]);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

		
		
		
		
		GenericDatumReader<GenericData.Record> datum = new GenericDatumReader<GenericData.Record>();
		
		
		DataFileReader<GenericData.Record> reader = new DataFileReader<GenericData.Record>(file, datum);
		Schema schema = reader.getSchema();


		GenericData.Record record = new GenericData.Record(schema);
		Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
		KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);
		Boolean defaultLoop = new Boolean(false);
		
		
		/* Re-factor, with no duplicated code and you'll create a class */
		
		
		if(args.length==4 && args[3].equals("-loop")) defaultLoop=true;
		
		if(defaultLoop){
			while(true){
				reader = new DataFileReader<GenericData.Record>(file, datum);
				while (reader.hasNext()) {
					reader.next(record);
					byte[] bytes = recordInjection.apply(record);
					ProducerRecord<String, byte[]> theRecord = new ProducerRecord<String, byte[]>(args[2], bytes);
					producer.send(theRecord);
				}
				System.out.println("--------------------EOF---------------------\n");
				Thread.sleep(1000);
				
			}
		}
		else {
			reader = new DataFileReader<GenericData.Record>(file, datum);
			
			while (reader.hasNext()) {
				reader.next(record);
				byte[] bytes = recordInjection.apply(record);
				ProducerRecord<String, byte[]> theRecord = new ProducerRecord<String, byte[]>(args[2], bytes);
				producer.send(theRecord);
			}
			System.out.println("EOF");
			reader.close();
			producer.close();
			
		}
			
	
		
	}
}