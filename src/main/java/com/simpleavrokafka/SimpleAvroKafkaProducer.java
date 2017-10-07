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

	public static void main(String[] args) throws IOException, InterruptedException {

		if (args.length > 4 || args.length < 3) {
			System.out.println("Please, check the right syntax on README file");
			return;
		}

		File file = new File(args[0]);

		Properties props = kakfaPropertiesConstructor(args[1]);
		try {

			Boolean defaultLoop = new Boolean(false);
			
			if (args.length == 4 && args[3].equals("-loop"))
				defaultLoop = true;

			do {
				readCompleteAvroFile(args[2], file, props);
				System.out.println("--------------------EOF---------------------\n");
				Thread.sleep(15000);
			} while (defaultLoop);
			System.out.println("EOF");

		} catch (IOException e) {
			System.out.println(e);
		} 

	}

	private static Properties kakfaPropertiesConstructor(String bootstrapServerName) {
		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrapServerName);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		return props;
	}

	private static void readCompleteAvroFile(String topicName, File file, Properties props) throws IOException {
		
		GenericDatumReader<GenericData.Record> datum = new GenericDatumReader<GenericData.Record>();
		DataFileReader<GenericData.Record> reader = new DataFileReader<GenericData.Record>(file, datum);
		Schema schema = reader.getSchema();
		GenericData.Record record = new GenericData.Record(schema);
		Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
		KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);
		
		while (reader.hasNext()) {
			reader.next(record);
			byte[] bytes = recordInjection.apply(record);
			ProducerRecord<String, byte[]> theRecord = new ProducerRecord<String, byte[]>(topicName, bytes);
			producer.send(theRecord);
		}
		reader.close();
		producer.close();
	}
}