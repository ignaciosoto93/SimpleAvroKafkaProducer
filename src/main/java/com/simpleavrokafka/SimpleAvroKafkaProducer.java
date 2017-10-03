package com.simpleavrokafka;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

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

		File file = new File(args[0]);
		Properties props = new Properties();
		props.put("bootstrap.servers", args[1]);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

		GenericDatumReader<GenericData.Record> datum = new GenericDatumReader<GenericData.Record>();
		DataFileReader<GenericData.Record> reader = new DataFileReader<GenericData.Record>(file, datum);

		GenericData.Record record = new GenericData.Record(reader.getSchema());
		Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(reader.getSchema());
		KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);

		while (reader.hasNext()) {
			reader.next(record);
			byte[] bytes = recordInjection.apply(record);
			ProducerRecord<String, byte[]> theRecord = new ProducerRecord<String, byte[]>(args[2], bytes);
			producer.send(theRecord);
		}
		reader.close();
		producer.close();
	}
}