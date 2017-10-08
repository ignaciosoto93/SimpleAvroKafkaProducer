package com.simpleavrokafka;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class SimpleAvroKafkaProducer {

	public static void main(String[] args) throws IOException, InterruptedException {

		if (args.length > 4 || args.length < 3) {
			System.out.println("Please, check the right syntax on README file");
			return;
		}

		File file = new File(args[0]);
		Properties props = kakfaPropertiesConstructor(args[1]);

		try {
			AvroFileToTopic avroFile = new AvroFileToTopic(file,props);

			Boolean defaultLoop = new Boolean(false);
			
			if (args.length == 4 && args[3].equals("-loop"))
				defaultLoop = true;

			do {
				avroFile.sendWholeFile(args[2]);
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


}