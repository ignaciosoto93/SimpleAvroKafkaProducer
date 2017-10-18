package com.simpleavrokafka;

import java.io.IOException;

public interface AvroFileToTopicInterface {
	
	void sendWholeFile(String topicName) throws IOException, InterruptedException;
}
