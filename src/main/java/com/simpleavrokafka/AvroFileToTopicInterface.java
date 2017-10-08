package com.simpleavrokafka;

import java.io.IOException;

public interface AvroFileToTopicInterface {
	
	void sendEntireFile(String topicName) throws IOException;
}
