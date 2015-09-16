package com.gslab.kafka.outbound;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;

import com.gslab.kafka.user.User;

public class OutboundRunner {
	private static final String CONFIG = "kafkaOutboundAdapterParserTests-context.xml";
	private static final Log LOG = LogFactory.getLog(OutboundRunner.class);

	public static void main(final String args[]) {
		final ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(CONFIG, OutboundRunner.class);
		ctx.start();

		final MessageChannel channel = ctx.getBean("inputToKafka", MessageChannel.class);
		LOG.info(channel.getClass());

		//sending 100,000 messages to Kafka server for topic test1
			final User user = new User();
			user.setFirstName("Bipeen");
			user.setLastName("Sawant");
			channel.send(	
					MessageBuilder.withPayload(user)
							.setHeader("messageKey", String.valueOf("topic1-Key"))
							.setHeader("topic", "Topic1").build());
			KeyedMessage<String, String> message =new KeyedMessage<String, String>("Topic1","Bipeen Sawant");
			
			/*channel.send(
					MessageBuilder.withPayload(user)
							.setHeader("messageKey", String.valueOf("topic2-Key"))
							.setHeader("topic", "Topic2").build());*/
			
			LOG.info("message sent ");

		/*//sending 5,000 messages to kafka server for topic test2
		for (int i = 0; i < 50; i++) {
			channel.send(
				MessageBuilder.withPayload("hello Fom ob adapter test2 -  " + i)
					.setHeader("messageKey", String.valueOf(i))
					.setHeader("topic", "test2").build());

			LOG.info("message sent " + i);
		}*/

		/*//Send some messages to multiple topics matching regex.
		for (int i = 0; i < 10; i++) {
			channel.send(
				MessageBuilder.withPayload("hello Fom ob adapter regextopic1 -  " + i)
					.setHeader("messageKey", String.valueOf(i))
					.setHeader("topic", "regextopic1").build());

			LOG.info("message sent " + i);
		}
		for (int i = 0; i < 10; i++) {
			channel.send(
				MessageBuilder.withPayload("hello Fom ob adapter regextopic2 -  " + i)
					.setHeader("messageKey", String.valueOf(i))
					.setHeader("topic", "regextopic2").build());

			LOG.info("message sent " + i);
		}*/
	}
}
