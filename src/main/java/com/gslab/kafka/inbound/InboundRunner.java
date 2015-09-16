package com.gslab.kafka.inbound;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.integration.handler.ServiceActivatingHandler;
import org.springframework.integration.kafka.serializer.avro.AvroReflectDatumBackedKafkaDecoder;
import org.springframework.integration.kafka.support.ConsumerConfigFactoryBean;
import org.springframework.integration.kafka.support.ConsumerConfiguration;
import org.springframework.integration.kafka.support.ConsumerConnectionProvider;
import org.springframework.integration.kafka.support.ConsumerMetadata;
import org.springframework.integration.kafka.support.KafkaConsumerContext;
import org.springframework.integration.kafka.support.MessageLeftOverTracker;
import org.springframework.integration.kafka.support.TopicFilterConfiguration;
import org.springframework.integration.kafka.support.ZookeeperConnect;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;

public class InboundRunner {
	private static final String CONFIG = "kafkaInboundAdapterParserTests-context.xml";
	
	private static final Log LOG = LogFactory.getLog(InboundRunner.class);
	
	final static ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(CONFIG, InboundRunner.class);
	
	public static void main(final String args[]) {
		ctx.start();
		final MessageChannel channel = ctx.getBean("inputToKafka", MessageChannel.class);
		LOG.info(channel.getClass());
		System.out.println("#################Adding consumer 1");
		addConsumer("Topic1", "default1");
		/*System.out.println("#################Adding consumer 2");
		addConsumer("Topic2", "default2");*/
		
		
	}
	
	public static void addConsumer(String topicId, String groupId) {

	    MessageChannel inputChannel = ctx.getBean("inputFromKafka", MessageChannel.class);

	    ServiceActivatingHandler serviceActivator = new ServiceActivatingHandler(new Object(){
	        @SuppressWarnings("unused") public Message<?> handle(    Message<?> message){
	            return message;
	          }
	        }
	      ,"handle");
	    ((SubscribableChannel) inputChannel).subscribe(serviceActivator);

	    KafkaConsumerContext<String, String> kafkaConsumerContext = ctx.getBean("consumerContext", KafkaConsumerContext.class);
	    try {
	        TopicFilterConfiguration topicFilterConfiguration = new TopicFilterConfiguration(topicId, 1, false);

	        ConsumerMetadata<String,String> consumerMetadata = new ConsumerMetadata<String, String>();
	        consumerMetadata.setGroupId(groupId);
	        consumerMetadata.setTopicFilterConfiguration(topicFilterConfiguration);
	        consumerMetadata.setConsumerTimeout("1000");
	        consumerMetadata.setKeyDecoder(new AvroReflectDatumBackedKafkaDecoder<String>(java.lang.String.class));
	        consumerMetadata.setValueDecoder(new AvroReflectDatumBackedKafkaDecoder<String>(java.lang.String.class));


	        ZookeeperConnect zkConnect = ctx.getBean("zookeeperConnect", ZookeeperConnect.class);

	        ConsumerConfigFactoryBean<String, String> consumer = new ConsumerConfigFactoryBean<String, String>(consumerMetadata,
	                zkConnect);

	        ConsumerConnectionProvider consumerConnectionProvider = new ConsumerConnectionProvider(consumer.getObject());
	        MessageLeftOverTracker<String,String> messageLeftOverTracker = new MessageLeftOverTracker<String, String>();
	        ConsumerConfiguration<String, String> consumerConfiguration = new ConsumerConfiguration<String, String>(consumerMetadata, consumerConnectionProvider, messageLeftOverTracker);

	        kafkaConsumerContext.getConsumerConfigurations().put(groupId, consumerConfiguration);
	    } catch (Exception exp) {
	        exp.printStackTrace();
	    }
	}
}

