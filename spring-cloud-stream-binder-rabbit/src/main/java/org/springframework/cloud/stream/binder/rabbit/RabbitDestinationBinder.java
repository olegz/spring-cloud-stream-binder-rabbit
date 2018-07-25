package org.springframework.cloud.stream.binder.rabbit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.core.BatchingRabbitTemplate;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.support.BatchingStrategy;
import org.springframework.amqp.rabbit.core.support.SimpleBatchingStrategy;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.support.postprocessor.DelegatingDecompressingPostProcessor;
import org.springframework.amqp.support.postprocessor.GZipPostProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties.Retry;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.DestinationBinder;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.HeaderMode;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitExtendedBindingProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.messaging.MessageListeningContainer;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.integration.amqp.outbound.AmqpOutboundEndpoint;
import org.springframework.integration.amqp.support.AmqpHeaderMapper;
import org.springframework.integration.amqp.support.DefaultAmqpHeaderMapper;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.support.DefaultMessageBuilderFactory;
import org.springframework.messaging.Message;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;

public class RabbitDestinationBinder<C extends ExtendedConsumerProperties<RabbitConsumerProperties>,
	P extends ExtendedProducerProperties<RabbitProducerProperties>> extends DestinationBinder<C, P> {

	private final ConnectionFactory connectionFactory;

	private final MessagePostProcessor decompressingPostProcessor = new DelegatingDecompressingPostProcessor();

	private final MessagePostProcessor compressingPostProcessor = new GZipPostProcessor();

	private final RabbitExtendedBindingProperties extendedBindingProperties;

	private static final MessagePropertiesConverter INBOUND_MESSAGE_PROP_CONVERTER =
			new DefaultMessagePropertiesConverter() {

				@Override
				public MessageProperties toMessageProperties(AMQP.BasicProperties source, Envelope envelope,
						String charset) {
					MessageProperties properties = super.toMessageProperties(source, envelope, charset);
					properties.setDeliveryMode(null);
					return properties;
				}

			};

	@Autowired
	private RabbitProperties rabbitProperties;

	public RabbitDestinationBinder(ConnectionFactory connectionFactory,
			RabbitExtendedBindingProperties extendedBindingProperties,
			ProvisioningProvider<C, P> provisioningProvider,
			FunctionCatalog functionCatalog, FunctionInspector functionInspector,
			CompositeMessageConverterFactory converterFactory) {
		super(provisioningProvider, functionCatalog, functionInspector, converterFactory);
		this.connectionFactory = connectionFactory;
		this.extendedBindingProperties = extendedBindingProperties;
	}

	@Override
	public C getExtendedConsumerProperties(String destination) {
		return (C) new ExtendedConsumerProperties(this.extendedBindingProperties.getExtendedConsumerProperties(destination));
	}

	@Override
	public P getExtendedProducerProperties(String destination) {
		return (P) new ExtendedProducerProperties(this.extendedBindingProperties.getExtendedProducerProperties(destination));
	}

	@Override
	protected MessageListeningContainer createMessageListeningContainer(String inputDestinationName, C consumerProperties) {
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = getExtendedConsumerProperties(inputDestinationName);
		SimpleMessageListenerContainer listenerContainer = new SimpleMessageListenerContainer(connectionFactory);
		listenerContainer.setAcknowledgeMode(AcknowledgeMode.MANUAL);
		listenerContainer.setChannelTransacted(properties.getExtension().isTransacted());
		listenerContainer.setDefaultRequeueRejected(properties.getExtension().isRequeueRejected());
		listenerContainer.setConcurrentConsumers(1);
		listenerContainer.setMaxConcurrentConsumers(1);
		listenerContainer.setPrefetchCount(properties.getExtension().getPrefetch());
		listenerContainer.setRecoveryInterval(properties.getExtension().getRecoveryInterval());
		listenerContainer.setTxSize(properties.getExtension().getTxSize());
		listenerContainer.setTaskExecutor(new SimpleAsyncTaskExecutor(inputDestinationName + "-"));
		String[] queues = StringUtils.tokenizeToStringArray(inputDestinationName, ",", true, true);
		listenerContainer.setQueueNames(queues);
		listenerContainer.setAfterReceivePostProcessors(this.decompressingPostProcessor);
		listenerContainer.setMessagePropertiesConverter(INBOUND_MESSAGE_PROP_CONVERTER);
		listenerContainer.setExclusive(properties.getExtension().isExclusive());
		listenerContainer.setMissingQueuesFatal(properties.getExtension().getMissingQueuesFatal());
		if (properties.getExtension().getQueueDeclarationRetries() != null) {
			listenerContainer.setDeclarationRetries(properties.getExtension().getQueueDeclarationRetries());
		}
		if (properties.getExtension().getFailedDeclarationRetryInterval() != null) {
			listenerContainer.setFailedDeclarationRetryInterval(
					properties.getExtension().getFailedDeclarationRetryInterval());
		}

		return new RabbitMessageListeningContainer(listenerContainer);
	}


	@Override
	protected Consumer<Message<?>> createOutboundConsumer(String destinationName, P producerProperties) {
		Assert.state(!HeaderMode.embeddedHeaders.equals(producerProperties.getHeaderMode()),
				"the RabbitMQ binder does not support embedded headers since RabbitMQ supports headers natively");
		String prefix = producerProperties.getExtension().getPrefix();
		String exchangeName = destinationName;
		String destination = StringUtils.isEmpty(prefix) ? exchangeName : exchangeName.substring(prefix.length());
		AmqpOutboundEndpoint endpoint = new AmqpOutboundEndpoint(
				buildRabbitTemplate(producerProperties.getExtension(), true));
		endpoint.setExchangeName(exchangeName);
		RabbitProducerProperties extendedProperties = producerProperties.getExtension();
		boolean expressionInterceptorNeeded = expressionInterceptorNeeded(extendedProperties);
		String routingKeyExpression = extendedProperties.getRoutingKeyExpression();
		if (!producerProperties.isPartitioned()) {
			if (routingKeyExpression == null) {
				endpoint.setRoutingKey(destination);
			}
			else {
				if (expressionInterceptorNeeded) {
					endpoint.setRoutingKeyExpressionString("headers['"
							+ RabbitExpressionEvaluatingInterceptor.ROUTING_KEY_HEADER + "']");
				}
				else {
					endpoint.setRoutingKeyExpressionString(routingKeyExpression);
				}
			}
		}
		else {
			if (routingKeyExpression == null) {
				endpoint.setRoutingKeyExpressionString(buildPartitionRoutingExpression(destination, false));
			}
			else {
				if (expressionInterceptorNeeded) {
					endpoint.setRoutingKeyExpressionString(buildPartitionRoutingExpression("headers['"
							+ RabbitExpressionEvaluatingInterceptor.ROUTING_KEY_HEADER + "']", true));
				}
				else {
					endpoint.setRoutingKeyExpressionString(buildPartitionRoutingExpression(routingKeyExpression, true));
				}
			}
		}
		if (extendedProperties.getDelayExpression() != null) {
			if (expressionInterceptorNeeded) {
				endpoint.setDelayExpressionString("headers['"
						+ RabbitExpressionEvaluatingInterceptor.DELAY_HEADER + "']");
			}
			else {
				endpoint.setDelayExpressionString(extendedProperties.getDelayExpression());
			}
		}
		DefaultAmqpHeaderMapper mapper = DefaultAmqpHeaderMapper.outboundMapper();
		List<String> headerPatterns = new ArrayList<>(extendedProperties.getHeaderPatterns().length + 1);
		headerPatterns.add("!" + BinderHeaders.PARTITION_HEADER);
		headerPatterns.addAll(Arrays.asList(extendedProperties.getHeaderPatterns()));
		mapper.setRequestHeaderNames(headerPatterns.toArray(new String[headerPatterns.size()]));
		endpoint.setHeaderMapper(mapper);
		endpoint.setDefaultDeliveryMode(extendedProperties.getDeliveryMode());
		endpoint.setBeanFactory(this.getApplicationContext().getBeanFactory());

		return endpoint::handleMessage;
	}

	private String buildPartitionRoutingExpression(String expressionRoot, boolean rootIsExpression) {
		return rootIsExpression
				? expressionRoot + " + '-' + headers['" + BinderHeaders.PARTITION_HEADER + "']"
				: "'" + expressionRoot + "-' + headers['" + BinderHeaders.PARTITION_HEADER + "']";
	}

	private boolean expressionInterceptorNeeded(RabbitProducerProperties extendedProperties) {
		return extendedProperties.getRoutingKeyExpression() != null
					&& extendedProperties.getRoutingKeyExpression().contains("payload")
				|| (extendedProperties.getDelayExpression() != null
					&& extendedProperties.getDelayExpression().contains("payload"));
	}

	@Override
	protected void onCommit(Message<?> message) {
		long deliveryTag = (long) message.getHeaders().get(AmqpHeaders.DELIVERY_TAG);
		System.out.println("COMMITING: " + deliveryTag + "/" + message);
		Channel amqpChannel = (Channel) message.getHeaders().get(AmqpHeaders.CHANNEL);
		try {
			amqpChannel.basicAck(deliveryTag, true);
			RabbitUtils.commitIfNecessary(amqpChannel);
		}
		catch (Exception e) {
			e.printStackTrace();
//			this.onCancel();
		}
	}

	private RabbitTemplate buildRabbitTemplate(RabbitProducerProperties properties, boolean mandatory) {
		RabbitTemplate rabbitTemplate;
		if (properties.isBatchingEnabled()) {
			BatchingStrategy batchingStrategy = new SimpleBatchingStrategy(
					properties.getBatchSize(),
					properties.getBatchBufferLimit(),
					properties.getBatchTimeout());
			rabbitTemplate = new BatchingRabbitTemplate(batchingStrategy,
					getApplicationContext().getBean(IntegrationContextUtils.TASK_SCHEDULER_BEAN_NAME,
							TaskScheduler.class));
		}
		else {
			rabbitTemplate = new RabbitTemplate();
		}
		rabbitTemplate.setChannelTransacted(properties.isTransacted());
		rabbitTemplate.setConnectionFactory(this.connectionFactory);
		rabbitTemplate.setUsePublisherConnection(true);
//		if (properties.isCompress()) {
//			rabbitTemplate.setBeforePublishPostProcessors(this.compressingPostProcessor);
//		}
		rabbitTemplate.setMandatory(mandatory); // returned messages
		if (rabbitProperties != null && rabbitProperties.getTemplate().getRetry().isEnabled()) {
			Retry retry = rabbitProperties.getTemplate().getRetry();
			RetryPolicy retryPolicy = new SimpleRetryPolicy(retry.getMaxAttempts());
			ExponentialBackOffPolicy backOff = new ExponentialBackOffPolicy();
			backOff.setInitialInterval(retry.getInitialInterval().toMillis());
			backOff.setMultiplier(retry.getMultiplier());
			backOff.setMaxInterval(retry.getMaxInterval().toMillis());
			RetryTemplate retryTemplate = new RetryTemplate();
			retryTemplate.setRetryPolicy(retryPolicy);
			retryTemplate.setBackOffPolicy(backOff);
			rabbitTemplate.setRetryTemplate(retryTemplate);
		}
		rabbitTemplate.afterPropertiesSet();
		return rabbitTemplate;
	}

	private static class RabbitMessageListeningContainer implements MessageListeningContainer {

		private final SimpleMessageListenerContainer listenerContainer;

		RabbitMessageListeningContainer(SimpleMessageListenerContainer listenerContainer) {
			this.listenerContainer = listenerContainer;
		}
		@Override
		public void stop() {
			if (this.isRunning()) {
				listenerContainer.stop();
			}
		}

		@Override
		public void start() {
			if (!this.isRunning()) {
				listenerContainer.start();
			}
		}

		@Override
		public boolean isRunning() {
			return listenerContainer.isRunning();
		}

		@Override
		public void setListener(Consumer<Message<?>> messageConsumer) {

			ChannelAwareMessageListener listener = new ChannelAwareMessageListener() {

				private volatile MessageConverter messageConverter = new SimpleMessageConverter();

				private volatile AmqpHeaderMapper headerMapper = DefaultAmqpHeaderMapper.inboundMapper();

				private final DefaultMessageBuilderFactory messageBuilderFactory = new DefaultMessageBuilderFactory();

				@Override
				public void onMessage(org.springframework.amqp.core.Message message, Channel channel) throws Exception {
					messageConsumer.accept(this.createMessage(message, channel));
				}

				private org.springframework.messaging.Message<Object> createMessage(org.springframework.amqp.core.Message message, Channel channel) {
					Object payload = (Object) this.messageConverter.fromMessage(message);
					Map<String, Object> headers = this.headerMapper
							.toHeadersFromRequest(message.getMessageProperties());
					headers.put(AmqpHeaders.DELIVERY_TAG, message.getMessageProperties().getDeliveryTag());
					headers.put(AmqpHeaders.CHANNEL, channel);

					final org.springframework.messaging.Message<Object> messagingMessage = this.messageBuilderFactory
							.withPayload(payload)
							.copyHeaders(headers)
							.build();
					return messagingMessage;
				}
			};

			listenerContainer.setMessageListener(listener);
		}
	}

}
