package org.springframework.cloud.stream.binder.rabbit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.BatchingRabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.support.BatchingStrategy;
import org.springframework.amqp.rabbit.core.support.SimpleBatchingStrategy;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties.Retry;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.HeaderMode;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitExtendedBindingProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.function.TypeAwareFunction;
import org.springframework.cloud.stream.newbinder.DestinationBinder;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.context.Lifecycle;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.integration.amqp.outbound.AmqpOutboundEndpoint;
import org.springframework.integration.amqp.support.DefaultAmqpHeaderMapper;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.messaging.Message;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
/**
 *
 * @author Oleg Zhurakousky
 *
 * @param <C>
 * @param <P>
 */
public class RabbitDestinationMessageBinder<C extends ExtendedConsumerProperties<RabbitConsumerProperties>, P extends ExtendedProducerProperties<RabbitProducerProperties>>
						extends DestinationBinder<C, P> {

	private static final MessagePropertiesConverter INBOUND_MESSAGE_PROPERTIES_CONVERTER =
			new DefaultMessagePropertiesConverter() {

				@Override
				public MessageProperties toMessageProperties(AMQP.BasicProperties source, Envelope envelope,
						String charset) {
					MessageProperties properties = super.toMessageProperties(source, envelope, charset);
					properties.setDeliveryMode(null);
					return properties;
				}

			};

	private final RabbitExtendedBindingProperties extendedBindingProperties;

	private final ConnectionFactory connectionFactory;

	@Autowired
	private RabbitProperties rabbitProperties;

	public RabbitDestinationMessageBinder(FunctionCatalog functionCatalog, FunctionInspector functionInspector, ProvisioningProvider<C, P> provisioningProvider,
			RabbitExtendedBindingProperties extendedBindingProperties, ConnectionFactory connectionFactory, CompositeMessageConverterFactory converterFactory) {
		super(provisioningProvider, functionCatalog, functionInspector, converterFactory); // what are the last two?
		this.extendedBindingProperties = extendedBindingProperties;
		this.connectionFactory = connectionFactory;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public C getExtendedConsumerProperties(String destination) {
		return (C) new ExtendedConsumerProperties(this.extendedBindingProperties.getExtendedConsumerProperties(destination));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public P getExtendedProducerProperties(String destination) {
		return (P) new ExtendedProducerProperties(this.extendedBindingProperties.getExtendedProducerProperties(destination));
	}

	@Override
	protected Lifecycle createInboundListener(String destinationName, C consumerProperties, TypeAwareFunction messageHandler) {
		int prefetch = 3; //TODO externalize

		SimpleMessageListenerContainer listenerContainer = new SimpleMessageListenerContainer(this.connectionFactory);
		listenerContainer.setAcknowledgeMode(AcknowledgeMode.MANUAL);
		listenerContainer.setChannelTransacted(true);

//		TODO address concurrency (not sure how it will operate with Flux)

//		int concurrency = consumerProperties.getConcurrency();
//		concurrency = concurrency > 0 ? concurrency : 1;
//		listenerContainer.setConcurrentConsumers(concurrency);
//		int maxConcurrency = consumerProperties.getExtension().getMaxConcurrency();
//		if (maxConcurrency > concurrency) {
//			listenerContainer.setMaxConcurrentConsumers(maxConcurrency);
//		}

		listenerContainer.setPrefetchCount(prefetch);
		listenerContainer.setRecoveryInterval(consumerProperties.getExtension().getRecoveryInterval());

		listenerContainer.setTxSize(prefetch); //TODO should it be tied to prefetch or new property
		listenerContainer.setTaskExecutor(new SimpleAsyncTaskExecutor(destinationName + "-"));
		listenerContainer.setQueueNames(destinationName);

		listenerContainer.setQueueNames(destinationName);
		listenerContainer.setMessagePropertiesConverter(INBOUND_MESSAGE_PROPERTIES_CONVERTER);
		listenerContainer.setExclusive(consumerProperties.getExtension().isExclusive());
		listenerContainer.setMissingQueuesFatal(consumerProperties.getExtension().getMissingQueuesFatal());
		if (consumerProperties.getExtension().getQueueDeclarationRetries() != null) {
			listenerContainer.setDeclarationRetries(consumerProperties.getExtension().getQueueDeclarationRetries());
		}
		if (consumerProperties.getExtension().getFailedDeclarationRetryInterval() != null) {
			listenerContainer.setFailedDeclarationRetryInterval(
					consumerProperties.getExtension().getFailedDeclarationRetryInterval());
		}

		if (getApplicationContext() != null) {
			listenerContainer.setApplicationEventPublisher(getApplicationContext());
		}

//		listenerContainer.setMessageListener(new FluxListener(messageHandler, listenerContainer, prefetch));
		listenerContainer.setMessageListener(new RabbitStreamingListener(messageHandler, listenerContainer, prefetch));

		return listenerContainer;
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
					endpoint.setRoutingKeyExpressionString(buildPartitionRoutingExpression(routingKeyExpression,
							true));
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
//		if (errorChannel != null) {
//			checkConnectionFactoryIsErrorCapable();
//			endpoint.setReturnChannel(errorChannel);
//			endpoint.setConfirmNackChannel(errorChannel);
//			endpoint.setConfirmCorrelationExpressionString("#root");
//			endpoint.setErrorMessageStrategy(new DefaultErrorMessageStrategy());
//		}
		return new Consumer<Message<?>>() {
			@Override
			public void accept(Message<?> message) {
				endpoint.handleMessage(message);
			}
		};
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

	private boolean expressionInterceptorNeeded(RabbitProducerProperties extendedProperties) {
		return extendedProperties.getRoutingKeyExpression() != null
					&& extendedProperties.getRoutingKeyExpression().contains("payload")
				|| (extendedProperties.getDelayExpression() != null
					&& extendedProperties.getDelayExpression().contains("payload"));
	}

	private String buildPartitionRoutingExpression(String expressionRoot, boolean rootIsExpression) {
		return rootIsExpression
				? expressionRoot + " + '-' + headers['" + BinderHeaders.PARTITION_HEADER + "']"
				: "'" + expressionRoot + "-' + headers['" + BinderHeaders.PARTITION_HEADER + "']";
	}
}
