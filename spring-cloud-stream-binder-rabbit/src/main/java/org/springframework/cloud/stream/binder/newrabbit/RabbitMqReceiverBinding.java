/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cloud.stream.binder.newrabbit;

import java.util.Map;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.support.postprocessor.DelegatingDecompressingPostProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.HeaderMode;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties;
import org.springframework.cloud.stream.newbinder.AbstractReceiverBinding;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.core.AttributeAccessor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.integration.amqp.support.AmqpHeaderMapper;
import org.springframework.integration.amqp.support.AmqpMessageHeaderErrorMessageStrategy;
import org.springframework.integration.amqp.support.DefaultAmqpHeaderMapper;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageUtils;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

/**
 * @author Oleg Zhurakousky
 *
 */
public class RabbitMqReceiverBinding
	extends AbstractReceiverBinding<ExtendedProducerProperties<RabbitProducerProperties>, ExtendedConsumerProperties<RabbitConsumerProperties>> {

	private static final ThreadLocal<AttributeAccessor> attributesHolder = new ThreadLocal<AttributeAccessor>();

	@Autowired
	private ConnectionFactory connectionFactory;

	private MessageConverter messageConverter = new SimpleMessageConverter();

	private volatile AmqpHeaderMapper headerMapper = DefaultAmqpHeaderMapper.inboundMapper();

	private RetryTemplate retryTemplate;

	private MessagePostProcessor decompressingPostProcessor = new DelegatingDecompressingPostProcessor();

	private SimpleMessageListenerContainer listenerContainer;

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

	public RabbitMqReceiverBinding(String name) {
		super(name);
	}

	@Override
	public void doStart() {
		if (this.listenerContainer != null && this.listenerContainer.isActive()){
			return; //TODO should probably never happen due to earlier lifecycle checks, so revisit
		}
		ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperies = this.getConsumerProperies();
		this.retryTemplate = this.buildRetryTemplate(consumerProperies);
		Assert.state(!HeaderMode.embeddedHeaders.equals(consumerProperies.getHeaderMode()),
				"the RabbitMQ binder does not support embedded headers since RabbitMQ supports headers natively");
		String groupName = null; //TODO where are we getting it from?
		String destinationName = "input"; //TODO figure out where it's coming from
		ConsumerDestination consumerDestination = this.getProvisioningProvider().provisionConsumerDestination(destinationName, groupName, consumerProperies);
		this.listenerContainer = new SimpleMessageListenerContainer(this.connectionFactory);
		this.listenerContainer.setAcknowledgeMode(consumerProperies.getExtension().getAcknowledgeMode());
		this.listenerContainer.setChannelTransacted(consumerProperies.getExtension().isTransacted());
		this.listenerContainer.setDefaultRequeueRejected(consumerProperies.getExtension().isRequeueRejected());
		int concurrency = consumerProperies.getConcurrency();
		concurrency = concurrency > 0 ? concurrency : 1;
		this.listenerContainer.setConcurrentConsumers(concurrency);
		int maxConcurrency = consumerProperies.getExtension().getMaxConcurrency();
		if (maxConcurrency > concurrency) {
			this.listenerContainer.setMaxConcurrentConsumers(maxConcurrency);
		}
		this.listenerContainer.setPrefetchCount(consumerProperies.getExtension().getPrefetch());
		this.listenerContainer.setRecoveryInterval(consumerProperies.getExtension().getRecoveryInterval());
		this.listenerContainer.setTxSize(consumerProperies.getExtension().getTxSize());
		this.listenerContainer.setTaskExecutor(new SimpleAsyncTaskExecutor(consumerDestination.getName() + "-"));
		this.listenerContainer.setQueueNames(consumerDestination.getName());
		this.listenerContainer.setAfterReceivePostProcessors(this.decompressingPostProcessor);
		this.listenerContainer.setMessagePropertiesConverter(INBOUND_MESSAGE_PROP_CONVERTER);
		this.listenerContainer.setExclusive(consumerProperies.getExtension().isExclusive());
		this.listenerContainer.setMissingQueuesFatal(consumerProperies.getExtension().getMissingQueuesFatal());
		if (consumerProperies.getExtension().getQueueDeclarationRetries() != null) {
			this.listenerContainer.setDeclarationRetries(consumerProperies.getExtension().getQueueDeclarationRetries());
		}
		if (consumerProperies.getExtension().getFailedDeclarationRetryInterval() != null) {
			this.listenerContainer.setFailedDeclarationRetryInterval(
					consumerProperies.getExtension().getFailedDeclarationRetryInterval());
		}

		this.listenerContainer.afterPropertiesSet();

		this.listenerContainer.setMessageListener(new Listener());
		this.listenerContainer.start();
	}

	@Override
	public void doStop() {
		if (this.listenerContainer != null && this.listenerContainer.isActive()){
			//TODO should probably never happen due to earlier lifecycle checks, so revisit
			this.listenerContainer.stop();
		}
	}

	@Override
	public String toString() {
		return "RabbitMqReceiverBinding:";
	}

	/**
	 * Create and configure a retry template.
	 *
	 * @param properties The properties.
	 * @return The retry template
	 */
	private RetryTemplate buildRetryTemplate(ConsumerProperties properties) {
		RetryTemplate template = new RetryTemplate();
		SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
		retryPolicy.setMaxAttempts(properties.getMaxAttempts());
		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		backOffPolicy.setInitialInterval(properties.getBackOffInitialInterval());
		backOffPolicy.setMultiplier(properties.getBackOffMultiplier());
		backOffPolicy.setMaxInterval(properties.getBackOffMaxInterval());
		template.setRetryPolicy(retryPolicy);
		template.setBackOffPolicy(backOffPolicy);


		return template;
	}

	private class Listener implements ChannelAwareMessageListener, RetryListener {

		private RecoveryCallback<? extends Object> recoveryCallback;

		@Override
		public void onMessage(Message message, Channel channel) throws Exception {
			try {
				if (retryTemplate == null) {
					try {
						processMessage(message, channel);
					}
					finally {
						attributesHolder.remove();
					}
				}
				else {
//					retryTemplate.execute(context -> {
//								processMessage(message, channel);
//								return null;
//							}, (RecoveryCallback<Object>) recoveryCallback);
					retryTemplate.execute(context -> {
						processMessage(message, channel);
						return null;
					});
				}
			}
			catch (RuntimeException e) {
				throw new IllegalStateException("Failed ", e);
			}
		}

		private void processMessage(Message message, Channel channel) {
			byte[] payload = (byte[]) messageConverter.fromMessage(message);
			Map<String, Object> headers = headerMapper.toHeadersFromRequest(message.getMessageProperties());
			if (listenerContainer.getAcknowledgeMode() == AcknowledgeMode.MANUAL) {
				headers.put(AmqpHeaders.DELIVERY_TAG, message.getMessageProperties().getDeliveryTag());
				headers.put(AmqpHeaders.CHANNEL, channel);
			}
			MessageHeaderAccessor headerAccessor = new MessageHeaderAccessor();
			headerAccessor.setLeaveMutable(true);
			headerAccessor.copyHeaders(headers);

			org.springframework.messaging.Message<byte[]> messagingMessage = MessageBuilder.createMessage(payload, headerAccessor.getMessageHeaders());
			setAttributesIfNecessary(message, messagingMessage);
			RabbitMqReceiverBinding.this.getReceiver().accept(messagingMessage);
		}

		@Override
		public <T, E extends Throwable> boolean open(RetryContext context, RetryCallback<T, E> callback) {
			if (recoveryCallback != null) {
				attributesHolder.set(context);
			}
			return true;
		}

		@Override
		public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback,
				Throwable throwable) {
			attributesHolder.remove();
		}

		@Override
		public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback,
				Throwable throwable) {
			// Empty
		}
		/**
		 * If there's a retry template, it will set the attributes holder via the listener. If
		 * there's no retry template, but there's an error channel, we create a new attributes
		 * holder here. If an attributes holder exists (by either method), we set the
		 * attributes for use by the {@link ErrorMessageStrategy}.
		 * @param amqpMessage the AMQP message to use.
		 * @param message the Spring Messaging message to use.
		 */
		private void setAttributesIfNecessary(Message amqpMessage, org.springframework.messaging.Message<?> message) {
			boolean needHolder = retryTemplate == null;
			boolean needAttributes = needHolder || retryTemplate != null;
			if (needHolder) {
				attributesHolder.set(ErrorMessageUtils.getAttributeAccessor(null, null));
			}
			if (needAttributes) {
				AttributeAccessor attributes = attributesHolder.get();
				if (attributes != null) {
					attributes.setAttribute(ErrorMessageUtils.INPUT_MESSAGE_CONTEXT_KEY, message);
					attributes.setAttribute(AmqpMessageHeaderErrorMessageStrategy.AMQP_RAW_MESSAGE, amqpMessage);
				}
			}
		}
	}

}
