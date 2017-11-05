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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.BatchingRabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.support.BatchingStrategy;
import org.springframework.amqp.rabbit.core.support.SimpleBatchingStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties.Retry;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.HeaderMode;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties;
import org.springframework.cloud.stream.newbinder.AbstractSenderBinding;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.context.ApplicationContext;
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

/**
 * @author Oleg Zhurakousky
 *
 */
public class RabbitMqSenderBinding
	extends AbstractSenderBinding<ExtendedProducerProperties<RabbitProducerProperties>, ExtendedConsumerProperties<RabbitConsumerProperties>> {

	@Autowired
	private ConnectionFactory connectionFactory;

	@Autowired
	private ApplicationContext applicationContext;

	@Autowired
	private RabbitProperties rabbitProperties;

	private AmqpOutboundEndpoint sendingEndpoint;

	public RabbitMqSenderBinding(String name) {
		super(name);
	}

	@Override
	public Consumer<Message<byte[]>> getSender() {
		return x -> {
			System.out.println("SENDER SENDING MESSAGE: " + x);
			this.sendingEndpoint.handleMessage(x);

		};
	}

	@Override
	public void doStart() {
		ExtendedProducerProperties<RabbitProducerProperties> producerProperties = this.getProducerProperties();
		Assert.state(!HeaderMode.embeddedHeaders.equals(producerProperties.getHeaderMode()),
				"the RabbitMQ binder does not support embedded headers since RabbitMQ supports headers natively");
		String destinationName = "output"; //TODO figure out where it's coming from

		//ConsumerDestination destination = this.getProvisioningProvider().provisionConsumerDestination(destinationName, "", null);
		//destination.

		ProducerDestination producerDestination = this.getProvisioningProvider().provisionProducerDestination(destinationName, producerProperties);

		String prefix = producerProperties.getExtension().getPrefix();
		String exchangeName = producerDestination.getName();
		String destination = StringUtils.isEmpty(prefix) ? exchangeName : exchangeName.substring(prefix.length());
//		AmqpOutboundEndpoint endpoint = new AmqpOutboundEndpoint(buildRabbitTemplate(producerProperties.getExtension(), errorChannel != null));
		this.sendingEndpoint = new AmqpOutboundEndpoint(buildRabbitTemplate(producerProperties.getExtension(), false));
		this.sendingEndpoint.setExchangeName(producerDestination.getName());
		RabbitProducerProperties extendedProperties = producerProperties.getExtension();
		String routingKeyExpression = extendedProperties.getRoutingKeyExpression();
		if (!producerProperties.isPartitioned()) {
			if (routingKeyExpression == null) {
				this.sendingEndpoint.setRoutingKey(destination);
			}
			else {
				this.sendingEndpoint.setRoutingKeyExpressionString(routingKeyExpression);
			}
		}
		else {
			if (routingKeyExpression == null) {
				this.sendingEndpoint.setRoutingKeyExpressionString(buildPartitionRoutingExpression(destination, false));
			}
			else {
				this.sendingEndpoint.setRoutingKeyExpressionString(buildPartitionRoutingExpression(routingKeyExpression, true));
			}
		}
		if (extendedProperties.getDelayExpression() != null) {
			this.sendingEndpoint.setDelayExpressionString(extendedProperties.getDelayExpression());
		}
		DefaultAmqpHeaderMapper mapper = DefaultAmqpHeaderMapper.outboundMapper();
		List<String> headerPatterns = new ArrayList<>(extendedProperties.getHeaderPatterns().length + 1);
		headerPatterns.add("!" + BinderHeaders.PARTITION_HEADER);
		headerPatterns.addAll(Arrays.asList(extendedProperties.getHeaderPatterns()));
		mapper.setRequestHeaderNames(headerPatterns.toArray(new String[headerPatterns.size()]));
		this.sendingEndpoint.setHeaderMapper(mapper);
		this.sendingEndpoint.setDefaultDeliveryMode(extendedProperties.getDeliveryMode());
		this.sendingEndpoint.setBeanFactory(this.applicationContext);
//		if (errorChannel != null) {
//			checkConnectionFactoryIsErrorCapable();
//			endpoint.setReturnChannel(errorChannel);
//			endpoint.setConfirmNackChannel(errorChannel);
//			endpoint.setConfirmCorrelationExpressionString("#root");
//			endpoint.setErrorMessageStrategy(new DefaultErrorMessageStrategy());
//		}
		this.sendingEndpoint.afterPropertiesSet();
		this.sendingEndpoint.start();
	}

	@Override
	public void doStop() {
		// TODO Auto-generated method stub

	}

	@Override
	public String toString() {
		return "RabbitMqSenderBinding:";
	}

	private RabbitTemplate buildRabbitTemplate(RabbitProducerProperties properties, boolean mandatory) {
		RabbitTemplate rabbitTemplate;
		if (properties.isBatchingEnabled()) {
			BatchingStrategy batchingStrategy = new SimpleBatchingStrategy(
					properties.getBatchSize(),
					properties.getBatchBufferLimit(),
					properties.getBatchTimeout());
			rabbitTemplate = new BatchingRabbitTemplate(batchingStrategy,
					this.applicationContext.getBean(IntegrationContextUtils.TASK_SCHEDULER_BEAN_NAME,
							TaskScheduler.class));
		}
		else {
			rabbitTemplate = new RabbitTemplate();
		}
		rabbitTemplate.setChannelTransacted(properties.isTransacted());
		if (rabbitTemplate.isChannelTransacted()) {
			rabbitTemplate.setConnectionFactory(this.connectionFactory);
		}
		else {
			rabbitTemplate.setConnectionFactory(this.connectionFactory);
		}
//		if (properties.isCompress()) {
//			rabbitTemplate.setBeforePublishPostProcessors(this.compressingPostProcessor);
//		}
		rabbitTemplate.setMandatory(mandatory); // returned messages
		if (rabbitProperties != null && rabbitProperties.getTemplate().getRetry().isEnabled()) {
			Retry retry = rabbitProperties.getTemplate().getRetry();
			RetryPolicy retryPolicy = new SimpleRetryPolicy(retry.getMaxAttempts());
			ExponentialBackOffPolicy backOff = new ExponentialBackOffPolicy();
			backOff.setInitialInterval(retry.getInitialInterval());
			backOff.setMultiplier(retry.getMultiplier());
			backOff.setMaxInterval(retry.getMaxInterval());
			RetryTemplate retryTemplate = new RetryTemplate();
			retryTemplate.setRetryPolicy(retryPolicy);
			retryTemplate.setBackOffPolicy(backOff);
			rabbitTemplate.setRetryTemplate(retryTemplate);
		}
		rabbitTemplate.afterPropertiesSet();
		return rabbitTemplate;
	}

	private String buildPartitionRoutingExpression(String expressionRoot, boolean rootIsExpression) {
		return rootIsExpression
				? expressionRoot + " + '-' + headers['" + BinderHeaders.PARTITION_HEADER + "']"
				: "'" + expressionRoot + "-' + headers['" + BinderHeaders.PARTITION_HEADER + "']";
	}
}
