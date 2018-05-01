/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.config;

import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionNameStrategy;
import org.springframework.amqp.support.postprocessor.DelegatingDecompressingPostProcessor;
import org.springframework.amqp.support.postprocessor.GZipPostProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.rabbit.RabbitDestinationMessageBinder;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitExtendedBindingProperties;
import org.springframework.cloud.stream.binder.rabbit.provisioning.RabbitExchangeQueueProvisioner;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.function.ApplicationContextFunctionRegistry;
import org.springframework.cloud.stream.function.FunctionRegistry;
import org.springframework.cloud.stream.function.TypeAwareBeanPostProcessor;
import org.springframework.cloud.stream.newbinder.DestinationBinder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.Message;

/**
 * Implementation of the Rabbit based {@link DestinationBinder}
 *
 * @author Oleg Zhurakousky
 *
 */
@Configuration
@Import({RabbitServiceAutoConfiguration.class, PropertyPlaceholderAutoConfiguration.class })
@EnableConfigurationProperties({BindingServiceProperties.class, RabbitBinderConfigurationProperties.class, RabbitExtendedBindingProperties.class })
public class RabbitDestinationBinderConfiguration<T> {

	@Autowired
	private ConnectionFactory rabbitConnectionFactory;

	@Autowired
	private RabbitBinderConfigurationProperties rabbitBinderConfigurationProperties;
	@Bean
	MessagePostProcessor deCompressingPostProcessor() {
		return new DelegatingDecompressingPostProcessor();
	}

	@Bean
	MessagePostProcessor gZipPostProcessor() {
		GZipPostProcessor gZipPostProcessor = new GZipPostProcessor();
		gZipPostProcessor.setLevel(this.rabbitBinderConfigurationProperties.getCompressionLevel());
		return gZipPostProcessor;
	}

	@Bean
	RabbitExchangeQueueProvisioner provisioningProvider() {
		return new RabbitExchangeQueueProvisioner(this.rabbitConnectionFactory);
	}

	@Bean
	@ConditionalOnMissingBean(ConnectionNameStrategy.class)
	@ConditionalOnProperty("spring.cloud.stream.rabbit.binder.connection-name-prefix")
	public ConnectionNameStrategy connectionNamer(CachingConnectionFactory cf) {
		final AtomicInteger nameIncrementer = new AtomicInteger();
		ConnectionNameStrategy namer = f -> this.rabbitBinderConfigurationProperties.getConnectionNamePrefix()
				+ "#" + nameIncrementer.getAndIncrement();
		// TODO: this can be removed when Boot 2.0.1 wires it in
		cf.setConnectionNameStrategy(namer);
		return namer;
	}


	@Bean("rabbit")
	public DestinationBinder<Message<?>, ? extends ConsumerProperties, ? extends ProducerProperties> destinationRabbitBinder(RabbitExchangeQueueProvisioner provisioner,
			RabbitExtendedBindingProperties extendedBindingProperties, ConnectionFactory connectionFactory, FunctionRegistry<Message<?>> functionRegistry) {
		return (DestinationBinder<Message<?>, ? extends ConsumerProperties, ? extends ProducerProperties>)
				new RabbitDestinationMessageBinder<>(functionRegistry, provisioner, extendedBindingProperties, connectionFactory);
	}

	// CORE CONFIG: ================================

	@Bean
	public TypeAwareBeanPostProcessor handlerPostProcessor() {
		return new TypeAwareBeanPostProcessor();
	}

	@Bean
	public FunctionRegistry<Message<?>> functionRegistry() {
		return new ApplicationContextFunctionRegistry();
	}
}
