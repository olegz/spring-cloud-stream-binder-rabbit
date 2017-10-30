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

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties;
import org.springframework.cloud.stream.binder.rabbit.provisioning.RabbitExchangeQueueProvisioner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({RabbitProducerProperties.class, RabbitConsumerProperties.class})
public class RabbitMqBinderContextConfiguration {

	private static final String BINDER_NAME = "rabbit";

	@Autowired
	private ConnectionFactory connectionFactory;

	@Autowired
	private RabbitProducerProperties producerProperties;

	@Autowired
	private RabbitConsumerProperties consumerProperties;

	@Bean
	public RabbitMqSenderBinding rabbitMqSenderBinding() {
		return new RabbitMqSenderBinding(BINDER_NAME);
	}

	@Bean
	public RabbitMqReceiverBinding rabbitMqReceiverBinding() {
		return new RabbitMqReceiverBinding(BINDER_NAME);
	}

	@Bean
	public RabbitExchangeQueueProvisioner rabbitMqDestinationProvisioner() {
		return new RabbitExchangeQueueProvisioner(this.connectionFactory);
	}

	@Bean
	public ExtendedProducerProperties<RabbitProducerProperties> producerProperties() {
		return new ExtendedProducerProperties<RabbitProducerProperties>(producerProperties);
	}

	@Bean
	public ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties() {
		return new ExtendedConsumerProperties<RabbitConsumerProperties>(consumerProperties);
	}
}
