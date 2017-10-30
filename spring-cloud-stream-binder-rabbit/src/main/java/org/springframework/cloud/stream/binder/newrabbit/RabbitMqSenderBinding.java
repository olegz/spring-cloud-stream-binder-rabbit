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

import java.util.function.Consumer;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties;
import org.springframework.cloud.stream.newbinder.AbstractSenderBinding;
import org.springframework.messaging.Message;

/**
 * @author Oleg Zhurakousky
 *
 */
public class RabbitMqSenderBinding
	extends AbstractSenderBinding<ExtendedProducerProperties<RabbitProducerProperties>, ExtendedConsumerProperties<RabbitConsumerProperties>> {

	@Autowired
	private ConnectionFactory connectionFactory;

	public RabbitMqSenderBinding(String name) {
		super(name);
	}

	@Override
	public Consumer<Message<byte[]>> getSender() {
		return x -> {
			System.out.println("SENDER SENDING MESSAGE: " + x);
		};
	}

	@Override
	public void doStart() {
		// TODO Auto-generated method stub

	}

	@Override
	public void doStop() {
		// TODO Auto-generated method stub

	}

	@Override
	public String toString() {
		return "RabbitMqSenderBinding:";
	}
}
