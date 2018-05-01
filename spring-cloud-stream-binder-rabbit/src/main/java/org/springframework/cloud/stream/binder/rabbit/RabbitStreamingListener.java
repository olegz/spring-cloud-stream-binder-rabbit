package org.springframework.cloud.stream.binder.rabbit;

import java.util.Map;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.cloud.stream.function.TypeAwareFunction;
import org.springframework.cloud.stream.newbinder.StreamingConsumer;
import org.springframework.context.Lifecycle;
import org.springframework.integration.amqp.support.AmqpHeaderMapper;
import org.springframework.integration.amqp.support.DefaultAmqpHeaderMapper;
import org.springframework.integration.support.DefaultMessageBuilderFactory;

import com.rabbitmq.client.Channel;

class RabbitStreamingListener extends StreamingConsumer implements ChannelAwareMessageListener {

	private volatile MessageConverter messageConverter = new SimpleMessageConverter();

	private volatile AmqpHeaderMapper headerMapper = DefaultAmqpHeaderMapper.inboundMapper();

	private final DefaultMessageBuilderFactory messageBuilderFactory = new DefaultMessageBuilderFactory();

	public RabbitStreamingListener(TypeAwareFunction messageHandler, Lifecycle container) {
		super(messageHandler, container);
	}

	public RabbitStreamingListener(TypeAwareFunction messageHandler, Lifecycle container, int txSize) {
		super(messageHandler, container, txSize);
	}

	@Override
	public void onMessage(Message message, Channel channel) throws Exception {
		this.accept(this.createMessage(message, channel));
	}

	/**
	 *
	 */
	private org.springframework.messaging.Message<Object> createMessage(Message message, Channel channel) {
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

	@Override
	public void doOnCommit(org.springframework.messaging.Message<?> messagingMessage) {
		long deliveryTag = (long) messagingMessage.getHeaders().get(AmqpHeaders.DELIVERY_TAG);
		System.out.println("COMMITING: " + deliveryTag + "/" + messagingMessage);
		Channel amqpChannel = (Channel) messagingMessage.getHeaders().get(AmqpHeaders.CHANNEL);
		try {
			amqpChannel.basicAck(deliveryTag, true);
			RabbitUtils.commitIfNecessary(amqpChannel);
		}
		catch (Exception e) {
			e.printStackTrace();
			this.onCancel();
		}
	}
}
