package org.springframework.cloud.stream.binder.rabbit;

import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.cloud.stream.function.FunctionInvoker;
import org.springframework.messaging.Message;

/**
 *
 * @author Oleg Zhurakousky
 *
 */
public class AmqpFunctionInvokingAdapter extends AmqpInboundAdapter {

	private final FunctionInvoker<Message<?>> functionInvoker;

	public AmqpFunctionInvokingAdapter(AbstractMessageListenerContainer listenerContainer, FunctionInvoker<Message<?>> functionInvoker) {
		super(listenerContainer);
		this.functionInvoker = functionInvoker;
	}

	@Override
	protected void handleMessagingMessage(org.springframework.messaging.Message<Object> messagingMessage) {
		if (this.functionInvoker != null) {
			this.functionInvoker.accept(messagingMessage);
		}
	}
}
