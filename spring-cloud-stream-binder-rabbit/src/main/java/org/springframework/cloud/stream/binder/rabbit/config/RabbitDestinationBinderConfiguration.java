package org.springframework.cloud.stream.binder.rabbit.config;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.DestinationBinder;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.rabbit.RabbitDestinationBinder;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitExtendedBindingProperties;
import org.springframework.cloud.stream.binder.rabbit.provisioning.RabbitExchangeQueueProvisioner;
import org.springframework.cloud.stream.config.ContentTypeConfiguration;
import org.springframework.cloud.stream.config.FunctionProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({ PropertyPlaceholderAutoConfiguration.class, ContentTypeConfiguration.class })
@EnableConfigurationProperties({ RabbitBinderConfigurationProperties.class, RabbitExtendedBindingProperties.class, FunctionProperties.class })
public class RabbitDestinationBinderConfiguration {


	@Bean
	public DestinationBinder<ConsumerProperties, ProducerProperties> myBinder(FunctionCatalog functionCatalog, FunctionInspector functionInspector,
			CompositeMessageConverterFactory converterFactory, ConnectionFactory connectionFactory, RabbitExtendedBindingProperties extendedBindingProperties,
			RabbitExchangeQueueProvisioner provisioningProvider) {
		System.out.println("Creating Rabbit binder");
		return new RabbitDestinationBinder(connectionFactory, extendedBindingProperties, provisioningProvider, functionCatalog, functionInspector, converterFactory);
	}

	@Bean
	RabbitExchangeQueueProvisioner provisioningProvider(ConnectionFactory connectionFactory) {
		return new RabbitExchangeQueueProvisioner(connectionFactory);
	}
}
