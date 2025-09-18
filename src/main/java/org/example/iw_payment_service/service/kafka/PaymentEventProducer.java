package org.example.iw_payment_service.service.kafka;

import lombok.AllArgsConstructor;
import org.example.iw_payment_service.dto.PaymentResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@AllArgsConstructor
public class PaymentEventProducer {

    private static final Logger logger = LoggerFactory.getLogger(PaymentEventProducer.class);
    private static final String TOPIC = "create-payment-topic";

    private final KafkaTemplate<String, PaymentResponse> kafkaTemplate;


    public void sendCreatePaymentEvent(PaymentResponse event) {
        logger.info("Sending CREATE_PAYMENT event: {}", event);

        CompletableFuture<SendResult<String, PaymentResponse>> future =
                kafkaTemplate.send(TOPIC, Long.toString(event.getOrderId()), event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                logger.info("Successfully sent CREATE_PAYMENT event for orderId: {} with offset: {}",
                        event.getOrderId(), result.getRecordMetadata().offset());
            } else {
                logger.error("Failed to send CREATE_PAYMENT event for orderId: {}",
                        event.getOrderId(), ex);
            }
        });
    }
}
