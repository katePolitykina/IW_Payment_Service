package org.example.iw_payment_service.service.kafka;

import lombok.extern.slf4j.Slf4j;
import org.example.iw_payment_service.dto.PaymentRequest;
import org.example.iw_payment_service.service.PaymentService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class OrderEventConsumer {

    private final PaymentService paymentService;


    public OrderEventConsumer(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @KafkaListener(topics = "create-order-topic", groupId = "payment-service-group")
    public void handleCreateOrderEvent(
            @Payload PaymentRequest request,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("Received CREATE_PAYMENT event from topic: {}, partition: {}, offset: {}, event: {}",
                topic, partition, offset, request);

        paymentService.processPayment(request);
    }
}