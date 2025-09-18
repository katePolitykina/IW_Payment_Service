package org.example.iw_payment_service.model;

import lombok.Data;
import org.example.iw_payment_service.model.enums.PaymentStatus;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Document(collection = "payments")
@Data
public class Payment {

    @Id
    private String id;

    private Long orderId;

    private Long userId;

    private PaymentStatus status;

    private  LocalDateTime timestamp;

    private BigDecimal paymentAmount;

}
