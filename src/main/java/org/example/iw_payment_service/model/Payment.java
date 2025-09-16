package org.example.iw_payment_service.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Document(collection = "payments")
@Data
public class Payment {

    @Id
    private String id;

    private Long orderId;

    private Long userId;

    private String status;

    private LocalDateTime timestamp;

    private Double paymentAmount;

}
