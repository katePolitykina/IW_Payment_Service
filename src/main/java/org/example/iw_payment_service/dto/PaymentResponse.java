package org.example.iw_payment_service.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.example.iw_payment_service.model.enums.PaymentStatus;

import java.math.BigDecimal;
import java.time.LocalDateTime;
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PaymentResponse {
    private String id;
    private Long orderId;
    private Long userId;
    private LocalDateTime timestamp;
    private PaymentStatus status;
    private BigDecimal paymentAmount;
}
