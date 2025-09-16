package org.example.iw_payment_service.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Data;
import org.example.iw_payment_service.model.enums.PaymentStatus;

import java.time.LocalDateTime;

@Data
public class PaymentRequest {
    @NotNull
    private Long orderId;
    @NotNull
    private Long userId;
    @NotNull
    @Positive
    private Double paymentAmount;

}
