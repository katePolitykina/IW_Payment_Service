package org.example.iw_payment_service.mapper;

import org.example.iw_payment_service.dto.PaymentRequest;
import org.example.iw_payment_service.dto.PaymentResponse;
import org.example.iw_payment_service.model.Payment;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper(componentModel = "spring")
public interface PaymentMapper {

    PaymentResponse toDTO(Payment payment);
    @Mapping(target = "id", ignore = true)
    @Mapping(target = "status", ignore = true)
    @Mapping(target = "timestamp", ignore = true)
    Payment toEntity(PaymentRequest dto);

}
