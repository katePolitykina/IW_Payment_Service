package org.example.iw_payment_service.service;

import lombok.AllArgsConstructor;
import org.example.iw_payment_service.dto.PaymentRequest;
import org.example.iw_payment_service.dto.PaymentResponse;
import org.example.iw_payment_service.mapper.PaymentMapper;
import org.example.iw_payment_service.model.Payment;
import org.example.iw_payment_service.model.enums.PaymentStatus;
import org.example.iw_payment_service.repository.PaymentRepository;
import org.example.iw_payment_service.service.kafka.PaymentEventProducer;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;
import java.util.List;

@Service
@AllArgsConstructor
public class PaymentService {

    private final PaymentRepository repository;
    private final PaymentMapper mapper;
    private final RestTemplate restTemplate;
    private final PaymentEventProducer paymentEventProducer;

    public void processPayment(PaymentRequest dto) {
        String response = restTemplate.getForObject(
                "https://www.random.org/integers/?num=1&min=1&max=100&col=1&base=10&format=plain&rnd=new",
                String.class
        );
        Integer randomNumber = Integer.valueOf(response.trim());

        Payment payment = mapper.toEntity(dto);

        payment.setStatus(randomNumber % 2 == 0 ? PaymentStatus.SUCCESS : PaymentStatus.FAILED);
        payment.setTimestamp(LocalDateTime.now());

        Payment saved = repository.save(payment);
        paymentEventProducer.sendCreatePaymentEvent(mapper.toDTO(saved));
    }

    public List<PaymentResponse> getPaymentsByUserId(Long userId) {
        return repository.findByUserId(userId).stream()
                .map(mapper::toDTO)
                .toList();
    }

    public List<PaymentResponse> getPaymentsByOrderId(Long orderId) {
        return repository.findByOrderId(orderId).stream()
                .map(mapper::toDTO)
                .toList();
    }

    public List<PaymentResponse> getPaymentsByStatuses(List<String> statuses) {
        return repository.findByStatusIn(statuses).stream()
                .map(mapper::toDTO)
                .toList();
    }

    public Double getTotalPayments(LocalDateTime start, LocalDateTime end) {
        Double total = repository.getTotalPaymentAmountForPeriod(start, end);
        return total != null ? total : 0.0;
    }
}
