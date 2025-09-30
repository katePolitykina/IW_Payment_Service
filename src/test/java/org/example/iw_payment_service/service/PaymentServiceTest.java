package org.example.iw_payment_service.service;

import org.example.iw_payment_service.dto.PaymentRequest;
import org.example.iw_payment_service.dto.PaymentResponse;
import org.example.iw_payment_service.mapper.PaymentMapper;
import org.example.iw_payment_service.model.Payment;
import org.example.iw_payment_service.model.enums.PaymentStatus;
import org.example.iw_payment_service.repository.PaymentRepository;
import org.example.iw_payment_service.service.kafka.PaymentEventProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    @Mock
    private PaymentRepository repository;

    @Mock
    private PaymentMapper mapper;

    @Mock
    private RestTemplate restTemplate;

    @Mock
    private PaymentEventProducer paymentEventProducer;

    @InjectMocks
    private PaymentService paymentService;

    private PaymentRequest paymentRequest;
    private Payment payment;
    private PaymentResponse paymentResponse;



    @BeforeEach
    void setUp() {
        paymentRequest = new PaymentRequest();
        paymentRequest.setOrderId(1L);
        paymentRequest.setUserId(100L);
        paymentRequest.setPaymentAmount(BigDecimal.valueOf(150.00));

        payment = new Payment();
        payment.setId("payment123");
        payment.setOrderId(1L);
        payment.setUserId(100L);
        payment.setPaymentAmount(BigDecimal.valueOf(150.00));
        payment.setStatus(PaymentStatus.SUCCESS);
        payment.setTimestamp(LocalDateTime.now());

        paymentResponse = new PaymentResponse(
                "payment123",
                1L,
                100L,
                LocalDateTime.now(),
                PaymentStatus.SUCCESS,
                BigDecimal.valueOf(150.00)
        );
        paymentService.setRandomNumberApiUrl("http://fake-url");
    }

    @Test
    void processPayment_ShouldCreateSuccessfulPayment_WhenRandomNumberIsEven() {

        when(restTemplate.getForObject(anyString(), eq(String.class))).thenReturn("42");
        when(mapper.toEntity(paymentRequest)).thenReturn(payment);
        when(repository.save(any(Payment.class))).thenReturn(payment);
        when(mapper.toDTO(payment)).thenReturn(paymentResponse);
        paymentService.processPayment(paymentRequest);
        verify(restTemplate).getForObject(anyString(), eq(String.class));
        verify(mapper).toEntity(paymentRequest);
        verify(repository).save(argThat(p ->
                p.getStatus() == PaymentStatus.SUCCESS &&
                        p.getTimestamp() != null
        ));
        verify(paymentEventProducer).sendCreatePaymentEvent(paymentResponse);
    }

    @Test
    void processPayment_ShouldCreateFailedPayment_WhenRandomNumberIsOdd() {

        when(restTemplate.getForObject(anyString(), eq(String.class))).thenReturn("43");
        when(mapper.toEntity(paymentRequest)).thenReturn(payment);
        when(repository.save(any(Payment.class))).thenReturn(payment);
        when(mapper.toDTO(payment)).thenReturn(paymentResponse);

        paymentService.processPayment(paymentRequest);

        verify(repository).save(argThat(p ->
                p.getStatus() == PaymentStatus.FAILED &&
                        p.getTimestamp() != null
        ));

        verify(paymentEventProducer).sendCreatePaymentEvent(paymentResponse);
    }

    @Test
    void getPaymentsByUserId_ShouldReturnPaymentResponses() {

        Long userId = 100L;
        List<Payment> payments = Collections.singletonList(payment);
        when(repository.findByUserId(userId)).thenReturn(payments);
        when(mapper.toDTO(payment)).thenReturn(paymentResponse);

        List<PaymentResponse> result = paymentService.getPaymentsByUserId(userId);

        assertThat(result).hasSize(1);
        assertThat(result.get(0)).isEqualTo(paymentResponse);
        verify(repository).findByUserId(userId);
        verify(mapper).toDTO(payment);
    }

    @Test
    void getPaymentsByUserId_ShouldReturnEmptyList_WhenNoPaymentsFound() {

        Long userId = 999L;
        when(repository.findByUserId(userId)).thenReturn(List.of());

        List<PaymentResponse> result = paymentService.getPaymentsByUserId(userId);

        assertThat(result).isEmpty();
        verify(repository).findByUserId(userId);
    }

    @Test
    void getPaymentsByOrderId_ShouldReturnPaymentResponses() {

        Long orderId = 1L;
        List<Payment> payments = Collections.singletonList(payment);
        when(repository.findByOrderId(orderId)).thenReturn(payments);
        when(mapper.toDTO(payment)).thenReturn(paymentResponse);

        List<PaymentResponse> result = paymentService.getPaymentsByOrderId(orderId);

        assertThat(result).hasSize(1);
        assertThat(result.get(0)).isEqualTo(paymentResponse);
        verify(repository).findByOrderId(orderId);
        verify(mapper).toDTO(payment);
    }

    @Test
    void getPaymentsByOrderId_ShouldReturnEmptyList_WhenNoPaymentsFound() {
        Long orderId = 999L;
        when(repository.findByOrderId(orderId)).thenReturn(List.of());

        List<PaymentResponse> result = paymentService.getPaymentsByOrderId(orderId);

        assertThat(result).isEmpty();
        verify(repository).findByOrderId(orderId);
    }

    @Test
    void getPaymentsByStatuses_ShouldReturnPaymentResponses() {
        List<String> statuses = Arrays.asList("SUCCESS", "FAILED");
        List<Payment> payments = Collections.singletonList(payment);
        when(repository.findByStatusIn(statuses)).thenReturn(payments);
        when(mapper.toDTO(payment)).thenReturn(paymentResponse);

        List<PaymentResponse> result = paymentService.getPaymentsByStatuses(statuses);

        assertThat(result).hasSize(1);
        assertThat(result.get(0)).isEqualTo(paymentResponse);
        verify(repository).findByStatusIn(statuses);
        verify(mapper).toDTO(payment);
    }

    @Test
    void getTotalPayments_ShouldReturnTotalAmount_WhenPaymentsExist() {

        LocalDateTime start = LocalDateTime.of(2024, 1, 1, 0, 0);
        LocalDateTime end = LocalDateTime.of(2024, 12, 31, 23, 59);
        Double expectedTotal = 1500.00;
        when(repository.getTotalPaymentAmountForPeriod(start, end)).thenReturn(expectedTotal);

        Double result = paymentService.getTotalPayments(start, end);

        assertThat(result).isEqualTo(expectedTotal);
        verify(repository).getTotalPaymentAmountForPeriod(start, end);
    }

    @Test
    void getTotalPayments_ShouldReturnZero_WhenNoPaymentsFound() {

        LocalDateTime start = LocalDateTime.of(2024, 1, 1, 0, 0);
        LocalDateTime end = LocalDateTime.of(2024, 12, 31, 23, 59);
        when(repository.getTotalPaymentAmountForPeriod(start, end)).thenReturn(null);

        Double result = paymentService.getTotalPayments(start, end);

        assertThat(result).isEqualTo(0.0);
        verify(repository).getTotalPaymentAmountForPeriod(start, end);
    }


}