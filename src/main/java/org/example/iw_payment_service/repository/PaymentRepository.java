package org.example.iw_payment_service.repository;

import org.example.iw_payment_service.model.Payment;
import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.time.LocalDateTime;
import java.util.List;

public interface PaymentRepository extends MongoRepository<Payment, String> {

    List<Payment> findByOrderId(Long orderId);

    List<Payment> findByUserId(Long userId);

    List<Payment> findByStatusIn(List<String> statuses);

    @Aggregation(pipeline = {
            "{ '$match': { 'timestamp': { '$gte': ?0, '$lte': ?1 } } }",
            "{ '$group': { '_id': null, 'total': { '$sum': '$paymentAmount' } } }"
    })
    Double getTotalPaymentAmountForPeriod(LocalDateTime start, LocalDateTime end);
}
