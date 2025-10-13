package org.example.iw_payment_service.service;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.example.iw_payment_service.dto.PaymentRequest;
import org.example.iw_payment_service.dto.PaymentResponse;
import org.example.iw_payment_service.model.Payment;
import org.example.iw_payment_service.model.enums.PaymentStatus;
import org.example.iw_payment_service.repository.PaymentRepository;
import org.example.iw_payment_service.utils.KafkaTestHelper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@ActiveProfiles("test")
@WireMockTest
class PaymentServiceIntegrationTest {

    @Container
    static MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:6.0"))
            .withExposedPorts(27017);

    @Container
    static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
            .withEmbeddedZookeeper();

    @RegisterExtension
    static WireMockExtension wireMockServer = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort().dynamicPort())
            .build();
    private static KafkaTestHelper<PaymentResponse> kafkaTestHelper;

    @Autowired
    private PaymentRepository paymentRepository;
    private static final String randomOrgUrl = "/integers";


    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", mongoDBContainer::getReplicaSetUrl);
        registry.add("spring.data.mongodb.database", () -> "testdb");
        registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
        registry.add("spring.kafka.producer.bootstrap-servers", kafkaContainer::getBootstrapServers);
        registry.add("spring.kafka.consumer.bootstrap-servers", kafkaContainer::getBootstrapServers);
        registry.add("random.server.url", () ->
                wireMockServer.baseUrl() + randomOrgUrl
        );
    }

    @BeforeAll
    static void setupWireMockAndKafkaHelper() {
        kafkaTestHelper = new KafkaTestHelper<>(kafkaContainer.getBootstrapServers(), "create-payment-topic", PaymentResponse.class);
    }

    @BeforeEach
    void setUp() {
        paymentRepository.deleteAll();

    }
    @AfterAll
    static void tearDown() {
        kafkaTestHelper.close();
    }

    @Test
    @DisplayName("Should fail payment when random.org is unavailable (500 error)")
    void shouldFailPayment_WhenRandomOrgIsUnavailable() {
        PaymentRequest paymentRequest = createPaymentRequest();

        wireMockServer.stubFor(get(urlPathEqualTo(randomOrgUrl))
                .willReturn(aResponse().withStatus(500)));

        kafkaTestHelper.sendMessage("create-order-topic", String.valueOf(paymentRequest.getOrderId() ), paymentRequest);
        List<PaymentResponse> publishedEvents =kafkaTestHelper.consumeMessages(Duration.ofSeconds(10));


        assertThat(publishedEvents).hasSize(1);

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            List<Payment> payments = paymentRepository.findByOrderId(paymentRequest.getOrderId());
            assertThat(payments).hasSize(1);
            Payment payment = payments.get(0);
            assertThat(payment.getStatus()).isEqualTo(PaymentStatus.FAILED);
        });
    }

    @Test
    @DisplayName("Should process payment successfully when random.org returns even number")
    void shouldProcessPaymentSuccessfully_WhenRandomOrgReturnsEvenNumber(){

        PaymentRequest paymentRequest = createPaymentRequest();

        stubRandomOrgResponse("42");
        kafkaTestHelper.sendMessage("create-order-topic", String.valueOf(paymentRequest.getOrderId() ), paymentRequest);
        List<PaymentResponse> publishedEvents =kafkaTestHelper.consumeMessages(Duration.ofSeconds(10));

        assertThat(publishedEvents).hasSize(1);
        PaymentResponse publishedEvent = publishedEvents.get(0);
        assertThat(publishedEvent.getOrderId()).isEqualTo(paymentRequest.getOrderId());
        assertThat(publishedEvent.getUserId()).isEqualTo(paymentRequest.getUserId());
        assertThat(publishedEvent.getStatus()).isEqualTo(PaymentStatus.SUCCESS);
        assertThat(publishedEvent.getPaymentAmount()).isEqualTo(paymentRequest.getPaymentAmount());

        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    List<Payment> payments = paymentRepository.findByOrderId(paymentRequest.getOrderId());
                    assertThat(payments).hasSize(1);

                    Payment payment = payments.get(0);
                    assertThat(payment.getOrderId()).isEqualTo(paymentRequest.getOrderId());
                    assertThat(payment.getUserId()).isEqualTo(paymentRequest.getUserId());
                    assertThat(payment.getPaymentAmount()).isEqualTo(paymentRequest.getPaymentAmount());
                    assertThat(payment.getStatus()).isEqualTo(PaymentStatus.SUCCESS);
                    assertThat(payment.getTimestamp()).isNotNull();
                    assertThat(payment.getId()).isNotNull();
                });

        wireMockServer.verify(getRequestedFor(urlEqualTo(randomOrgUrl)));
    }

    @Test
    @DisplayName("Should fail payment when random.org returns odd number")
    void shouldFailPayment_WhenRandomOrgReturnsOddNumber(){
        stubRandomOrgResponse("43");
        PaymentRequest paymentRequest = createPaymentRequest();
        kafkaTestHelper.sendMessage("create-order-topic", String.valueOf(paymentRequest.getOrderId()), paymentRequest);
        List<PaymentResponse> publishedEvents =kafkaTestHelper.consumeMessages(Duration.ofSeconds(10));

        assertThat(publishedEvents).hasSize(1);
        PaymentResponse publishedEvent = publishedEvents.get(0);
        assertThat(publishedEvent.getOrderId()).isEqualTo(paymentRequest.getOrderId());
        assertThat(publishedEvent.getStatus()).isEqualTo(PaymentStatus.FAILED);

        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    List<Payment> payments = paymentRepository.findByOrderId(paymentRequest.getOrderId());
                    assertThat(payments).hasSize(1);

                    Payment payment = payments.get(0);
                    assertThat(payment.getOrderId()).isEqualTo(paymentRequest.getOrderId());
                    assertThat(payment.getUserId()).isEqualTo(paymentRequest.getUserId());
                    assertThat(payment.getPaymentAmount()).isEqualTo(paymentRequest.getPaymentAmount());
                    assertThat(payment.getStatus()).isEqualTo(PaymentStatus.FAILED);
                    assertThat(payment.getTimestamp()).isNotNull();
                    assertThat(payment.getId()).isNotNull();
                });

    }
    private PaymentRequest createPaymentRequest() {
        PaymentRequest request = new PaymentRequest();
        Long random = ThreadLocalRandom.current().nextLong();
        request.setOrderId(random);
        request.setUserId(random);
        request.setPaymentAmount(BigDecimal.valueOf(random));
        return request;
    }

    private void stubRandomOrgResponse(String response) {
        wireMockServer.stubFor(get(urlPathEqualTo(randomOrgUrl))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "text/plain")
                        .withBody(response)));

    }



}