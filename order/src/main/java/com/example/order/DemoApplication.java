package com.example.order;

import jakarta.persistence.Entity;
import jakarta.persistence.EntityNotFoundException;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import lombok.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.data.repository.CrudRepository;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.*;

@SpringBootApplication
@EnableFeignClients
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

}

@RestController
@RequiredArgsConstructor
class OrderController {
	private final OrderService orderService;

	@PostMapping("/orders")
	@ResponseStatus(HttpStatus.CREATED)
	public void placeOrder(@RequestBody PlaceOrderRequest request) {
		System.out.println("Order placed: " + request);
		this.orderService.placeOrder(request);
	}
}

@Data
@NoArgsConstructor
@AllArgsConstructor
class PlaceOrderRequest {
	private String product;
	private double price;
}

@Service
@RequiredArgsConstructor
class OrderService {
	private final KafkaTemplate kafkaTemplate;
	private final  OrderRepository orderRepository;
 	private final InventoryClient inventoryClient;
	public void placeOrder(PlaceOrderRequest placeOrderRequest) {
		InventoryStatus inventoryStatus = inventoryClient.exists(placeOrderRequest.getProduct());
		if(!inventoryStatus.isExists()) {
			throw new EntityNotFoundException("Product does not exist");
		}
		Order order = new Order();
		order.setPrice(placeOrderRequest.getPrice());
		order.setProduct(placeOrderRequest.getProduct());
		order.setStatus("PLACED");
		Order o = this.orderRepository.save(order);
		this.kafkaTemplate.send("prod.orders.placed", String.valueOf(o.getId()),OrderPlacedEvent
				.builder()
				.price(placeOrderRequest.getPrice())
						.id(order.getId().intValue())
				.product(placeOrderRequest.getProduct()).build());
	}

	@KafkaListener(topics = "prod.orders.shipped", groupId = "order-group")
	public void handleOrderShippedEvent(String orderId) {
		this.orderRepository.findById(Long.valueOf(orderId)).ifPresent(order -> {
			order.setStatus("SHIPPED");
			this.orderRepository.save(order);
		});
	}
}

@Data
@Builder
class OrderPlacedEvent {
	private int id;
	private String product;
	private double price;
}

interface OrderRepository extends CrudRepository<Order, Long> {

}

@Entity(name = "orders")
@Data
class Order {
	@jakarta.persistence.Id
	@GeneratedValue
	private Long Id;

	private String product;

	private double price;
	private String status;
}

@FeignClient(url = "http://localhost:8093", name = "inventory")
interface InventoryClient {
	@GetMapping("/inventories")
	InventoryStatus exists(@RequestParam("productId") String productId);
}
@Data
class InventoryStatus {
	private boolean exists;
}