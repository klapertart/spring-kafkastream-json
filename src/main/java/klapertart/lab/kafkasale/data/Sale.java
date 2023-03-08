package klapertart.lab.kafkasale.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Sale {
	private String shopId;
	private Float amount;
}
