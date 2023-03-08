package klapertart.lab.kafkasale.controller;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

/**
 * @author TRITRONIK-PC_10040
 * @since 08/03/2023
 */

@RestController
@RequestMapping("/api")
public class AmountController {

    @Autowired
    private StreamsBuilderFactoryBean factoryBean;

    @GetMapping("/mount/{key}")
    public float getMountSale(@PathVariable String key){
        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
        ReadOnlyKeyValueStore<String, Float> store = kafkaStreams.store(StoreQueryParameters.fromNameAndType("amount-sale", QueryableStoreTypes.keyValueStore()));

        Float amount = store.get(key);

        Float aFloat = (amount != null) ? amount : 0f;

        return aFloat;
    }
}
