package klapertart.lab.kafkasale.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import klapertart.lab.kafkasale.data.Sale;
import klapertart.lab.kafkasale.data.Sales;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;


/**
 * @author kurakuraninja
 * @since 07/03/2023
 */

@Component
public class SaleProcessor {

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder){
        Serde<Sale> saleSerde = jsonSerde(Sale.class);
        Serde<Sales> salesSerde = jsonSerde(Sales.class);

        KGroupedStream<String, Sale> salesByshop = streamsBuilder.stream("sale-topic", Consumed.with(Serdes.String(), saleSerde)).groupByKey();


        KStream<String, Sale> salesAgregate = salesByshop
                .aggregate(
                        new Initializer<Float>() {
                            @Override
                            public Float apply() {
                                return Float.MIN_VALUE;
                            }
                        },
                        new Aggregator<String, Sale, Float>() {
                            @Override
                            public Float apply(String key, Sale sale, Float aFloat) {
                                return sale.getAmount() + aFloat;
                            }
                        },
                        Materialized.<String, Float>as(Stores.persistentKeyValueStore("amount-sale"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Float())
                )
                .toStream()
                .mapValues(Sale::new);

        salesAgregate
                .to("agregated-sale-topic", Produced.with(Serdes.String(), saleSerde));
    }

    private <T> Serde<T> jsonSerde(Class<T> targetClass) {
        return Serdes.serdeFrom(
                new JsonSerializer<>(mapper),
                new JsonDeserializer<>(targetClass, mapper, false)
        );
    }


}
