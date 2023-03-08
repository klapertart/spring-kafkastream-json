package klapertart.lab.kafkasale.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import klapertart.lab.kafkasale.data.Sale;
import klapertart.lab.kafkasale.data.Sales;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;


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
        JsonSerde<Sale> saleJsonSerde = new JsonSerde<>(Sale.class);
        JsonSerde<Sales> salesJsonSerde = new JsonSerde<>(Sales.class);

        KGroupedStream<String, Sale> salesByshop = streamsBuilder.stream("sale-topic", Consumed.with(Serdes.String(), saleJsonSerde)).groupByKey();


        KStream<String, Sale> saleAgregate = salesByshop
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


        //saleAgregate.to("agregated-sale-topic", Produced.with(Serdes.String(), salesJsonSerde));

        LocalDateTime now = LocalDateTime.now();
        KStream<String, Sales> salesKStream = saleAgregate
                .map((k,v) -> KeyValue.pair(k, new Sales(v.getShopId(),v.getAmount(),now,now)));

        salesKStream.to("agregated-sale-topic", Produced.with(Serdes.String(), salesJsonSerde));

    }
}
