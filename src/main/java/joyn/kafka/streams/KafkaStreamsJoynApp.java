package joyn.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.*;


import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;


import java.util.*;

public class KafkaStreamsJoynApp {
    public Topology createTopology() throws IOException {

        StreamsBuilder builder = new StreamsBuilder();

        // we get a global table out of Kafka. This table will be replicated on each Kafka Streams application
        // the key of our globalKTable is the user ID
        GlobalKTable<String, GenericRecord> users = builder.globalTable("users");
        KStream<String, GenericRecord> pageviews = builder.stream("pageviews");
        KStream<String, GenericRecord> pageviewskeyed = pageviews.selectKey((String key, GenericRecord value) -> value.get("userid").toString());
        final InputStream
                pageViewUserSchema =
                KafkaStreamsJoynApp.class.getClassLoader()
                        .getResourceAsStream("userviewsjoin.avsc");
        final Schema schema = new Schema.Parser().parse(pageViewUserSchema);
        final InputStream
                aggSchemaStream =
                KafkaStreamsJoynApp.class.getClassLoader()
                        .getResourceAsStream("viewaggs.avsc");
        final Schema aggschema = new Schema.Parser().parse(aggSchemaStream);

        final InputStream
                aggSchemaStreamFirst =
                KafkaStreamsJoynApp.class.getClassLoader()
                        .getResourceAsStream("viewaggfirst.avsc");
        final Schema aggschemaFirst = new Schema.Parser().parse(aggSchemaStreamFirst);
        /* joining users with views */
        KStream<String, GenericRecord> userviewsJoin =
                pageviewskeyed.join(users, (key, value) -> key,
                        (GenericRecord page, GenericRecord user) -> {
                            final GenericRecord userviews = new GenericData.Record(schema);
                            userviews.put("userid", user.get("userid"));
                            userviews.put("pageid", page.get("pageid"));
                            userviews.put("viewtime", page.get("viewtime"));
                            userviews.put("gender", user.get("gender"));
                            return userviews;
                        });

        KStream<String, GenericRecord> userviewskeyedJoin =
                userviewsJoin.selectKey((key, value) -> value.get("pageid").toString().concat(value.get("gender").toString()));
        /* Creating a hopping window of 1 min advancing by 10 seconds */
        TimeWindows hoppWindow =TimeWindows.of(Duration.ofMinutes(1));
        hoppWindow.advanceBy(Duration.ofMillis(10000));
        /* Creating a windowed KTable which contains the concatenated users and sum of view time grouped by key */
        KTable<Windowed<String>, GenericRecord> windowedPageViewCounts = userviewskeyedJoin
                .groupByKey()
                .windowedBy(hoppWindow)
                .aggregate(
                        ()-> null,
                        (String key, GenericRecord firstRecord, GenericRecord secondRecord) ->  {
                            final GenericRecord userviews = new GenericData.Record(aggschemaFirst);
                            Object user_acc = firstRecord.get("userid");
                            Object user2_acc = secondRecord.get("userid");

                            if(user2_acc!=null && user_acc!= null){
                                userviews.put("useridconcat",user_acc.toString()+"|"+user2_acc.toString());
                            }

                            Object firstValue = firstRecord.get("viewtime");
                            Object nextValue = secondRecord.get("viewtime");
                            long firstValueLong= firstValue!=null?Long.valueOf(firstValue.toString()):0;
                            long nextValueLong =nextValue!=null?Long.valueOf(nextValue.toString()):0;
                            userviews.put("pageid", firstRecord.get("pageid"));
                            userviews.put("totalviewtime", String.valueOf(firstValueLong+nextValueLong));
                            userviews.put("gender", firstRecord.get("gender"));
                            return userviews;
                        },
                        Materialized.as("AggregatedViews")

                );

        /* Splitting the concatenated users and getting a distinct user count */
        KStream<Windowed<String>, GenericRecord>windowedPageViewCountsStream=windowedPageViewCounts.toStream().mapValues(genericRecord -> {
                    final GenericRecord userviews = new GenericData.Record(aggschema);
                    String useridConcat =genericRecord.get("useridconcat").toString();
                    HashSet<String> useridSet = new HashSet(Arrays.asList(useridConcat.split("|")));
                    int count = useridSet.size();
                    userviews.put("pageid",genericRecord.get("pageid"));
                    userviews.put("totalviewtime", genericRecord.get("totalviewtime"));
                    userviews.put("gender", genericRecord.get("gender"));
                    userviews.put("useridcount",String.valueOf(count));
                    return userviews;


                });
        /* Finally writing the results to topic top_pages */
        windowedPageViewCountsStream.to("top_pages");


        //KStream<Windowed<String>, GenericRecord> streams_male = windowedPageViewCounts.filter((key, value) -> value.get("gender").toString() == "MALE").toStream();
        windowedPageViewCounts.toStream().peek((key,value) ->System.out.println("key " +key+ ", value " +value));
        Topology topology = builder.build();

        return topology;
    }
    public static void main(String[] args) throws IOException {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "sample-joins");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        //config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);

        KafkaStreamsJoynApp app = new KafkaStreamsJoynApp();

        Topology topology = app.createTopology();


        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.cleanUp(); // only do this in dev - not in prod
        streams.start();
        ReadOnlyWindowStore<String,GenericRecord> windowStore=streams.store("AggregatedViews",QueryableStoreTypes.windowStore());
        KeyValueIterator<Windowed<String>, GenericRecord> iter = windowStore.all();
        List<String> top10Male=new ArrayList();
        List<String> top10Female = new ArrayList();
        List<String> top10Other = new ArrayList();
        while (iter.hasNext()) {
            KeyValue record=iter.next();
            String key = record.key.toString();
            GenericRecord value = (GenericRecord) record.value;


        }

        // print the topology
        System.out.println(topology.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
