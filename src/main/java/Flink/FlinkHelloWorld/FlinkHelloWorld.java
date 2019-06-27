package Flink.FlinkHelloWorld;

import java.util.Properties;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class FlinkHelloWorld {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		DataStream<String> source = env.addSource(new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties));
		
		DataStream<String> results = source.map(new RichMapFunction<String, String>() {
		    @Override
		    public String map(String value) {
		        return "Hello "+value;
		    }
		});
        results.print();
		try {
			env.execute("Flink Streaming Java API Skeleton");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
