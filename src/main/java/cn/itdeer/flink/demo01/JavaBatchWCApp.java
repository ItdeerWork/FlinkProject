package cn.itdeer.flink.demo01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Description : wordcount 的批计算例子
 * PackageName : cn.itdeer.flink.demo01
 * ProjectName : FlinkProject
 * CreatorName : itdeer.cn
 * CreateTime : 2019/7/19/16:44
 */
public class JavaBatchWCApp {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String path = "F:\\Code\\Study\\FlinkProject\\src\\main\\java\\cn\\itdeer\\javadata\\p2";

        DataSource<String> text = env.readTextFile(path);

        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = s.toLowerCase().split(" ");
                for (String token : tokens) {
                    if (token.length() > 0) {
                        collector.collect(new Tuple2<>(token, 1));
                    }
                }
            }
        }).groupBy(0).sum(1).print();
    }
}
