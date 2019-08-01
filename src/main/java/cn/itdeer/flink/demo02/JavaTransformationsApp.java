package cn.itdeer.flink.demo02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.ArrayList;
import java.util.List;

/**
 * Description :
 * PackageName : cn.itdeer.flink.demo02
 * ProjectName : FlinkProject
 * CreatorName : itdeer.cn
 * CreateTime : 2019/8/1/16:53
 */
public class JavaTransformationsApp {

    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//        mapFunction(env);
        filterFunction(env);
    }

    /**
     * Map 函数
     * @param env
     * @throws Exception
     */
    private static void mapFunction(ExecutionEnvironment env) throws Exception{
        List<Integer> list = new ArrayList<Integer>();

        for (int i = 1; i <= 10; i++){
            list.add(i);
        }

        DataSource<Integer> data = env.fromCollection(list);

        // Lambda 写法
        data.map(X -> X + 1).print();

        //传统写法
//        data.map(new MapFunction<Integer, Integer>() {
//            @Override
//            public Integer map(Integer input) throws Exception {
//                return input + 1;
//            }
//        }).print();
    }


    private static void filterFunction(ExecutionEnvironment env) throws Exception{

        List<Integer> list = new ArrayList<>();
        for (int i=1;i<=10;i++){
            list.add(i);
        }

        DataSource<Integer> data = env.fromCollection(list);

        // Lambda 写法
        data.map(x -> x *2).filter(x -> x > 5).print();


        //传统写法
//        data.map(new MapFunction<Integer, Integer>() {
//            @Override
//            public Integer map(Integer input) throws Exception {
//                return input * 2;
//            }
//        }).filter(new FilterFunction<Integer>() {
//            @Override
//            public boolean filter(Integer input) throws Exception {
//                return input > 5;
//            }
//        }).print();

    }

    /**
     * mapPartition 函数
     * @param env
     * @throws Exception
     */
    private static void mapPartitionFunction(ExecutionEnvironment env) throws Exception{
        List<String> list = new ArrayList<String>();

        for (int i=1;i<=100;i++){
            list.add("students:" + i);
        }


        DataSource<String> data = env.fromCollection(list);

        // Lambda 写法

//        data.map((x) -> {
//            String connection = DBUtils.getConnection();
//            System.out.println(connection);
//            DBUtils.returnConnection(connection);
//
//        }).print();


    }
}
