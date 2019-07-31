package cn.itdeer.flink.demo02;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Description :
 * PackageName : cn.itdeer.flink.demo02
 * ProjectName : FlinkProject
 * CreatorName : itdeer.cn
 * CreateTime : 2019/7/24/15:30
 */
public class JavaDataSourceApp {
    public static void main(String[] args) throws Exception{

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        fromCollection(env);
//        fromTextFile(env);
//        fromCSV(env);
//        fromTextFiles(env);
        formCompressedFile(env);
    }

    /**
     * DataSource 为集合
     * @param env
     * @throws Exception
     */
    private static void fromCollection(ExecutionEnvironment env) throws Exception{
        List<Integer> list = new ArrayList<>();
        for (int i = 1; i <= 10; i++){
            list.add(i);
        }

        env.fromCollection(list).print();
    }

    /**
     * DataSource 为文件或者单层目录
     * @param env
     * @throws Exception
     */
    private static void fromTextFile(ExecutionEnvironment env) throws Exception{

        String path1 = "F:\\Code\\Study\\FlinkProject\\src\\main\\java\\cn\\itdeer\\javadata\\p2";
        env.readTextFile(path1).print();

        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        String path2 = "F:\\Code\\Study\\FlinkProject\\src\\main\\java\\cn\\itdeer\\javadata\\inputs";
        env.readTextFile(path2).print();
    }

    /**
     * DataSource 为CSV文件
     * @param env
     * @throws Exception
     */
    private static void fromCSV(ExecutionEnvironment env) throws Exception{
        String path1 = "F:\\Code\\Study\\FlinkProject\\src\\main\\java\\cn\\itdeer\\javadata\\data02.csv";

//        env.readCsvFile(path1).ignoreFirstLine().types(String.class, Integer.class, String.class).print();

//        env.readCsvFile(path1).ignoreFirstLine().includeFields(true,true).types(String.class, Integer.class).print();

//        env.readCsvFile(path1).ignoreFirstLine().includeFields(2).types(String.class).print();

          env.readCsvFile(path1).ignoreFirstLine().pojoType(Person.class,"name","age","address").print();

    }

    /**
     * DataSource 为文件,多层目录
     * @param env
     * @throws Exception
     */
    private static void fromTextFiles(ExecutionEnvironment env) throws Exception{
        String path = "F:\\Code\\Study\\FlinkProject\\src\\main\\java\\cn\\itdeer\\javadata\\inputs";

        Configuration parameters = new Configuration();
        parameters.setBoolean("recursive.file.enumeration",true);
        env.readTextFile(path).withParameters(parameters).print();
    }

    /**
     * DataSource 为压缩文件
     * @param env
     * @throws Exception
     */
    private static void formCompressedFile(ExecutionEnvironment env) throws Exception{
        String path = "F:\\Code\\Study\\FlinkProject\\src\\main\\java\\cn\\itdeer\\javadata\\data03.gz";
        env.readTextFile(path).print();
    }

}
