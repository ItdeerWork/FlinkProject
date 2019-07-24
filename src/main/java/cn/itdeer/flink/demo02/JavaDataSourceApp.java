package cn.itdeer.flink.demo02;

import org.apache.flink.api.java.ExecutionEnvironment;

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
        fromTextFile(env);

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
     * DataSource 为文件
     * @param env
     * @throws Exception
     */
    private static void fromTextFile(ExecutionEnvironment env) throws Exception{

        String path1 = "F:\\Code\\Study\\FlinkProject\\src\\main\\java\\cn\\itdeer\\javadata\\data01";
        env.readTextFile(path1).print();

        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        String path2 = "F:\\Code\\Study\\FlinkProject\\src\\main\\java\\cn\\itdeer\\javadata\\inputs";
        env.readTextFile(path2).print();

    }


}
