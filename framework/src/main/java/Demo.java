import api.DataQuanta;
import api.PlanBuilder;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.HashMap;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:40 下午
 */
public class Demo {
    public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException {
        try {
            PlanBuilder planBuilder = new PlanBuilder();
            // 设置udf路径   例如udfPath值是TestSmallWebCaseFunc.class的绝对路径
            //planBuilder.setPlatformUdfPath("java", "/data/TestSmallWebCaseFunc.class");
            planBuilder.setPlatformUdfPath("java", "/Users/zhuxingpo/Downloads/Due/TestSmallWebCaseFunc.class");
            //供测试生成文件使用   例如udfPath值是TestSmallWebCaseFunc.class的绝对路径
            //planBuilder.setPlatformUdfPath("spark", "/data/TestSmallWebCaseFunc.class");
            planBuilder.setPlatformUdfPath("spark", "/Users/zhuxingpo/Downloads/Due/TestSmallWebCaseFunc.class");

            // 创建节点   例如该map的value值是本项目test.csv的绝对路径
            DataQuanta sourceNode = planBuilder.readDataFrom(new HashMap<String, String>() {{
<<<<<<< HEAD
                //put("inputPath", "hdfs://hdfs-namenode:8020/zxpDir/test.csv");
                put("inputPath", "hdfs://localhost:8020/input/test.csv");
            }});
=======
                put("inputPath", "/Users/jason/Desktop/test.csv");
            }}).withTargetPlatform("spark");
>>>>>>> master

            DataQuanta filterNode = DataQuanta.createInstance("filter", new HashMap<String, String>() {{
                put("udfName", "filterFunc");
            }}).withTargetPlatform("spark");

            DataQuanta mapNode = DataQuanta.createInstance("map", new HashMap<String, String>() {{
                put("udfName", "mapFunc");
            }}).withTargetPlatform("spark");

            DataQuanta reduceNode = DataQuanta.createInstance("reduce-by-key", new HashMap<String, String>() {{
                put("udfName", "reduceFunc");
                put("keyName", "reduceKey");
            }}).withTargetPlatform("spark");

            DataQuanta sortNode = DataQuanta.createInstance("sort", new HashMap<String, String>() {{
                put("udfName", "sortFunc");
            }}).withTargetPlatform("spark");

            // 最终结果的输出路径   例如该map的value值是本项目output.csv的绝对路径
            DataQuanta sinkNode = DataQuanta.createInstance("sink", new HashMap<String, String>() {{
<<<<<<< HEAD
                //put("outputPath", "hdfs://hdfs-namenode:8020/zxpDir/output.csv"); // 具体resources的路径通过配置文件获得
                put("outputPath", "hdfs://localhost:8020/output/output.csv"); // 具体resources的路径通过配置文件获得
            }});
=======
                put("outputPath", "/tmp/clic_output/output.csv"); // 具体resources的路径通过配置文件获得
            }}).withTargetPlatform("spark");
>>>>>>> master

            planBuilder.addVertex(sourceNode);
            planBuilder.addVertex(filterNode);
            planBuilder.addVertex(mapNode);
            planBuilder.addVertex(reduceNode);
            planBuilder.addVertex(sortNode);
            planBuilder.addVertex(sinkNode);

            // 链接节点，即构建DAG
            planBuilder.addEdge(sourceNode, filterNode);
            planBuilder.addEdge(filterNode, mapNode);
            planBuilder.addEdge(mapNode, reduceNode);
            planBuilder.addEdge(reduceNode, sortNode);
            planBuilder.addEdge(sortNode, sinkNode);

            planBuilder.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
