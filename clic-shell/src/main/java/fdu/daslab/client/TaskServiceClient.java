package fdu.daslab.client;

import fdu.daslab.thrift.master.TaskService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * 与 task service连接获取信息的client
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/10/22 16:47
 */
public class TaskServiceClient {
    private static Logger logger = LoggerFactory.getLogger(TaskServiceClient.class);
    private TaskService.Client client;
    private TTransport transport;

    /**
     * @param masterHost    driver的hostip信息
     * @param masterPort    driver的port信息
     */
    public TaskServiceClient(String masterHost, Integer masterPort) {
        this.transport = new TSocket(masterHost, masterPort);
        TProtocol protocol = new TBinaryProtocol(transport);
        TMultiplexedProtocol tMultiplexedProtocol = new TMultiplexedProtocol(protocol, "TaskService");
        this.client = new TaskService.Client(tMultiplexedProtocol);

    }

    /**
     * 获取所有task列表信息
     * @return tasklist
     */
    public List<String> getTaskList() {
        List<String>  allTaskList = new LinkedList<>();
        try {
            transport.open();
            allTaskList = client.listAllTask();
            transport.close();
        } catch (TException e) {
            logger.error("An Error occur when CLIC shell get taskList");
            e.getStackTrace();
        }
        return  allTaskList;
    }

    /**
     * 提交任务，异步
     *
     * @param planName plan名称
     * @param planDagPath plan的Dag的yaml文件的路径
     * @throws TException thrift异常
     */
    public void submitPlan(String planName, String planDagPath) {
        try {
            transport.open();
            client.submitPlan(planName, planDagPath);
            transport.close();
        } catch (TException e) {
            logger.error("An Error occur when CLIC shell submit plan");
            e.getStackTrace();
        }
    }



}
