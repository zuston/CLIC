package fdu.daslab.shellservice;

import fdu.daslab.client.TaskServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 根据 planName和planDagPath提交一个task任务
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/11/04 17:44
 */
public class ShellSubmitPlan {
    private static  Logger logger = LoggerFactory.getLogger(ShellSubmitPlan.class);

    public static void main(String[] args) {
        int size = args.length;
        //取最后两个参数作为ip和port
        String masterHost = args[size - 2];
        Integer masterPort = Integer.parseInt(args[size - 1]);
        TaskServiceClient taskServiceClient = new TaskServiceClient(masterHost, masterPort);
        String planName = args[0];
        String planDagPath = args[1];
        taskServiceClient.submitPlan(planName, planDagPath);

        logger.info(planName + " has been submitted!");

    }
}
