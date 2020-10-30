package fdu.daslab.shellservice;

import fdu.daslab.client.TaskServiceClient;
import fdu.daslab.utils.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;

/**
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/10/22 18:14
 */
public class ShellGetTaskList {

    private static String masterHost;
    private static Integer masterPort;
    private static Configuration configuration;
    private static Logger logger = LoggerFactory.getLogger(ShellGetTaskList.class);

    public static void main(String[] args) {
        int size = args.length;
        //取最后两个参数作为ip和port
        masterHost = args[size-2];
        masterPort = Integer.parseInt(args[size-1]);
        TaskServiceClient taskServiceClient = new TaskServiceClient(masterHost, masterPort);
        List<String> allTaskList = taskServiceClient.getTaskList();

        System.out.format("%-17s%-17s%-17s%-17s%-17s%-17s\n","PlanName","StageIdList","SubmitTime","StartTime","CompleteTime","TaskStatus");
        //如果列表为空
        if (allTaskList.isEmpty()) {
            System.out.println();
        }else {
            List<List<String>> taskListInfo = parseTaskList(allTaskList);
            taskListInfo.forEach(task->{
                task.forEach(info->{
                    System.out.format("%-17s",info);
                });
                System.out.format("\n");
            });
        }
    }

    public static List<List<String>> parseTaskList(List<String> taskList) {
        List<List<String>> taskListInfo = null;
        taskList.forEach(task-> {
               String[] info = task.split("-");
               taskListInfo.add(Arrays.asList(info));
        });
        return taskListInfo;
    }
}
