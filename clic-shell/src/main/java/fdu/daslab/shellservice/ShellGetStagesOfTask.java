package fdu.daslab.shellservice;

import fdu.daslab.client.TaskServiceClient;

/**
 * 获取指定task的所有stage的详细信息
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/11/05 09:48
 */
public class ShellGetStagesOfTask {
    private static String masterHost;
    private static Integer masterPort;

    public static void main(String[] args) {
        int size = args.length;
        //取最后两个参数作为ip和port
        masterHost = args[size-2];
        masterPort = Integer.parseInt(args[size-1]);
        TaskServiceClient taskServiceClient = new TaskServiceClient(masterHost, masterPort);

    }
}
