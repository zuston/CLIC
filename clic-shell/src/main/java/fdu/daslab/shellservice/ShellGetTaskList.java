package fdu.daslab.shellservice;

import fdu.daslab.client.TaskServiceClient;
import fdu.daslab.consoletable.ConsoleTable;
import fdu.daslab.consoletable.table.Cell;
import fdu.daslab.utils.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.ArrayList;
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

    public static void main(String[] args) {
        int size = args.length;
        //取最后两个参数作为ip和port
        masterHost = args[size-2];
        masterPort = Integer.parseInt(args[size-1]);
        TaskServiceClient taskServiceClient = new TaskServiceClient(masterHost, masterPort);
        List<String> allTaskList = taskServiceClient.getTaskList();
        List<Cell> header = new ArrayList<Cell>(){{
            add(new Cell("PlanName"));
            add(new Cell("StageIdList"));
            add(new Cell("SubmitTime"));
            add(new Cell("StartTime"));
            add(new Cell("CompleteTime"));
            add(new Cell("TaskStatus"));
        }};
        List<List<Cell>> body = new ArrayList<List<Cell>>();
        List<Cell> row = new ArrayList<Cell>();
        //如果列表为空
        if (allTaskList.isEmpty()) {
            System.out.println();
        }else {
            List<List<String>> taskListInfo = parseTaskList(allTaskList);
            taskListInfo.forEach(task->{
                task.forEach(info->{
                    row.add(new Cell(info));
                });
               body.add(row);
            });
        }
        new ConsoleTable.ConsoleTableBuilder().addHeaders(header).addRows(body).build().print();
    }

    public static List<List<String>> parseTaskList(List<String> taskList) {
        List<List<String>> taskListInfo = new ArrayList<>();
        taskList.forEach(task-> {
               String[] info = task.split("&");
               taskListInfo.add(Arrays.asList(info));
        });
        return taskListInfo;
    }
}
