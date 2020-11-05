package fdu.daslab.shellservice;

import fdu.daslab.client.TaskServiceClient;
import fdu.daslab.consoletable.ConsoleTable;
import fdu.daslab.consoletable.table.Cell;
import fdu.daslab.utils.FieldName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * 根据task name获取具体task的详细信息
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/11/05 09:37
 */
public class ShellGetTaskInfo {
    private static  Logger logger = LoggerFactory.getLogger(ShellGetTaskInfo.class);
    public static void main(String[] args) {
        int size = args.length;
        //取最后两个参数作为ip和port
        String masterHost = args[size - 2];
        Integer masterPort = Integer.parseInt(args[size - 1]);
        TaskServiceClient taskServiceClient = new TaskServiceClient(masterHost, masterPort);
        String planName = args[0];
        Map<String, String> taskInfo = taskServiceClient.getTaskInfo(planName);

        List<Cell> header = new ArrayList<Cell>() { {
            add(new Cell(FieldName.TASK_PLAN_NAME));
            add(new Cell(FieldName.TASK_SUBMIT_TIME));
            add(new Cell(FieldName.TASK_START_TIME));
            add(new Cell(FieldName.TASK_COMPLETE_TIME));
            add(new Cell(FieldName.TASK_STATUS));
            add(new Cell(FieldName.TASK_STAGE_NUM));
            add(new Cell(FieldName.TASK_STAGE_LIST));
        }};
        List<List<Cell>> body = new ArrayList<List<Cell>>();
        //如果stageinfo不为空
        if (!taskInfo.isEmpty()) {
            List<Cell> row = new ArrayList<Cell>() { {
                add(new Cell(taskInfo.get(FieldName.TASK_PLAN_NAME)));
                add(new Cell(taskInfo.get(FieldName.TASK_SUBMIT_TIME)));
                add(new Cell(taskInfo.get(FieldName.TASK_START_TIME)));
                add(new Cell(taskInfo.get(FieldName.TASK_COMPLETE_TIME)));
                add(new Cell(taskInfo.get(FieldName.TASK_STATUS)));
                add(new Cell(taskInfo.get(FieldName.TASK_STAGE_NUM)));
                add(new Cell(taskInfo.get(FieldName.TASK_STAGE_LIST)));
            }};
            body.add(row);
        }
        new ConsoleTable.ConsoleTableBuilder().addHeaders(header).addRows(body).build().print();
        System.out.println();
        logger.info("get task info has completed!");
    }

}
