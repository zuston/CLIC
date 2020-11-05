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
 * 获取指定task的所有stage的详细信息
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/11/05 09:48
 */
public class ShellGetStagesOfTask {
    private static Logger logger = LoggerFactory.getLogger(ShellGetStagesOfTask.class);
    public static void main(String[] args) {
        int size = args.length;
        //取最后两个参数作为ip和port
        String masterHost = args[size - 2];
        Integer masterPort = Integer.parseInt(args[size - 1]);
        TaskServiceClient taskServiceClient = new TaskServiceClient(masterHost, masterPort);
        String planName = args[0];
        List<String> stageIdList = taskServiceClient.getStageIdOfTask(planName);
        List<Cell> header = new ArrayList<Cell>() { {
            add(new Cell(FieldName.STAGE_ID));
            add(new Cell(FieldName.STAGE_PLATFORM));
            add(new Cell(FieldName.STAGE_START_TIME));
            add(new Cell(FieldName.STAGE_COMPLETE_TIME));
            add(new Cell(FieldName.STAGE_RETRY_COUNT));
            add(new Cell(FieldName.STAGE_PARENT_LIST));
            add(new Cell(FieldName.STAGE_CHILDREN_LIST));
        }};
        List<List<Cell>> body = new ArrayList<List<Cell>>();
        //遍历id数组，获取所有id对应的stage信息
        stageIdList.forEach(stageId -> {
            Map<String, String> stageInfo = taskServiceClient.getStageInfo(stageId);
            //如果stageinfo不为空
            if (!stageInfo.isEmpty()) {
                List<Cell> row = new ArrayList<Cell>() { {
                    add(new Cell(stageInfo.get(FieldName.STAGE_ID)));
                    add(new Cell(stageInfo.get(FieldName.STAGE_PLATFORM)));
                    add(new Cell(stageInfo.get(FieldName.STAGE_START_TIME)));
                    add(new Cell(stageInfo.get(FieldName.STAGE_COMPLETE_TIME)));
                    add(new Cell(stageInfo.get(FieldName.STAGE_RETRY_COUNT)));
                    add(new Cell(stageInfo.get(FieldName.STAGE_PARENT_LIST)));
                    add(new Cell(stageInfo.get(FieldName.STAGE_CHILDREN_LIST)));
                }};
                body.add(row);
            }
        });

        new ConsoleTable.ConsoleTableBuilder().addHeaders(header).addRows(body).build().print();
        System.out.println();
        logger.info("get stage info of task has completed!");
    }

}
