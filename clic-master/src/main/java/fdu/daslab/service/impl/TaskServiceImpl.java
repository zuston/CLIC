package fdu.daslab.service.impl;

import fdu.daslab.kubernetes.KubernetesStage;
import fdu.daslab.scheduler.TaskScheduler;
import fdu.daslab.scheduler.event.TaskSubmitEvent;
import fdu.daslab.scheduler.model.Task;
import fdu.daslab.thrift.master.TaskService;
import fdu.daslab.util.FieldName;
import org.apache.thrift.TException;

import java.util.*;
import java.text.SimpleDateFormat;

/**
 *
 * 任务提交的实现类
 *
 * @author 唐志伟, Du Qinghua
 * @version 1.0
 * @since 2020/10/23 8:15 PM
 */
public class TaskServiceImpl implements TaskService.Iface {

    private TaskScheduler taskScheduler;

    public TaskServiceImpl(TaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
    }

    @Override
    public void submitPlan(String planName, String planDagPath) {
        try {
            taskScheduler.post(new TaskSubmitEvent(planName, planDagPath));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    //查看所有任务的状态
    @Override
    public List<Map<String, String>> listAllTask() {
        List<Map<String, String>> result = new ArrayList<>();
        List<Task> allTaskList = taskScheduler.handlerListAllEvent();
        allTaskList.forEach(
                task -> {
                    Map<String, String> taskMap = new HashMap<>();
                    taskMap.put(FieldName.TASK_PLAN_NAME, transToString(task.getPlanName()));
                    taskMap.put(FieldName.TASK_SUBMIT_TIME, transToString(task.getSubmitTime()));
                    taskMap.put(FieldName.TASK_START_TIME, transToString((task.getStartTime())));
                    taskMap.put(FieldName.TASK_COMPLETE_TIME, transToString(task.getCompleteTime()));
                    taskMap.put(FieldName.TASK_STATUS, transToString(task.getTaskStatus()));
                    result.add(taskMap);
                }
        );
        return result;
    }

    @Override
    public Map<String, String> getTaskInfo(String planName) throws TException {
        Map<String, String> result = new HashMap<>();
        Task task = taskScheduler.getTaskByTaskName(planName);
        if (task != null){
            result.put(FieldName.TASK_PLAN_NAME, transToString(task.getPlanName()));
            result.put(FieldName.TASK_SUBMIT_TIME, transToString(task.getSubmitTime()));
            result.put(FieldName.TASK_START_TIME, transToString(task.getStartTime()));
            result.put(FieldName.TASK_COMPLETE_TIME, transToString(task.getCompleteTime()));
            result.put(FieldName.TASK_STATUS, transToString(task.getTaskStatus()));
            List<String> stageIdList = task.getStageIdList();
            int stageNum = stageIdList.size();
            result.put(FieldName.TASK_STAGE_NUM, Integer.toString(stageNum));
            result.put(FieldName.TASK_STAGE_LIST, transToString(stageIdList));
        }
        return result;
    }

    @Override
    public Map<String, String> getStageInfo(String stageId) throws TException {
        Map<String, String> result = new HashMap<>();
        KubernetesStage stage = taskScheduler.getStageInfoByStageId(stageId);
        if (stage != null){
            result.put(FieldName.STAGE_ID, transToString(stage.getStageId()));
            result.put(FieldName.STAGE_PLATFORM, transToString(stage.getPlatformName()));
            List<String> parentStageIds = new ArrayList<>(stage.getParentStageIds());
            result.put(FieldName.STAGE_PARENT_LIST, transToString(parentStageIds));
            List<String> childStageIds = new ArrayList<>(stage.getChildStageIds());
            result.put(FieldName.STAGE_CHILDREN_LIST, transToString(childStageIds));
            result.put(FieldName.STAGE_START_TIME, transToString(stage.getStartTime()));
            result.put(FieldName.STAGE_COMPLETE_TIME, transToString(stage.getCompleteTime()));
            result.put(FieldName.STAGE_RETRY_COUNT, transToString(stage.getRetryCounts()));
            result.put(FieldName.STAGE_JOB_INFO, transToString(stage.getJobInfo()));
        }
        return result;
    }

    @Override
    public List<String> getStageIdOfTask(String planName) throws TException {
        Task task = taskScheduler.getTaskByTaskName(planName);
        List<String> stageIdList = task.getStageIdList();
        return stageIdList;
    }

    private String transToString(Object obj) {
        String res = null;
        if(obj == null){
            res = "   -";
        }else {
            if (obj instanceof Date){
                res = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(obj);
            }else if(obj instanceof List){
                res = listToString((List<String>) obj);
            }else {
                res = obj.toString();
            }
        }
        return res;
    }

    public static String listToString(List<String> list){
        if(list==null){
            return null;
        }
        StringBuilder result = new StringBuilder();
        boolean first = true;
        //第一个前面不拼接","
        for(String string :list) {
            if(first) {
                first=false;
            }else{
                result.append(",");
            }
            result.append(string);
        }
        return result.toString();
    }

}
