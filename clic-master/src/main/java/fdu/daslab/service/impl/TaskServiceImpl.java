package fdu.daslab.service.impl;

import fdu.daslab.kubernetes.KubernetesStage;
import fdu.daslab.scheduler.TaskScheduler;
import fdu.daslab.scheduler.event.TaskSubmitEvent;
import fdu.daslab.scheduler.model.Task;
import fdu.daslab.thrift.master.TaskService;
import fdu.daslab.util.FieldName;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public List<Map<String,String>> listAllTask(){
        List<Map<String,String>> result = new ArrayList<>();
        List<Task> allTaskList = taskScheduler.handlerListAllEvent();
        allTaskList.forEach(
                task -> {
                    Map<String,String> taskMap = new HashMap<>();
                    taskMap.put(FieldName.TASK_PLAN_NAME, task.getPlanName());
                    taskMap.put(FieldName.TASK_SUBMIT_TIME, task.getSubmitTime().toString());
                    taskMap.put(FieldName.TASK_START_TIME, task.getStartTime().toString());
                    taskMap.put(FieldName.TASK_COMPLETE_TIME, task.getCompleteTime().toString());
                    taskMap.put(FieldName.TASK_STATUS, task.getTaskStatus().toString());
                    result.add(taskMap);
                }
        );
        return result;
    }

    @Override
    public Map<String, String> getTaskInfo(String planName) throws TException {
        Map<String,String> result = new HashMap<>();
        Task task = taskScheduler.getTaskByTaskName(planName);
        result.put(FieldName.TASK_PLAN_NAME, task.getPlanName());
        result.put(FieldName.TASK_SUBMIT_TIME, task.getSubmitTime().toString());
        result.put(FieldName.TASK_START_TIME, task.getStartTime().toString());
        result.put(FieldName.TASK_COMPLETE_TIME, task.getCompleteTime().toString());
        result.put(FieldName.TASK_STATUS, task.getTaskStatus().toString());
        List<String> stageIdList = task.getStageIdList();
        int stageNum = stageIdList.size();
        result.put(FieldName.TASK_STAGE_NUM, Integer.toString(stageNum));
        result.put(FieldName.TASK_STAGE_LIST, stageIdList.toString());

        return result;
    }

    @Override
    public Map<String, String> getStageInfo(String stageId) throws TException {
        Map<String,String> result = new HashMap<>();
        KubernetesStage stage = taskScheduler.getStageInfoByStageId(stageId);
        result.put(FieldName.STAGE_ID, stage.getStageId());
        result.put(FieldName.STAGE_PLATFORM, stage.getPlatformName());
        result.put(FieldName.STAGE_PARENT_LIST, stage.getParentStageIds().toString());
        result.put(FieldName.STAGE_CHILDREN_LIST, stage.getChildStageIds().toString());
        result.put(FieldName.STAGE_START_TIME, stage.getStartTime().toString());
        result.put(FieldName.STAGE_COMPLETE_TIME, stage.getCompleteTime().toString());
        result.put(FieldName.STAGE_RETRY_COUNT, stage.getRetryCounts().toString());
        result.put(FieldName.STAGE_JOB_INFO, stage.getJobInfo().toString());

        return result;
    }

    @Override
    public List<String> getStageIdOfTask(String planName) throws TException {
        Task task = taskScheduler.getTaskByTaskName(planName);
        List<String> stageIdList = task.getStageIdList();
        return stageIdList;
    }


}
