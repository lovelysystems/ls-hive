package com.lovelysystems.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.mapred.TaskAttemptID;

@UDFType(deterministic = false)
@Description(name = "sequenceid",
        value = "_FUNC_(existing) - Returns a unique sequence id per call or existing if not null")
public class SequenceIdUDF extends UDF {
    
    private String workdir = null;
    private long taskId = -1;
    private long ts = 0;
    private long seq = 0;
    
    private static final int MAX_SEQ=1<<20;
    private static final int MAX_TASKID=1<<12;
    

    public void setWorkdir(){
        setWorkdir(System.getenv().get("HADOOP_WORK_DIR"));
    }
    
    public void setWorkdir(String workdir){
        assert(workdir!=null && workdir.length()>0);
        this.workdir = workdir;
    }
    
    private long getTaskId(){
        if (taskId == -1){
            String[] parts = workdir.split("/");
            TaskAttemptID res = TaskAttemptID.forName(parts[parts.length-2]);
            this.taskId = res.getTaskID().getId();
            assert(taskId<=MAX_TASKID);
        }
        return taskId;
    }
    
    private long getSequenceId(){
        if (ts == 0 || seq >= MAX_SEQ){
            int newTS = (int) (System.currentTimeMillis() / 1000L);
            if (newTS <= ts){
                newTS = (int) ts + 1;
            }
            this.ts = newTS;
            this.seq = 0;
        }
        this.seq ++;
        return (ts<<32) + (getTaskId()<<20) +  seq-1;
    }
    
    public Long evaluate(Long existing) {
        if (existing != null){
            return existing;
        }
        if (workdir == null){
            setWorkdir();
        }
        return getSequenceId();
    }
}
