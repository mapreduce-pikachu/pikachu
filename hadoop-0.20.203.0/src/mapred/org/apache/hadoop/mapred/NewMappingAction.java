package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapred.TaskTrackerAction.ActionType;

public class NewMappingAction extends TaskTrackerAction {
  private JobID jobId;
  private virtualMapper virtualMapper;
  public NewMappingAction() {
    super(ActionType.NEW_MAPPING);
    this.jobId = new JobID();
    this.virtualMapper = new virtualMapper();    
  }
  
  public NewMappingAction(JobID jobId, virtualMapper virtualMapper) {
    super(ActionType.NEW_MAPPING);
    this.jobId = jobId;
    this.virtualMapper = virtualMapper;
  }
  
  public JobID getJobID() {
    return jobId;
  }
  
  public virtualMapper getVirtualMapper() {
    return virtualMapper;
  }
  
  public void write(DataOutput out) throws IOException {
    jobId.write(out);
    // TODO: test writing virtual mapper;
    virtualMapper.write(out);
  }
  
  public void readFields(DataInput in) throws IOException {
    jobId.readFields(in);
    // TODO: test reading virtual mapper;
    virtualMapper.readFields(in);
  }

}
