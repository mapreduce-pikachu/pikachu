package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * A class that represents the communication between the tasktracker and child
 * reduce tasks w.r.t the update of virtual mappings.
 */
public class VirtualMappingUpdate implements Writable {

  private virtualMapper[] virtualMappers;
  boolean reset;

  public VirtualMappingUpdate() {
  }

  public VirtualMappingUpdate(virtualMapper[] virtualMappers) {
    this.virtualMappers = virtualMappers;
  }
  
  public virtualMapper[] getVirtualMappers () {
    return virtualMappers;
  }


  public void write(DataOutput out) throws IOException {
    out.writeInt(virtualMappers.length);
    for (virtualMapper vm : virtualMappers) {
      vm.write(out);
    }
  }

  public void readFields(DataInput in) throws IOException {
    virtualMappers = new virtualMapper[in.readInt()];
    for (int i = 0; i < virtualMappers.length; ++i) {
      virtualMappers[i] = new virtualMapper();
      virtualMappers[i].readFields(in);
    }
  }

}
