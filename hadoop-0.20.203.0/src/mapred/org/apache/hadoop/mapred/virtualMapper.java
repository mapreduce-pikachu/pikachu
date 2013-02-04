package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class virtualMapper {
  private int[] virtualMap;
  private int maxVirtualBins;
  private int version;
  private boolean setDefault;

  public virtualMapper() {
    setDefault = false;
  }
  
  /**
   * It's used to create a dummy virtualMapper
   * @param version Specify a version number
   */
  public virtualMapper(int version) {
    setDefault = false;
    this.version = version;
    this.maxVirtualBins = 1;
    this.virtualMap = new int[maxVirtualBins];
  }
  
  public void setDefault(JobConf conf) {
	  int phyReducerCount = conf.getNumReduceTasks();
	  String[] vbin = conf.getStrings("maxVirtualBins");
	  maxVirtualBins = Integer.parseInt(vbin[0]);
	  
	  /* This is to run tarazu from pikachu start */
	  String[] temp = conf.getStrings("isTarazu");
	  int isTarazu = Integer.parseInt(temp[0]);
	  
	  temp = conf.getStrings("totalReduceSlots");
	  int nodeReduceSlots1 = Integer.parseInt(temp[0]);
	  int nodeReduceSlots2 = Integer.parseInt(temp[1]);
	  /* This is to run tarazu from pikachu end 1 */
	  
	  System.out.println("virtualMapper maxVirtualBins = " + maxVirtualBins + " " + System.currentTimeMillis() / 1000);
	  System.out.println("virtualMapper phyReducerCount = " + phyReducerCount);
	  virtualMap = new int[maxVirtualBins];
	  int i = 0;
	  
	  /* This is to run tarazu from pikachu start */
	  if (isTarazu == 1){
		  temp = conf.getStrings("tarazuR");
		  float setR = Float.parseFloat(temp[0]);
//		  int setR = Integer.parseInt(temp[0]);
		  
		  while (i <  setR * 10 * nodeReduceSlots1){
			  virtualMap[i] = (int) Math.floor(i / (setR * 10)); // TODO: Remove this once tested for R=4s
			  i++;
		  }

		  while (i < maxVirtualBins) {
			  virtualMap[i] = (int) Math.floor((i - setR * 10 * nodeReduceSlots1) / 10) + nodeReduceSlots1; // By default there are 10 vbins/red
			  i++;
		  }
	  }
	  /* This is pikachu */
	  else{
		  while (i < maxVirtualBins) {
			  virtualMap[i] = (int) Math.floor(i / 10) ; // By default there are 10 vbins/red
			  i++;
		  }
	  }
	  //
	  
	  /*
	  int setR = 4;
	  while (i <  setR * 10 * 4){
		  virtualMap[i] = (int) Math.floor(i / (setR * 10)); // TODO: Remove this once tested for R=4s
		  i++;
	  }

	  while (i < maxVirtualBins) {
		  virtualMap[i] = (int) Math.floor((i - setR * 10 * 4) / 10) + 4; // By default there are 10 vbins/red
		  i++;
	  }
	  */
	  setDefault = true;
  }

  public boolean set(int[] bins, int myversion) {
	  if (setDefault == false) {
		  this.maxVirtualBins = bins.length;
		  virtualMap = Arrays.copyOf(bins, bins.length);
		  System.out.println("rohanvbins" + Arrays.toString(virtualMap));
		  version = myversion;
		  return true;
	  } else {
		  return false;
	  }
  }

  // public void virtualMapper() {
  //
  // String[] vbin = conf.getStrings("maxVirtualBins");
  // String[] tempMap = conf.getStrings("hostsReducerShare");
  // maxVirtualBins = Integer.parseInt(vbin[0]);
  // // System.out.println("maxVirtualBins = " + maxVirtualBins);
  // virtualMap = new int[maxVirtualBins];
  //
  // for (int i = 0; i < Integer.parseInt(vbin[0]); i++) {
  // virtualMap[i] = Integer.parseInt(tempMap[i]);
  // }
  // }

  public List<Integer> getReducerBins(int reduce) {
    List<Integer> returnBins = new ArrayList<Integer>();
    for (int i = 0; i < maxVirtualBins; i++) {
      if (virtualMap[i] == reduce) {
        returnBins.add(i);
        // System.out.println("virtual bin added" + i);
      }
    }
//    System.out.println("rohanvbins GET for " + reduce + "\t" +  Arrays.toString(returnBins.toArray()));
    return returnBins;
  }

  public int getVersion() {
    return version;
  }

  public void write(DataOutput out) throws IOException {    
    // TODO: test write
    out.writeInt(version);
    out.writeInt(maxVirtualBins);
    for (int i = 0; i < maxVirtualBins; i++) {
      out.writeInt(virtualMap[i]);
    }
  }

  public void readFields(DataInput in) throws IOException {
    // TODO: test read
    version = in.readInt();
    maxVirtualBins = in.readInt();
    virtualMap = new int[maxVirtualBins];
    for (int i = 0; i < maxVirtualBins; i++) {
      virtualMap[i] = in.readInt();
    }
  }
}
