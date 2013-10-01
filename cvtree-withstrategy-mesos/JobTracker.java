


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Filters;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Protos.Value;



public class JobTracker{
	
	static class JobScheduler implements Scheduler{

	    public JobScheduler(ExecutorInfo executor, int totalTasks) {
	      this.executor = executor;
	      this.totalTasks = totalTasks;
	      this.alltaskmap=SystemRun.preparetoruntwo("/home/yifan/Public/All/");
	    }
	

	    @Override
	    public void registered(SchedulerDriver driver, 
	                           FrameworkID frameworkId, 
	                           MasterInfo masterInfo) {
	      System.out.println("Registered! ID = " + frameworkId.getValue());
	    }

	    @Override
	    public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {}

	    @Override
	    public void disconnected(SchedulerDriver driver) {}
	    
	    
	    public void smartchoose(Offer offer,SchedulerDriver driver,HashMap<Integer,List<List<File>>> alltaskmap)
	    {
	    	
	    	List<List<File>> taskforthree = new ArrayList<List<File>>();
	    	List<List<File>> taskforfour = new ArrayList<List<File>>();
	    	List<List<File>> taskforfive = new ArrayList<List<File>>();
	    	List<List<File>> taskforsix = new ArrayList<List<File>>();
	    	
	    	String Stringvalue;
	    	
	    	taskforthree=alltaskmap.get(0);
	    	taskforfour=alltaskmap.get(1);
	    	taskforfive=alltaskmap.get(2);
	    	taskforsix=alltaskmap.get(3);
	    
	    	
	    	if(offer.getHostname().contains("3"))
	    	{
	    	    List<TaskInfo> tasks = new ArrayList<TaskInfo>();
		        if (launchedTasks < totalTasks) {
		        	
		        	Stringvalue=taskforthree.get(indexthree).get(0).getName()+" "+taskforthree.get(indexthree).get(1).getName();
		        	
		        	
		        	
		          TaskID taskId = TaskID.newBuilder()
		        		  .setValue(Stringvalue).build();

		          System.out.println("Launching task " + taskId.getValue());
		          
		          
		         

		          TaskInfo task = TaskInfo.newBuilder()
		            .setName("Comparetask " + taskId.getValue())
		            .setTaskId(taskId)
		            .setSlaveId(offer.getSlaveId())
		            .addResources(Resource.newBuilder()
		                          .setName("cpus")
		                          .setType(Value.Type.SCALAR)
		                          .setScalar(Value.Scalar.newBuilder()
		                                     .setValue(1)
		                                     .build())
		                          .build())
		            .addResources(Resource.newBuilder()
		                          .setName("mem")
		                          .setType(Value.Type.SCALAR)
		                          .setScalar(Value.Scalar.newBuilder()
		                                     .setValue(1000)
		                                     .build())
		                          .build())
		            .setExecutor(executor)
		            .build();
		          tasks.add(task);
		        }
		        Filters filters = Filters.newBuilder().setRefuseSeconds(3).build();
		        driver.launchTasks(offer.getId(), tasks, filters);
		        indexthree++;
		        launchedTasks++;
		        
		      }
	  
	    	if(offer.getHostname().contains("4"))
	    	{
	    	    List<TaskInfo> tasks = new ArrayList<TaskInfo>();
		        if (launchedTasks < totalTasks) {
		        	
		        	Stringvalue=taskforfour.get(indexfour).get(0).getName()+" "+taskforfour.get(indexfour).get(1).getName();
		        	
		        	
		        	
		          TaskID taskId = TaskID.newBuilder()
		        		  .setValue(Stringvalue).build();

		          System.out.println("Launching task " + taskId.getValue());
		          
		          
		         

		          TaskInfo task = TaskInfo.newBuilder()
		            .setName("Comparetask " + taskId.getValue())
		            .setTaskId(taskId)
		            .setSlaveId(offer.getSlaveId())
		            .addResources(Resource.newBuilder()
		                          .setName("cpus")
		                          .setType(Value.Type.SCALAR)
		                          .setScalar(Value.Scalar.newBuilder()
		                                     .setValue(1)
		                                     .build())
		                          .build())
		            .addResources(Resource.newBuilder()
		                          .setName("mem")
		                          .setType(Value.Type.SCALAR)
		                          .setScalar(Value.Scalar.newBuilder()
		                                     .setValue(1000)
		                                     .build())
		                          .build())
		            .setExecutor(executor)
		            .build();
		          tasks.add(task);
		        }
		        Filters filters = Filters.newBuilder().setRefuseSeconds(3).build();
		        driver.launchTasks(offer.getId(), tasks, filters);
		        indexfour++;
		        launchedTasks++;
	    	
	    	}
	    	
	    	if(offer.getHostname().contains("5"))
	    	{
	    		
	    	    List<TaskInfo> tasks = new ArrayList<TaskInfo>();
		        if (launchedTasks < totalTasks) {
		        	
		        	Stringvalue=taskforfive.get(indexfive).get(0).getName()+" "+taskforfive.get(indexfive).get(1).getName();
		        	
		        	
		        	
		          TaskID taskId = TaskID.newBuilder()
		        		  .setValue(Stringvalue).build();

		          System.out.println("Launching task " + taskId.getValue());
		          
		          
		         

		          TaskInfo task = TaskInfo.newBuilder()
		            .setName("Comparetask " + taskId.getValue())
		            .setTaskId(taskId)
		            .setSlaveId(offer.getSlaveId())
		            .addResources(Resource.newBuilder()
		                          .setName("cpus")
		                          .setType(Value.Type.SCALAR)
		                          .setScalar(Value.Scalar.newBuilder()
		                                     .setValue(1)
		                                     .build())
		                          .build())
		            .addResources(Resource.newBuilder()
		                          .setName("mem")
		                          .setType(Value.Type.SCALAR)
		                          .setScalar(Value.Scalar.newBuilder()
		                                     .setValue(1000)
		                                     .build())
		                          .build())
		            .setExecutor(executor)
		            .build();
		          tasks.add(task);
		        }
		        Filters filters = Filters.newBuilder().setRefuseSeconds(3).build();
		        driver.launchTasks(offer.getId(), tasks, filters);
		        indexfive++;
		        launchedTasks++;
	    
	    	}
	    	
	    	if(offer.getHostname().contains("6"))
	    	{
	    	    List<TaskInfo> tasks = new ArrayList<TaskInfo>();
		        if (launchedTasks < totalTasks) {
		        	
		        	Stringvalue=taskforsix.get(indexsix).get(0).getName()+" "+taskforsix.get(indexsix).get(1).getName();
		        	
		        	
		        	
		          TaskID taskId = TaskID.newBuilder()
		        		  .setValue(Stringvalue).build();

		          System.out.println("Launching task " + taskId.getValue());
		          
		          
		         

		          TaskInfo task = TaskInfo.newBuilder()
		            .setName("Comparetask " + taskId.getValue())
		            .setTaskId(taskId)
		            .setSlaveId(offer.getSlaveId())
		            .addResources(Resource.newBuilder()
		                          .setName("cpus")
		                          .setType(Value.Type.SCALAR)
		                          .setScalar(Value.Scalar.newBuilder()
		                                     .setValue(1)
		                                     .build())
		                          .build())
		            .addResources(Resource.newBuilder()
		                          .setName("mem")
		                          .setType(Value.Type.SCALAR)
		                          .setScalar(Value.Scalar.newBuilder()
		                                     .setValue(1000)
		                                     .build())
		                          .build())
		            .setExecutor(executor)
		            .build();
		          tasks.add(task);
		        }
		        Filters filters = Filters.newBuilder().setRefuseSeconds(3).build();
		        driver.launchTasks(offer.getId(), tasks, filters);
		        indexsix++;
		        launchedTasks++;
	    	}
	    	
	    	
	    }
	   

	    @Override
	    public void resourceOffers(SchedulerDriver driver,
	                               List<Offer> offers) {
	      for (Offer offer : offers) {
	    	  
	    	  smartchoose(offer,driver,alltaskmap);
	    	
	      }
	    }

	    @Override
	    public void offerRescinded(SchedulerDriver driver, OfferID offerId) {}

	    @Override
	    public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
	      System.out.println("Status update: task " + status.getTaskId().getValue() +
	                         " is in state " + status.getState());
	      if (status.getState() == TaskState.TASK_FINISHED) {
	        finishedTasks++;
	        System.out.println("Finished tasks: " + finishedTasks);
	        if (finishedTasks == totalTasks) {
	          driver.stop();
	        }
	      }
	    }

	    @Override
	    public void frameworkMessage(SchedulerDriver driver,
	                                 ExecutorID executorId,
	                                 SlaveID slaveId,
	                                 byte[] data) {}

	    @Override
	    public void slaveLost(SchedulerDriver driver, SlaveID slaveId) {}

	    @Override
	    public void executorLost(SchedulerDriver driver,
	                             ExecutorID executorId,
	                             SlaveID slaveId,
	                             int status) {}

	    public void error(SchedulerDriver driver, String message) {
	      System.out.println("Error: " + message);
	    }

	    private final ExecutorInfo executor;
	    private int totalTasks;
	    private int launchedTasks = 0;
	    private int finishedTasks = 0;
	    HashMap<Integer,List<List<File>>> alltaskmap;
	    int indexthree=0;
    	int indexfour=0;
    	int indexfive=0;
    	int indexsix=0;
	  }

	
	 private static void usage() {
		    String name = JobTracker.class.getName();
		    System.err.println("Usage: " + name + " master <tasks>");
		  }

	  public static void main(String[] args) throws Exception {
	    if (args.length < 1 || args.length > 2) {
	    	usage();
	    	System.exit(1);
	    }
	    
	    int totaltasknumbers;

	    HashMap<Integer,List<List<File>>> alltaskmap;
	    alltaskmap=SystemRun.preparetoruntwo("/home/yifan/Public/All/");
	    
	    totaltasknumbers=alltaskmap.get(0).size()+alltaskmap.get(1).size()+alltaskmap.get(2).size()+alltaskmap.get(3).size();
	    

	    String uri = new File("./test-executor").getCanonicalPath();

	    
	    
	    ExecutorInfo executor = ExecutorInfo.newBuilder()
	      .setExecutorId(ExecutorID.newBuilder().setValue("PreprocessandCompare").build())
	      .setCommand(CommandInfo.newBuilder().setValue(uri).build())
	      .build();

	    FrameworkInfo framework = FrameworkInfo.newBuilder()
	        .setUser("") // Have Mesos fill in the current user.
	        .setName("Test Framework (CVTREE)")
	        .build();

	    MesosSchedulerDriver driver =  new MesosSchedulerDriver(
	        new JobScheduler(executor, totaltasknumbers),
	        framework,
	        args[0]);

	    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
	  }
	
}	