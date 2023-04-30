/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim.examples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.CloudletList;
import org.cloudbus.cloudsim.lists.VmList;

/**
 * DatacentreBroker represents a broker acting on behalf of a user. It hides VM management, as vm
 * creation, sumbission of cloudlets to this VMs and destruction of VMs.
 * 
 * @author Rodrigo N. Calheiros
 * @author Anton Beloglazov
 * @since CloudSim Toolkit 1.0
 */
public class DatacenterBroker extends SimEntity {

	private static int vmNum=2;
	
	private static List <Cloudlet> sortList= new ArrayList<Cloudlet>();
	/** The vm list. */
	protected List<? extends Vm> vmList;

	/** The vms created list. */
	protected List<? extends Vm> vmsCreatedList;

	/** The cloudlet list. */
	protected List<? extends Cloudlet> cloudletList;

	/** The cloudlet submitted list. */
	protected List<? extends Cloudlet> cloudletSubmittedList;

	/** The cloudlet received list. */
	protected List<? extends Cloudlet> cloudletReceivedList;

	/** The cloudlets submitted. */
	protected int cloudletsSubmitted;

	/** The vms requested. */
	protected int vmsRequested;

	/** The vms acks. */
	protected int vmsAcks;

	/** The vms destroyed. */
	protected int vmsDestroyed;

	/** The datacenter ids list. */
	protected List<Integer> datacenterIdsList;

	/** The datacenter requested ids list. */
	protected List<Integer> datacenterRequestedIdsList;

	/** The vms to datacenters map. */
	protected Map<Integer, Integer> vmsToDatacentersMap;

	/** The datacenter characteristics list. */
	protected Map<Integer, DatacenterCharacteristics> datacenterCharacteristicsList;

	/**
	 * Created a new DatacenterBroker object.
	 * 
	 * @param name name to be associated with this entity (as required by Sim_entity class from
	 *            simjava package)
	 * @throws Exception the exception
	 * @pre name != null
	 * @post $none
	 */
	public DatacenterBroker(String name) throws Exception {
		super(name);

		setVmList(new ArrayList<Vm>());
		setVmsCreatedList(new ArrayList<Vm>());
		setCloudletList(new ArrayList<Cloudlet>());
		setCloudletSubmittedList(new ArrayList<Cloudlet>());
		setCloudletReceivedList(new ArrayList<Cloudlet>());

		cloudletsSubmitted = 0;
		setVmsRequested(0);
		setVmsAcks(0);
		setVmsDestroyed(0);

		setDatacenterIdsList(new LinkedList<Integer>());
		setDatacenterRequestedIdsList(new ArrayList<Integer>());
		setVmsToDatacentersMap(new HashMap<Integer, Integer>());
		setDatacenterCharacteristicsList(new HashMap<Integer, DatacenterCharacteristics>());
	}

	/**
	 * This method is used to send to the broker the list with virtual machines that must be
	 * created.
	 * 
	 * @param list the list
	 * @pre list !=null
	 * @post $none
	 */
	public void submitVmList(List<? extends Vm> list) {
		getVmList().addAll(list);
	}

	/**
	 * This method is used to send to the broker the list of cloudlets.
	 * 
	 * @param list the list
	 * @pre list !=null
	 * @post $none
	 */
	public void submitCloudletList(List<? extends Cloudlet> list) {
		getCloudletList().addAll(list);
	}

	/**
	 * Specifies that a given cloudlet must run in a specific virtual machine.
	 * 
	 * @param cloudletId ID of the cloudlet being bount to a vm
	 * @param vmId the vm id
	 * @pre cloudletId > 0
	 * @pre id > 0
	 * @post $none
	 */
	public void bindCloudletToVm(int cloudletId, int vmId) {
		CloudletList.getById(getCloudletList(), cloudletId).setVmId(vmId);
	}

	/**
	 * Processes events available for this Broker.
	 * 
	 * @param ev a SimEvent object
	 * @pre ev != null
	 * @post $none
	 */
	@Override
	public void processEvent(SimEvent ev) {
		switch (ev.getTag()) {
		// Resource characteristics request
			case CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
				processResourceCharacteristicsRequest(ev);
				break;
			// Resource characteristics answer
			case CloudSimTags.RESOURCE_CHARACTERISTICS:
				processResourceCharacteristics(ev);
				break;
			// VM Creation answer
			case CloudSimTags.VM_CREATE_ACK:
				processVmCreate(ev);
				break;
			// A finished cloudlet returned
			case CloudSimTags.CLOUDLET_RETURN:
				processCloudletReturn(ev);
				break;
			// if the simulation finishes
			case CloudSimTags.END_OF_SIMULATION:
				shutdownEntity();
				break;
			// other unknown tags are processed by this method
			default:
				processOtherEvent(ev);
				break;
		}
	}

	/**
	 * Process the return of a request for the characteristics of a PowerDatacenter.
	 * 
	 * @param ev a SimEvent object
	 * @pre ev != $null
	 * @post $none
	 */
	protected void processResourceCharacteristics(SimEvent ev) {
		DatacenterCharacteristics characteristics = (DatacenterCharacteristics) ev.getData();
		getDatacenterCharacteristicsList().put(characteristics.getId(), characteristics);

		if (getDatacenterCharacteristicsList().size() == getDatacenterIdsList().size()) {
			setDatacenterRequestedIdsList(new ArrayList<Integer>());
			createVmsInDatacenter(getDatacenterIdsList().get(0));
		}
	}

	/**
	 * Process a request for the characteristics of a PowerDatacenter.
	 * 
	 * @param ev a SimEvent object
	 * @pre ev != $null
	 * @post $none
	 */
	protected void processResourceCharacteristicsRequest(SimEvent ev) {
		setDatacenterIdsList(CloudSim.getCloudResourceList());
		setDatacenterCharacteristicsList(new HashMap<Integer, DatacenterCharacteristics>());

		Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloud Resource List received with "
				+ getDatacenterIdsList().size() + " resource(s)");

		for (Integer datacenterId : getDatacenterIdsList()) {
			sendNow(datacenterId, CloudSimTags.RESOURCE_CHARACTERISTICS, getId());
		}
	}

	/**
	 * Process the ack received due to a request for VM creation.
	 * 
	 * @param ev a SimEvent object
	 * @pre ev != null
	 * @post $none
	 */
	protected void processVmCreate(SimEvent ev) {
		int[] data = (int[]) ev.getData();
		int datacenterId = data[0];
		int vmId = data[1];
		int result = data[2];

		if (result == CloudSimTags.TRUE) {
			getVmsToDatacentersMap().put(vmId, datacenterId);
			getVmsCreatedList().add(VmList.getById(getVmList(), vmId));
			Log.printLine(CloudSim.clock() + ": " + getName() + ": VM #" + vmId
					+ " has been created in Datacenter #" + datacenterId + ", Host #"
					+ VmList.getById(getVmsCreatedList(), vmId).getHost().getId());
		} else {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": Creation of VM #" + vmId
					+ " failed in Datacenter #" + datacenterId);
		}

		incrementVmsAcks();

		// all the requested VMs have been created
		if (getVmsCreatedList().size() == getVmList().size() - getVmsDestroyed()) {
			submitCloudlets();
		} else {
			// all the acks received, but some VMs were not created
			if (getVmsRequested() == getVmsAcks()) {
				// find id of the next datacenter that has not been tried
				for (int nextDatacenterId : getDatacenterIdsList()) {
					if (!getDatacenterRequestedIdsList().contains(nextDatacenterId)) {
						createVmsInDatacenter(nextDatacenterId);
						return;
					}
				}

				// all datacenters already queried
				if (getVmsCreatedList().size() > 0) { // if some vm were created
					submitCloudlets();
				} else { // no vms created. abort
					Log.printLine(CloudSim.clock() + ": " + getName()
							+ ": none of the required VMs could be created. Aborting");
					finishExecution();
				}
			}
		}
	}

	/**
	 * Process a cloudlet return event.
	 * 
	 * @param ev a SimEvent object
	 * @pre ev != $null
	 * @post $none
	 */
	protected void processCloudletReturn(SimEvent ev) {
		Cloudlet cloudlet = (Cloudlet) ev.getData();
		getCloudletReceivedList().add(cloudlet);
		Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloudlet " + cloudlet.getCloudletId()
				+ " received");
		cloudletsSubmitted--;
		if (getCloudletList().size() == 0 && cloudletsSubmitted == 0) { // all cloudlets executed
			Log.printLine(CloudSim.clock() + ": " + getName() + ": All Cloudlets executed. Finishing...");
			clearDatacenters();
			finishExecution();
		} else { // some cloudlets haven't finished yet
			if (getCloudletList().size() > 0 && cloudletsSubmitted == 0) {
				// all the cloudlets sent finished. It means that some bount
				// cloudlet is waiting its VM be created
				clearDatacenters();
				createVmsInDatacenter(0);
			}

		}
	}

	/**
	 * Overrides this method when making a new and different type of Broker. This method is called
	 * by {@link #body()} for incoming unknown tags.
	 * 
	 * @param ev a SimEvent object
	 * @pre ev != null
	 * @post $none
	 */
	protected void processOtherEvent(SimEvent ev) {
		if (ev == null) {
			Log.printLine(getName() + ".processOtherEvent(): " + "Error - an event is null.");
			return;
		}

		Log.printLine(getName() + ".processOtherEvent(): "
				+ "Error - event unknown by this DatacenterBroker.");
	}

	/**
	 * Create the virtual machines in a datacenter.
	 * 
	 * @param datacenterId Id of the chosen PowerDatacenter
	 * @pre $none
	 * @post $none
	 */
	protected void createVmsInDatacenter(int datacenterId) {
		// send as much vms as possible for this datacenter before trying the next one
		int requestedVms = 0;
		String datacenterName = CloudSim.getEntityName(datacenterId);
		for (Vm vm : getVmList()) {
			if (!getVmsToDatacentersMap().containsKey(vm.getId())) {
				Log.printLine(CloudSim.clock() + ": " + getName() + ": Trying to Create VM #" + vm.getId()
						+ " in " + datacenterName);
				sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
				requestedVms++;
			}
		}

		getDatacenterRequestedIdsList().add(datacenterId);

		setVmsRequested(requestedVms);
		setVmsAcks(0);
	}

	/**
	 * Submit cloudlets to the created VMs.
	 * 
	 * @pre $none
	 * @post $none
	 */
	protected void submitCloudlets() {
		int vmIndex = 0;
		for (Cloudlet cloudlet : getCloudletList()) {
			Vm vm;
			// if user didn't bind this cloudlet and it has not been executed yet
			if (cloudlet.getVmId() == -1) {
				vm = getVmsCreatedList().get(vmIndex);
			} else { // submit to the specific vm
				vm = VmList.getById(getVmsCreatedList(), cloudlet.getVmId());
				if (vm == null) { // vm was not created
					Log.printLine(CloudSim.clock() + ": " + getName() + ": Postponing execution of cloudlet "
							+ cloudlet.getCloudletId() + ": bount VM not available");
					continue;
				}
			}

			Log.printLine(CloudSim.clock() + ": " + getName() + ": Sending cloudlet "
					+ cloudlet.getCloudletId() + " to VM #" + vm.getId());
			cloudlet.setVmId(vm.getId());
			sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
			cloudletsSubmitted++;
			vmIndex = (vmIndex + 1) % getVmsCreatedList().size();
			getCloudletSubmittedList().add(cloudlet);
		}

		// remove submitted cloudlets from waiting list
		for (Cloudlet cloudlet : getCloudletSubmittedList()) {
			getCloudletList().remove(cloudlet);
		}
	}

	/**
	 * Destroy the virtual machines running in datacenters.
	 * 
	 * @pre $none
	 * @post $none
	 */
	protected void clearDatacenters() {
		for (Vm vm : getVmsCreatedList()) {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": Destroying VM #" + vm.getId());
			sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.VM_DESTROY, vm);
		}

		getVmsCreatedList().clear();
	}

	/**
	 * Send an internal event communicating the end of the simulation.
	 * 
	 * @pre $none
	 * @post $none
	 */
	protected void finishExecution() {
		sendNow(getId(), CloudSimTags.END_OF_SIMULATION);
	}

	/*
	 * (non-Javadoc)
	 * @see cloudsim.core.SimEntity#shutdownEntity()
	 */
	@Override
	public void shutdownEntity() {
		Log.printLine(getName() + " is shutting down...");
	}

	/*
	 * (non-Javadoc)
	 * @see cloudsim.core.SimEntity#startEntity()
	 */
	@Override
	public void startEntity() {
		Log.printLine(getName() + " is starting...");
		schedule(getId(), 0, CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST);
	}

	/**
	 * Gets the vm list.
	 * 
	 * @param <T> the generic type
	 * @return the vm list
	 */
	@SuppressWarnings("unchecked")
	public <T extends Vm> List<T> getVmList() {
		return (List<T>) vmList;
	}

	/**
	 * Sets the vm list.
	 * 
	 * @param <T> the generic type
	 * @param vmList the new vm list
	 */
	protected <T extends Vm> void setVmList(List<T> vmList) {
		this.vmList = vmList;
	}

	/**
	 * Gets the cloudlet list.
	 * 
	 * @param <T> the generic type
	 * @return the cloudlet list
	 */
	@SuppressWarnings("unchecked")
	public <T extends Cloudlet> List<T> getCloudletList() {
		return (List<T>) cloudletList;
	}

	/**
	 * Sets the cloudlet list.
	 * 
	 * @param <T> the generic type
	 * @param cloudletList the new cloudlet list
	 */
	protected <T extends Cloudlet> void setCloudletList(List<T> cloudletList) {
		this.cloudletList = cloudletList;
	}

	/**
	 * Gets the cloudlet submitted list.
	 * 
	 * @param <T> the generic type
	 * @return the cloudlet submitted list
	 */
	@SuppressWarnings("unchecked")
	public <T extends Cloudlet> List<T> getCloudletSubmittedList() {
		return (List<T>) cloudletSubmittedList;
	}

	/**
	 * Sets the cloudlet submitted list.
	 * 
	 * @param <T> the generic type
	 * @param cloudletSubmittedList the new cloudlet submitted list
	 */
	protected <T extends Cloudlet> void setCloudletSubmittedList(List<T> cloudletSubmittedList) {
		this.cloudletSubmittedList = cloudletSubmittedList;
	}

	/**
	 * Gets the cloudlet received list.
	 * 
	 * @param <T> the generic type
	 * @return the cloudlet received list
	 */
	@SuppressWarnings("unchecked")
	public <T extends Cloudlet> List<T> getCloudletReceivedList() {
		return (List<T>) cloudletReceivedList;
	}

	/**
	 * Sets the cloudlet received list.
	 * 
	 * @param <T> the generic type
	 * @param cloudletReceivedList the new cloudlet received list
	 */
	protected <T extends Cloudlet> void setCloudletReceivedList(List<T> cloudletReceivedList) {
		this.cloudletReceivedList = cloudletReceivedList;
	}

	/**
	 * Gets the vm list.
	 * 
	 * @param <T> the generic type
	 * @return the vm list
	 */
	@SuppressWarnings("unchecked")
	public <T extends Vm> List<T> getVmsCreatedList() {
		return (List<T>) vmsCreatedList;
	}

	/**
	 * Sets the vm list.
	 * 
	 * @param <T> the generic type
	 * @param vmsCreatedList the vms created list
	 */
	protected <T extends Vm> void setVmsCreatedList(List<T> vmsCreatedList) {
		this.vmsCreatedList = vmsCreatedList;
	}

	/**
	 * Gets the vms requested.
	 * 
	 * @return the vms requested
	 */
	protected int getVmsRequested() {
		return vmsRequested;
	}

	/**
	 * Sets the vms requested.
	 * 
	 * @param vmsRequested the new vms requested
	 */
	protected void setVmsRequested(int vmsRequested) {
		this.vmsRequested = vmsRequested;
	}

	/**
	 * Gets the vms acks.
	 * 
	 * @return the vms acks
	 */
	protected int getVmsAcks() {
		return vmsAcks;
	}

	/**
	 * Sets the vms acks.
	 * 
	 * @param vmsAcks the new vms acks
	 */
	protected void setVmsAcks(int vmsAcks) {
		this.vmsAcks = vmsAcks;
	}

	/**
	 * Increment vms acks.
	 */
	protected void incrementVmsAcks() {
		vmsAcks++;
	}

	/**
	 * Gets the vms destroyed.
	 * 
	 * @return the vms destroyed
	 */
	protected int getVmsDestroyed() {
		return vmsDestroyed;
	}

	/**
	 * Sets the vms destroyed.
	 * 
	 * @param vmsDestroyed the new vms destroyed
	 */
	protected void setVmsDestroyed(int vmsDestroyed) {
		this.vmsDestroyed = vmsDestroyed;
	}

	/**
	 * Gets the datacenter ids list.
	 * 
	 * @return the datacenter ids list
	 */
	protected List<Integer> getDatacenterIdsList() {
		return datacenterIdsList;
	}

	/**
	 * Sets the datacenter ids list.
	 * 
	 * @param datacenterIdsList the new datacenter ids list
	 */
	protected void setDatacenterIdsList(List<Integer> datacenterIdsList) {
		this.datacenterIdsList = datacenterIdsList;
	}

	/**
	 * Gets the vms to datacenters map.
	 * 
	 * @return the vms to datacenters map
	 */
	protected Map<Integer, Integer> getVmsToDatacentersMap() {
		return vmsToDatacentersMap;
	}

	/**
	 * Sets the vms to datacenters map.
	 * 
	 * @param vmsToDatacentersMap the vms to datacenters map
	 */
	protected void setVmsToDatacentersMap(Map<Integer, Integer> vmsToDatacentersMap) {
		this.vmsToDatacentersMap = vmsToDatacentersMap;
	}

	/**
	 * Gets the datacenter characteristics list.
	 * 
	 * @return the datacenter characteristics list
	 */
	protected Map<Integer, DatacenterCharacteristics> getDatacenterCharacteristicsList() {
		return datacenterCharacteristicsList;
	}

	/**
	 * Sets the datacenter characteristics list.
	 * 
	 * @param datacenterCharacteristicsList the datacenter characteristics list
	 */
	protected void setDatacenterCharacteristicsList(
			Map<Integer, DatacenterCharacteristics> datacenterCharacteristicsList) {
		this.datacenterCharacteristicsList = datacenterCharacteristicsList;
	}

	/**
	 * Gets the datacenter requested ids list.
	 * 
	 * @return the datacenter requested ids list
	 */
	protected List<Integer> getDatacenterRequestedIdsList() {
		return datacenterRequestedIdsList;
	}

	/**
	 * Sets the datacenter requested ids list.
	 * 
	 * @param datacenterRequestedIdsList the new datacenter requested ids list
	 */
	protected void setDatacenterRequestedIdsList(List<Integer> datacenterRequestedIdsList) {
		this.datacenterRequestedIdsList = datacenterRequestedIdsList;
	}
	
/**BEGINNING OF OUR ALGORITHM**/
	
	public void bindCloutletToVmsSuffrage() {
		/** The cloudlet list. */
		

		
		int vmNum = vmList.size();
		int count = 1; // iteration number
		
		//FCFS algorithm
		/*int cloudletNum=cloudletList.size();
		int idx=0;
		for(int i=0;i<cloudletNum;i++){
			cloudletList.get(i).setVmId(vmList.get(idx).getId());
			idx=(idx+1)%vmNum;
		}*/
		
		// ready time for each machine initially to be 0 //
		Double[] readyTime = new Double[vmNum];
		for (int i = 0; i < readyTime.length; i++) {
			readyTime[i] = 0.0;
		}
		
		// list to record if the vm has been assigned //
		List<Double[]> vmSuffrage = new ArrayList<Double[]>();
		for (int i = 0; i < vmNum; i++) {
			// 1st is index of the cloudlet (can be double or int)
			// 2nd is corresponding suffrage, if the suffrage is 0.0 (also initial value(
			// then it means it has not been assigned yet
			Double[] vmIdAndSuffrage = {0.0, 0.0};
			vmSuffrage.add(i, vmIdAndSuffrage);
		}
		
		// initialize the 2-dimensional matrix with ready time + completion time //
		List<List<Double>> tasksVmsMatrix = create2DMatrix(cloudletList, vmList);
		int rowNum = tasksVmsMatrix.size();
		int colNum = tasksVmsMatrix.get(0).size();
		int colNumWithoutLastColumn = colNum -1;
		int targetRow = 0;
		int targetCol = 0;
		
		do {
			System.out.println("========================");
			System.out.println("This is start of iteration " + count);
			System.out.println();
			print2DArrayList(tasksVmsMatrix);
			System.out.println();

			// step 1: mark all machines as unassigned //
			resetSuffrage(vmSuffrage);
			System.out.println();
			
			Double min = 0.0; // initialize to store the min value //
			
			// nested loop
			for (int row = 0; row < tasksVmsMatrix.size()/5; row++) {
				// step 2: find the vm that gives the earliest time //
				int colIndexOfMin = 0;
				// assuming first one in a row as min //
				min = tasksVmsMatrix.get(row).get(colIndexOfMin);
				
				// get the cloudlet_id //
				Integer targetCloudletId = tasksVmsMatrix.get(row).get(colNumWithoutLastColumn).intValue();
				
				// array to store row info includes row, column and cloudlet_id //
				Integer[] rowAndMinColAndCloudletId = {row, colIndexOfMin, targetCloudletId};
				
				// array to store the each element (the expected total completion time)
				// in a row except last column (the cloudlet id) for later suffrage calculation
				Double[] currentRowWithoutLastCol = new Double[colNumWithoutLastColumn];
				
				// inner loop to get the smaller //
				for (int col = 0; col < colNumWithoutLastColumn; col++) {
					Double current = tasksVmsMatrix.get(row).get(col);
					// add current into the array //
					currentRowWithoutLastCol[col] = current;
					if (current < min) {
						colIndexOfMin = col;
						min = current;
					}
				}
				
				// step 3: calculate the suffrage time //
				Double currentSuffrage = calculateSuffrage(currentRowWithoutLastCol);
				
				// step 4: check if the machine is assigned //
				checkSuffrage(vmSuffrage, colIndexOfMin, targetCloudletId, currentSuffrage);
				System.out.println("\nEnd of scanning a row and current vm suffrage list is as below");
				printSuffrageList(vmSuffrage);
				System.out.println();
				
			}
			
			//Get greatest suffrage value
			Double greatest = 0.0;
			Double minimum = 10000000.0;
			Double row = 0.0;
			Double column = 0.0;
			for (int i=0; i<vmSuffrage.size(); i++) {
				if (vmSuffrage.get(i)[1] > greatest) {
					greatest = vmSuffrage.get(i)[1];
					row = vmSuffrage.get(i)[0];
				}
			}
			for (int i=0; i<tasksVmsMatrix.get(0).size() - 1; i++) {
				Double current = tasksVmsMatrix.get(row.intValue()).get(i);
				if (current < minimum) {
					minimum = current;
					column = (double)(i);
				}
			}
			System.out.println("Column value is " + column.toString());
			Integer targetCloudletId = tasksVmsMatrix.get(row.intValue()).get(colNumWithoutLastColumn).intValue();
			int colIndexOfMin = 0;
			//step 5: scan the vmSuffrage list to assign cloudlet to vm;
			cloudletList.get(targetCloudletId).setVmId(vmList.get(column.intValue()).getId());
			
			//step 6: add the cloudlet to sorted list
			Cloudlet cloudlet =cloudletList.get(targetCloudletId);
			sortList.add(cloudlet);	
			//step 8: update ready times
			assignAndUpdate(vmSuffrage, readyTime, tasksVmsMatrix, cloudletList, vmList, row.intValue(), column.intValue());
			//step 7: remove the row after the cloudlet has been assigned
			removeRow(tasksVmsMatrix, targetCloudletId);
			
			
			System.out.println("This is end of iteration " + count);
			System.out.println("========================");
			++count;
		} while (tasksVmsMatrix.size() > 0);
	}

	/* HELPER FUNCTIONS FOR SUFFRAGE ALGORITHM!! */
	private static List<List<Double>> create2DMatrix(List<? extends Cloudlet> cloudletList,List<? extends Vm> vmList){
		List<List<Double>> table= new ArrayList<List<Double>>();
		for(int i= 0; i<cloudletList.size(); i++){
			//original cloudlet id is added as last column
			Double originalCloudletId = (double) cloudletList.get(i).getCloudletId();
			//System.out.println("the original cloudlet id is:" + originalCloudletId);
			List<Double> temp=new ArrayList<Double>();
			
			for(int j = 0; j<vmList.size(); j++){
				Double load = cloudletList.get(i).getCloudletLength() / vmList.get(j).getMips();
				temp.add(load);
			}
			temp.add(originalCloudletId); 
			table.add(temp);
		}
		return table;
	}
	
	private static void print2DArrayList(List<List<Double>> table) {
		 String indent="           ";
		 System.out.println("The current required exceution time matrix is as below,with size of "+ table.size()+" by "+table.get(0).size());
		 //System.out.printf(indent);
		 for(int j=0;j<vmNum + 1;j++)
		 {
			 System.out.printf("    Vm"+j+indent);
		 }
		 System.out.println("cloudletNum");
		 for(int i=0;i<table.size();i++)
		 {
	 		//System.out.printf(indent);
	 		 String indent2="   "; 
			 for(int j=0;j<table.get(i).size();j++)
			 {
				 System.out.printf("%-15.5f", table.get(i).get(j));
				 System.out.printf(indent2);
			 }
			 System.out.printf("\n");
		 }
		 
		 
	 }

	/* reset the vm suffrage record */
	private static void resetSuffrage(List<Double[]> vmSuffrage) {
		for (int i = 0; i < vmSuffrage.size(); i++) {
			Double[] temp = {0.0, 0.0};
			vmSuffrage.set(i, temp);
		}
		System.out.println("Suffrage reset successfully and is shown as below");
		printSuffrageList(vmSuffrage);
	}
	
	/*calculate the difference between first and second smallest */
	private static Double calculateSuffrage(Double[] arr) {
		Double first, largest;
		first = largest = Double.MAX_VALUE;
		int arr_size = arr.length;
		
		/* There should be at least two elements */
		if (arr_size < 2) {
			System.out.println("Invalid Input");
			return 0.0;
		}
		
		for (int i = 0; i < arr_size; i++) {
			/*if current element is no greater than first then update both
			 * first and second; should use <= b/c there may be a tie
			*/
			if (arr[i] <= first) {
				first = arr[i];
				if (i == 0) {
					largest = first;
				}
			}
			 /* If arr[i] is greater than largest then update largest
			 */
			else if (arr[i] > largest && arr[i] > first)
				largest = arr[i];
		}
		if (largest == Double.MAX_VALUE)
			System.out.println("There is no largest element");
		else
			System.out.printf("The smallest is %.4f and largest is %.4f \n", first, largest);
		Double suffrage = largest - first;
		System.out.printf("The suffrage is %.4f \n", suffrage);
		return suffrage;
	}
	
	/* check if the vm has been assigned */
	private static void checkSuffrage(List<Double[]> vmSuffrage, Integer colIndexOfMin, Integer targetCloudletId, Double currentSuffrage) {
		// colIndexOfMin is actually the vm id, which is also the vm position in
		// the vmSuffrage array
		Double existingCloudletId = vmSuffrage.get(colIndexOfMin)[0];
		Double existingSuffrage = vmSuffrage.get(colIndexOfMin)[1];
		if (existingCloudletId != 0.0) {
			System.out.printf("Existing cloudlet id is %d and suffrage is %.4f \n", existingCloudletId.intValue(), existingSuffrage);
		} else 
	       System.out.printf("There is no cloudlet assigned to vm %d yet\n", colIndexOfMin);
		System.out.println("So the cloudlet " + targetCloudletId + " will try vm " + colIndexOfMin);
		
		
		// case when the cloudlet has not assigned or will be replaced
		if (existingSuffrage < currentSuffrage) {
			Double[] temp = { (double) targetCloudletId, currentSuffrage};
			System.out.printf("Current suffrage %.4f > existing one %.4f \n", currentSuffrage, existingSuffrage);
			// MUST use set to change the value!
			vmSuffrage.set(colIndexOfMin, temp);
			System.out.printf("Cloudlet %d is assigned to vm %d for now \n", targetCloudletId, colIndexOfMin);
		}
	}
	
	private static void assignAndUpdate(List<Double[]> vmSuffrage, Double[] readyTime, List<List<Double>> tasksVmsMatrix, List<? extends Cloudlet> cloudletList,List<? extends Vm> vmList, int row, int colIndexOfMin) {
		
		double min = tasksVmsMatrix.get(row).get(colIndexOfMin);
		
		//step 7: update ready time; 
		Double oldReadyTime = readyTime[colIndexOfMin];
		readyTime[colIndexOfMin] = min;
		System.out.printf("The ready time array is %s \n", Arrays.toString(readyTime));
		
		//step 8: update total time; update the cloudlet-vm matrix with current ready time
		updateTotalTimeMatrix(colIndexOfMin, oldReadyTime, readyTime, tasksVmsMatrix);

	}
	
	/* update the expected completion time */
	private static void updateTotalTimeMatrix(Integer colIndexOfMin, Double oldReadyTime, Double[] readyTime, List<List<Double>> tasksVmsMatrix) {
		// by adding current ready time to old corresponding expected completion time
		Double newReadyTime = readyTime[colIndexOfMin];
		Double readyTimeDifference = newReadyTime - oldReadyTime;
		for (int row = 0; row < tasksVmsMatrix.size(); row++) {
			Double oldTotalTime = tasksVmsMatrix.get(row).get(colIndexOfMin);
			Double newTotalTime = oldTotalTime + newReadyTime;
			tasksVmsMatrix.get(row).set(colIndexOfMin,  newTotalTime);
		}
		
		
	}
	
	/* print out the list of double[] */
	private static void printSuffrageList(List<Double[]> list) {
		for (int i = 0; i < list.size(); i++) {
			Integer storedCloudletId = list.get(i)[0].intValue();
			Double storedSuffrage = list.get(i)[1];
			System.out.printf("vm %d => cloudlet %d with suffrage %.4f \n", i, storedCloudletId, storedSuffrage);
		}
	}
	
	/* remove a row from the matrix based on cloudlet id */
	private static void removeRow(List<List<Double>> matrix, Integer cloudletId) {
		// this method is necessary since with removal of rows, the row number
		// will change such that we can not use matrix.remove(cloudletId);
		// instead we need to use the cloudlet to find which row to be removed;
		int rowNum = matrix.size();
		int colNum = matrix.get(0).size();
		// System.out.println("row number is " + rowNum);
		// System.out.println("column number is " + colNum);
		System.out.println("Looking for cloudlet " + cloudletId + " in the matrix ......");
		for (int row = 0; row < rowNum; row++) {
			Double target = matrix.get(row).get(colNum - 1);
			// System.out.println("current cloudlet id is " + target);
			if (target.intValue() == cloudletId) {
				System.out.println("Found! And row " + row + " will be removed from the matrix");
				matrix.remove(row);
				break;
			}
		}
	}

}
