package com.mvc.sample;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
/**
 * The class Main has the main purpose of generating Secondary Dense Index for large chunk of data
 * and then printing all records of a specific age as per the input provided by the user.
 * 
 * @author Leila Abdollahi Vayghan, Armaghan Sikandar & Amarpreet Singh
 **/

public class Main {
	//Declaring variables for the program
	static int[] blockLimiter = new int[100];
	static HashMap<Integer, ArrayList<Integer>> locMap;
	static long bytesCounterGlobal = 0;
	static int globalWriteCounter = 0;
	static final int RECORD_SIZE = 100; // 100 bytes
	static int BUFFER_SIZE = 0;
	static int WRITE_BATCH_SIZE = 2600;


	//========================= PATHS (Armagha) ==================================

	//Setting up path for various Directories

	static final String DEFAULT_PATH = "D:/ADBProject/";
	static final String INDEX_PATH = "D:/ADBProject/index.txt";
	static final String INDEX_DIRECTORY = "D:/ADBProject/index/";
	static final String default_directory = "D:/ADBProject/person_fnl.txt";

	//static final String RESULT_PATH = "D:/ADBProject/search_result.txt";
	//static final String STAT_PATH = "D:/ADBProject/statistics.txt"; 
	//static final String default_directory = "D:/ADBProject/person.txt";
	//static final String default_directory = "D:/ADBProject/person_100_MB.txt";
	//static final String default_directory = "D:/ADBProject/person_1_GB.txt";
	//static final String default_directory = "D:/ADBProject/person_5_GB.txt";
	//static final String default_directory = "D:/ADBProject/person_10_GB.txt";
	//static final String default_directory = "C:/users/zee/person_20_GB.txt"; 

	//========================= PATHS (Amarpreet) ==================================

	//Setting up path for various Directories
	//static final String DEFAULT_PATH = "/home/amarpreet911/Documents/MyWorkSpace/File_by_armaghan/FileIndexer_20/";
	//static final String RESULT_PATH = "/home/amarpreet911/Documents/MyWorkSpace/File_by_armaghan/FileIndexer_20/search_result.txt";
	//static final String STAT_PATH = "/home/amarpreet911/Documents/MyWorkSpace/File_by_armaghan/FileIndexer_20/statistics.txt"; 
	//static final String INDEX_PATH = "/home/amarpreet911/Documents/MyWorkSpace/File_by_armaghan/FileIndexer_20/index.txt";
	//static final String INDEX_DIRECTORY = "/home/amarpreet911/Documents/MyWorkSpace/File_by_armaghan/FileIndexer_20/index/";
	//static final String default_directory = "/home/amarpreet911/Documents/MyWorkSpace/File_by_armaghan/FileIndexer_20/person_fnl.txt";

	public static void main(String[] args) throws IOException, ClassNotFoundException{

		Runtime runtime = Runtime.getRuntime();	
		long availableMemory = runtime.freeMemory();
		System.out.println("Available Memory : " + (availableMemory/1024)/1024 + " MB");

		if(availableMemory > 3){
			WRITE_BATCH_SIZE = 1800;	// No. of records to write as soon as the array reaches a size of "WRITE_BATCH_SIZE"
		}else{
			WRITE_BATCH_SIZE = 350;
		}


		File file = new File(default_directory);
		if(file.length()<=BUFFER_SIZE){
			BUFFER_SIZE = (int) file.length();
		}

		//		System.out.println("BUFFER_SIZE : " + BUFFER_SIZE);
		//		System.out.println("WRITE_BATCH_SIZE : " + WRITE_BATCH_SIZE);
		//		System.out.println("File Length : " + file.length());
		//		System.out.println("Record count: " + file.length()/100);
		//		System.out.println("Average age group count: " + (file.length()/100)/81);
		//		System.out.println("========================================================");
		//



		//inputFunction();
		//calculateAvgIncome();

	}

	/**
	 *  The function inputFunction takes age input from the user and performs various 
	 *  other activities for performing age parsing
	 */
	private static void inputFunction()  throws IOException, ClassNotFoundException {
		//new File("D:/ADBProject").mkdir();
		//new File("/home/amarpreet911/Documents/MyWorkSpace/File_by_armaghan/FileIndexer_20/").mkdir();
		while(true){
			System.out.println("\n\nEnter age or [0] to create index: ");
			@SuppressWarnings("resource")
			int query = new Scanner(System.in).nextInt();

			// Checking up the condition whether index exists or not and taking the necessary action after that. 
			if(!isIndexGenerated() || query == 0){
				createIndex();
			}

			if(query>=18 && query<=99){
				readIndexAndExecuteQuery(query, false);
			}else{
				if(query!=0)
					System.out.println("ERROR: Entered age not found! , Please try again.");
			}

		}
	}

	/**
	 * The function isIndexGenerated checks whether the index is generated or not
	 * @return boolean showing whether the index file has already been generated or not.
	 */
	private static boolean isIndexGenerated(){
		if(new File(INDEX_DIRECTORY).listFiles().length > 0)
			return true;
		else
			return false;
	}

	/**
	 * The function getIndexBlockSize helps us calculate the block size used by our index
	 * @return block size of the index file
	 */
	private static long getIndexBlockSize(){
		File index_dir = new File(INDEX_DIRECTORY);
		File[] files = index_dir.listFiles();
		long blockSize = 0;
		for (File indexFile : files) {
			if(indexFile.isFile()) {
				blockSize += indexFile.length();
			}
		}

		blockSize = (blockSize)/4000;

		return blockSize;
	}

	/**
	 * This function creates the index for us and also helps to calculate the corresponding time involved with it
	 * @throws IOException
	 */
	private static void createIndex() throws IOException {
		System.out.println("Creating index....");
		Timer timer = new Timer();
		timer.startTimer();
		File file = new File(default_directory);

		int locNo = 0;
		int writeIOs = 0;
		int readIOs = 0;

		locMap = new HashMap<Integer, ArrayList<Integer>>();
		FileInputStream fis = new FileInputStream(file);
		try {
			byte[] buffer = new byte[BUFFER_SIZE];
			int remaining = buffer.length;   // remaining is the number of bytes to read to fill the buffer
			int age;
			int read;
			// Here we start and infinite loop to read and write locations of an corresponding age to its index
			while (true) {
				read = fis.read(buffer, buffer.length - remaining, remaining);
				bytesCounterGlobal +=read;
				readIOs++;
				if (read >= 0) { // some bytes were read
					remaining -= read;
					if (remaining == 0) { // the buffer is full
						remaining = buffer.length;

						for(int i=0; i<=remaining-RECORD_SIZE; i=i+RECORD_SIZE){
							locNo++;
							age = getAge(buffer,i);
							if(!locMap.containsKey(age)){
								locMap.put(age, new ArrayList<Integer>());
							}
							locMap.get(age).add(locNo);
							if(locMap.get(age).size() >= WRITE_BATCH_SIZE){
								writeLocs(age, locMap.get(age));
								writeIOs++;
								locMap.remove(age);  // flush the newly written locations
							}
						}
						//System.out.println("AGE: "+age + "  ---  Location: " + locOffset);
					}
				} else {
					// the end of the file was reached. If some bytes are in the buffer
					// they are written to the last output file
					if (remaining < buffer.length) {
						//System.out.println("Remaining Bytes: "+ remaining);
						//System.out.println("Remaining Buffer size: "+ buffer.length);
						for(int i=0; i<=(buffer.length-remaining)-RECORD_SIZE; i=i+RECORD_SIZE){
							locNo++;
							age = getAge(buffer,i);
							//System.out.println("AGE: "+age + "  ---  Location: " + locNo);
							//System.out.println("AGE: " + new String(buffer) );

							if(!locMap.containsKey(age)){
								locMap.put(age, new ArrayList<Integer>());
							}

							locMap.get(age).add(locNo); //Adding up location number of a corresponding age to the index

							if(locMap.get(age).size() >= WRITE_BATCH_SIZE){
								writeLocs(age, locMap.get(age));
								writeIOs++;
								locMap.remove(age); 
							}
						}
					}
					break;
				}
			}
		}

		// In finally block we print certain required results as well as close the file input stream
		finally {
			fis.close();
			globalWriteCounter = writeIOs;
			writeRemainingLocs();
			locMap = null;
			System.out.println("Created index in: " + timer.getTimeElapsed(timer.stopTimer()));
			System.out.println("Blockes used: " + getIndexBlockSize());
			System.out.println("Disk I/O's: " + (readIOs + writeIOs));
		}
	}


	/**
	 * Reads locations from the index as per the parameter age
	 * @param age required age
	 * @return Record Count
	 */
	private static int readIndexAndExecuteQuery(int age, boolean calculateIncome) throws IOException, ClassNotFoundException {
		System.out.println("Searching....");
		Timer timer = new Timer();
		timer.startTimer();

		long totalIncome = 0;

		File file = new File(INDEX_DIRECTORY + age + ".txt");

		BUFFER_SIZE = 4000;
		if(file.exists() && file.length() <= BUFFER_SIZE){
			BUFFER_SIZE = (int) file.length();
		}	

		//System.out.println("File size: "+ file.length());
		//System.out.println("Index BUFFER_SIZE: "+ BUFFER_SIZE);

		//int readIOs = -1;
		int recordCount = 0;
		FileInputStream fis = new FileInputStream(file);
		try {
			byte[] buffer = new byte[BUFFER_SIZE];
			// remaining is the number of bytes to read to fill the buffer
			int remaining = buffer.length;  
			int read;

			while (true) {
				read = fis.read(buffer, buffer.length - remaining, remaining);
				//readIOs++;
				// If some bytes were read then perform the given operations and parseAge
				if (read >= 0) { 
					remaining -= read;
					// the buffer is full
					if (remaining == 0) { 
						remaining = buffer.length;
						//Splits up the locations via "," separator
						String[] locs = new String(buffer).trim().split(",");
						totalIncome += parseAges(age, locs, calculateIncome);
						recordCount += locs.length;
						locs = null;
					}
				} else {
					if (remaining < buffer.length) {
						byte[] remainingBuffer = new byte[buffer.length-remaining];
						for(int i = 0; i<buffer.length-remaining; i++){
							remainingBuffer[i] = buffer[i];
							//System.out.println(new String(remainingBuffer));
						}
						String[] locs = new String(remainingBuffer).trim().split(",");
						totalIncome += parseAges(age, locs, calculateIncome);
						recordCount += locs.length;
						locs = null;
					}
					break;
				}
			}
		}
		//The Finally block release the resources for file input stream and prints the required results
		finally {
			fis.close();
			//System.out.println("readIOs: "+ readIOs);
			String result = "\nFound " + recordCount +" records in : " + timer.getTimeElapsed(timer.stopTimer());
			System.out.println(result);
			//			print(result);

			System.out.println(age + " -- AVERAGE income : " + (totalIncome/recordCount));
		}

		return recordCount;
	}

	/**
	 * Parses the records for a particular age value
	 * @param age required age
	 * @param locations location of the record
	 */
	private static long parseAges(int age, String[] locations, boolean calculateIncome) throws IOException {
		File file = new File(default_directory);
		FileInputStream fis = new FileInputStream(file);
		long recordOffset;
		long newOffset;
		long totalIncome = 0;
		long count = 0;
		try {
			byte[] buffer = new byte[RECORD_SIZE];
			int remaining = buffer.length;

			for(String locPosition: locations) {
				if(locPosition.length()>0){
					recordOffset  = getLocOffset(Long.parseLong(locPosition.trim()));
					newOffset = recordOffset - fis.getChannel().position();

					if(newOffset>-1){
						//System.out.println(""+newOffset);
						fis.skip(newOffset);

						int read = fis.read(buffer, 0, RECORD_SIZE);
						// If some bytes were read
						if (read >= 0) { 
							remaining -= read;
							// If the buffer is full store buffer length to remaining and increment the block number
							if (remaining == 0) {
								remaining = buffer.length;
								count++;
								//int parsedAge  = getAge(buffer);
								//System.out.println(new String(buffer));
								//writeRecord(new String(buffer));
								if(calculateIncome){
									totalIncome += getIncome(buffer);
									long income = getIncome(buffer);
									//									System.out.println("Income : " + income);
								}
							}
						}else { 
							if (remaining < buffer.length) {
							}
							break;
						}
					}
				}
			}
		}
		finally {
			fis.close();
			//			System.out.println(age + " -- AVERAGE income : " + (totalIncome/(count-1)));
		}


		return totalIncome;

	}

	/**
	 * This function helps us calculate the Average Income for different age groups
	 * @throws NumberFormatException
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	private static void calculateAvgIncome() throws NumberFormatException, ClassNotFoundException, IOException{
		String indexDir = INDEX_DIRECTORY;
		File dir = new File(indexDir);
		File[] files = dir.listFiles();
		for (File file : files) {
			if(file.isFile()) {
				String fileName = file.getName().substring(0,2);
				readIndexAndExecuteQuery(Integer.parseInt(fileName), true);
			}
		}
	}

	/**
	 * Writes locations for ages in index
	 */
	public static void writeRemainingLocs() {
		for(Integer age:locMap.keySet()) {
			// If there are more locations remaining then continue writing them for the age
			if(locMap.get(age).size()>0){ 
				writeLocs(age,locMap.get(age));
				globalWriteCounter ++;
			}
		}
	}

	/**
	 * Writes Locations for us in the index
	 * Corresponding to each age we form a different file for storing locations
	 * @param age required age
	 * @param locations corresponding locations
	 */
	public static void writeLocs(int age, ArrayList<Integer> locations) {
		try {
			File file = new File(DEFAULT_PATH + "index/"+age+".txt");
			if (!file.exists()) {
				file.createNewFile();
			}
			BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));

			for(Integer loc: locations){
				String line = loc + ",";

				blockLimiter[age] +=line.length();
				if(blockLimiter[age]>=3992){
					for(int i = blockLimiter[age]; i<4000; i++){
						line += " ";
					}
					blockLimiter[age]=0;
				}

				bw.write(line);
				bw.flush();
			}
			bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	//	/**
	//	 *  The function writeRecord function writes records on a specified directory
	//	 * @param recod the required record
	//	 */
	//	private static void writeRecord(String recod) {
	//		try {
	//			//File file = new File(DEFAULT_PATH + "index/"+age+".txt");
	//			//if (!file.exists()) {
	//			//	file.createNewFile();
	//			//}
	//			BufferedWriter bw = new BufferedWriter(new FileWriter(DEFAULT_PATH + "/search_result.txt", true));
	//			bw.write(recod);
	//			bw.newLine();
	//			bw.flush();
	//			bw.close();
	//		} catch (IOException e) {
	//			e.printStackTrace();
	//		}
	//	}



	/**
	 * gets the income of an inout buffer
	 *  @param buffer buffer
	    @throws IOException
	    @return income
	 */
	private static Integer getIncome(byte[] buffer) throws IOException {
		String income = "";
		for(int i = 41; i<51; i++){
			income+= (char)(buffer[i] & 0xFF);
		}
		return Integer.parseInt(income);
	}
	//	/** The function getAge returns age of records present in the buffer 
	//    @param buffer buffer
	//    @throws IOException
	//	@return age
	//	 */
	//	private static Integer getAge(byte[] buffer) throws IOException {
	//		char a = (char)(buffer[39] & 0xFF);
	//		char b = (char)(buffer[40] & 0xFF);
	//		return Integer.parseInt(""+a+b);
	//	}

	/** Returns age of records present in the buffer 
    @param buffer buffer
    @throws IOException
	@return age
	 */
	private static Integer getAge(byte[] buffer,int i) throws IOException {
		char a = (char)(buffer[i+39] & 0xFF);
		char b = (char)(buffer[i+40] & 0xFF);
		return Integer.parseInt(""+a+b);
	}
	//	/**
	//	 * return location of record depending on the block number and the buffer size
	//	 * @param blockNumber
	//	 * @param i
	//	 * @return location
	//	 */
	//	private static long getLocOffset(long blockNumber,int i){
	//		return (blockNumber * BUFFER_SIZE) - BUFFER_SIZE + i ;
	//	}

	/**
	 * location of record depending on the record number
	 * @param recordNumber
	 * @return location
	 */
	private static long getLocOffset(long recordNumber){
		return (recordNumber * RECORD_SIZE) - RECORD_SIZE;
	}


}