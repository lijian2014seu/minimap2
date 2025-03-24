import java.io.*;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.UUID;
import java.math.*;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;
import redis.clients.jedis.Jedis;
import PreProcess.FileOperator;
import scala.Tuple2;
import scala.collection.JavaConverters;


public class MinimapSpark {
	static String ip;
	static String inputName;
	static String databaseName;
	static String outputName;
	static String logPath;
	static int inputNum;
	static int dbNum;
	static int backupNum;
	static int nodeNum;
	static String command = "";
	static boolean isReady = false;

	public static int getTargetIndex(String x, int n) {
		int idx = 0;
		for(int i = 0; i < n; ++i) {
			idx = x.indexOf("\t", idx) + 1;
		}
		return idx;
	}

	public static void main(String[] args) throws IOException {
		JavaRDD<String> filereads = null;
		JavaRDD<String> wholeFile = null;
		String commands = "MinimapSpark: ";
		for(int i = 0 ; i < args.length ; i++) {
			commands+=" "+args[i];
		}
		System.out.println(commands);
		String appName=new String();
		for (int i = 0; i < args.length; i++) {
			System.out.println(args[i]);
			switch (args[i].toUpperCase()) {
				case "-MASTER":
					ip=args[++i];
					System.out.println(ip);
					break;
				case "-MINIMAPS":
					break;
				case "-H":
					helpinfo();
					break;
				case "-N":
					nodeNum = Integer.parseInt(args[++i]);
					System.out.println(nodeNum);
					break;
				case "-I":
					inputName = args[++i];
					System.out.println(inputName);
					break;
				case "-NI":
					inputNum = Integer.parseInt(args[++i]);
					System.out.println(inputNum);
					break;
				case "-D":
					databaseName = args[++i];
					System.out.println(databaseName);
					break;
				case "-ND":
					dbNum = Integer.parseInt(args[++i]);
					System.out.println(dbNum);
					break;
				case "-NB":
					backupNum = Integer.parseInt(args[++i]);
					System.out.println(backupNum);
					break;
				case "-O":
					outputName = args[++i];
					System.out.println(outputName);
					break;
				case "-L":
					logPath = args[++i];
					System.out.println(logPath);
					break;
				case "-mini2":
					command= args[++i]+" ";
					break;
			}
		}
		SparkConf conf = new SparkConf().setAppName(appName);
		System.err.println(Integer.toString(dbNum) + " " + Integer.toString(inputNum));
		JavaSparkContext spark = new JavaSparkContext(conf);
		SparkSession ss = SparkSession.builder().appName(appName).master("yarn").getOrCreate();
		String randomString = UUID.randomUUID().toString().replaceAll("-", "");
		File inputFile = new File(inputName);
		BufferedReader bd = new BufferedReader(new FileReader(inputFile));
		String tempfileName[] = new String[inputNum];
		File tempfile[] = new File[inputNum];
		BufferedWriter writer[] = new BufferedWriter[inputNum];
		if (inputName.substring(inputName.lastIndexOf(".") + 1).equals("gz")){
			System.out.println("This is a fastq gz file");
			wholeFile = spark.textFile(FileOperator.gzfile(inputName,ip));
			System.out.println("file partitions num: "+wholeFile.getNumPartitions());
			filereads = Readfastq(wholeFile);
		}
		else if(inputName.substring(inputName.lastIndexOf(".") + 1).equals("fa")||inputName.substring(inputName.lastIndexOf(".") + 1).equals("fasta")){
			System.out.println("This is a fasta file");
			for(int i = 0; i < inputNum; ++i) {
				tempfile[i] = File.createTempFile("TempQuery_", ".fa");
				tempfile[i].deleteOnExit();
				writer[i] = new BufferedWriter(new FileWriter(tempfile[i]));
			}
			String l;
			// int numseqs = 0;
			int idx = 0;
			long size = 0; 
			while((l = bd.readLine()) != null) {
				/*if(line.length() > 0 && line.charAt(0) == '>') {
					numseqs++;
				}*/
				if(l.length() > 0 && l.charAt(0) == '>') {
					writer[idx].flush();
					if(size + tempfile[idx].length() >= Math.ceil(inputFile.length() * 1.0 * (idx + 1) / inputNum)) {
						size += tempfile[idx].length();
						idx++;
					}
				}
				writer[idx].write(l);
				writer[idx].newLine();
			}
			bd.close();
			for(int i = 0; i < inputNum; ++i) {
				writer[i].flush();
				writer[i].close();
			}
			System.out.println("partition finish");
		}
		else if (inputName.substring(inputName.lastIndexOf(".") + 1).equals("fastq")) {
			System.out.println("This is a fastq file");
			for(int i = 0; i < inputNum; ++i) {
				tempfile[i] = File.createTempFile("TempQuery_", ".fastq");
				tempfile[i].deleteOnExit();
				writer[i] = new BufferedWriter(new FileWriter(tempfile[i]));
				System.out.println(tempfile[i].getAbsolutePath());
			}
			String line;
			// int numseqs = 0;
			ArrayList<String> wholefile = new ArrayList<String>();
			while((line = bd.readLine()) != null) {
				wholefile.add(line);
			}
			int idx = 0;
			for(String l: wholefile) {
				if(l.length() > 0 && l.charAt(0) == '@') {
					writer[idx].flush();
					if(tempfile[idx].length() >= Math.ceil(inputFile.length() * 1.0 * (idx + 1)/ inputNum)) {
						idx++;
					}
				}
				writer[idx].write(l);
				writer[idx].newLine();
			}
			bd.close();
			for(int i = 0; i < inputNum; ++i) {
				writer[i].flush();
				writer[i].close();
			}
		}
		for(int j = 0; j < inputNum; ++j) {
			spark.addFile(tempfile[j].getAbsolutePath());
		}
		if (command == "") {
			command = "/opt/share/minimap2/minimap2 -x asm10 -k 15 -w 10 -f 100 -K 1g";
		}
		Jedis jedis = new Jedis("redis", 6379);
		System.err.println(Integer.toString(dbNum) + " " + Integer.toString(inputNum));
		for(int i = 0; i < dbNum; ++i) {
			for(int j = 0; j < inputNum; ++j) {
				System.out.println("Jedis push " + randomString + ":" +  databaseName + "_" + Integer.toString(i) + ".mmi" + tempfile[j].getAbsolutePath());	
				jedis.lpush(randomString + ":" + databaseName + "_" + Integer.toString(i) + ".mmi", tempfile[j].getAbsolutePath().substring(tempfile[j].getAbsolutePath().lastIndexOf("/") + 1));		
			}
		}
		jedis.close();
		ArrayList<ArrayList<String>> dbList = new ArrayList<ArrayList<String>>();
		for(int i = 0; i < nodeNum; ++i) {
			dbList.add(new ArrayList<String>());
		}
		int idx = 0;
		for(int i = 0; i < backupNum; ++i) {
			for(int j = 0; j < dbNum; ++j) {
				if(i % 2 == 0) {
					dbList.get(idx).add(databaseName + "_" + Integer.toString(j) + ".mmi");
				} else {
					dbList.get(idx).add(databaseName + "_" + Integer.toString(dbNum - 1 - j) + ".mmi");
				}
				idx = (idx + 1) % nodeNum;
			}
			System.out.println();
		}
		for(int i = 0; i < nodeNum; ++i) {
			for(int j = 0; j < dbList.get(i).size(); ++j) {
				System.out.print(dbList.get(i).get(j) + " ");
			}
			System.out.println();
		}
		Broadcast<String> log = spark.broadcast(logPath);
		Broadcast<String> uuid = spark.broadcast(randomString);
		Broadcast<String> cmd = spark.broadcast(command);
		long startTime = System.currentTimeMillis();
		JavaRDD<String> result = spark.parallelize(dbList, nodeNum).mapPartitions(x -> {
				ArrayList<String> res = new ArrayList<String>();
				ArrayList<String> dbNames = x.next();
				long ministart = System.currentTimeMillis();
				Jedis exejedis = new Jedis("redis", 6379);
				int cnt = 0;
				for(String dbName: dbNames) {
					String queryName;
					System.out.println(dbName);
					System.out.println(uuid.value());
					while((queryName = exejedis.rpop(uuid.value() + ":" + dbName)) != null) {
						cnt++;
						queryName = SparkFiles.get(queryName);
						System.out.println(cmd.value() + " " + dbName + " " + queryName);
						final Process p = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", cmd.value() + " " + dbName + " " + queryName});
						Runtime.getRuntime().addShutdownHook(new Thread() {
							@Override
							public void run() {
								p.destroy();
							}
						});
						try {
							new Thread() {  
								public void run() {  
								BufferedReader br1 = new BufferedReader(new InputStreamReader(p.getInputStream()));  
									try {  
										String line1 = null;  
										while ((line1 = br1.readLine()) != null) {  
											res.add(line1); 
										}  
									} catch (IOException e) {  
										e.printStackTrace();  
									}  
									finally{  
										try {  
										p.getInputStream().close();  
										} catch (IOException e) {  
											e.printStackTrace();  
										}  
									}  
								}  
							}.start();  
							final String dName = dbName, qName = queryName;					
							new Thread() {   
								public void  run() {   
								BufferedReader br2 = new  BufferedReader(new  InputStreamReader(p.getErrorStream()));   
									try {   
										String line2 = null ; 
										String logPath = log.value();
										File logFile = new File(logPath + dName.substring(dName.lastIndexOf("/") + 1, dName.lastIndexOf(".")) + "_" + qName.substring(qName.lastIndexOf("/") + 1) + ".log"); 
										System.out.println(logFile.getAbsolutePath());
										if(!logFile.getParentFile().exists()) {
											logFile.getParentFile().mkdirs();
										}
										logFile.createNewFile();
										BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFile));
										while ((line2 = br2.readLine()) !=  null) {   
											System.out.println(line2);
											logWriter.write(line2);
											logWriter.newLine();
										}   
										logWriter.flush();
										logWriter.close();
									} catch (IOException e) {   
											e.printStackTrace();  
									}   
									finally{  
										try {  
											p.getErrorStream().close();  
										} catch (IOException e) {  
											e.printStackTrace();  
										}  
									}  
								}   
							}.start();    
															
							p.waitFor();  
							p.destroy();   
						} catch (Exception e) {  
							try {  
								p.getErrorStream().close();  
								p.getInputStream().close();
							}  
							catch(Exception ee){}  
						}  
						
					}
				}
				exejedis.close();
				long miniend = System.currentTimeMillis();
				System.out.println("processed " + cnt + " tasks");
				System.out.println("minimap process time: " + (miniend - ministart) + " ms");
				System.out.println("return to MinimapS main");
				return res.iterator();
			});

		List<String> results = result.collect();
		System.out.println("outputpath:" + outputName);
		File output = new File(outputName);
		BufferedWriter bwout = new BufferedWriter(new FileWriter(output));
		bwout.newLine();
		for(String lines: results) {
			bwout.write(lines);
			bwout.newLine();
		}
		bwout.flush();
		bwout.close();
		long endTime = System.currentTimeMillis();
		System.out.println("minimap process time: " + (endTime - startTime) + " ms");
		spark.close();
	
	}

	private static JavaRDD<String> Readfastq(JavaRDD<String> fastqReads){
		return fastqReads.zipWithIndex().mapToPair(x -> {
			Tuple2<Long, Tuple2<Integer, String>> tp = new Tuple2<Long, Tuple2<Integer, String>>(x._2/4, new Tuple2<Integer, String>((int)(x._2%4), x._1));
			return tp;
		}).groupByKey().map(x -> {
			String rec=new String();
			String[] lines = new String[4];
			for(Tuple2<Integer, String> tp : x._2) {
				lines[tp._1]=tp._2;
			}
			if(lines[0]==null || lines[1]==null || lines[3]==null) {
				System.out.println("file format is wrong");
				return null;
			}
			if(lines[0].charAt(0)=='@') {
				int end = lines[0].indexOf(" ");
				if(end<0) end = lines[0].length();
				rec = lines[0].substring(0,end) + " " + lines[1] + " " + lines[3];
			}
			return rec;
		});
	}

	private static JavaRDD<String> Readfasta(JavaRDD<String> fastaReads){
		return fastaReads.zipWithIndex().mapToPair(x -> {
			Tuple2<Long, Tuple2<Integer, String>> tp = new Tuple2<Long, Tuple2<Integer, String>>(x._2/2, new Tuple2<Integer, String>((int)(x._2%2), x._1));
			return tp;
		}).groupByKey().map(x -> {
			String rec="";
			String[] lines = new String[2];
			for(Tuple2<Integer, String> tp : x._2) {
				lines[tp._1]=tp._2;
			}
			if(lines[0]==null || lines[1]==null) {
				return null;
			}
			if(lines[0].charAt(0)=='>') {
				int end = lines[0].indexOf(" ");
				if(end<0) end = lines[0].length();
				rec = lines[0].substring(0,end) + " " + lines[1];
			}
//			System.out.println("rec:"+rec);*/
			return rec;
		});
	}

	private static void helpinfo(){
		System.out.println("usage sample:\tspark-submit --class MinimapS --master [masterip] --executor-memory 10G --dirver-memory 2G MinimapS.jar minimaps -I [inputfile] -O out.sam");
		System.out.println("");
		System.out.println("MASTER:\t\tIdentify Spark Master local, yarn or ip of spark standalone master");
		System.out.println("");
		System.out.println("inputfile:\tInput reads ");
	}

}
