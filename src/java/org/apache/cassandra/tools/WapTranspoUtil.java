package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * 
 * @author barala
 *
 * This is Util class which contains all helper functions 
 */
public class WapTranspoUtil {

    /**
     * To generate absolute path of all the sstables for given directory
     *
     * @param String path of dirtectory
     * @return list of abssolute path of all the sstables
     */
    public static List<String> generateAbsolutePathOfAllSSTables(String directoryPath) {
	List<String> absolutePathOfAllSSTables = new ArrayList<String>();
	File dir = new File(directoryPath);
	if (dir.isDirectory()) {
	    File[] directoryListing = dir.listFiles();
	    for (File file : directoryListing) {
		if (file.getName().endsWith("Data.db")) {
		    absolutePathOfAllSSTables.add(file.getAbsolutePath());
		}
	    }
	    return absolutePathOfAllSSTables;
	}
	absolutePathOfAllSSTables.add(directoryPath);
	return absolutePathOfAllSSTables;
    }
    
    
    /**
     * To count total sstables for given directory
     * 
     * @param String data directory path
     */
    
    public static int countTotalSSTables(String directoryPath){
	return generateAbsolutePathOfAllSSTables(directoryPath).size();
    }
    
    /**
     * To generate the directory
     *
     * @param String path to directory
     */
    public static File createDirectory(String pathToDirectory) {
	File modifiedDataDirectory = new File(pathToDirectory);

	if (!modifiedDataDirectory.isDirectory()) {
	    if (modifiedDataDirectory.mkdirs()) {
	    } else {
		System.err.println("unable to create direcotry for new generated sstables.");
		throw new IllegalArgumentException("don't have permission to create directory");
	    }
	}
	return modifiedDataDirectory;
    }

    
    /**
     * To generate file
     *
     * @param String path of file
     */
    public static File createFile(String filePath) {
	File file = new File(filePath);
	try {
	    file.createNewFile();
	} catch (IOException e) {
	    System.err.println("Unable to create log file");
	    e.printStackTrace();
	}
	return file;
    }
    
    
    /**
    *
    * @param path
    * @return part of directory after "KS" i.e '/ks/cf'
    */
   public static String getDataDirectoryName(String SSTableFileName) {
	List<String> allDirectories = Arrays.asList(SSTableFileName.split("/"));
	if (allDirectories.size() > 1) {
	    return allDirectories.get(allDirectories.size() - 1);
	}
	return allDirectories.get(0);
   }

   /**
    * this function checks that first PK of given decorated key contains
    * tenantId
    *
    * @param tenantId
    * @param DecoratedKey
    * @return whether above decorated key contains above tenantId as a first
    *         partition key or not
    */
   public static boolean doesContainTargetKey(String tenantId, DecoratedKey decoratedKey) {
	return firskPk(decoratedKey).equals(tenantId);
   }

   /**
    * To get the first partition key as a string
    *
    * @param Decoratedkey
    * @return first partition key as a string
    */
   public static String firskPk(DecoratedKey decoratedKey) {
	ByteBuffer firstPkBf = ByteBufferUtil.readBytesWithShortLength(decoratedKey.getKey().duplicate());
	return UTF8Type.instance.getString(firstPkBf);
   }
   
   
   /**
    * To print progress after each SSTable processed
    * 
    * @param int total no of SSTables
    * @param int total no of processed SSTables
    * @param int total failure of SSTables 
    */
   public static void printCurrentProgress(PrintStream logStdOut,int totalProcessedSSTables, int totalSSTableForgivenCF, int totalFailureSSTables){
	printAndWriteToFile(logStdOut, "==========================================================================================");
	printAndWriteToFile(logStdOut, "Total Processed", "Total SSTables","total failure", "total %");
	printAndWriteToFile(logStdOut, totalProcessedSSTables, totalSSTableForgivenCF, totalFailureSSTables, ((totalProcessedSSTables+totalFailureSSTables)*100)/totalSSTableForgivenCF);
	printAndWriteToFile(logStdOut, "==========================================================================================");

   }
   
   public static void printAndWriteToFile(PrintStream logOut, Object...msg ) {
	String format = "%20s | %20s | %20s | %20s |";
	System.out.println(String.format(format, msg));
	logOut.println(String.format(format, msg));
   }

   
}
