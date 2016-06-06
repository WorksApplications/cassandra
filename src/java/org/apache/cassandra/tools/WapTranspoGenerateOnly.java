package org.apache.cassandra.tools;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.BufferCounterCell;
import org.apache.cassandra.db.BufferExpiringCell;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CounterCell;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletedCell;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.ExpiringCell;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.composites.Composites;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import jdk.nashorn.internal.runtime.Context.ThrowErrorManager;

/**
 *
 * @author barala
 * @purpose read SStables, filter for given tenantId, modify first Pk(String)
 *          and Generate SSTables
 * 
 *
 *          CMD arguments 
 *          ----------------- 
 *          (1) tenantId 
 *          (2) replaceWith 
 *          (3) path of sstable
 *          (4) target keyspace
 *          (5) mode
 *		-> Generate only
 *		-> Transfer only
 *		-> Both          
 *
 *
 *
 *          Filter and Modification 
 *          ---------------------- 
 *          readSSTable function to read SStable
 *          serialize row, store modified row in sorted map //(because of murmur3 hashing algorithm)
 *
 *
 *          Generate Corresponding SSTables 
 *          -------------------------------
 *          instantiate a SSTable writer 
 *          append all columns 
 *          append all decorated keys
 *          clear column family
 *          
 *
 *
 *          Transfer SSTables 
 *          ------------------
 *          pass arguments to WapBulkLoader(main)
 *	    remaining task will be taken care by `SSTableLoader`          
 *          
 *
 *          Logic to Remove SSTable 
 *          ------------------------------
 *          Remove table after successful transfer
 *          delete the directory
 *
 *
 *          Logic to store not transferred SSTable
 *          --------------------------------------------
 *          Store all failure SSTable in transpoFailure/data/KS/CF/SSTables
 *          
 *          Logging Logic
 *	    -------------
 *	    This will generate one log.txt file corresponding to each CF which will be having info of all SSTables 
 *
 *
 *
 */


public class WapTranspoGenerateOnly {
    private static final String TENANT_ID = "tenantId";
    private static final String REPLACE_WITH = "replaceWith";
    private static final String NODE_ADDRESS = "d";
    private static final String TARGET_KEYSPACE = "k";
    private static final String MODE = "mode";
    private static final Options options = new Options();
    private static CommandLine cmd;
    private static Integer keyCountToImport = 0;
    private static List<Object> allColumnsForGivenRow = new ArrayList<Object>();
    private static SortedMap<DecoratedKey, List<Object>> allSortedModifiedRow = new TreeMap<DecoratedKey, List<Object>>();
    private static final int MSTOHOUR = 3600000;
    private static final int MSTOMIN = 60000;
    private static final int MSTOSEC = 1000;
    private static List<String> allFailedSSTable = new ArrayList<String>();
    private static final String LOGFILTEREDDIRPATH = System.getProperty("user.dir") + "/transpoLog/Filtered";
    private static final String LOGGENERATEDDIRPATH = System.getProperty("user.dir") + "/transpoLog/Generated";
    private static final String LOGTRANSPODDIRPATH = System.getProperty("user.dir") + "/transpoLog/Transpo";
    private static final String BASEMODIFIEDDIR = System.getProperty("user.dir") + "/modifiedData/Generated";
    private static final String BASEFILTEREDDDIR = System.getProperty("user.dir") + "/modifiedData/Filtered";
    private static boolean isLogDirectoryCreated = false;
    private static PrintStream logStdOut = null;
    private static String TIMESTAMP;   

    static {

	TIMESTAMP = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()); 

	Option tenantId = new Option(TENANT_ID, true, "tenantId to filter the data");
	options.addOption(tenantId);

	Option replacingTenantId = new Option(REPLACE_WITH, true, "to replace given tenantId");
	options.addOption(replacingTenantId);

	Option nodeAddress = new Option(NODE_ADDRESS, true, "pass the address of all target nodes");
	options.addOption(nodeAddress);

	Option targetKeyspace = new Option(TARGET_KEYSPACE, true, "to get the remote keyspace name");
	options.addOption(targetKeyspace);


	// this is to ensure at which mode we want to use this utility
	Option mode = new Option(MODE, true, "to get mode");
	options.addOption(mode);

    }

    private static class JsonColumn<T> {
	private ByteBuffer name;
	private ByteBuffer value;
	private long timestamp;

	private String kind;
	// Expiring columns
	private int ttl;
	private int localExpirationTime;

	// Counter columns
	private long timestampOfLastDelete;

	public JsonColumn(T json, CFMetaData meta) {
	    if (json instanceof List) {
		CellNameType comparator = meta.comparator;
		List fields = (List<?>) json;

		assert fields.size() >= 3 : "Cell definition should have at least 3";

		name = stringAsType((String) fields.get(0), comparator.asAbstractType());
		timestamp = (Long) fields.get(2);
		kind = "";

		if (fields.size() > 3) {
		    kind = (String) fields.get(3);
		    if (isExpiring()) {
			ttl = (Integer) fields.get(4);
			localExpirationTime = (Integer) fields.get(5);
		    } else if (isCounter()) {
			timestampOfLastDelete = ((Integer) fields.get(4));
		    } else if (isRangeTombstone()) {
			localExpirationTime = (Integer) fields.get(4);
		    }
		}

		if (isDeleted()) {
		    value = ByteBufferUtil.bytes((Integer) fields.get(1));
		} else if (isRangeTombstone()) {
		    value = stringAsType((String) fields.get(1), comparator.asAbstractType());
		} else {
		    assert meta.isCQL3Table() || name.hasRemaining() : "Cell name should not be empty";
		    value = stringAsType((String) fields.get(1), meta.getValueValidator(name.hasRemaining()
			    ? comparator.cellFromByteBuffer(name) : meta.comparator.rowMarker(Composites.EMPTY)));
		}
	    }
	}

	public boolean isDeleted() {
	    return kind.equals("d");
	}

	public boolean isExpiring() {
	    return kind.equals("e");
	}

	public boolean isCounter() {
	    return kind.equals("c");
	}

	public boolean isRangeTombstone() {
	    return kind.equals("t");
	}

	public ByteBuffer getName() {
	    return name.duplicate();
	}

	public ByteBuffer getValue() {
	    return value.duplicate();
	}
    }


    /**
     * Convert a string to bytes (ByteBuffer) according to type
     *
     * @param content
     *            string to convert
     * @param type
     *            type to use for conversion
     * @return byte buffer representation of the given string
     */
    private static ByteBuffer stringAsType(String content, AbstractType<?> type) {
	try {
	    return type.fromString(content);
	} catch (MarshalException e) {
	    throw new RuntimeException(e.getMessage());
	}
    }



    public static void main(String[] args) {
	long startTime = System.currentTimeMillis();
	String usage = String.format(
		"Usage: %s -k KS -d <node1,node2,node3> -tenantId [tenant1] -replaceWith [tenant1Staging] <directoryPathForSSTables> %n",
		SSTableExport.class.getName());

	String modeUsage = String.format("There are three mode we support \n"
		+ "(1). %s \n"
		+ "(2). %s \n"
		+ "(3). %s \n",
		"F (for filter Only)",
		"G (for generate modified SStable for different tenantId)",
		"GT (for generate and transfer all sstables for given tenant and replace with 'replacing tenantId')");

	CommandLineParser parser = new PosixParser();

	try {
	    cmd = parser.parse(options, args);
	} catch (ParseException e1) {
	    System.err.println(e1.getMessage());
	    System.err.println(usage);
	    System.exit(1);
	}

	if (cmd.getArgs().length != 1) {
	    System.err.println("You must supply exactly one sstable/Directory");
	    System.err.println(usage);
	    System.exit(1);
	}

	if (!cmd.hasOption(NODE_ADDRESS)) {
	    System.err.println("Initial hosts must be specified (-d)");
	    System.err.println(usage);
	    System.exit(1);
	}

	if(!cmd.hasOption(MODE)){
	    System.err.println("You must define one mode out of three modes");
	    System.err.println("for generate only, use :-> G");
	    System.err.println("for transfer only, use :-> T");
	    System.err.println("for generate and transfer, use :-> GT");
	    System.exit(1);
	}

	if(!cmd.hasOption(TENANT_ID)){
	    System.err.println("Please provide the tenantId");
	    System.exit(1);
	}

	if(!cmd.hasOption(TARGET_KEYSPACE)){
	    System.out.println("------------------------------------------------------------------------------");
	    System.out.println("Since you haven't specified the target keyspace then It's going to use default");
	    System.out.println("------------------------------------------------------------------------------");
	}

	String tenantId = cmd.getOptionValue(TENANT_ID);
	String replacingTenantId = cmd.getOptionValue(REPLACE_WITH);
	String SSTableDirectoryName = new File(cmd.getArgs()[0]).getAbsolutePath();
	String nodes = cmd.getOptionValue(NODE_ADDRESS);
	String mode = cmd.getOptionValue(MODE);
	String targetKeyspace = cmd.getOptionValue(TARGET_KEYSPACE);
	

	// this is to set mode for given task

	DatabaseDescriptor.loadSchemas(false);

	switch(mode){
	case "F": filterOnly(SSTableDirectoryName,tenantId);
	break;
	case "T": transferOnly(SSTableDirectoryName,nodes,targetKeyspace);
	break;
	case "G": generateOnly(SSTableDirectoryName,tenantId,replacingTenantId);
	break;
	default : System.err.println("Invalid mode!!\n" + modeUsage);
	System.exit(1);
	}

	// last time to print final summary
	
	printTimeSummary(startTime);
	System.exit(1);
    }


    // this will generate modified SSTable(manipulte the tenantID) and remove them
    private static void generateOnly(String SSTableDirectoryName,String tenantId, String replacingTenantId) {
	// TODO Auto-generated method stub
	if(replacingTenantId==null){
	    System.out.println("******************************************");
	    System.err.println("replacing tenantId can't be null");
	    System.out.println("use option -replaceWith <modifiedTenantId>");
	    System.out.println("******************************************");
	    System.exit(1);
	}else if(replacingTenantId.isEmpty()){
	    System.out.println("*********************************");
	    System.err.println("replacing tenantId can't be empty");
	    System.out.println("*********************************");
	    System.exit(1);
	}

	int totalProcessedSSTables =0; 
	int totalSSTableForgivenCF=0;
	int totalFailureSSTables=0;
	
	List<String> listOfAllSSTables = WapTranspoUtil.generateAbsolutePathOfAllSSTables(SSTableDirectoryName);
	totalSSTableForgivenCF = listOfAllSSTables.size();
	if(listOfAllSSTables.size()==0){
	    System.err.println("You are passing wrong directory: please pass prpoer path");
	    System.exit(1);
	}

	for(String SSTableFileName : listOfAllSSTables){


	    Descriptor descriptor = getDescriptor(SSTableFileName);
	    validationOfCf(descriptor, SSTableFileName);

	    Keyspace keyspace = Keyspace.open(descriptor.ksname);

	    String columnFamilyName = descriptor.cfname;
	    if (descriptor.cfname.contains(".")) {
		String[] parts = descriptor.cfname.split("\\.", 2);
		columnFamilyName = parts[0];
	    }


	    // here we also initialize our file in which we will redirect our all logs
	    if (!isLogDirectoryCreated) {
		isLogDirectoryCreated = initializeLogDirectory(keyspace.getName(), columnFamilyName, LOGGENERATEDDIRPATH);
	    }

	    // create base directory
	    String modifiedSSTablePath = BASEMODIFIEDDIR + "/data" + "/" + TIMESTAMP + "/" + keyspace.getName() + "/" + columnFamilyName+"/"
		    + WapTranspoUtil.getDataDirectoryName(SSTableFileName);
	    String modifiedDataDirectoryPath = modifiedSSTablePath.substring(0, modifiedSSTablePath.lastIndexOf("/"));	
	    WapTranspoUtil.createDirectory(modifiedDataDirectoryPath);
	    printAndWriteToFile(logStdOut, "Generating SSTable for tenant "+replacingTenantId+"  :- ");
	    printAndWriteToFile(logStdOut, "----------------------------------------------");
	    printAndWriteToFile(logStdOut, modifiedSSTablePath);
	    printAndWriteToFile(logStdOut, "");
	    printAndWriteToFile(logStdOut, "");


	    ColumnFamilyStore cfStore = null;

	    try {
		cfStore = keyspace.getColumnFamilyStore(columnFamilyName);
	    } catch (IllegalArgumentException e) {
		System.err.println(String.format(
			"The provided column family is not part of this cassandra keyspace: keyspace = %s, column family = %s",
			descriptor.ksname, descriptor.cfname));
		System.exit(1);
	    }

	    try {
		readSSTable(descriptor, cfStore.metadata, tenantId, replacingTenantId, modifiedSSTablePath);
		totalProcessedSSTables++;
	    } catch (IOException e) {
		totalFailureSSTables++;
		e.printStackTrace(System.err);
	    }
	}
	printCurrentProgress(totalProcessedSSTables, totalSSTableForgivenCF, totalFailureSSTables);

    }

    // this will only transfer the sstables
    private static void transferOnly(String SSTableDirectoryName, String nodes, String targetKeyspace) {
	int totalProcessedSSTables =0; 
	int totalSSTableForgivenCF=0;
	int totalFailureSSTables=0;

	List<String> listOfALlSSTables = WapTranspoUtil.generateAbsolutePathOfAllSSTables(SSTableDirectoryName);
	totalSSTableForgivenCF = listOfALlSSTables.size();
	
	if(totalSSTableForgivenCF==0){
	    System.err.println("There is not a single sstable to transfer..in dir "+SSTableDirectoryName);
	    System.exit(1);
	}
	
	Descriptor descriptor = getDescriptor(listOfALlSSTables.get(0));


	// to initialize log dir/file
	if(!isLogDirectoryCreated){
	    isLogDirectoryCreated = initializeLogDirectory(descriptor.ksname, descriptor.cfname, LOGTRANSPODDIRPATH);
	}

	printCurrentProgress("transferrinig", SSTableDirectoryName);
	System.out.println("I'm going to pass the modified keyspace :: " + targetKeyspace);
	String[] args = new String[] {"-d " + nodes, "-k"+targetKeyspace, SSTableDirectoryName };
	System.out.println(args.toString());
	try{
	    WapBulkLoader.main(args, logStdOut);
	}catch(Throwable e){
	    //TODO
	    // copy all failure sstable to failure dir.
	    System.err.println("Failed to transfer sstable :: "  + SSTableDirectoryName);
	}
	System.out.println("all sstable has been transferred successfully!!");
    }


    // this will only generate SSTable for given tenant
    private static void filterOnly(String SSTableDirectoryName, String tenantId) {
	// TODO Auto-generated method stub
	int totalProcessedSSTables =0; 
	int totalSSTableForgivenCF=0;
	int totalFailureSSTables=0;
	
	List<String> listOfAllSSTables = WapTranspoUtil.generateAbsolutePathOfAllSSTables(SSTableDirectoryName);

	if(listOfAllSSTables.size()==0){
	    System.err.println("You are passing wrong directory: please pass prpoer path");
	    System.exit(1);
	}
	totalSSTableForgivenCF = listOfAllSSTables.size();
	for(String SSTableFileName : listOfAllSSTables){
	    Descriptor descriptor = getDescriptor(SSTableFileName);
	    validationOfCf(descriptor, SSTableFileName);

	    Keyspace keyspace = Keyspace.open(descriptor.ksname);

	    String columnFamilyName = descriptor.cfname;
	    if (descriptor.cfname.contains(".")) {
		String[] parts = descriptor.cfname.split("\\.", 2);
		columnFamilyName = parts[0];
	    }


	    if (!isLogDirectoryCreated) {
		isLogDirectoryCreated = initializeLogDirectory(keyspace.getName(), columnFamilyName, LOGFILTEREDDIRPATH);
	    }

	    // create base directory
	    String modifiedSSTablePath = BASEFILTEREDDDIR + "/data" + "/" + TIMESTAMP + "/" + keyspace.getName() + "/" + columnFamilyName+"/"
		    + WapTranspoUtil.getDataDirectoryName(SSTableFileName);
	    String modifiedDataDirectoryPath = modifiedSSTablePath.substring(0, modifiedSSTablePath.lastIndexOf("/"));	
	    WapTranspoUtil.createDirectory(modifiedDataDirectoryPath);
	    printAndWriteToFile(logStdOut, "Filtering SSTable for tenant "+tenantId+"  :- ");
	    printAndWriteToFile(logStdOut, "----------------------------------------------");
	    printAndWriteToFile(logStdOut, modifiedSSTablePath);
	    printAndWriteToFile(logStdOut, "");
	    printAndWriteToFile(logStdOut, "");

	    ColumnFamilyStore cfStore = null;

	    try {
		cfStore = keyspace.getColumnFamilyStore(columnFamilyName);
	    } catch (IllegalArgumentException e) {
		System.err.println(String.format(
			"The provided column family is not part of this cassandra keyspace: keyspace = %s, column family = %s",
			descriptor.ksname, descriptor.cfname));
		System.exit(1);
	    }

	    try {
		totalProcessedSSTables++;
		readSSTable(descriptor, cfStore.metadata, tenantId, modifiedSSTablePath);
	    } catch (IOException e) {
		totalFailureSSTables++;
		e.printStackTrace(System.err);
	    }
	}
	printCurrentProgress(totalProcessedSSTables, totalSSTableForgivenCF, totalFailureSSTables);
    }

    private static void generateSSTable(){
	// TODO refactor in future
    }

    private static void transferSSTable(String args){
	//TODO call our bulkloader
	// add change in keyspace @apply zhao's patch
    }

    private static Descriptor getDescriptor(String SSTableFileName){
	return Descriptor.fromFilename(SSTableFileName);
    }

    private static void validationOfCf(Descriptor descriptor, String SSTableFileName){
	if(Schema.instance.getKSMetaData(descriptor.ksname)==null){
	    System.err.println(String.format("Filename %s references to nonexistent keyspace: %s!", SSTableFileName,
		    descriptor.ksname));
	    System.exit(1);
	}
    }

    private static boolean initializeLogDirectory(String keyspaceName, String cfName, String logDirPath){
	String logDir = logDirPath + "/" + keyspaceName + "/" + cfName;
	String logFileName = logDir + "/" + cfName + "_log_"+new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date())+".txt";
	WapTranspoUtil.createDirectory(logDir);
	File logFile = WapTranspoUtil.createFile(logFileName);
	try {
	    logStdOut = new PrintStream(new FileOutputStream(logFile));

	    printAuthorDetails();

	    printAndWriteToFile(logStdOut, "");
	    printAndWriteToFile(logStdOut, "");
	    printCurrentProgress("Log Directory :-", logDir);
	} catch (FileNotFoundException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	return true;
    }


    /**
     * This is to read SSTable and re generate sstable after replacing tenantId
     *
     * @param descthe descriptor of the sstable to read from
     * @param outs PrintStream to write the output to
     * @param excludes keys to exclude from export
     * @param metadata Metadata to print keys in a proper format
     * @throws IOException on failure to read/write input/output
     */
    private static void readSSTable(Descriptor desc, CFMetaData metadata, String tenantId, String replacingTenantId, String modifiedSSTablePath)
	    throws IOException {
	readSSTable(SSTableReader.open(desc), metadata, tenantId, replacingTenantId, modifiedSSTablePath);
    }


    static void readSSTable(SSTableReader reader, CFMetaData metadata, String tenantId, String replacingTenantId, String modifiedSSTablePath)
	    throws IOException {
	SSTableIdentityIterator row;
	ISSTableScanner scanner = reader.getScanner();
	SortedMap<DecoratedKey, DecoratedKey> sortedModifiedDecoratedKey = new TreeMap<DecoratedKey,DecoratedKey>();


	try {
	    while (scanner.hasNext()) {
		row = (SSTableIdentityIterator) scanner.next();
		DecoratedKey decoratedKey = row.getKey();
		if (!WapTranspoUtil.doesContainTargetKey(tenantId, decoratedKey))
		    continue;

		String modifiedDecoratedKey = row.getColumnFamily().metadata().getKeyValidator().getString(row.getKey().getKey()).replaceAll(tenantId,
			replacingTenantId);
		sortedModifiedDecoratedKey.put(modifyDecoratedKey(row, modifiedDecoratedKey),row.getKey());
		keyCountToImport++;
	    }

	    if(keyCountToImport!=0){
		SSTableWriter writer = new SSTableWriter(modifiedSSTablePath, keyCountToImport,
			ActiveRepairService.UNREPAIRED_SSTABLE);

		// now iterate over the decorated key to fetch the row
		RandomAccessReader dfile = reader.openDataReader();
		for(Map.Entry<DecoratedKey, DecoratedKey> decoratedKey : sortedModifiedDecoratedKey.entrySet()){
		    List<Object> columnsForGivenRow = new ArrayList<Object>();
		    RowIndexEntry entry = reader.getPosition(decoratedKey.getValue(), SSTableReader.Operator.EQ);
		    if(entry==null){
			continue;
		    }
		    dfile.seek(entry.position);
		    ByteBufferUtil.readWithShortLength(dfile);
		    DeletionInfo deletionInfo = new DeletionInfo(DeletionTime.serializer.deserialize(dfile));
		    Iterator<OnDiskAtom> atomIterator = reader.metadata.getOnDiskIterator(dfile, reader.descriptor.version);
		    while (atomIterator.hasNext()) {
			columnsForGivenRow.add(serializeAtom(atomIterator.next(), metadata));
		    }

		    ColumnFamily cfamily = ArrayBackedSortedColumns.factory.create(metadata.ksName,metadata.cfName);

		    addColumnsToCF(columnsForGivenRow, cfamily);
		    writer.append(decoratedKey.getKey(),cfamily);
		    cfamily.clear();
		}

		// store all th
		writer.closeAndOpenReader().selfRef().release();
		keyCountToImport=0;
	    }
	} finally {
	    scanner.close();
	    reader.selfRef().release();
	}
    }


    /**
     * This method is to filter the sstable for given tenant
     * first we count the number of keys in sstable for given tenantId If it is 0 then we skip this SSTable else we write all those rows in new sstable
     * 
     * @param desc
     * @param metadata
     * @param tenantId
     * @param modifiedSSTablePath
     * @throws IOException
     */
    private static void readSSTable(Descriptor desc, CFMetaData metadata, String tenantId, String modifiedSSTablePath)
	    throws IOException {
	readSSTable(SSTableReader.open(desc), metadata, tenantId,  modifiedSSTablePath);
    }


    static void readSSTable(SSTableReader reader, CFMetaData metadata, String tenantId, String modifiedSSTablePath)
	    throws IOException {
	SSTableIdentityIterator row;
	ISSTableScanner scanner = reader.getScanner();
	keyCountToImport=0;
	try {
	    while (scanner.hasNext()) {
		row = (SSTableIdentityIterator) scanner.next();
		DecoratedKey decoratedKey = row.getKey();
		if (!WapTranspoUtil.doesContainTargetKey(tenantId, decoratedKey))
		    continue;

		keyCountToImport++;
	    }

	    // writing back to sstable
	    if(keyCountToImport!=0){
		scanner = reader.getScanner();
		SSTableWriter writer = new SSTableWriter(modifiedSSTablePath, keyCountToImport,
			ActiveRepairService.UNREPAIRED_SSTABLE);
		while(scanner.hasNext()){

		    row = (SSTableIdentityIterator) scanner.next();
		    DecoratedKey decoratedKey = row.getKey();
		    if (!WapTranspoUtil.doesContainTargetKey(tenantId, decoratedKey))
			continue;

		    Iterator<OnDiskAtom> atomIterator = row;
		    List<Object> columnsForGivenRow = new ArrayList<Object>();
		    while(atomIterator.hasNext()){
			columnsForGivenRow.add(serializeAtom(atomIterator.next(), metadata));
		    }
		    ColumnFamily cfamily = ArrayBackedSortedColumns.factory.create(metadata.ksName,metadata.cfName);
		    addColumnsToCF(columnsForGivenRow, cfamily);
		    writer.append(decoratedKey,cfamily);
		    cfamily.clear();
		}
		writer.closeAndOpenReader().selfRef().release();
	    }
	} finally {
	    if(keyCountToImport==0){
		System.out.println("##################################################################");
		System.out.println("there was no data for tenant "+tenantId+" in SSTable, Skipping....");
		System.out.println("##################################################################");
		System.out.println();
	    }
	    keyCountToImport=0;
	    scanner.close();
	    reader.selfRef().release();
	}
    }







    /**
     * this function converts string partition key to decorated key
     *
     * @param decorated key
     * @param modified partition key as String
     * @return modified decorated key
     */
    private static DecoratedKey modifyDecoratedKey(SSTableIdentityIterator row, String modifiedDecoratedKey) {
	IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
	return partitioner.decorateKey(getKeyValidator(row.getColumnFamily()).fromString(modifiedDecoratedKey));
    }

    /**
     * Get key validator for column family
     *
     * @param columnFamily column family instance
     * @return key validator for given column family
     */
    private static AbstractType<?> getKeyValidator(ColumnFamily columnFamily) {
	// this is a fix to support backward compatibility
	// which allows to skip the current key validator
	// please, take a look onto CASSANDRA-7498 for more details
	if ("true".equals(System.getProperty("skip.key.validator", "false"))) {
	    return BytesType.instance;
	}
	return columnFamily.metadata().getKeyValidator();
    }

    private static List<Object> serializeAtom(OnDiskAtom atom, CFMetaData cfMetaData) {
	if (atom instanceof Cell) {
	    return serializeColumn((Cell) atom, cfMetaData);
	} else {
	    assert atom instanceof RangeTombstone;
	    RangeTombstone rt = (RangeTombstone) atom;
	    ArrayList<Object> serializedColumn = new ArrayList<Object>();
	    serializedColumn.add(cfMetaData.comparator.getString(rt.min));
	    serializedColumn.add(cfMetaData.comparator.getString(rt.max));
	    serializedColumn.add(rt.data.markedForDeleteAt);
	    serializedColumn.add("t");
	    serializedColumn.add(rt.data.localDeletionTime);
	    return serializedColumn;
	}
    }

    /**
     * Serialize a given cell to a List of Objects that jsonMapper knows how to
     * turn into strings. Format is
     *
     * human_readable_name, value, timestamp, [flag, [options]]
     *
     * Value is normally the human readable value as rendered by the validator,
     * but for deleted cells we give the local deletion time instead.
     *
     * Flag may be exactly one of {d,e,c} for deleted, expiring, or counter: -
     * No options for deleted cells - If expiring, options will include the TTL
     * and local deletion time. - If counter, options will include timestamp of
     * last delete
     *
     * @param cell
     *            cell presentation
     * @param cfMetaData
     *            Column Family metadata (to get validator)
     * @return cell as serialized list
     */
    private static List<Object> serializeColumn(Cell cell, CFMetaData cfMetaData) {
	CellNameType comparator = cfMetaData.comparator;
	ArrayList<Object> serializedColumn = new ArrayList<Object>();

	serializedColumn.add(comparator.getString(cell.name()));

	if (cell instanceof DeletedCell) {
	    serializedColumn.add(cell.getLocalDeletionTime());
	} else {
	    AbstractType<?> validator = cfMetaData.getValueValidator(cell.name());
	    serializedColumn.add(validator.getString(cell.value()));
	}

	serializedColumn.add(cell.timestamp());

	if (cell instanceof DeletedCell) {
	    serializedColumn.add("d");
	} else if (cell instanceof ExpiringCell) {
	    serializedColumn.add("e");
	    serializedColumn.add(((ExpiringCell) cell).getTimeToLive());
	    serializedColumn.add(cell.getLocalDeletionTime());
	} else if (cell instanceof CounterCell) {
	    serializedColumn.add("c");
	    serializedColumn.add(((CounterCell) cell).timestampOfLastDelete());
	}

	return serializedColumn;
    }


    /**
     * Add columns to a column family.
     *
     * @param row
     *            the columns associated with a row
     * @param cfamily
     *            the column family to add columns to
     */
    private static void addColumnsToCF(List<?> row, ColumnFamily cfamily) {
	CFMetaData cfm = cfamily.metadata();
	assert cfm != null;

	// basically row is here-> each row corresponding to each columnn so
	// Each Real will have row + one for decorated key
	for (Object c : row) {
	    JsonColumn col = new JsonColumn<List>((List) c, cfm);
	    if (col.isRangeTombstone()) {
		Composite start = cfm.comparator.fromByteBuffer(col.getName());
		Composite end = cfm.comparator.fromByteBuffer(col.getValue());
		cfamily.addAtom(new RangeTombstone(start, end, col.timestamp, col.localExpirationTime));
		continue;
	    }

	    assert cfm.isCQL3Table() || col.getName().hasRemaining() : "Cell name should not be empty";
	    CellName cname = col.getName().hasRemaining() ? cfm.comparator.cellFromByteBuffer(col.getName())
		    : cfm.comparator.rowMarker(Composites.EMPTY);

	    if (col.isExpiring()) {
		cfamily.addColumn(
			new BufferExpiringCell(cname, col.getValue(), col.timestamp, col.ttl, col.localExpirationTime));
	    } else if (col.isCounter()) {
		cfamily.addColumn(
			new BufferCounterCell(cname, col.getValue(), col.timestamp, col.timestampOfLastDelete));
	    } else if (col.isDeleted()) {
		cfamily.addTombstone(cname, col.getValue(), col.timestamp);
	    } else if (col.isRangeTombstone()) {
		CellName end = cfm.comparator.cellFromByteBuffer(col.getValue());
		cfamily.addAtom(new RangeTombstone(cname, end, col.timestamp, col.localExpirationTime));
	    }
	    // cql3 row marker, see CASSANDRA-5852
	    else if (cname.isEmpty()) {
		cfamily.addColumn(cfm.comparator.rowMarker(Composites.EMPTY), col.getValue(), col.timestamp);
	    } else {
		cfamily.addColumn(cname, col.getValue(), col.timestamp);
	    }
	}
    }

    /**
     * To print Author contact details 
     */
    public static void printAuthorDetails(){
	logStdOut.println("######################################################");
	logStdOut.println("# Author : Varun Barala (oOo) 			 ");
	logStdOut.println("# <barala_v@worksap.co.jp>, slack (@varun_barala)     ");
	logStdOut.println("# KVA,(C)*						 ");
	logStdOut.println("######################################################");
	logStdOut.println();
	logStdOut.println();
    }

    /**To print final Time Summary for Given CF
     * @param long starting time
     */
    public static void printTimeSummary(long startingTime){
	long endingTime = System.currentTimeMillis();
	long elapsedTime = endingTime-startingTime;
	long HH = (elapsedTime/MSTOHOUR) ;
	long MM = ((elapsedTime%MSTOHOUR)/MSTOMIN);
	long SS = (((elapsedTime%MSTOHOUR)%MSTOMIN)/MSTOSEC);

	printAndWriteToFile(logStdOut, "========================================");
	printAndWriteToFile(logStdOut, "Total Time(HH:MM:SS) ::   "+HH+" : "+MM+" : "+SS+"    |");
	printAndWriteToFile(logStdOut, "========================================");

    }

    /**
     * @purpose To display at both level
     * @param printStream
     * @param String message
     *
     */
    public static void printAndWriteToFile(PrintStream logOut, String msg) {
	System.out.println(msg);
	logOut.println(msg);
    }

    public static void printAndWriteToFile(PrintStream logOut, Object...msg ) {
	String format = "%20s | %20s | %20s | %20s |";
	System.out.println(String.format(format, msg));
	logOut.println(String.format(format, msg));
    }


    public static void printCurrentProgress(String action, String fileName){
	printAndWriteToFile(logStdOut,action  +" :- ");
	printAndWriteToFile(logStdOut, "---------------------");
	printAndWriteToFile(logStdOut, fileName);
	printAndWriteToFile(logStdOut, "");
	printAndWriteToFile(logStdOut, "");
    }

    public static void printCurrentProgress(String action){
	printAndWriteToFile(logStdOut, "");
	printAndWriteToFile(logStdOut, "");
	printAndWriteToFile(logStdOut, action);
	printAndWriteToFile(logStdOut, "");
	printAndWriteToFile(logStdOut, "");
    }

    /**
     * To print progress after each SSTable processed
     * 
     * @param int total no of SSTables
     * @param int total no of processed SSTables
     * @param int total failure of SSTables 
     */
    public static void printCurrentProgress(int totalProcessedSSTables, int totalSSTableForgivenCF, int totalFailureSSTables){
	printAndWriteToFile(logStdOut, "==========================================================================================");
	printAndWriteToFile(logStdOut, "Total Processed", "Total SSTables","total failure", "total %");
	printAndWriteToFile(logStdOut, totalProcessedSSTables, totalSSTableForgivenCF, totalFailureSSTables, ((totalProcessedSSTables+totalFailureSSTables)*100)/totalSSTableForgivenCF);
	printAndWriteToFile(logStdOut, "==========================================================================================");

    }

}
