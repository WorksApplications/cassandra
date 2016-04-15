package org.apache.cassandra.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.io.util.FileUtils;
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
import org.apache.pig.data.SortedDataBag;

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
 *
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

public class WapModifyAndGenerateSSTablesUtility {

    private static final String TENANT_ID = "tenantId";
    private static final String REPLACE_WITH = "replaceWith";
    private static final String NODE_ADDRESS = "d";
    private static final Options options = new Options();
    private static CommandLine cmd;
    private static Integer keyCountToImport = 0;
    private static List<Object> allColumnsForGivenRow = new ArrayList<Object>();
    private static SortedMap<DecoratedKey, List<Object>> allSortedModifiedRow = new TreeMap<DecoratedKey, List<Object>>();
    private static final int MSTOHOUR = 3600000;
    private static final int MSTOMIN = 60000;
    private static final int MSTOSEC = 1000;
    private static List<String> allFailedSSTable = new ArrayList<String>();
    private static final String LOGDIRPATH = System.getProperty("user.dir") + "/transpoLog";
    private static final String BASEMODIFIEDDIR = System.getProperty("user.dir") + "/modifiedData";
    private static boolean isLogDirectoryCreated = false;
    private static PrintStream logStdOut = null;
   
    
    static {
	Option tenantId = new Option(TENANT_ID, true, "tenantId to filter the data");
	options.addOption(tenantId);

	Option replacingTenantId = new Option(REPLACE_WITH, true, "to replace given tenantId");
	options.addOption(replacingTenantId);

	Option nodeAddress = new Option(NODE_ADDRESS, true, "pass the address of all target nodes");
	options.addOption(nodeAddress);
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
     * Get portion of the columns and serialize in loop while not more columns
     * left in the row
     *
     * @param row
     *            SSTableIdentityIterator row representation with Column Family
     * @param key
     *            Decorated Key for the required row
     * @param out
     *            output stream
     */
    private static void serializeRow(SSTableIdentityIterator row, DecoratedKey key, String tenantId,
	    String replacingTenantId) {
	serializeRow(row.getColumnFamily().deletionInfo(), row, row.getColumnFamily().metadata(), key, tenantId,
		replacingTenantId);
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

    private static void serializeRow(DeletionInfo deletionInfo, Iterator<OnDiskAtom> atoms, CFMetaData metadata,
	    DecoratedKey key, String tenantId, String replacingTenantId) {
	String modifiedDecoratedKey = metadata.getKeyValidator().getString(key.getKey()).replaceAll(tenantId,
		replacingTenantId);

	// consider delete info case too :-> perform some test cases

	allColumnsForGivenRow.clear();
	List<Object> allColumnsForGivenRow = new ArrayList<Object>();
	while (atoms.hasNext()) {
	    allColumnsForGivenRow.add(serializeAtom(atoms.next(), metadata));
	}
	allSortedModifiedRow.put(modifyDecoratedKey((SSTableIdentityIterator) atoms, modifiedDecoratedKey),
		allColumnsForGivenRow);
    }

    /**
     * Export an SSTable and write the resulting JSON to a PrintStream.
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
	    // collecting keys to export
	    while (scanner.hasNext()) {
		row = (SSTableIdentityIterator) scanner.next();
		DecoratedKey decoratedKey = row.getKey();
		if (!doesContainTargetKey(tenantId, decoratedKey))
		    continue;

		// here we manipulate the decorated keys
		String modifiedDecoratedKey = row.getColumnFamily().metadata().getKeyValidator().getString(row.getKey().getKey()).replaceAll(tenantId,
			replacingTenantId);
		sortedModifiedDecoratedKey.put(modifyDecoratedKey(row, modifiedDecoratedKey),row.getKey());
		// convert string thing
		//serializeRow(row, row.getKey(), tenantId, replacingTenantId);
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

	    }
	} finally {
	    scanner.close();
	    reader.selfRef().release();
	}
    }

    /**
     * @param 
     * 
     */
    public static void writeIntoSSTable(){
	try{
	    
	}finally{
	    
	}
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
     * @param args command line arguments
     * @throws ParseException
     * @throws IOException on failure to open/read/write files or output streams
     * @throws ConfigurationException on configuration failure (wrong parameters given)
     *
     */
    public static void main(String[] args) {
	long startTime = System.currentTimeMillis();
	String usage = String.format(
		"Usage: %s -d <node1,node2,node3> -tenantId [tenant1] -replaceWith [tenant1Staging] <directoryPathForSSTables> %n",
		SSTableExport.class.getName());
	String successFullyGenerated = "SuccessFully generated SSTables :: %s";

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

	String tenantId = cmd.getOptionValue(TENANT_ID);
	String replacingTenantId = cmd.getOptionValue(REPLACE_WITH);
	String ssTableDirectoryName = new File(cmd.getArgs()[0]).getAbsolutePath();
	String nodes = cmd.getOptionValue(NODE_ADDRESS);

	DatabaseDescriptor.loadSchemas(false);
	
	List<String> listOfAllSSTables = generateAbsolutePathOfAllSSTables(ssTableDirectoryName);
	int totalSSTableForgivenCF=listOfAllSSTables.size();
	int totalProcessedSSTable=0;
	int totalFailureSSTable=0;
	
	for (String ssTableFileName : listOfAllSSTables) {

	    Descriptor descriptor = Descriptor.fromFilename(ssTableFileName);

	    // keyspace validation
	    if (Schema.instance.getKSMetaData(descriptor.ksname) == null) {
		System.err.println(String.format("Filename %s references to nonexistent keyspace: %s!", ssTableFileName,
			descriptor.ksname));
		System.exit(1);
	    }

	    Keyspace keyspace = Keyspace.open(descriptor.ksname);

	    // make it works for indexes too - find parent cf it necessary
	    String columnFamilyName = descriptor.cfname;
	    if (descriptor.cfname.contains(".")) {
		String[] parts = descriptor.cfname.split("\\.", 2);
		columnFamilyName = parts[0];
	    }

	    // if this is first time then create log directory
	    if (!isLogDirectoryCreated) {
		String logDir = LOGDIRPATH + "/" + keyspace.getName() + "/" + columnFamilyName;
		String logFileName = logDir + "/" + columnFamilyName + "_log.txt";
		createDirectory(logDir);
		File logFile = createFile(logFileName);
		try {
		    logStdOut = new PrintStream(new FileOutputStream(logFile));
		    
		    printAuthorDetails();
		    
		    printAndWriteToFile(logStdOut, "");
		    printAndWriteToFile(logStdOut, "");
		    printAndWriteToFile(logStdOut, "");
		    
		    printAndWriteToFile(logStdOut, "Log Directory :- ");
		    printAndWriteToFile(logStdOut, "-----------------");
		    printAndWriteToFile(logStdOut, logDir);
		    printAndWriteToFile(logStdOut, "");
		    printAndWriteToFile(logStdOut, "");
		    printAndWriteToFile(logStdOut, "");
		} catch (FileNotFoundException e) {
		    // TODO Auto-generated catch block
		    e.printStackTrace();
		}
		isLogDirectoryCreated = true;
	    }
	 
	    
	    printAndWriteToFile(logStdOut, "Processing SSTable:- ");
	    printAndWriteToFile(logStdOut, "*********************");
	    printAndWriteToFile(logStdOut, ssTableFileName);
	    printAndWriteToFile(logStdOut, "");
	    printAndWriteToFile(logStdOut, "");
	    
	    String modifiedSSTablePath = BASEMODIFIEDDIR + "/data" + "/" + keyspace.getName() + "/"
		    + getDataDirectoryName(ssTableFileName, keyspace.getName());
	    String modifiedDataDirectoryPath = modifiedSSTablePath.substring(0, modifiedSSTablePath.lastIndexOf("/"));
	    String transportationFailureSSTablesPath = System.getProperty("user.dir") + "/transportationFailure/data" + "/"
		    + keyspace.getName() + "/" + getDataDirectoryName(ssTableFileName, keyspace.getName());
	    String transportationFailureSSTables = transportationFailureSSTablesPath.substring(0,transportationFailureSSTablesPath.lastIndexOf("/"));
	    // to generate directory for modified sstables
	    File modifiedDataDirectory = createDirectory(modifiedDataDirectoryPath);
	    printAndWriteToFile(logStdOut, "Generating SSTable:- ");
	    printAndWriteToFile(logStdOut, "---------------------");
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
	    } catch (IOException e) {
		e.printStackTrace(System.err);
	    }
	    if (keyCountToImport != 0) {
		keyCountToImport=0;

		String[] argsForBulkLoader = new String[] { "-d " + nodes, modifiedDataDirectoryPath };

		printAndWriteToFile(logStdOut, "Transferring SSTable:- ");
		printAndWriteToFile(logStdOut, "-----------------------");

		try {
		    WapBulkLoader.main(argsForBulkLoader, logStdOut);
		    totalProcessedSSTable++;
		} catch (Throwable e) {
		    File transpoFailure = createDirectory(transportationFailureSSTables);
		    totalFailureSSTable++;
		    try {
			System.out.println("transferring file to transpoFailure Directory");
			printAndWriteToFile(logStdOut, "Failed while transferring, copying this sstable :- ");
			printAndWriteToFile(logStdOut, "---------------------------");
			printAndWriteToFile(logStdOut, modifiedSSTablePath);
			printAndWriteToFile(logStdOut, "");
			printAndWriteToFile(logStdOut, "");
			
			allFailedSSTable.add(transportationFailureSSTablesPath);
			copyDirectory(modifiedDataDirectory, transpoFailure);
		    } catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		    }
		}

		// clean data directory
		if (modifiedDataDirectory.isDirectory()) {
		    printAndWriteToFile(logStdOut, "removing SSTable:- ");
		    printAndWriteToFile(logStdOut, "-------------------");
		    printAndWriteToFile(logStdOut, modifiedSSTablePath);
		    printAndWriteToFile(logStdOut, "");
		    printAndWriteToFile(logStdOut, "");
		    
		    cleanDirectory(modifiedDataDirectory);
		}

	    } else {
		totalProcessedSSTable++;
		printAndWriteToFile(logStdOut,
			"There is no data for tenant : " + tenantId + " in SSTable :: ");
		printAndWriteToFile(logStdOut, "--------------------------------------");
		printAndWriteToFile(logStdOut, ssTableFileName);
		printAndWriteToFile(logStdOut, "");
		printAndWriteToFile(logStdOut, "");
	    }

	    
	    // It will print current progress after each SSTable
	    printCurrentProgress(totalProcessedSSTable,totalSSTableForgivenCF,totalFailureSSTable);
	}

	printAndWriteToFile(logStdOut, "Removing Data Directory: ");
	printAndWriteToFile(logStdOut, "-------------------------");
	printAndWriteToFile(logStdOut, BASEMODIFIEDDIR);

	try {
	    if (new File(BASEMODIFIEDDIR).isDirectory()) {
		FileUtils.deleteRecursive(new File(BASEMODIFIEDDIR));
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	}

	// to handle all failed SSTables- > It's important**
	if (allFailedSSTable.size() > 0) {
	    printAndWriteToFile(logStdOut, "list of all failed SSTables :");
	    printAndWriteToFile(logStdOut, "--------------------------");
	    for (String failedCf : allFailedSSTable) {
		printAndWriteToFile(logStdOut, failedCf);
	    }
	    printAndWriteToFile(logStdOut, "-------------------------");
	    printAndWriteToFile(logStdOut, "Check given dir for all failure sstables "
		    + System.getProperty("user.dir") + "/transportationFailure/data");
	} else {
	    printAndWriteToFile(logStdOut, "All SSTables has been processed Successfully!!");
	}

	printAndWriteToFile(logStdOut, "");
	printAndWriteToFile(logStdOut, "");

	printTimeSummary(startTime);
	System.exit(1);
    }

    /**
     *
     * @param path
     * @param keyspaceName
     * @return part of directory after "KS" i.e '/ks/cf'
     */
    private static String getDataDirectoryName(String path, String keyspaceName) {
	List<String> allDirectories = Arrays.asList(path.split("/" + keyspaceName + "/"));
	if (allDirectories.size() > 1) {
	    return allDirectories.get(allDirectories.size() - 1);
	}
	return allDirectories.get(0);
    }

    /**
     * To generate absolute path of all the sstables for given directory
     *
     * @param String path of dirtectory
     * @return list of abssolute path of all the sstables
     */
    private static List<String> generateAbsolutePathOfAllSSTables(String directoryPath) {
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
     * To generate the directory
     *
     * @param String path to directory
     */
    private static File createDirectory(String pathToDirectory) {
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
    private static File createFile(String filePath) {
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
     * @purpose To copy single file
     * @param source file
     * @param destfile
     */
    @SuppressWarnings("resource")
    public static void copyFile(File from, File to) throws IOException {

	if (!to.exists()) {
	    to.createNewFile();
	}

	try (FileChannel in = new FileInputStream(from).getChannel();
		FileChannel out = new FileOutputStream(to).getChannel()) {

	    out.transferFrom(in, 0, in.size());
	}
    }

    /**
     *
     * @purpose To copy whole directory
     * @param source Directory
     * @param dest.Directory
     *
     */
    public static void copyDirectory(File from, File to) throws IOException {

	if (!to.exists()) {
	    to.createNewFile();
	}

	if (from.isDirectory()) {
	    String[] children = from.list();
	    for (String child : children) {
		copyDirectory(new File(from, child), new File(to, child));
	    }
	} else {
	    copyFile(from, to);
	}
    }

    /**
     * @purpose : To remove all files in given directory
     * @param File Name
     */
    public static void cleanDirectory(File file) {
	if (!file.isDirectory()) {
	    System.out.println("File does not exist!!");
	    return;
	}
	for (File child : file.listFiles()) {
	    child.delete();
	}
    }

    /**
     * @purpose To display at both level
     * @param log file
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
	logOut.println(msg);
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
	printAndWriteToFile(logStdOut, totalProcessedSSTables, totalSSTableForgivenCF, totalFailureSSTables, (totalProcessedSSTables*100)/totalSSTableForgivenCF);
	printAndWriteToFile(logStdOut, "==========================================================================================");

    }
    
    /**
     * To print Author contact details 
     */
    public static void printAuthorDetails(){
	logStdOut.println("######################################################");
	logStdOut.println("# Auhtor : Varun Barala (oOo) 			 ");
	logStdOut.println("# <barala_v@worksap.co.jp>, slack (@varun_barala)     ");
	logStdOut.println("# KVA,(C)*						 ");
	logStdOut.println("######################################################");
	logStdOut.println();
	logStdOut.println();
    }

    /**To print final Time Summary for Given CF
     * @param long starting time
     * @param long end time
     */
    public static void printTimeSummary(long startingTime){
	long endingTime = System.currentTimeMillis();
	long elapsedTime = endingTime-startingTime;
	long HH = (elapsedTime/MSTOHOUR) ;
	long MM = (elapsedTime/MSTOMIN);
	long SS = (elapsedTime/MSTOSEC);

	printAndWriteToFile(logStdOut, "========================================");
	printAndWriteToFile(logStdOut, "Total Time(HH:MM:SS) ::   "+HH+" : "+MM+" : "+SS+"    |");
	printAndWriteToFile(logStdOut, "========================================");
	
    }
}
