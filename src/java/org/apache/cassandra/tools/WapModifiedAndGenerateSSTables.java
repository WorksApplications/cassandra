package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
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
import org.apache.cassandra.db.ExpiringCell;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RangeTombstone;
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
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 *
 * @author barala
 * @purpose read SStables, filter for given tenantId, modify first Pk(String) and Generate SSTables
 *
 *	     *
		 * CMD arguments
		 * -------------
		 * (1) tenantId
		 * (2) replaceWith
		 * (3) path of sstable
		 * 
		 
	
		 *
		 * Filter and Modification
		 * ----------------------
		 * write export function
		 * serialize row
		 * store modified row in sorted map //(because of murmur3 hashing algorithm)
		 * 
		 
	
		 *
		 * Generate Corresponding SSTables 
		 * -------------------------------
		 * create a SSTable writer
		 * append column
		 * flush it to given directory
		 * 
		 *
 */

public class WapModifiedAndGenerateSSTables {

	private static final String TENANT_ID="tenantId";
	private static final String REPLACE_WITH="replaceWith";
	private static final Options options = new Options();
	private static CommandLine cmd;
	private static Integer keyCountToImport = 0;
	private static List<Object> allColumnsForGivenRow = new ArrayList<Object>();
    private static SortedMap<DecoratedKey,List<Object>> allSortedModifiedRow = new TreeMap<DecoratedKey,List<Object>>();
	
	static
	{
		Option tenantId = new Option(TENANT_ID, true, "tenantId to filter the data");
		options.addOption(tenantId);
		
		Option replacingTenantId = new Option(REPLACE_WITH, true, "to replace given tenantId");
		options.addOption(replacingTenantId);
		
		//TODO :-> Not adding/initializing json part here
		
	}
	
	
	private static class JsonColumn<T>
    {
        private ByteBuffer name;
        private ByteBuffer value;
        private long timestamp;

        private String kind;
        // Expiring columns
        private int ttl;
        private int localExpirationTime;

        // Counter columns
        private long timestampOfLastDelete;

        public JsonColumn(T json, CFMetaData meta)
        {
            if (json instanceof List)
            {
                CellNameType comparator = meta.comparator;
                List fields = (List<?>) json;

                assert fields.size() >= 3 : "Cell definition should have at least 3";

                name  = stringAsType((String) fields.get(0), comparator.asAbstractType());
                timestamp = (Long) fields.get(2);
                kind = "";

                if (fields.size() > 3)
                {
                    kind = (String) fields.get(3);
                    if (isExpiring())
                    {
                        ttl = (Integer) fields.get(4);
                        localExpirationTime = (Integer) fields.get(5);
                    }
                    else if (isCounter())
                    {
                        timestampOfLastDelete = (long) ((Integer) fields.get(4));
                    }
                    else if (isRangeTombstone())
                    {
                        localExpirationTime = (Integer) fields.get(4);
                    }
                }

                if (isDeleted())
                {
                    value = ByteBufferUtil.bytes((Integer) fields.get(1));
                }
                else if (isRangeTombstone())
                {
                    value = stringAsType((String) fields.get(1), comparator.asAbstractType());
                }
                else
                {
                    assert meta.isCQL3Table() || name.hasRemaining() : "Cell name should not be empty";
                    value = stringAsType((String) fields.get(1), 
                            meta.getValueValidator(name.hasRemaining() 
                                    ? comparator.cellFromByteBuffer(name)
                                    : meta.comparator.rowMarker(Composites.EMPTY)));
                }
            }
        }

        public boolean isDeleted()
        {
            return kind.equals("d");
        }

        public boolean isExpiring()
        {
            return kind.equals("e");
        }

        public boolean isCounter()
        {
            return kind.equals("c");
        }

        public boolean isRangeTombstone()
        {
            return kind.equals("t");
        }

        public ByteBuffer getName()
        {
            return name.duplicate();
        }

        public ByteBuffer getValue()
        {
            return value.duplicate();
        }
    }
	
	
    /**
     * Convert a string to bytes (ByteBuffer) according to type
     * @param content string to convert
     * @param type type to use for conversion
     * @return byte buffer representation of the given string
     */
    private static ByteBuffer stringAsType(String content, AbstractType<?> type)
    {
        try
        {
            return type.fromString(content);
        }
        catch (MarshalException e)
        {
            throw new RuntimeException(e.getMessage());
        }
    }
	
	
	/**
     * Serialize a given cell to a List of Objects that jsonMapper knows how to turn into strings.  Format is
     *
     * human_readable_name, value, timestamp, [flag, [options]]
     *
     * Value is normally the human readable value as rendered by the validator, but for deleted cells we
     * give the local deletion time instead.
     *
     * Flag may be exactly one of {d,e,c} for deleted, expiring, or counter:
     *  - No options for deleted cells
     *  - If expiring, options will include the TTL and local deletion time.
     *  - If counter, options will include timestamp of last delete
     *
     * @param cell     cell presentation
     * @param cfMetaData Column Family metadata (to get validator)
     * @return cell as serialized list
     */
    private static List<Object> serializeColumn(Cell cell, CFMetaData cfMetaData)
    {
        CellNameType comparator = cfMetaData.comparator;
        ArrayList<Object> serializedColumn = new ArrayList<Object>();

        serializedColumn.add(comparator.getString(cell.name()));

        if (cell instanceof DeletedCell)
        {
            serializedColumn.add(cell.getLocalDeletionTime());
        }
        else
        {
            AbstractType<?> validator = cfMetaData.getValueValidator(cell.name());
            serializedColumn.add(validator.getString(cell.value()));
        }

        serializedColumn.add(cell.timestamp());

        if (cell instanceof DeletedCell)
        {
            serializedColumn.add("d");
        }
        else if (cell instanceof ExpiringCell)
        {
            serializedColumn.add("e");
            serializedColumn.add(((ExpiringCell) cell).getTimeToLive());
            serializedColumn.add(cell.getLocalDeletionTime());
        }
        else if (cell instanceof CounterCell)
        {
            serializedColumn.add("c");
            serializedColumn.add(((CounterCell) cell).timestampOfLastDelete());
        }

        return serializedColumn;
    }
	
	
    /**
     * Add columns to a column family.
     *
     * @param row the columns associated with a row
     * @param cfamily the column family to add columns to
     */
    private static void addColumnsToCF(List<?> row, ColumnFamily cfamily)
    {
        CFMetaData cfm = cfamily.metadata();
        assert cfm != null;

        // basically row is here-> each row corresponding to each columnn so Each Real will have row + one for decorated key
        for (Object c : row)
        {
            JsonColumn col = new JsonColumn<List>((List) c, cfm);
            if (col.isRangeTombstone())
            {
                Composite start = cfm.comparator.fromByteBuffer(col.getName());
                Composite end = cfm.comparator.fromByteBuffer(col.getValue());
                cfamily.addAtom(new RangeTombstone(start, end, col.timestamp, col.localExpirationTime));
                continue;
            }
            
            assert cfm.isCQL3Table() || col.getName().hasRemaining() : "Cell name should not be empty";
            CellName cname = col.getName().hasRemaining() ? cfm.comparator.cellFromByteBuffer(col.getName()) 
                    : cfm.comparator.rowMarker(Composites.EMPTY);

            if (col.isExpiring())
            {
                cfamily.addColumn(new BufferExpiringCell(cname, col.getValue(), col.timestamp, col.ttl, col.localExpirationTime));
            }
            else if (col.isCounter())
            {
                cfamily.addColumn(new BufferCounterCell(cname, col.getValue(), col.timestamp, col.timestampOfLastDelete));
            }
            else if (col.isDeleted())
            {
                cfamily.addTombstone(cname, col.getValue(), col.timestamp);
            }
            else if (col.isRangeTombstone())
            {
                CellName end = cfm.comparator.cellFromByteBuffer(col.getValue());
                cfamily.addAtom(new RangeTombstone(cname, end, col.timestamp, col.localExpirationTime));
            }
            // cql3 row marker, see CASSANDRA-5852
            else if (cname.isEmpty())
            {
                cfamily.addColumn(cfm.comparator.rowMarker(Composites.EMPTY), col.getValue(), col.timestamp);
            }
            else
            {
                cfamily.addColumn(cname, col.getValue(), col.timestamp);
            }
        }
    }
    
    
	
	 /**
     * Get portion of the columns and serialize in loop while not more columns left in the row
     *
     * @param row SSTableIdentityIterator row representation with Column Family
     * @param key Decorated Key for the required row
     * @param out output stream
     */
    private static void serializeRow(SSTableIdentityIterator row, DecoratedKey key, String tenantId, String replacingTenantId)
    {
        serializeRow(row.getColumnFamily().deletionInfo(), row, row.getColumnFamily().metadata(), key, tenantId, replacingTenantId);
    }
    
    
    private static List<Object> serializeAtom(OnDiskAtom atom, CFMetaData cfMetaData)
    {
        if (atom instanceof Cell)
        {
            return serializeColumn((Cell) atom, cfMetaData);
        }
        else
        {
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
    
    
    private static void serializeRow(DeletionInfo deletionInfo, Iterator<OnDiskAtom> atoms, CFMetaData metadata, DecoratedKey key, String tenantId, String replacingTenantId)
    {
    	String modifiedDecoratedKey = metadata.getKeyValidator().getString(key.getKey()).replaceAll(tenantId, replacingTenantId);
    	
    	// consider delete info case too :-> perform some test cases
    	
        allColumnsForGivenRow.clear();
        List<Object> dummy = new ArrayList<Object>();
        while (atoms.hasNext())
        {
        	dummy.add(serializeAtom(atoms.next(), metadata));
        }
        allSortedModifiedRow.put(modifyDecoratedKey((SSTableIdentityIterator)atoms,modifiedDecoratedKey), dummy);
    }
    
	
	/**
     * Export an SSTable and write the resulting JSON to a PrintStream.
     *
     * @param desc     the descriptor of the sstable to read from
     * @param outs     PrintStream to write the output to
     * @param excludes keys to exclude from export
     * @param metadata Metadata to print keys in a proper format
     * @throws IOException on failure to read/write input/output
     */
    private static void readSSTable(Descriptor desc, CFMetaData metadata, String tenantId, String replacingTenantId) throws IOException
    {
        readSSTable(SSTableReader.open(desc), metadata, tenantId, replacingTenantId);
    }
	
    static void readSSTable(SSTableReader reader, CFMetaData metadata, String tenantId, String replacingTenantId) throws IOException
    {
        SSTableIdentityIterator row;
        ISSTableScanner scanner = reader.getScanner();
        try
        {
            // collecting keys to export
            while (scanner.hasNext())
            {
                row = (SSTableIdentityIterator) scanner.next();
                DecoratedKey decoratedKey = row.getKey();
                String currentKey = row.getColumnFamily().metadata().getKeyValidator().getString(row.getKey().getKey());
                System.out.println(currentKey);
                if (!doesContainsTargetKey(tenantId, decoratedKey))
                    continue;
                
                serializeRow(row, row.getKey(), tenantId, replacingTenantId);
                keyCountToImport++;
            }

        }
        finally
        {
            scanner.close();
            reader.releaseSummary();
        }
    }
	

    /**
     * Get key validator for column family
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
       * @param decorated key
       * @return modified decorated key
       */
      private static DecoratedKey modifyDecoratedKey(SSTableIdentityIterator row, String modifiedDecoratedKey){
      	IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
      	return partitioner.decorateKey(getKeyValidator(row.getColumnFamily()).fromString((String) modifiedDecoratedKey));
      }
    
    
    
    /**
     * @param tenantId
     * @param DecoratedKey
     * @return whether above decorated key contains above tenantId as a first partition key or not
     */
    public static boolean doesContainsTargetKey(String tenantId, DecoratedKey decoratedKey){
    	return firskPk(decoratedKey).equals(tenantId);
    }
    
    
    /**
     * To get the first partition key as a string
     * @param Decoratedkey
     * @return first partition key as a string  
     */
    public static String firskPk(DecoratedKey decoratedKey){
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
		
		String usage = String.format("Usage: %s <sstable> -tenantId [tenant1] -replaceWith [tenant1Staging]%n", SSTableExport.class.getName());
		
		CommandLineParser parser = new PosixParser();
		
		try{
			cmd = parser.parse(options, args);
		}catch(ParseException e1){
			System.err.println(e1.getMessage());
			System.err.println(usage);
			System.exit(1);
		}
		
		if(cmd.getArgs().length!=1){
			System.err.println("You must supply exactly one sstable");
            System.err.println(usage);
            System.exit(1);
		}
		
		String tenantId = cmd.getOptionValue(TENANT_ID);
		String replacinTenantId = cmd.getOptionValue(REPLACE_WITH);
		String ssTableFileName = new File(cmd.getArgs()[0]).getAbsolutePath();
		
		DatabaseDescriptor.loadSchemas(false);
		Descriptor descriptor = Descriptor.fromFilename(ssTableFileName);
		
		//keyspace validation
		if(Schema.instance.getKSMetaData(descriptor.ksname)==null){
			System.err.println(String.format("Filename %s references to nonexistent keyspace: %s!",
                    ssTableFileName, descriptor.ksname));
			System.exit(1);
		}
		
		Keyspace keyspace = Keyspace.open(descriptor.ksname);
		
		// make it works for indexes too - find parent cf it necessary
		String baseName = descriptor.cfname;
		if(descriptor.cfname.contains(".")){
			String[] parts = descriptor.cfname.split("\\.",2);
			baseName = parts[0];
		}
		
		String modifiedSSTablePath = System.getProperty("user.dir")+"/modifiedData/data"+"/"+keyspace.getName()+"/"+getDataDirectoryName(ssTableFileName, keyspace.getName());
		String modifiedDataDirectoryPath = modifiedSSTablePath.substring(0,modifiedSSTablePath.lastIndexOf("/"));
		
		File modifiedDataDirectory = new File(modifiedDataDirectoryPath);
		
		if(!modifiedDataDirectory.isDirectory()){
			if(modifiedDataDirectory.mkdirs()){
				System.out.println("modified Direcotry Created : " + modifiedDataDirectoryPath);
			}else{
				System.err.println("unable to create direcotry for new generated sstables.");
				System.exit(1);
			}
		}
		
		ColumnFamilyStore cfStore = null;
		
		try{
			cfStore = keyspace.getColumnFamilyStore(baseName);
		}catch(IllegalArgumentException e){
			System.err.println(String.format("The provided column family is not part of this cassandra keyspace: keyspace = %s, column family = %s",
                    descriptor.ksname, descriptor.cfname));
			System.exit(1);
		}
		
		
		try{
			readSSTable(descriptor,cfStore.metadata, tenantId, replacinTenantId);
		}catch(IOException e){
            e.printStackTrace(System.err);
		}
		
        SSTableWriter writer = new SSTableWriter(modifiedSSTablePath, keyCountToImport, ActiveRepairService.UNREPAIRED_SSTABLE);
        ColumnFamily cfamily = ArrayBackedSortedColumns.factory.create(keyspace.getName(), cfStore.getColumnFamilyName());
  
        for(Map.Entry<DecoratedKey, List<Object>> row : allSortedModifiedRow.entrySet()){
        	addColumnsToCF(row.getValue(), cfamily);
        	writer.append(row.getKey(), cfamily);
        	cfamily.clear();
        }
        writer.close();
        System.err.println("SuccessFully generated modified SSTables !!");
        System.exit(1);
	}
	
	
	/**
	 * 
	 * @param path
	 * @param keyspaceName
	 * @return last part of directory path i.e '/ks/cf'
	 */
	 private static String getDataDirectoryName(String path,String keyspaceName){
	    	List<String> allDirectories = Arrays.asList(path.split("/"+keyspaceName+"/"));
	    	if(allDirectories.size()>1){
	    		return allDirectories.get(allDirectories.size()-1);
	    	}
	    	return allDirectories.get(0);
	    }
	
}
