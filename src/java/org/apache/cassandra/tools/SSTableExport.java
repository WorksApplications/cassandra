/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
import org.apache.cassandra.io.sstable.KeyIterator;
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
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Export SSTables to JSON format.
 */
public class SSTableExport
{
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    private static final String KEY_OPTION = "k";
    private static final String EXCLUDEKEY_OPTION = "x";
    private static final String ENUMERATEKEYS_OPTION = "e";
    private static boolean isSorted=false;
    private static Integer keyCountToImport = 0;
    private static String ssTablePath = "/home/barala/data/data/sstableloadertest/typestest-f36ad9d0f4a711e5807e55bc51b0510e/sstableloadertest-typestest-ka-100-Data.db"; 
    private static final Options options = new Options();
    private static CommandLine cmd;
    private static List<Object> allColumnsForGivenRow = new ArrayList<Object>();
    private static Object decoratedKey;
    static
    {
        Option optKey = new Option(KEY_OPTION, true, "Row key");
        // Number of times -k <key> can be passed on the command line.
        optKey.setArgs(500);
        options.addOption(optKey);

        Option excludeKey = new Option(EXCLUDEKEY_OPTION, true, "Excluded row key");
        // Number of times -x <key> can be passed on the command line.
        excludeKey.setArgs(500);
        options.addOption(excludeKey);

        Option optEnumerate = new Option(ENUMERATEKEYS_OPTION, false, "enumerate keys only");
        options.addOption(optEnumerate);

        // disabling auto close of the stream
        jsonMapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    }

    /**
     * Checks if PrintStream error and throw exception
     *
     * @param out The PrintStream to be check
     */
    private static void checkStream(PrintStream out) throws IOException
    {
        if (out.checkError())
            throw new IOException("Error writing output stream");
    }

    /**
     * JSON Hash Key serializer
     *
     * @param out   The output steam to write data
     * @param value value to set as a key
     */
    private static void writeKey(PrintStream out, String value)
    {
        writeJSON(out, value);
        out.print(": ");
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
     * Get portion of the columns and serialize in loop while not more columns left in the row
     *
     * @param row SSTableIdentityIterator row representation with Column Family
     * @param key Decorated Key for the required row
     * @param out output stream
     */
    private static void serializeRow(SSTableIdentityIterator row, DecoratedKey key, PrintStream out)
    {
        serializeRow(row.getColumnFamily().deletionInfo(), row, row.getColumnFamily().metadata(), key, out);
    }

    /**
     * to create modified sstable from current sstable
     */
    private static void createSSTable(String keyspace, String cf, String ssTablePath, SSTableIdentityIterator row){
    	 if (Schema.instance.getCFMetaData(keyspace, cf) == null)
             throw new IllegalArgumentException(String.format("Unknown keyspace/table %s.%s",
                                                              keyspace,
                                                              cf));

         ColumnFamily columnFamily = ArrayBackedSortedColumns.factory.create(keyspace, cf);
         IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
         
     //    int importedKeys = (isSorted) ? importSorted(row, columnFamily, ssTablePath, partitioner)
     //            : importUnsorted(row, columnFamily, ssTablePath, partitioner);

       //  if (importedKeys != -1)
        //	 System.out.printf("%d keys imported successfully.%n", importedKeys);
    }
    
    
    // You will initialize once so you won't call it again and again 
    // write inside the main logic
    
    
    private static void serializeRow(DeletionInfo deletionInfo, Iterator<OnDiskAtom> atoms, CFMetaData metadata, DecoratedKey key, PrintStream out)
    {
    	decoratedKey = metadata.getKeyValidator().getString(key.getKey()).replaceAll("tenant1", "tenant1Staging");
        out.print("{");
        writeKey(out, "key");
        writeJSON(out, metadata.getKeyValidator().getString(key.getKey()));
        out.print(",\n");

        if (!deletionInfo.isLive())
        {
            out.print(" ");
            writeKey(out, "metadata");
            out.print("{");
            writeKey(out, "deletionInfo");
            writeJSON(out, deletionInfo.getTopLevelDeletion());
            out.print("}");
            out.print(",\n");
        }

        out.print(" ");
        writeKey(out, "cells");
        out.print("[");
        allColumnsForGivenRow.clear();
        while (atoms.hasNext())
        {
            //writeJSON(out, serializeAtom(atoms.next(), metadata));
        	allColumnsForGivenRow.add(serializeAtom(atoms.next(), metadata));
            if (atoms.hasNext())
                out.print(",\n           ");
        }
        out.print("]");

        out.print("}");
    }

    /**
     * Enumerate row keys from an SSTableReader and write the result to a PrintStream.
     *
     * @param desc the descriptor of the file to export the rows from
     * @param outs PrintStream to write the output to
     * @param metadata Metadata to print keys in a proper format
     * @throws IOException on failure to read/write input/output
     */
    public static void enumeratekeys(Descriptor desc, PrintStream outs, CFMetaData metadata)
    throws IOException
    {
        KeyIterator iter = new KeyIterator(desc);
        try
        {
            DecoratedKey lastKey = null;
            while (iter.hasNext())
            {
                DecoratedKey key = iter.next();

                // validate order of the keys in the sstable
                if (lastKey != null && lastKey.compareTo(key) > 0)
                    throw new IOException("Key out of order! " + lastKey + " > " + key);
                lastKey = key;

                outs.println(metadata.getKeyValidator().getString(key.getKey()));
                checkStream(outs); // flushes
            }
        }
        finally
        {
            iter.close();
        }
    }

    /**
     * Export specific rows from an SSTable and write the resulting JSON to a PrintStream.
     *
     * @param desc     the descriptor of the sstable to read from
     * @param outs     PrintStream to write the output to
     * @param toExport the keys corresponding to the rows to export
     * @param excludes keys to exclude from export
     * @param metadata Metadata to print keys in a proper format
     * @throws IOException on failure to read/write input/output
     */
    public static void export(Descriptor desc, PrintStream outs, Collection<String> toExport, String[] excludes, CFMetaData metadata) throws IOException
    {
        SSTableReader sstable = SSTableReader.open(desc);
        RandomAccessReader dfile = sstable.openDataReader();
        try
        {
            IPartitioner partitioner = sstable.partitioner;

            if (excludes != null)
                toExport.removeAll(Arrays.asList(excludes));

            outs.println("[");

            int i = 0;

            // last key to compare order
            DecoratedKey lastKey = null;

            for (String key : toExport)
            {
                DecoratedKey decoratedKey = partitioner.decorateKey(metadata.getKeyValidator().fromString(key));

                if (lastKey != null && lastKey.compareTo(decoratedKey) > 0)
                    throw new IOException("Key out of order! " + lastKey + " > " + decoratedKey);

                lastKey = decoratedKey;

                RowIndexEntry entry = sstable.getPosition(decoratedKey, SSTableReader.Operator.EQ);
                if (entry == null)
                    continue;

                dfile.seek(entry.position);
                ByteBufferUtil.readWithShortLength(dfile); // row key
                DeletionInfo deletionInfo = new DeletionInfo(DeletionTime.serializer.deserialize(dfile));

                Iterator<OnDiskAtom> atomIterator = sstable.metadata.getOnDiskIterator(dfile, sstable.descriptor.version);
                checkStream(outs);

                if (i != 0)
                    outs.println(",");
                i++;
                serializeRow(deletionInfo, atomIterator, sstable.metadata, decoratedKey, outs);
            }

            outs.println("\n]");
            outs.flush();
        }
        finally
        {
            dfile.close();
        }
    }

    // This is necessary to accommodate the test suite since you cannot open a Reader more
    // than once from within the same process.
    static void export(SSTableReader reader, PrintStream outs, String[] excludes, CFMetaData metadata) throws IOException
    {
        Set<String> excludeSet = new HashSet<String>();

        if (excludes != null)
            excludeSet = new HashSet<String>(Arrays.asList(excludes));

        SSTableIdentityIterator row;
        ISSTableScanner scanner = reader.getScanner();
        try
        {
            outs.println("[");

            int i = 0;

            // collecting keys to export
            
            // create sstable writer here
            SSTableWriter writer = new SSTableWriter(ssTablePath, keyCountToImport, ActiveRepairService.UNREPAIRED_SSTABLE);
            IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
            
            // write a function which will take two argument:- one row and one writer
            // this above function will need one more argument to 
            
//            SortedMap<DecoratedKey,Map<?, ?>> decoratedKeys = new TreeMap<DecoratedKey,Map<?, ?>>(); // I can create this during readin the sstable //

            
            // here it puts all the rows in sstable
//            for (Object row : data)
//            {
//                Map<?,?> rowAsMap = (Map<?, ?>)row;
//                decoratedKeys.put(partitioner.decorateKey(getKeyValidator(columnFamily).fromString((String) rowAsMap.get("key"))), rowAsMap); // this function is useful to generate modified decorated key
//                // this function creates modified decoratedKey and store them 
//            }
//            
            
            while (scanner.hasNext())
            {
                row = (SSTableIdentityIterator) scanner.next();
                
                String currentKey = row.getColumnFamily().metadata().getKeyValidator().getString(row.getKey().getKey());

                if (excludeSet.contains(currentKey) || !doesContainsTargetKey("tenant1", row.getKey()))
                    continue;
                else if (i != 0)
                    outs.println(",");

                serializeRow(row, row.getKey(), outs);
                insertCurrentRowInSSTable(writer,row);
                checkStream(outs);

                i++;
            }

            outs.println("\n]");
            outs.flush();
            writer.closeAndOpenReader();
        }
        finally
        {
            scanner.close();
        }
    }

    /**
     * 
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
    
    
    private static void insertCurrentRowInSSTable(SSTableWriter writer, SSTableIdentityIterator row) {
		
		// create row to abstarct row ?
		//modify key here and 
		// add column function here
		//addColumns(); this function will deserialize each column and add that column to 
    	// copy that function here
    	writer.append(modifyDecoratedKey(row), row.getColumnFamily());
    	addColumnsToCF((List<?>) allColumnsForGivenRow, row.getColumnFamily());
    	
	}

    /**
     * @param decorated key
     * @return modified decorated key
     */
    private static DecoratedKey modifyDecoratedKey(SSTableIdentityIterator row){
    	IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
    	Object key = new String(row.getKey().getKey().array()).replaceAll("tenant1", "tenant1");
    	System.out.println(key);
    	return partitioner.decorateKey(getKeyValidator(row.getColumnFamily()).fromString((String) decoratedKey));
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
     * Export an SSTable and write the resulting JSON to a PrintStream.
     *
     * @param desc     the descriptor of the sstable to read from
     * @param outs     PrintStream to write the output to
     * @param excludes keys to exclude from export
     * @param metadata Metadata to print keys in a proper format
     * @throws IOException on failure to read/write input/output
     */
    public static void export(Descriptor desc, PrintStream outs, String[] excludes, CFMetaData metadata) throws IOException
    {
        export(SSTableReader.open(desc), outs, excludes, metadata);
    }

    /**
     * Export an SSTable and write the resulting JSON to standard out.
     *
     * @param desc     the descriptor of the sstable to read from
     * @param excludes keys to exclude from export
     * @param metadata Metadata to print keys in a proper format
     * @throws IOException on failure to read/write SSTable/standard out
     */
    public static void export(Descriptor desc, String[] excludes, CFMetaData metadata) throws IOException
    {
        export(desc, System.out, excludes, metadata);
    }

    /**
     * Given arguments specifying an SSTable, and optionally an output file,
     * export the contents of the SSTable to JSON.
     *
     * @param args command lines arguments
     * @throws IOException            on failure to open/read/write files or output streams
     * @throws ConfigurationException on configuration failure (wrong params given)
     */
    public static void main(String[] args) throws ConfigurationException
    {
        String usage = String.format("Usage: %s <sstable> [-k key [-k key [...]] -x key [-x key [...]]]%n", SSTableExport.class.getName());

        CommandLineParser parser = new PosixParser();
        try
        {
            cmd = parser.parse(options, args);
        }
        catch (ParseException e1)
        {
            System.err.println(e1.getMessage());
            System.err.println(usage);
            System.exit(1);
        }


        if (cmd.getArgs().length != 1)
        {
            System.err.println("You must supply exactly one sstable");
            System.err.println(usage);
            System.exit(1);
        }


        String[] keys = cmd.getOptionValues(KEY_OPTION);
        String[] excludes = cmd.getOptionValues(EXCLUDEKEY_OPTION);
        String ssTableFileName = new File(cmd.getArgs()[0]).getAbsolutePath();

        DatabaseDescriptor.loadSchemas(false);
        Descriptor descriptor = Descriptor.fromFilename(ssTableFileName);

        // Start by validating keyspace name
        if (Schema.instance.getKSMetaData(descriptor.ksname) == null)
        {
            System.err.println(String.format("Filename %s references to nonexistent keyspace: %s!",
                                             ssTableFileName, descriptor.ksname));
            System.exit(1);
        }
        Keyspace keyspace = Keyspace.open(descriptor.ksname);

        // Make it works for indexes too - find parent cf if necessary
        String baseName = descriptor.cfname;
        if (descriptor.cfname.contains("."))
        {
            String[] parts = descriptor.cfname.split("\\.", 2);
            baseName = parts[0];
        }

        // IllegalArgumentException will be thrown here if ks/cf pair does not exist
        ColumnFamilyStore cfStore = null;
        try
        {
            cfStore = keyspace.getColumnFamilyStore(baseName);
        }
        catch (IllegalArgumentException e)
        {
            System.err.println(String.format("The provided column family is not part of this cassandra keyspace: keyspace = %s, column family = %s",
                                             descriptor.ksname, descriptor.cfname));
            System.exit(1);
        }

        try
        {
            if (cmd.hasOption(ENUMERATEKEYS_OPTION))
            {
                enumeratekeys(descriptor, System.out, cfStore.metadata);
            }
            else
            {
                if ((keys != null) && (keys.length > 0))
                    export(descriptor, System.out, Arrays.asList(keys), excludes, cfStore.metadata);
                else
                    export(descriptor, excludes, cfStore.metadata);
            }
        }
        catch (IOException e)
        {
            // throwing exception outside main with broken pipe causes windows cmd to hang
            e.printStackTrace(System.err);
        }

        System.exit(0);
    }

    private static void writeJSON(PrintStream out, Object value)
    {
        try
        {
            jsonMapper.writeValue(out, value);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
