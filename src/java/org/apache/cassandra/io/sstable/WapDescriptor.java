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
package org.apache.cassandra.io.sstable;

import static org.apache.cassandra.io.sstable.Component.separator;

import java.io.File;
import java.util.StringTokenizer;

import org.apache.cassandra.io.sstable.Descriptor.Type;
import org.apache.cassandra.io.sstable.Descriptor.Version;
import org.apache.cassandra.utils.Pair;

/**
 * A SSTable is described by the keyspace and column family it contains data
 * for, a generation (where higher generations contain more recent data) and
 * an alphabetic version string.
 *
 * A descriptor can be marked as temporary, which influences generated filenames.
 */
public class WapDescriptor
{
    public static Descriptor fromFilename(String filename, String ksname)
    {
        File file = new File(filename);
        return fromFilename(file.getParentFile(), file.getName(), false, ksname).left;
    }
    
    
    /**
     * Filename of the form "<ksname>-<cfname>-[tmp-][<version>-]<gen>-<component>"
     *
     * @param directory The directory of the SSTable files
     * @param name The name of the SSTable file
     * @param skipComponent true if the name param should not be parsed for a component tag
     *
     * @return A Descriptor for the SSTable, and the Component remainder.
     */
    public static Pair<Descriptor,String> fromFilename(File directory, String name, boolean skipComponent ,String ksName)
    {
        // tokenize the filename
        StringTokenizer st = new StringTokenizer(name, String.valueOf(separator));
        String nexttok;

        // all filenames must start with keyspace and column family
        String ksname = st.nextToken();
        String cfname = st.nextToken();
        
        
        // overirding ks name
        ksname = ksName;
        // optional temporary marker
        nexttok = st.nextToken();
        Type type = Type.FINAL;
        if (nexttok.equals(Type.TEMP.marker))
        {
            type = Type.TEMP;
            nexttok = st.nextToken();
        }
        else if (nexttok.equals(Type.TEMPLINK.marker))
        {
            type = Type.TEMPLINK;
            nexttok = st.nextToken();
        }

        if (!Version.validate(nexttok))
            throw new UnsupportedOperationException("SSTable " + name + " is too old to open.  Upgrade to 2.0 first, and run upgradesstables");
        Version version = new Version(nexttok);

        nexttok = st.nextToken();
        int generation = Integer.parseInt(nexttok);

        // component suffix
        String component = null;
        if (!skipComponent)
            component = st.nextToken();
        directory = directory != null ? directory : new File(".");
        return Pair.create(new Descriptor(version, directory, ksname, cfname, generation, type), component);
    }
    
  }
