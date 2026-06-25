package simpledb.common;

import simpledb.common.Type;
import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
/*
The Catalog class is just the librarian of the database.
Honestly, not much to it.
It just initialises 4 maps that point from name to Id, then Id to the rest of the relevant
-fields.
Most of the methods are trivial. loadSchema() was provided.
addTable(): Adds a table to the catalog.
There's a check in front to see if the name already exists, and if it does, remove it so we 
can make a new one.
We then update all 4 fields so the Catalog remains updated and able to provide proper 
information.
*/
public class Catalog {

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */

    private Map<String, Integer> nameToId;
    private Map<Integer, DbFile> idToFile;
    private Map<Integer, TupleDesc> idToTupleDesc;
    private Map<Integer, String> idToPkeyField;
    
    public Catalog() {
        // some code goes here
        this.nameToId = new HashMap<>();
        this.idToFile = new HashMap<>();
        this.idToTupleDesc = new HashMap<>();
        this.idToPkeyField = new HashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * If there exists a table with the same name or ID, replace that old table with this one. 
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile. 
     * @param name the name of the table -- may be an empty string.  May not be null.  
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        if (name == null) {
            throw new IllegalArgumentException("Table name cannot be null");
        }
        int tableId = file.getId();
        // handle dupe names
        if (this.nameToId.containsKey(name)) {
            int oldTableId = this.nameToId.get(name);
            this.idToFile.remove(oldTableId);
            this.idToTupleDesc.remove(oldTableId);
            this.idToPkeyField.remove(oldTableId);
        }
        if (this.idToFile.containsKey(tableId)) {
            String oldName =  getTableName(tableId);
            if (oldName != null) {
                this.nameToId.remove(oldName);
            }
        }
        this.nameToId.put(name, tableId);
        this.idToFile.put(tableId, file);
        this.idToTupleDesc.put(tableId, file.getTupleDesc());
        this.idToPkeyField.put(tableId, pkeyField);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        Integer id = this.nameToId.get(name);
        if (id == null) {
            throw new NoSuchElementException("Table not found: " + name);
        }
        return id;
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        TupleDesc tupleDesc = this.idToTupleDesc.get(tableid);
        if (tupleDesc == null) {
            throw new NoSuchElementException("Table not found: " + tableid);
        }
        return tupleDesc;
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        DbFile dbFile = this.idToFile.get(tableid);
        if (dbFile == null) {
            throw new NoSuchElementException("Table not found: " + tableid);
        }
        return dbFile;
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        return this.idToPkeyField.get(tableid);
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        // return null;
        return this.idToFile.keySet().iterator();
    }

    public String getTableName(int id) {
        // some code goes here
        // return null;
        for (Map.Entry<String, Integer> entry : this.nameToId.entrySet()) {
            if (entry.getValue().equals(id)) {
                return entry.getKey();
            }
        }
        return null;
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        this.nameToId.clear();
        this.idToFile.clear();
        this.idToTupleDesc.clear();
        this.idToPkeyField.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

