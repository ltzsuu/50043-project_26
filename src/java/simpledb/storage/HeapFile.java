package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
/*
This is the whole table, made up of multiple HeapPages.
The really important method here is readPage. I believe the function is well-commented, but
here is a simple explanation. I still recommend reading the function for better understanding.
Read specific page. Which page? Provided in the argument.
We set offset in bytes to find the start of the requested page. Pages start from 0 (important)
We simply multiply the size of each page with the number of the page. So if each page were 10
bytes (it's not), then page 0 would start at 0x10=0 bytes, page 5 would start at 5x10=50 bytes.
We use raf (RandomAccessFile) so we can jump to specific bytes. By jumping to specific bytes
calculated above, we jump to the start of the requested file. We then read the size of a page,
and create a new HeapPage object with the read bytes and return it.

There's also another non-trivial method, the implementation of iterator. Look for it at HeapFileIterator.java.
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;
    private RandomAccessFile raf;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;

        try {
            this.raf = new RandomAccessFile(f, "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        // file path (unique) -> hash it
        return f.getAbsoluteFile().hashCode();
        // throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
        // throw new UnsupportedOperationException("implement this");
    }
    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        // offset first
        int pageSize = getPageSize();
        // get offset of page in the file in bytes
        int offset = pid.getPageNumber() * pageSize;
        // go to offset
        try {
            raf.seek(offset);
            // create byte array to hold data of page
            byte[] data = new byte[pageSize];
            // read data from file into byte array
            raf.readFully(data);
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

        // return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // length of file divided by page size
        return (int) (f.length() / BufferPool.getPageSize());
        // return 0;
    }

    public int getPageSize() {
        // return BufferPool.DEFAULT_PAGE_SIZE;
        return 4096;
        // temp
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }


    // see DbFile.java for javadocs
    /**
     * Returns an iterator over all the tuples stored in this DbFile. The
     * iterator must use {@link BufferPool#getPage}, rather than
     * {@link #readPage} to iterate through the pages.
     *
     * @return an iterator over all the tuples stored in this DbFile.
     */
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        // return null;
        return new HeapFileIterator(this, tid);
    }

}