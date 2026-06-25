package simpledb.storage;

import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.xml.crypto.Data;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/*
Just implementing the abstract methods of DbFileIterator.
We use this to get all the rows (tuples) in the table, 
jumping across pages to retrieve what we need.
We have 4 fields here.
HeapFile is the table we're scanning. Go back to HeapFile.java and read the comment if 
you're unsure of what this means.
Transaction ID: just a identifier to track which request is currently happening.
currentPageIndex: think this is pretty obvious. Just how far along we are.
currentTupeIterator: Same thing as page index, but an index for tuples. Which row in the 
page in the table are we currently at? The indexing for tuples is local to each page,
so we need the two indexes instead of just one.
think with those defined it should be pretty obvious.
With open(), we start reading from page 0. We set the current tuple iterator to position 0
on page 0.
With hasNext() (this function does most of the heavy lifting):
some basic checks: if close(), or if invalid output from open(), then we just say there's no next.
If we're not done iterating through the tuples in the current page, we return true. There is a next.
Else, that means we're done with the current page, but we don't know if there's a next page.
We use a while statement here, as if the loop is completed without returning, that implies there's
still a next page, but the current page has no entries.
We constantly increment the page index while checking if there's tuples to read in the current page.
When we land on the right page with tuples to read, we return true.
The rest of the methods are pretty trivial.
 */
public class HeapFileIterator implements DbFileIterator {

    private HeapFile heapFile;
    private TransactionId tid;
    private int currentPageIndex;
    private Iterator<Tuple> currentTupleIterator;

    public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
        this.heapFile = heapFile;
        this.tid = tid;
        this.currentPageIndex = 0;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        // TODO Auto-generated method stub
        // throw new UnsupportedOperationException("Unimplemented method 'open'");
        currentPageIndex = 0;
        if (currentPageIndex < heapFile.numPages()) {
            PageId pid = new HeapPageId(heapFile.getId(), currentPageIndex);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            currentTupleIterator = page.iterator();
        } else {
            currentTupleIterator = null;
        }
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        // TODO Auto-generated method stub
        //throw new UnsupportedOperationException("Unimplemented method 'hasNext'");
        if (currentTupleIterator == null) {
            return false;
        }
        if (currentTupleIterator.hasNext()) return true;
        while (++currentPageIndex < heapFile.numPages())
        {
            HeapPageId pid = new HeapPageId(heapFile.getId(), currentPageIndex);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            currentTupleIterator = page.iterator();
            if (currentTupleIterator.hasNext()) return true; 
        }
        return false;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        // TODO Auto-generated method stub
        // throw new UnsupportedOperationException("Unimplemented method 'next'");
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return currentTupleIterator.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        // TODO Auto-generated method stub
        //throw new UnsupportedOperationException("Unimplemented method 'rewind'");
        open();

    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        // throw new UnsupportedOperationException("Unimplemented method 'close'");
        currentTupleIterator = null;

    }
    

}
