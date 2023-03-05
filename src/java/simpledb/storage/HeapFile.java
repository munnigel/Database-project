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
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
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
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        if (pid.getTableId() != getId()) {
            throw new IllegalArgumentException();
        }
    
        if (pid.getPageNumber() >= numPages()) {
            throw new NoSuchElementException();
        }
        long offset = pid.getPageNumber() * Database.getBufferPool().getPageSize();
        byte[] data = new byte[Database.getBufferPool().getPageSize()];
    
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            raf.seek(offset);
            raf.read(data);
            raf.close();
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // HeapPageId pid = page.getId();
        // byte[] data = page.getPageData();
        // try{
        //     RandomAccessFile raf = new RandomAccessFile(file, "rw");
        //     int offset = pid.getPageNumber() * Database.getBufferPool().getPageSize();
        //     raf.seek(offset);
        //     raf.write(data);
        //     raf.close();
        // } catch (IOException e) {
        //     throw new IllegalArgumentException();
        // }
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
            raf.write(page.getPageData());
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil(file.length() / (double) Database.getBufferPool().getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
                ArrayList<Page> modifiedPages = new ArrayList<>();
                for (int currentPageNo = 0; currentPageNo < this.numPages(); currentPageNo++) {
                    HeapPageId pageId = new HeapPageId(this.getId(), currentPageNo);
                    HeapPage currentPage = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                    if (currentPage.getNumEmptySlots() > 0) {
                        currentPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
                        currentPage.insertTuple(t);
                        modifiedPages.add(currentPage);
                        break;
                    } else {
                        Database.getBufferPool().unsafeReleasePage(tid, pageId);
                    }
                }
                if (modifiedPages.isEmpty()) {
                    HeapPage newPage = new HeapPage(new HeapPageId(getId(), numPages()), new byte[BufferPool.getPageSize()]);
                    newPage.insertTuple(t);
                    this.writePage(newPage);
                    modifiedPages.add(newPage);
                }
        
                return modifiedPages;
            }
        

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
                PageId pid = t.getRecordId().getPageId();
                ArrayList<Page> affectedPages = new ArrayList<>();
                for (int i = 0; i < numPages(); i++) {
                    if (i == pid.getPageNumber()) {
                        HeapPage affectedPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                        affectedPage.deleteTuple(t);
                        affectedPages.add(affectedPage);
                        break;
                    }
                }
                if (affectedPages.isEmpty()) {
                    throw new DbException("Tuple " + t + " is not in this table.");
                }
                return affectedPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this.getId(), tid, this.numPages()); //Muzi u need to change this. Jamestiotio added a new HeapFileIterator class, but we dont have one
    }

}

