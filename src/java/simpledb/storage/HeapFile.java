package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
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

    private final File file;
    private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
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
//        throw new UnsupportedOperationException("implement this");
        return this.file.getAbsolutePath().hashCode();
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
        int pageNumber = pid.getPageNumber();
        int tableId = pid.getTableId();
        int pageSize = BufferPool.getPageSize();
        RandomAccessFile randomAccess = null;
        try {
            randomAccess = new RandomAccessFile(this.file, "r");
            byte[] buffer = new byte[pageSize];
            randomAccess.seek(pageSize * pageNumber);
            randomAccess.read(buffer);
            return new HeapPage((HeapPageId) pid, buffer);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            try {
                randomAccess.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        }


        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pid = page.getId();

        int pageNumber = pid.getPageNumber();
        int pageSize = BufferPool.getPageSize();
        RandomAccessFile randomAccess = null;
        try {
            randomAccess = new RandomAccessFile(this.file, "rw");
            byte[] buffer = page.getPageData();
            randomAccess.seek(pageSize * pageNumber);
            randomAccess.write(buffer);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            try {
                randomAccess.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int pageSize = BufferPool.getPageSize();

        return (int) Math.ceil (file.length() * 1.0 / pageSize);
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
//        Database.getBufferPool().getPage()
        for (int num = 0; num < this.numPages(); num++) {
            HeapPageId heapPageId = new HeapPageId(this.getId(), num);
            // hashmap是引用，会修改原来的pool中的page
            HeapPage heapPage = (HeapPage)Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            if(heapPage.getNumEmptySlots() != 0){
                heapPage.insertTuple(t);
                return new ArrayList<Page>(Arrays.asList(heapPage));
            }
        }
        // 当所有的页都满时,我们需要创建新的页并写入文件中
        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file, true));
        byte[] emptyPageData = HeapPage.createEmptyPageData();
        // 向文件末尾添加数据
        outputStream.write(emptyPageData);
        outputStream.close();

        // 添加完成后的最后一页
        HeapPageId heapPageId = new HeapPageId(this.getId(), this.numPages() - 1);
        HeapPage heapPage = (HeapPage)Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);

        heapPage.insertTuple(t);
//        writePage(heapPage);  不写回

        return new ArrayList<Page>(Arrays.asList(heapPage));

        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        int pageNum = t.getRecordId().getPageId().getPageNumber();
        if(pageNum < 0 || pageNum > this.numPages()){
            throw new DbException("Tuple has a invalid page num");
        }

        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);

        return new ArrayList<Page>(Arrays.asList(page));
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    private class HeapFileIterator implements  DbFileIterator{

        private final HeapFile heapFile;
        private final TransactionId tid;
        private int pageNum = 0;
        private Iterator<Tuple> iterator;

        public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
            this.heapFile = heapFile;
            this.tid = tid;

        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pageNum = 0;
            HeapPageId pid = new HeapPageId(heapFile.getId(), pageNum);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            iterator = page.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (iterator == null){
                return false;
            }

            while (!iterator.hasNext()){
                pageNum ++ ;
                if (pageNum >= heapFile.numPages()){
                    return false;
                }
                HeapPageId pid = new HeapPageId(heapFile.getId(), pageNum);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                iterator = page.iterator();
            }


            return iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (iterator == null || !iterator.hasNext()){
                throw new NoSuchElementException("No next tuple");
            }

            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        public void close() {
            iterator = null;
        }
    }

}

