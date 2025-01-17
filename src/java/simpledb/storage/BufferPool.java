package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.storage.evict.EvictStrategy;
import simpledb.storage.evict.FIFOEvict;
import simpledb.storage.evict.LRUEvict;
import simpledb.storage.lock.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private Integer numPages;
    private Map<PageId, Page> pageCache;
    private LockManager lockManager;

    EvictStrategy evict;


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.pageCache = new ConcurrentHashMap<PageId, Page>();
//        this.evict = new FIFOEvict();
        this.evict = new LRUEvict();
        this.lockManager = new LockManager();

    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {

//        long start = System.currentTimeMillis();
//        long timeout = new Random().nextInt(2000) + 1000;
//        while (true) {
//            if (lockManager.acquire(pid, tid, perm)) {
//                break;
//            }
//            long now = System.currentTimeMillis();
//            if (now - start > timeout) {
//                throw new TransactionAbortedException();
//            }
//            try {
//                Thread.sleep(100);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
        while(!lockManager.acquire(pid, tid, perm)){
            try {
                Thread.sleep(new Random().nextInt(100));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        evict.modifyData(pid);
        if (!pageCache.containsKey(pid)) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            if (pageCache.size() == numPages) {
                evictPage();
            }
            pageCache.put(pid, page);
        }
        return pageCache.get(pid);
        // some code goes here
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.release(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else {
            recoverPages(tid);
        }
        lockManager.completeTransaction(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        DbFile table = Database.getCatalog().getDatabaseFile(tableId);

//        t.setRecordId(t.getRecordId().getPageId());
        updateBufferPool(table.insertTuple(tid, t), tid);

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile table = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        updateBufferPool(table.deleteTuple(tid, t), tid);
    }

    private void updateBufferPool(List<Page> pages, TransactionId tid) throws DbException {
        for (Page page : pages) {
            page.markDirty(true, tid);
//            if (pageCache.size() == numPages) {
//                evictPage();
//            }
            pageCache.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Iterator<Map.Entry<PageId, Page>> iterator = pageCache.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<PageId, Page> entry = iterator.next();
            PageId pageId = entry.getKey();
            Page page = entry.getValue();
            if (page.isDirty() != null){
                flushPage(pageId);
                iterator.remove();
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        if (pageCache.containsKey(pid)) {
            pageCache.remove(pid);
        }
    }

    private synchronized void recoverPages(TransactionId tid){
        Iterator<Map.Entry<PageId, Page>> iterator = pageCache.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<PageId, Page> entry = iterator.next();
            PageId pageId = entry.getKey();
            Page page = entry.getValue();
            if (page.isDirty() == tid){
                discardPage(pageId);
                DbFile dbFile = Database.getCatalog().getDatabaseFile(pageId.getTableId());
                page = dbFile.readPage(pageId);
                pageCache.put(pageId, page);
            }
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        DbFile table = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = this.pageCache.get(pid);

        TransactionId dirtier = page.isDirty();
        if (dirtier != null){
            Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
            Database.getLogFile().force();
        }

        if (page.isDirty() != null){
            table.writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        Iterator<Map.Entry<PageId, Page>> iterator = pageCache.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<PageId, Page> entry = iterator.next();
            PageId pageId = entry.getKey();
            Page page = entry.getValue();
            if (page.isDirty() == tid){
                flushPage(pageId);
                // use current page contents as the before-image
                // for the next transaction that modifies this page.
                page.setBeforeImage();
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        PageId evictPageId = evict.getEvictPageId();
        Page page = pageCache.get(evictPageId);
        // 有可能pool中该page已经被丢弃了，如rollback
        while (page == null){
            page = pageCache.get(evict.getEvictPageId());
        }
        if (page.isDirty() != null){
            try {
                flushPage(evictPageId);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        pageCache.remove(evictPageId);

        // NO STEAL  for lab4
//        for (int i = 0; i < pageCache.size(); i++) {
//            PageId evictPageId = evict.getEvictPageId();
//            Page page = pageCache.get(evictPageId);
//            if (page.isDirty() == null){
//                pageCache.remove(evictPageId);
//                return ;
//            }
//            evict.modifyData(page.getId());
//        }
//
//        throw new DbException("no clean page to evict");
    }

}
