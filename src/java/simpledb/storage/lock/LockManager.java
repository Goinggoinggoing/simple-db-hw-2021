package simpledb.storage.lock;

import simpledb.common.Permissions;
import simpledb.storage.Page;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    private Map<PageId, Map<TransactionId, LockInfo>> pageLockMap;

    private Map<TransactionId, TidNode> tidMap;

    public LockManager(){
        pageLockMap = new ConcurrentHashMap<>();
        tidMap = new ConcurrentHashMap<>();
    }

    public synchronized boolean acquire(PageId pid, TransactionId tid, Permissions perm) throws TransactionAbortedException {
        Map<TransactionId, LockInfo> locks = pageLockMap.getOrDefault(pid, new ConcurrentHashMap<TransactionId, LockInfo>());

        Iterator<Map.Entry<TransactionId, LockInfo>> iterator = locks.entrySet().iterator();

        while (iterator.hasNext()){
            Map.Entry<TransactionId, LockInfo> next = iterator.next();
            if (next.getKey().equals(tid)){
                continue;
            }

            if (next.getValue().getLockType() == LockType.EXCLUSIVE || perm == Permissions.READ_WRITE){
                TidNode tidNow = tidMap.getOrDefault(tid, new TidNode(tid));
                tidNow.next = tidMap.get(next.getValue().getTid());
                checkCycle(new HashSet<TidNode>(), tidNow);

                return false;
            }
        }
        locks.put(tid, new LockInfo(pid, tid, perm));
        // 第一次访问时要写回
        pageLockMap.put(pid, locks);
        tidMap.put(tid, new TidNode(tid));
        return true;
    }

    // 如果有环路则抛出异常
    private void checkCycle(HashSet<TidNode> visited, TidNode tidNode) throws TransactionAbortedException {
        if(visited.contains(tidNode)){
            throw new TransactionAbortedException();
        }
        visited.add(tidNode);
        if(tidNode.next != null){
            checkCycle(visited, tidNode.next);
        }
    }

    public synchronized void release(TransactionId tid, PageId pid){
        Map<TransactionId, LockInfo> locks = pageLockMap.getOrDefault(pid, new ConcurrentHashMap<TransactionId, LockInfo>());

        if (locks.containsKey(tid)) {
            locks.remove(tid);
        }
        return ;
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid){
        Map<TransactionId, LockInfo> locks = pageLockMap.getOrDefault(pid, new ConcurrentHashMap<TransactionId, LockInfo>());

        return locks.containsKey(tid);
    }

    public synchronized void completeTransaction(TransactionId tid){
        for (PageId pageId : pageLockMap.keySet()) {
            release(tid, pageId);
        }
        tidMap.remove(tid);

    }

}


class LockInfo{

    private PageId pid;
    private TransactionId tid;
    LockType lockType;

    public LockInfo(PageId pid, TransactionId tid, Permissions perm) {

        this.pid = pid;
        this.tid = tid;
        if(perm == Permissions.READ_ONLY){
            lockType = LockType.SHARE;
        }else{
            lockType = LockType.EXCLUSIVE;
        }
    }

    public PageId getPid() {
        return pid;
    }

    public TransactionId getTid() {
        return tid;
    }

    public LockType getLockType() {
        return lockType;
    }

    @Override
    public String toString() {
        return "LockInfo{" +
                "pid=" + pid +
                ", tid=" + tid +
                ", lockType=" + lockType +
                '}';
    }
}

enum LockType {
    SHARE,
    EXCLUSIVE
}

// next代表当前tid在等待他
class TidNode{
    TidNode next;

    TransactionId tid;

    public TidNode(TransactionId tid){
        this.tid = tid;
    }
}