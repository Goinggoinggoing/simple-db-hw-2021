package simpledb.storage.evict;

import simpledb.storage.PageId;

import java.util.*;

public class FIFOEvict implements EvictStrategy{

    Queue<PageId> queue;

    Set<PageId> set;

    public FIFOEvict(){
        queue = new ArrayDeque<>();
        set = new HashSet<>();
    }

    @Override
    public void modifyData(PageId pageId) {
        if (set.contains(pageId)) return ;
        queue.offer(pageId);
        set.add(pageId);
    }

    @Override
    public PageId getEvictPageId() {
        PageId peek = queue.poll();
        set.remove(peek);
//        System.out.println("evict " + peek.getPageNumber());
        return peek;
    }
}
