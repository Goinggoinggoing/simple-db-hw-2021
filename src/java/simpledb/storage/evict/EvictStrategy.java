package simpledb.storage.evict;

import simpledb.storage.Page;
import simpledb.storage.PageId;

public interface EvictStrategy {

    void modifyData(PageId pageId);

    PageId getEvictPageId();
}
