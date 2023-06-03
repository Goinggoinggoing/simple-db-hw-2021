package simpledb.storage.evict;

import simpledb.storage.PageId;

import java.util.HashMap;
import java.util.LinkedHashMap;

public class LRUEvict implements EvictStrategy{

    Node tail;
    Node head;
    HashMap<PageId, Node> hashMap;

    public LRUEvict(){
        hashMap = new HashMap<>();
        tail = new Node(null);
        head = new Node(null);
        head.next = tail;
        tail.prev = head;
    }

    @Override
    public void modifyData(PageId pageId) {
        if(hashMap.containsKey(pageId)){
            Node node = hashMap.get(pageId);
            moveToHead(node);
        }else{
            Node node = new Node(pageId);
            addHead(node);
            hashMap.put(pageId, node);
        }
    }
    @Override
    public PageId getEvictPageId() {
        Node prev = tail.prev;
        hashMap.remove(prev.pageId);
        removeNode(prev);
//        System.out.println(prev.pageId.getPageNumber());

        return prev.pageId;
    }

    public void addHead(Node node){
        node.next = head.next;
        head.next.prev = node;
        node.prev = head;
        head.next = node;
    }

    public void moveToHead(Node node){
        removeNode(node);
        addHead(node);
    }

    private void removeNode(Node node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    class Node {
        PageId pageId;
        Node prev;
        Node next;
        public Node(PageId pageId) {
            this.pageId = pageId;
        }
    }

}
