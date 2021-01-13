package net.kaaass.rumbase.page;

import net.kaaass.rumbase.page.exception.BufferException;

import java.util.HashMap;
import java.util.Map;

public class Replacer {
    private static Replacer instance = null;
    private Replacer() {
        this.head = null;
        this.tail = null;
        this.table = new HashMap<>();
    }

    public static Replacer getInstance() {
        if (instance == null) {
            synchronized (Replacer.class) {
                if (instance == null) {
                    instance = new Replacer();
                }
            }
        }
        return instance;
    }

    public void insert(RumPage value) {
        synchronized (this) {
            if (!this.table.containsKey(value)) {//如果页没在内存中
                this.tail.next = new Node(value, null);
                this.tail = this.tail.next;
                this.table.put(value, this.tail);
                ++size;
            } else {
                Node hitNode = this.table.get(value);
                hit(hitNode);
            }
        }
    }

    /**
     * 从链表中取出受害者页，若链表为空则返回null
     * @return 返回受害者页面
     * @throws BufferException  若链表中所有的页均被钉住，则抛出异常
     */
    public RumPage victim() throws BufferException {
        synchronized (this) {
            if (size == 0) {
                return null;
            }
            Node tmp = head;
            while (tmp != null && tmp.pinned()){
                tmp = tmp.next;
            }
            if(tmp == null){
                throw new BufferException(2);
            }
            Node pre = head;//找tmp上一个节点
            if(pre!=tmp){
                while(pre.next != tmp){
                    pre = pre.next;
                }
            }
            if(tmp == head){
                head = head.next;
                size--;
                return tmp.getData();
            }else{
                pre.next = tmp.next;
                size--;
                return tmp.getData();
            }
        }
    }

    public int size() {
        return size;
    }

    /**
     * 将命中的页移至链表尾
     *
     * @param p 命中的节点
     */
    private void hit(Node p) {
        Node tmp = head;
        while (tmp.next != null) {
            if (tmp.next == p) {
                break;
            } else {
                tmp = tmp.next;
            }
        }
        if (p.next != null) {
            tmp.next = p.next;
            this.tail.next = p;
            this.tail = this.tail.next;
        }
    }

    private int size;
    Node tail;
    Node head;
    Map<RumPage, Node> table;

}

class Node {
    private final RumPage data;
    Node next = null;

    public Node(RumPage data, Node p) {
        this.data = data;
        this.next = p;
    }

    public boolean pinned(){
        return data.pinned();
    }

    public RumPage getData(){
        return data;
    }
}
