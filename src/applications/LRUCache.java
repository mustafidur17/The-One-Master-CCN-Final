/**
@implementation of Least Recent Used cache
*/

package applications; 
import java.util.HashMap;
 
public class LRUCache {
	private HashMap<Integer, DoubleLinkedListNode> map 
		= new HashMap<Integer, DoubleLinkedListNode>();
	private DoubleLinkedListNode head;
	private DoubleLinkedListNode end;
	private int capacity;
	private int len;
	private String lastEvictedValue;
 
	public LRUCache(int capacity) {
		this.capacity = capacity;
		len = 0;
		lastEvictedValue = null;
	}
	
	public LRUCache(LRUCache another){
		this.map = another.map;
		this.head = another.head;
		this.end = another.end;
		this.capacity = another.capacity;
		this.len = another.len;
		this.lastEvictedValue = null;
	}
 
	public String get(int key) {
		if (map.containsKey(key)) {
			DoubleLinkedListNode latest = map.get(key);
			removeNode(latest);
			setHead(latest);
			return latest.val;
		} else {
			return "";
		}
	}
	
	public void print_cache(){
		for(int key:map.keySet()){
			System.out.println("[" + map.get(key).key + "] = " + map.get(key).val);
		}
	}
 
	public void removeNode(DoubleLinkedListNode node) {
		DoubleLinkedListNode cur = node;
		DoubleLinkedListNode pre = cur.pre;
		DoubleLinkedListNode post = cur.next;
 
		if (pre != null) {
			pre.next = post;
		} else {
			head = post;
		}
 
		if (post != null) {
			post.pre = pre;
		} else {
			end = pre;
		}
	}
 
	public void setHead(DoubleLinkedListNode node) {
		node.next = head;
		node.pre = null;
		if (head != null) {
			head.pre = node;
		}
 
		head = node;
		if (end == null) {
			end = node;
		}
	}
 
	public void set(int key, String value) {
		lastEvictedValue = null;
		if (map.containsKey(key)) {
			DoubleLinkedListNode oldNode = map.get(key);
			oldNode.val = value;
			removeNode(oldNode);
			setHead(oldNode);
		} else {
			DoubleLinkedListNode newNode =
				new DoubleLinkedListNode(key, value);
			if (len < capacity) {
				setHead(newNode);
				map.put(key, newNode);
				len++;
			} else {
				lastEvictedValue = end.val;
				map.remove(end.key);
				end = end.pre;
				if (end != null) {
					end.next = null;
				}

				setHead(newNode);
				map.put(key, newNode);
			}
		}
	}

	public String getLastEvictedValue() {
		return this.lastEvictedValue;
	}

	public int getCapacity() {
		return this.capacity;
	}

	public int countFalseContent() {
		int count = 0;
		for (DoubleLinkedListNode node : map.values()) {
			if (node.val != null &&
					node.val.startsWith("FALSE_TRAFFIC_CONTENT_")) {
				count++;
			}
		}
		return count;
	}
	
	public int get_len(){
		return this.len;
	}
}
 
class DoubleLinkedListNode {
	public String val;
	public int key;
	public DoubleLinkedListNode pre;
	public DoubleLinkedListNode next;
 
	public DoubleLinkedListNode(int key, String value) {
		val = value;
		this.key = key;
	}
}
