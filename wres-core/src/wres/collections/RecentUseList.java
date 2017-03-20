/**
 * 
 */
package wres.collections;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ctubbs
 *
 */
public class RecentUseList<U> {
	
	public void add(U value)
	{
		if (contained_values.contains(value))
		{
			promote(value);
		}
		else
		{
			Node node = new Node(value);
			contained_values.add(value);
			if (head == null)
			{
				head = node;
			}
			else
			{
				node.set_next(head);
				head = node;
			}
		}		
	}
	
	public U drop_last()
	{
		U value = null; 
		
		if (size() == 1)
		{
			value = head.value;
			head = null;
			tail = null;
		}
		else if (size() > 0)
		{
			value = tail.value;			
			tail = tail.previous;
		}
		
		if (value != null)
		{
			contained_values.remove(value);
		}
		
		return value;
	}
	
	private Node find(U value)
	{
		Node found_node = null;
		Node current = head;
		
		do
		{
			if (current.get_value() == value)
			{
				found_node = current;
				break;
			}
			current = current.get_next();
		} while (current != null);
		
		return found_node;
	}
	
	private void promote(U value)
	{
		
		if (size() != 1)
		{
			Node promotee = find(value);
			promotee.get_previous().set_next(null);
			promotee.set_previous(null);
			promotee.set_next(head);
			head = promotee;
		}
	}
	
	public int size()
	{
		return contained_values.size();
	}
	
	private Node head;
	private Node tail;
	private List<U> contained_values = new ArrayList<U>();
	
	private class Node
	{
		public Node(U value)
		{
			this.value = value;
		}
		
		public U get_value()
		{
			return value;
		}
		
		public Node get_previous()
		{
			return previous;
		}
		
		public Node get_next()
		{
			return next;
		}
		
		public void set_next(Node next)
		{
			this.next = next;
		}
		
		public void set_previous(Node previous)
		{
			this.previous = previous;
		}
		
		public String toString()
		{
			return value.toString() + " ";
		}
		
		private U value;
		private Node previous;
		private Node next;
	}
}
