/**
 * 
 */
package collections;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A set of values ordered by their order of addition
 */
public class RecentUseList<U> 
{
	/**
	 * Creates the list with an infinite capacity
	 */
	public RecentUseList(){}
	
	/**
	 * Creates a list with a finite capacity
	 * @param capacity The maximum allowable number of contained values
	 */
	public RecentUseList(int capacity)
	{
		this.maximum_capacity = capacity;
	}
	/**
	 * Returns the most recently added value
	 */
	public U get_head()
	{
		return head.value;
	}
	
	/**
	 * Adds the value to the top of the list. If the value is already contained, if is promoted to the top of the list
	 * If the maximum number of allowable values is reached, the least recently used item is removed from the list
	 * @param value The value to add
	 */
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
		
		if (maximum_capacity <= this.size())
		{
			drop_last();
		}
	}
	
	/**
	 * Removes the last item from the list
	 * @return The last item from the list
	 */
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
	
	/**
	 * Finds, promotes, and returns the most recently used value that is deemed acceptable by the 
	 * passed in expression
	 * @param expression A function that accepts the contained type and returns a boolean value
	 * @return The first value that passes the test
	 */
	public U get(Predicate<U> expression)
	{
		U value = null;
		Node current = head;
		
		while (current != null)
		{
			if (expression.test(current.value))
			{
				value = current.value;
				promote(current);
				break;
			}
			current = current.get_next();
		}		
		
		return value;
	}
	
	/**
	 * Finds, promotes, and returns the most recently used value that is deemed acceptable by the 
	 * passed in expression
	 * @param expression A function that accepts the contained type and returns a boolean value
	 * @return The first value that passes the test
	 */
	public U get(Function<U, Boolean> expression)
	{
		U value = null;
		Node current = head;
		
		while (current != null)
		{
			if (expression.apply(current.value))
			{
				value = current.value;
				promote(current);
				break;
			}
			current = current.get_next();
		}		
		
		return value;
	}
	
	/**
	 * Finds the node containing the last value
	 * @param value The value to find
	 * @return The node containing the value
	 */
	private Node find(U value)
	{
		Node found_node = null;
		Node current = head;
		
		while (current != null)
		{
			if (current.get_value() == value)
			{
				found_node = current;
				break;
			}
			current = current.get_next();
		}
		
		return found_node;
	}
	
	/**
	 * Moves the value to the head of the list
	 * @param value The value to find and promote
	 */
	private void promote(U value)
	{
		
		if (size() != 1 && contained_values.contains(value))
		{
			Node promotee = find(value);
			promote(promotee);
		}
	}
	
	/**
	 * Promotes the given node to the front of the list
	 * @param promotee The node to promote
	 */
	private void promote(Node promotee)
	{
		promotee.get_previous().set_next(promotee.get_next());
		promotee.set_previous(null);
		promotee.set_next(head);
		head = promotee;
	}
	
	/**
	 * The number of items in the list
	 * @return The number of items in the list
	 */
	public int size()
	{
		return contained_values.size();
	}
	
	private Node head;
	private Node tail;
	private List<U> contained_values = new ArrayList<U>();
	private Integer maximum_capacity = null;
	
	/**
	 * A node for the doubly linked list
	 */
	private class Node
	{
		public Node(U value)
		{
			this.value = value;
		}
		
		/**
		 * Retrieves the value for the node
		 */
		public U get_value()
		{
			return value;
		}
		
		/**
		 * Returns a node containing a more recently used value contained within this one
		 */
		public Node get_previous()
		{
			return previous;
		}
		
		/**
		 * Returns a node containing a value less recently used value than the value contained within this one
		 * @return
		 */
		public Node get_next()
		{
			return next;
		}
		
		/**
		 * Sets the node after this one to the passed in node. If there is already a next node, the passed in
		 * Node is added between this and the next node
		 * @param next The node to insert
		 */
		public void set_next(Node next)
		{
			if (this.next != null)
			{
				this.next.set_previous(next);
				next.set_next(this.next);
			}
			this.next = next;
		}
		
		/**
		 * Sets the previous node to the passed in node. If there is already a node before this one, the passed in
		 * Node is inserted between this and the previous
		 * @param previous The node to insert
		 */
		public void set_previous(Node previous)
		{
			if (this.previous != null)
			{
				this.previous.set_next(previous);
				previous.set_previous(this.previous);
			}
			this.previous = previous;
		}
		
		/**
		 * Returns the String representation of the contained value
		 */
		public String toString()
		{
			return value.toString() + " ";
		}
		
		private U value;
		private Node previous;
		private Node next;
	}
}
