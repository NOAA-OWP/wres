/**
 * 
 */
package wres.collections;

/**
 * @author ctubbs
 *
 */
public class TwoTuple<T, U> {

	/**
	 * 
	 */
	public TwoTuple(T item_one, U item_two) {
		this.item_one = item_one;
		this.item_two = item_two;
		// TODO Auto-generated constructor stub
	}
	
	public T get_item_one()
	{
		return item_one;
	}
	
	public U get_item_two()
	{
		return item_two;
	}
	
	protected void set_item_one(T item_one)
	{
		this.item_one = item_one;
	}
	
	protected void set_item_two(U item_two)
	{
		this.item_two = item_two;
	}
	
	private T item_one;
	private U item_two;
}
