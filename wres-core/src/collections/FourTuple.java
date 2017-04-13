/**
 * 
 */
package collections;

/**
 * @author ctubbs
 *
 */
public class FourTuple<T extends Comparable<T>, U extends Comparable<U>, V extends Comparable<V>, W extends Comparable<W>> implements Comparable<FourTuple<T, U, V, W>>{

	/**
	 * 
	 */
	public FourTuple(T item_one, U item_two, V item_three, W item_four) {
		this.item_one = item_one;
		this.item_two = item_two;
		this.item_three = item_three;
		this.item_four = item_four;
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compareTo(FourTuple<T, U, V, W> o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public T get_item_one()
	{
		return this.item_one;
	}
	
	public U get_item_two()
	{
		return this.item_two;
	}
	
	public V get_item_three()
	{
		return this.item_three;
	}
	
	public W get_item_four()
	{
		return this.item_four;
	}

	private final T item_one;
	private final U item_two;
	private final V item_three;
	private final W item_four;
}
