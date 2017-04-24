/**
 * 
 */
package collections;

/**
 * @author ctubbs
 *
 */
public interface Group<T> extends Comparable<T> {
	public boolean isFull();
	public boolean isEmpty();
	public T copy();
}
