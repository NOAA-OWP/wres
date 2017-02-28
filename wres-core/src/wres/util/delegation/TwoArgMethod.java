/**
 * 
 */
package wres.util.delegation;

/**
 * @author ctubbs
 *
 */
public interface TwoArgMethod<T, U> {
	public void apply(T arg0, U arg1);
}
