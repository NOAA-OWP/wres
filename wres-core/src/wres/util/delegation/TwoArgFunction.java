/**
 * 
 */
package wres.util.delegation;

/**
 * @author ctubbs
 *
 */
public interface TwoArgFunction<T, U, result> {
	public result apply(T arg0, U arg1);
}
