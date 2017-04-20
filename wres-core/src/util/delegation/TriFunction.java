/**
 * 
 */
package util.delegation;

/**
 * @author ctubbs
 *
 */
public interface TriFunction<T, U, V, result> {
	public result apply(T arg0, U arg1, V arg2);
}
