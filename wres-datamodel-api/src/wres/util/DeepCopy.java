/**
 * 
 */
package wres.util;

/**
 * Returns a deep copy of the implementing object whereby all of the instance variables are independent copies in
 * memory.
 * 
 * @author james.brown@hydrosolved.com
 * @author ctubbs
 */

public interface DeepCopy<T>
{
    T deepCopy();
}
