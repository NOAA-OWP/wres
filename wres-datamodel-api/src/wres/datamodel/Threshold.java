package wres.datamodel;

import java.util.function.Predicate;

/**
 * Stores a threshold value and associated logical condition. A threshold may comprise one or two threshold values. If
 * the threshold comprises two values, {@link #getCondition()} must return {@link Operator#BETWEEN} and
 * {@link #getThresholdUpper()} must return a non-null value. The reverse is also true, i.e. if the condition is
 * {@link Operator#BETWEEN}, {@link #getThresholdUpper()} must return null.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface Threshold extends Comparable<Threshold>, Predicate<Double>
{

    /**
     * Operators associated with a {@link Threshold}.
     */

    public enum Operator
    {

        /**
         * Identifier for less than.
         */

        LESS,

        /**
         * Identifier for greater than.
         */

        GREATER,

        /**
         * Identifier for less than or equal to.
         */

        LESS_EQUAL,

        /**
         * Identifier for greater than or equal to
         */

        GREATER_EQUAL,

        /**
         * Identifier for equality.
         */

        EQUAL,

        /**
         * Identifier for between.
         */

        BETWEEN
    }

    /**
     * Returns the threshold value, which may comprise the lower bound of a {@link Operator#BETWEEN}
     * 
     * @return the threshold value
     */

    Double getThreshold();

    /**
     * Returns the logical condition associated with the threshold.
     * 
     * @return the logical condition associated with the threshold
     */

    Operator getCondition();

    /**
     * Returns the upper bound of a {@link Operator#BETWEEN} condition or null.
     * 
     * @return the upper threshold value or null
     */

    Double getThresholdUpper();

    /**
     * Returns true if the threshold condition corresponds to a {@link Operator#BETWEEN} condition and, hence, that
     * {@link #getThresholdUpper()} returns a non-null threshold value.
     * 
     * @return true if the condition is a {@link Operator#BETWEEN} condition, false otherwise.
     */

    boolean hasBetweenCondition();
    
    /**
     * Returns true if {@link Double#isFinite(double)} returns true for all threshold values, false otherwise.
     * 
     * @return true if the threshold is finite, false otherwise
     */
    
    boolean isFinite();

    /**
     * Returns a string representation of the threshold.
     * 
     * @return a string representation
     */
    @Override
    String toString();
}
