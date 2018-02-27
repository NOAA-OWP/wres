package wres.datamodel;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * Stores a threshold value and associated logical condition. A threshold may comprise one or two threshold values. If
 * the threshold comprises two values, {@link #getCondition()} must return {@link Operator#BETWEEN} and
 * {@link #getThresholdUpper()} must return a non-null value. The reverse is also true, i.e. if the condition is
 * {@link Operator#BETWEEN}, {@link #getThresholdUpper()} must return null. The threshold may comprise ordinary 
 * threshold values and/or probability values. When both are defined, the threshold is a "quantile".
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
     * Returns <code>true</code> if the threshold contains one or more ordinary (non-probability) values, otherwise
     * <code>false</code>.
     * 
     * @return true if ordinary values are defined, false otherwise
     */

    default boolean hasOrdinaryValues()
    {
        return Objects.nonNull( getThreshold() ) || Objects.nonNull( getThresholdUpper() );
    }

    /**
     * Returns <code>true</code> if the threshold contains one or more probability values, otherwise <code>false</code>.
     * 
     * @return true if probability values are defined, false otherwise
     */

    default boolean hasProbabilityValues()
    {
        return Objects.nonNull( getThresholdProbability() ) || Objects.nonNull( getThresholdUpperProbability() );
    }

    /**
     * Returns <code>true</code> if {@link #getLabel()} returns a non-null label, otherwise <code>false</code>.
     * 
     * @return true if the threshold has a label, false otherwise
     */

    default boolean hasLabel()
    {
        return Objects.nonNull( getLabel() );
    }

    /**
     * Returns <code>true</code> if the threshold is a quantile; that is, when {@link #hasOrdinaryValues()} and 
     * {@link #hasProbabilityValues()} both return <code>true</code>.
     * 
     * @return true if the threshold is a quantile, false otherwise
     */

    default boolean isQuantile()
    {
        return hasOrdinaryValues() && hasProbabilityValues();
    }

    /**
     * Returns the threshold value, which may comprise the lower bound of a {@link Operator#BETWEEN}, or null if no
     * threshold value is defined.
     * 
     * @return the threshold value or null
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
     * Returns the probability associated with the {@link #getThreshold()}, which may comprise the lower bound of a
     * {@link Operator#BETWEEN}, or null if no probability is defined.
     * 
     * @return a probability or null
     */

    Double getThresholdProbability();

    /**
     * Returns the probability associated with the {@link #getThresholdUpper()} or null if no upper bound is defined.
     * 
     * @return the upper threshold probability or null
     */

    Double getThresholdUpperProbability();

    /**
     * Returns the label associated with the {@link Threshold} or null if no label is defined.
     * 
     * @return the label or null
     */

    String getLabel();

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
     * Returns a string representation of the threshold that does not contain spaces or other special characters.
     * 
     * @return a string representation without spaces or special characters
     */

    String toStringSafe();

    /**
     * Returns a string representation of the threshold.
     * 
     * @return a string representation
     */
    @Override
    String toString();
}
