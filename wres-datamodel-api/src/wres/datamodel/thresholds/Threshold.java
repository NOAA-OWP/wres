package wres.datamodel.thresholds;

import java.util.Objects;
import java.util.function.Predicate;

import wres.datamodel.Dimension;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdType;

/**
 * <p>Stores a threshold value and associated logical condition. A threshold comprises one or both of: 
 * 
 * <ol>
 * <li>One or two real values, contained in a {@link OneOrTwoDoubles}.</li>
 * <li>One or two probability values, contained in a {@link OneOrTwoDoubles}.</li>
 * </ol>
 * 
 * <p>The presence of the former is determined by {@link #hasValues()}. The presence of the latter is determined by 
 * {@link #hasProbabilities()}. If both are present, the threshold is a "quantile", as revealed by 
 * {@link #isQuantile()}. A summary of the threshold type can be obtained from {@link #getType()}.</p>
 * 
 * <p>Additionally, a threshold comprises an {@link Operator}, denoting the type of threshold condition. Optionally,
 * a threshold may comprise a label and a {@link Dimension} that describes the units of the real-valued thresholds.</p>
 * 
 * @author james.brown@hydrosolved.com
 */

public interface Threshold extends Comparable<Threshold>, Predicate<Double>
{

    /**
     * Returns <code>true</code> if the threshold contains one or more ordinary (non-probability) values, otherwise
     * <code>false</code>.
     * 
     * @return true if ordinary values are defined, false otherwise
     */

    default boolean hasValues()
    {
        return Objects.nonNull( this.getValues() );
    }

    /**
     * Returns <code>true</code> if the threshold contains one or more probability values, otherwise <code>false</code>.
     * 
     * @return true if probability values are defined, false otherwise
     */

    default boolean hasProbabilities()
    {
        return Objects.nonNull( this.getProbabilities() );
    }

    /**
     * Returns <code>true</code> if {@link #getLabel()} returns a non-null label, otherwise <code>false</code>.
     * 
     * @return true if the threshold has a label, false otherwise
     */

    default boolean hasLabel()
    {
        return Objects.nonNull( this.getLabel() );
    }

    /**
     * Returns <code>true</code> if the threshold is a quantile; that is, when {@link #hasValues()} and 
     * {@link #hasProbabilities()} both return <code>true</code>.
     * 
     * @return true if the threshold is a quantile, false otherwise
     */

    default boolean isQuantile()
    {
        return this.hasValues() && this.hasProbabilities();
    }

    /**
     * Returns <code>true</code> if {@link #getUnits()} returns a non-null {@link Dimension}, 
     * otherwise <code>false</code>.
     * 
     * @return true if the threshold units are defined, false otherwise
     */

    default boolean hasUnits()
    {
        return Objects.nonNull( this.getUnits() );
    }

    /**
     * Returns the {@link ThresholdType}.
     * 
     * @return the threshold type
     */

    default ThresholdType getType()
    {
        if ( this.isQuantile() )
        {
            return ThresholdType.PROBABILITY_AND_VALUE;
        }
        if ( this.hasProbabilities() )
        {
            return ThresholdType.PROBABILITY_ONLY;
        }
        return ThresholdType.VALUE_ONLY;
    }

    /**
     * Returns the {@link ThresholdDataType} to which the threshold applies.
     * 
     * @return the threshold data type
     */

    ThresholdDataType getDataType();

    /**
     * Returns the threshold values or null if no threshold values are defined. If no threshold values are defined,
     * {@link #getProbabilities()} always returns non-null.
     * 
     * @return the threshold values or null
     */

    OneOrTwoDoubles getValues();

    /**
     * Returns the probability values or null if no probability values are defined. If no probability values are 
     * defined, {@link #getValues()} always returns non-null.
     * 
     * @return the threshold values or null
     */

    OneOrTwoDoubles getProbabilities();

    /**
     * Returns the units associated with the {@link Threshold} or null. Always returns null when 
     * {@link #hasValues()} returns <code>false</code>.
     * 
     * @return the units or null
     */

    Dimension getUnits();

    /**
     * Returns the logical condition associated with the threshold.
     * 
     * @return the logical condition associated with the threshold
     */

    Operator getCondition();

    /**
     * Returns the label associated with the {@link Threshold} or null if no label is defined.
     * 
     * @return the label or null
     */

    String getLabel();

    /**
     * Returns <code>true</code> if the {@link Threshold} condition corresponds to a {@link Operator#BETWEEN} condition.
     * 
     * @return true if the condition is a {@link Operator#BETWEEN} condition, false otherwise.
     */

    boolean hasBetweenCondition();

    /**
     * Returns <code>true</code> if {@link Double#isFinite(double)} returns <code>true</code> for all threshold values,
     * otherwise <code>false</code>.
     * 
     * @return true if the threshold is finite, false otherwise
     */

    boolean isFinite();

    /**
     * Returns a string representation of the {@link Threshold} that contains only alphanumeric characters A-Z, a-z, 
     * and 0-9 and, additionally, the underscore character to separate between elements, and the period character as
     * a decimal separator.
     * 
     * @return a safe string representation
     */

    String toStringSafe();

    /**
     * Returns a string representation of the {@link Threshold} without any units. This is useful when forming string
     * representions of a collection of {@link Threshold} and abstracting the common units to a higher level.
     * 
     * @return a string without any units
     */

    String toStringWithoutUnits();

    /**
     * Returns a string representation of the {@link Threshold}.
     * 
     * @return a string representation
     */
    @Override
    String toString();
}
