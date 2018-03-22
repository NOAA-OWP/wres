package wres.datamodel;

/**
 * A store of threshold enumerations.
 * 
 * @author james.brown@hydrosolved.com
 */

public final class ThresholdConstants
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
     * An enumeration of the composition of a {@link Threshold}.
     */

    public enum ThresholdComposition
    {

        /**
         * A {@link Threshold} that comprises one or two probability values only. A {@link Threshold} has two 
         * probability values if {@link Threshold#hasBetweenCondition()} returns <code>true</code>, otherwise only 
         * one value.
         */

        PROBABILITY,

        /**
         * A {@link Threshold} that comprises one or two real values only. A {@link Threshold} has two 
         * real values if {@link Threshold#hasBetweenCondition()} returns <code>true</code>, otherwise only one 
         * value.
         */

        VALUE,

        /**
         * A {@link Threshold} that comprises both real values and probability values. It contains the same number of 
         * each. A {@link Threshold} has two values for each if {@link Threshold#hasBetweenCondition()} returns 
         * <code>true</code>, otherwise one value for each.
         */

        QUANTILE;

    }    

    /**
     * An enumeration of types to which the thresholds should be applied.
     */

    public enum ApplicationType
    {

        /**
         * Apply to all data.
         */

        ALL,

        /**
         * Apply to the left side of paired data.
         */

        LEFT,

        /**
         * Apply to the right side of paired data.
         */

        RIGHT,

        /**
         * Apply to the mean value of the right side of paired data.
         */

        RIGHT_MEAN;

    }    
    
    /**
     * An enumeration of threshold types.
     */
    
    public enum ThresholdType
    {
        
        /**
         * Probability threshold.
         */
        
        PROBABILITY,
        
        /**
         * Value threshold.
         */
        
        VALUE,
        
        /**
         * Quantile threshold.
         */
        
        QUANTILE,
        
        /**
         * Probability classifier threshold.
         */
        
        PROBABILITY_CLASSIFIER;        
        
    }    
    
}
