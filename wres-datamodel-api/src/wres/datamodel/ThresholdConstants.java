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
         * Identifier for greater than or equal to.
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
     * An enumeration of the components within a single threshold.
     */

    public enum ThresholdType
    {

        /**
         * A {@link Threshold} that comprises one or two probability values only. A {@link Threshold} has two 
         * probability values if {@link Threshold#hasBetweenCondition()} returns <code>true</code>, otherwise only 
         * one value.
         */

        PROBABILITY_ONLY,

        /**
         * A {@link Threshold} that comprises one or two real values only. A {@link Threshold} has two 
         * real values if {@link Threshold#hasBetweenCondition()} returns <code>true</code>, otherwise only one 
         * value.
         */

        VALUE_ONLY,

        /**
         * A {@link Threshold} that comprises both real values and probability values. It contains the same number of 
         * each. A {@link Threshold} has two values for each if {@link Threshold#hasBetweenCondition()} returns 
         * <code>true</code>, otherwise one value for each.
         */

        PROBABILITY_AND_VALUE;

    }    

    /**
     * An enumeration of the data types to which a {@link Threshold} should be applied.
     */

    public enum ThresholdDataType
    {

        /**
         * Left side of a pair meets the threshold condition.
         */

        LEFT,

        /**
         * Right side of a pair meets the threshold condition.
         */

        RIGHT,
        
        /**
         * Left side and all values of the right side of a pair meet the threshold condition.
         */

        LEFT_AND_RIGHT,
        
        /**
         * Any value on the right side of a pair meet the threshold condition. 
         */
        
        ANY_RIGHT,
        
        /**
         * Left side and any value on the right side of a pair meet the threshold condition.
         */

        LEFT_AND_ANY_RIGHT,

        /**
         * The mean value of the right side of a pair meets the threshold condition.
         */

        RIGHT_MEAN,
        
        /**
         * The left side and the mean value of the right side of a pair meets the threshold condition.
         */

        LEFT_AND_RIGHT_MEAN;

    }    
    
    /**
     * An enumeration of threshold groups.
     */
    
    public enum ThresholdGroup
    {
        
        /**
         * A group of thresholds that denote probabilities.
         */
        
        PROBABILITY,
        
        /**
         * A group of thresholds that denote real values.
         */
        
        VALUE,
        
        /**
         * A group of thresholds that denote quantiles.
         */
        
        QUANTILE,
        
        /**
         * A group of thresholds that denote probabilities used for classification.
         */
        
        PROBABILITY_CLASSIFIER;        
        
    }    
    
}
