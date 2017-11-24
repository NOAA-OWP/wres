package wres.datamodel;

import java.util.Objects;

/**
 * Concrete implementation of a {@link Threshold}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class SafeThreshold implements Threshold
{

    /**
     * The threshold value
     */

    private final Double threshold;

    /**
     * The upper threshold value or null.
     */

    private final Double thresholdUpper;

    /**
     * The probability associated with the quantile.
     */

    private final Double probability;

    /**
     * The probability associated with the upper quantile or null.
     */

    private final Double probabilityUpper;

    /**
     * The threshold condition.
     */

    private final Operator condition;

    @Override
    public Double getThreshold()
    {
        return threshold;
    }

    @Override
    public Operator getCondition()
    {
        return condition;
    }

    @Override
    public Double getThresholdUpper()
    {
        return thresholdUpper;
    }

    @Override
    public Double getThresholdProbability()
    {
        return probability;
    }

    @Override
    public Double getThresholdUpperProbability()
    {
        return probabilityUpper;
    }

    @Override
    public boolean hasBetweenCondition()
    {
        return condition.equals( Operator.BETWEEN );
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof SafeThreshold ) )
        {
            return false;
        }
        final SafeThreshold in = (SafeThreshold) o;
        boolean returnMe =
                hasOrdinaryValues() == in.hasOrdinaryValues() && hasProbabilityValues() == in.hasProbabilityValues()
                           && getCondition().equals( in.getCondition() );
        if ( hasOrdinaryValues() )
        {
            returnMe = returnMe && getThreshold().equals( in.getThreshold() );
            if ( hasBetweenCondition() )
            {
                returnMe = returnMe && getThresholdUpper().equals( in.getThresholdUpper() );
            }
        }
        if ( hasProbabilityValues() )
        {
            returnMe = returnMe && getThresholdProbability().equals( in.getThresholdProbability() );
            if ( hasBetweenCondition() )
            {
                returnMe = returnMe && getThresholdUpperProbability().equals( in.getThresholdUpperProbability() );
            }
        }
        return returnMe;
    }

    @Override
    public int hashCode()
    {
        int returnMe = 53728;
        if ( hasOrdinaryValues() )
        {
            returnMe = returnMe * 37 + threshold.hashCode();
            returnMe = returnMe * 37 + condition.hashCode();
            if ( hasBetweenCondition() )
            {
                returnMe = returnMe * 37 + thresholdUpper.hashCode();
            }
        }
        if ( hasProbabilityValues() )
        {
            returnMe = returnMe * 37 + probability.hashCode();
            if ( hasBetweenCondition() )
            {
                returnMe = returnMe * 37 + probabilityUpper.hashCode();
            }
        }
        return returnMe;
    }

    @Override
    public String toString()
    {
        if ( threshold.equals( Double.NEGATIVE_INFINITY ) )
        {
            return "All data";
        }
        final String c = getConditionID();
        if ( hasOrdinaryValues() && hasProbabilityValues() )
        {
            String common = " [Pr = ";
            if ( hasBetweenCondition() )
            {
                return ">= " + threshold
                       + common
                       + probability
                       + "] && < "
                       + thresholdUpper
                       + common
                       + probabilityUpper
                       + "]";
            }
            return c + threshold + common + probability + "]";
        }
        else
        {
            if ( hasBetweenCondition() )
            {
                return ">= " + threshold + " && < " + thresholdUpper;
            }
            return c + threshold;
        }
    }

    @Override
    public int compareTo( final Threshold o )
    {
        Objects.requireNonNull( o, "Specify a non-null threshold for comparison" );
        //Compare condition
        int returnMe = condition.compareTo( o.getCondition() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }
        //Check for equal status of the values available
        if ( hasOrdinaryValues() != o.hasOrdinaryValues() )
        {
            if ( hasOrdinaryValues() )
            {
                return 1;
            }
            return -1;
        }
        if ( hasProbabilityValues() != o.hasProbabilityValues() )
        {
            if ( hasProbabilityValues() )
            {
                return 1;
            }
            return -1;
        }
        //Compare ordinary values
        if ( hasOrdinaryValues() )
        {
            returnMe = compareOrdinaryValues( o );
            if ( returnMe != 0 )
            {
                return returnMe;
            }
        }
        //Compare probability values
        if ( hasProbabilityValues() )
        {
            returnMe = compareProbabilityValues( o );
            if ( returnMe != 0 )
            {
                return returnMe;
            }
        }
        return 0;
    }

    @Override
    public boolean test( Double t )
    {
        switch ( condition )
        {
            case GREATER:
                return t > threshold;
            case LESS:
                return t < threshold;
            case GREATER_EQUAL:
                return t >= threshold;
            case LESS_EQUAL:
                return t <= threshold;
            case BETWEEN:
                return t >= threshold && t < thresholdUpper;
            case EQUAL:
                return Double.compare( t, threshold ) == 0;
            default:
                throw new UnsupportedOperationException( "Unexpected logical condition." );
        }
    }

    @Override
    public boolean isFinite()
    {
        boolean returnMe = true;
        if ( hasOrdinaryValues() )
        {
            returnMe = Double.isFinite( threshold );
            if ( hasBetweenCondition() )
            {
                returnMe = returnMe && Double.isFinite( thresholdUpper );
            }
        }
        if ( hasProbabilityValues() )
        {
            returnMe = returnMe && Double.isFinite( probability );
            if ( hasBetweenCondition() )
            {
                returnMe = returnMe && Double.isFinite( thresholdUpper );
            }
        }
        return returnMe;
    }

    /**
     * A {@link DefaultPairedInputBuilder} to build the metric input.
     */

    static class ThresholdBuilder
    {

        /**
         * The threshold condition.
         */

        private Operator condition;

        /**
         * The threshold value
         */

        private Double threshold;

        /**
         * The upper threshold value or null.
         */

        private Double thresholdUpper;

        /**
         * The probability associated with the quantile.
         */

        private Double probability;

        /**
         * The probability associated with the upper quantile or null.
         */

        private Double probabilityUpper;

        /**
         * Sets the {@link Operator} associated with the threshold
         * 
         * @param condition the threshold condition
         * @return the builder
         */

        ThresholdBuilder setCondition( Operator condition )
        {
            this.condition = condition;
            return this;
        }

        /**
         * Sets the threshold value
         * 
         * @param threshold the threshold
         * @return the builder
         */

        ThresholdBuilder setThreshold( Double threshold )
        {
            this.threshold = threshold;
            return this;
        }

        /**
         * Sets the upper threshold value.
         * 
         * @param thresholdUpper the upper threshold
         * @return the builder
         */

        ThresholdBuilder setThresholdUpper( Double thresholdUpper )
        {
            this.thresholdUpper = thresholdUpper;
            return this;
        }

        /**
         * Sets the probability.
         * 
         * @param probability the threshold probability
         * @return the builder
         */

        ThresholdBuilder setThresholdProbability( Double probability )
        {
            this.probability = probability;
            return this;
        }


        /**
         * Sets the probability for the upper threshold.
         * 
         * @param probabilityUpper the probability for the upper threshold
         * @return the builder
         */

        ThresholdBuilder setThresholdProbabilityUpper( Double probabilityUpper )
        {
            this.probabilityUpper = probabilityUpper;
            return this;
        }

        /**
         * Return the {@link Threshold}
         * 
         * @return the {@link Threshold}
         */
        public Threshold build()
        {
            return new SafeThreshold( this );
        }
    }

    /**
     * Construct the threshold.
     *
     * @param builder the builder
     * @param condition the condition
     */

    private SafeThreshold( ThresholdBuilder builder )
    {
        //Set, then validate
        this.condition = builder.condition;
        this.threshold = builder.threshold;
        this.thresholdUpper = builder.thresholdUpper;
        this.probability = builder.probability;
        this.probabilityUpper = builder.probabilityUpper;

        //Bounds checks
        Objects.requireNonNull( condition, "Specify a non-null condition." );
        //Do not allow only an upper threshold or all null thresholds
        if ( !hasOrdinaryValues() && !hasProbabilityValues() )
        {
            throw new IllegalArgumentException( "Specify one or more values for the threshold." );
        }
        //Check the probability
        if ( Objects.nonNull( probability ) && !probability.equals( Double.NEGATIVE_INFINITY )
             && ( probability < 0.0 || probability > 1.0 ) )
        {
            throw new IllegalArgumentException( "The threshold probability is out of bounds [0,1]: " + probability );
        }
        //Check a two-sided threshold
        if ( hasBetweenCondition() )
        {
            validateTwoSidedThreshold();
        }
        //Check a one-sided threshold
        else
        {
            validateOneSidedThreshold();
        }
    }

    /**
     * Validates the parameters for a one-sided threshold.  
     * 
     * @throws IllegalArgumentException if the validation fails
     */

    void validateOneSidedThreshold()
    {

        if ( Objects.nonNull( thresholdUpper ) )
        {
            throw new IllegalArgumentException( "Specify a null upper threshold or define an appropriate "
                                                + "BETWEEN condition." );
        }
        if ( Objects.nonNull( probabilityUpper ) )
        {
            throw new IllegalArgumentException( "Specify a non-null upper threshold probability or define an "
                                                + "appropriate BETWEEN condition." );
        }
    }

    /**
     * Validates the parameters for a two-sided threshold.  
     * 
     * @throws IllegalArgumentException if the validation fails
     */

    void validateTwoSidedThreshold()
    {
        //Do not allow a partially defined pair of thresholds/probabilities          
        if ( Objects.nonNull( probability ) != Objects.nonNull( probabilityUpper ) ||
             Objects.nonNull( threshold ) != Objects.nonNull( thresholdUpper ) )
        {
            throw new IllegalArgumentException( "When constructing a BETWEEN condition, thresholds must be defined "
                                                + "in pairs." );
        }

        if ( Objects.isNull( thresholdUpper ) && Objects.isNull( probabilityUpper ) )
        {
            throw new IllegalArgumentException( "Specify an upper threshold for a BETWEEN condition." );
        }
        if ( Objects.nonNull( threshold ) && thresholdUpper <= threshold )
        {
            throw new IllegalArgumentException( "The upper threshold must be greater than the lower threshold: ["
                                                + threshold + "," + thresholdUpper + "]." );
        }
        if ( Objects.nonNull( probability ) )
        {
            if ( probabilityUpper <= probability )
            {
                throw new IllegalArgumentException( "The upper threshold probability must be greater than the "
                                                    + "lower threshold probability: ["
                                                    + probability
                                                    + ","
                                                    + probabilityUpper
                                                    + "]." );
            }
            if ( !probability.equals( Double.NEGATIVE_INFINITY )
                 && ( probabilityUpper < 0.0 || probabilityUpper > 1.0 ) )
            {
                throw new IllegalArgumentException( "The threshold probability is out of bounds [0,1]: "
                                                    + probabilityUpper );
            }
        }
    }

    /**
     * Returns a string representation of a condition that is not a {@link Operator#BETWEEN}
     * condition.
     * 
     * @return a string for the elementary condition
     */

    private String getConditionID()
    {
        switch ( condition )
        {
            case GREATER:
                return "> ";
            case LESS:
                return "< ";
            case GREATER_EQUAL:
                return ">= ";
            case LESS_EQUAL:
                return "<= ";
            case EQUAL:
                return "= ";
            default:
                return null;
        }
    }

    /**
     * Compares the ordinary values associated with an input threshold.
     * 
     * @param o the threshold
     * @return a negative, zero, or positive integer if this threshold is less than, equal to, or greater than the 
     *            input, respectively
     */
    private int compareOrdinaryValues( final Threshold o )
    {
        //Compare ordinary values
        if ( hasOrdinaryValues() )
        {
            int returnMe = Double.compare( threshold, o.getThreshold() );
            if ( returnMe != 0 )
            {
                return returnMe;
            }
            if ( hasBetweenCondition() )
            {
                returnMe = Double.compare( thresholdUpper, o.getThresholdUpper() );
                if ( returnMe != 0 )
                {
                    return returnMe;
                }
            }
        }
        return 0;
    }

    /**
     * Compares the probability values associated with an input threshold.
     * 
     * @param o the threshold
     * @return a negative, zero, or positive integer if this threshold is less than, equal to, or greater than the 
     *            input, respectively
     */
    private int compareProbabilityValues( final Threshold o )
    {
        //Compare probability values
        if ( hasProbabilityValues() )
        {
            int returnMe = Double.compare( probability, o.getThresholdProbability() );
            if ( returnMe != 0 )
            {
                return returnMe;
            }
            if ( hasBetweenCondition() )
            {
                returnMe = Double.compare( probabilityUpper, o.getThresholdUpperProbability() );
                if ( returnMe != 0 )
                {
                    return returnMe;
                }
            }
        }
        return 0;
    }
}
