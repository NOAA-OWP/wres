package wres.datamodel;

import java.util.Objects;

/**
 * Concrete implementation of a {@link Threshold}.
 * 
 * @author james.brown@hydrosolved.com
 */
class SafeThreshold implements Threshold
{

    /**
     * The real values or null.
     */

    private final SafeOneOrTwoDoubles values;

    /**
     * The probability values or null.
     */

    private final SafeOneOrTwoDoubles probabilities;

    /**
     * The threshold condition.
     */

    private final Operator condition;

    /**
     * The label associated with the threshold.
     */

    private final String label;

    /**
     * The units associated with the threshold.
     */

    private final Dimension units;

    @Override
    public OneOrTwoDoubles getValues()
    {
        return values;
    }

    @Override
    public OneOrTwoDoubles getProbabilities()
    {
        return probabilities;
    }

    @Override
    public Double getThreshold()
    {
        return values.first();
    }

    @Override
    public Operator getCondition()
    {
        return condition;
    }

    @Override
    public Double getThresholdUpper()
    {
        return values.second();
    }

    @Override
    public Double getThresholdProbability()
    {
        return probabilities.first();
    }

    @Override
    public Double getThresholdUpperProbability()
    {
        return probabilities.second();
    }

    @Override
    public String getLabel()
    {
        return label;
    }

    @Override
    public Dimension getUnits()
    {
        return units;
    }

    @Override
    public boolean hasBetweenCondition()
    {
        return this.getCondition().equals( Operator.BETWEEN );
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof SafeThreshold ) )
        {
            return false;
        }

        final SafeThreshold in = (SafeThreshold) o;
        boolean first = this.hasValues() == in.hasValues()
                        && this.hasProbabilities() == in.hasProbabilities();
        boolean second = hasLabel() == in.hasLabel()
                         && this.getCondition().equals( in.getCondition() )
                         && this.hasUnits() == in.hasUnits();

        return first && second && this.areOptionalStatesEqual( in );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( values, probabilities, condition, label, units );
    }

    @Override
    public String toString()
    {

        // Label not used for all data
        if ( !this.isFinite() )
        {
            return "All data";
        }
        String append = "";
        if ( this.hasLabel() )
        {
            append = " (" + this.getLabel() + ")";
        }

        final String c = this.getConditionID();

        String units = "";
        if ( this.hasUnits() )
        {
            units = " " + this.getUnits().toString();
        }

        // Quantile
        if ( this.isQuantile() )
        {
            String common = " [Pr = ";
            if ( this.hasBetweenCondition() )
            {
                return ">= " + this.getValues().first()
                       + units
                       + common
                       + this.getProbabilities().first()
                       + "] AND < "
                       + this.getValues().second()
                       + units
                       + common
                       + this.getProbabilities().second()
                       + "]"
                       + append;
            }
            return c + this.getValues().first() + units + common + this.getProbabilities().first() + "]" + append;
        }
        // Real value only
        else if ( this.hasValues() )
        {
            if ( this.hasBetweenCondition() )
            {
                return ">= " + this.getValues().first()
                       + units
                       + " AND < "
                       + this.getValues().second()
                       + units
                       + append;
            }
            return c + this.getValues().first() + units + append;
        }
        // Probability only
        else
        {
            if ( this.hasBetweenCondition() )
            {
                return "Pr >= " + this.getProbabilities().first()
                       + " AND < "
                       + this.getProbabilities().second()
                       + append;
            }
            return "Pr " + c + this.getProbabilities().first() + append;
        }
    }

    @Override
    public String toStringWithoutUnits()
    {
        if ( hasUnits() )
        {
            return toString().replaceAll( " " + this.getUnits().toString(), "" );
        }
        return toString();
    }

    @Override
    public String toStringSafe()
    {
        String safe = toString();
        // Replace spaces and special characters: note the order of application matters
        safe = safe.replaceAll( ">=", "GTE" );
        safe = safe.replaceAll( "<=", "LTE" );
        safe = safe.replaceAll( ">", "GT" );
        safe = safe.replaceAll( "<", "LT" );
        safe = safe.replaceAll( "=", "EQ" );
        safe = safe.replaceAll( "Pr ", "Pr_" );
        safe = safe.replaceAll( " ", "_" );
        safe = safe.replace( "[", "" );
        safe = safe.replace( "]", "" );
        safe = safe.replace( "(", "" );
        safe = safe.replace( ")", "" );
        return safe;
    }

    @Override
    public int compareTo( final Threshold o )
    {
        Objects.requireNonNull( o, "Specify a non-null threshold for comparison" );

        //Compare condition
        int returnMe = this.getCondition().compareTo( o.getCondition() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }

        //Check for status of optional elements
        returnMe = comparePresenceAbsence( o );

        if ( returnMe != 0 )
        {
            return returnMe;
        }

        //Compare ordinary values
        if ( hasValues() )
        {
            returnMe = this.getValues().compareTo( o.getValues() );
            if ( returnMe != 0 )
            {
                return returnMe;
            }
        }

        //Compare probability values
        if ( hasProbabilities() )
        {
            returnMe = this.getProbabilities().compareTo( o.getProbabilities() );
            if ( returnMe != 0 )
            {
                return returnMe;
            }
        }

        //Compare labels
        if ( hasLabel() )
        {
            returnMe = this.getLabel().compareTo( o.getLabel() );
            if ( returnMe != 0 )
            {
                return returnMe;
            }
        }

        //Compare units
        if ( hasUnits() )
        {
            returnMe = this.getUnits().compareTo( o.getUnits() );
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
        Double lowerBound;
        Double upperBound;

        // Ordinary values are canonical
        if ( hasValues() )
        {
            lowerBound = this.getValues().first();
            upperBound = this.getValues().second();
        }
        else
        {
            lowerBound = this.getProbabilities().first();
            upperBound = this.getProbabilities().second();
        }

        switch ( condition )
        {
            case GREATER:
                return t > lowerBound;
            case LESS:
                return t < lowerBound;
            case GREATER_EQUAL:
                return t >= lowerBound;
            case LESS_EQUAL:
                return t <= lowerBound;
            case BETWEEN:
                return t >= lowerBound && t < upperBound;
            case EQUAL:
                return Math.abs( t - lowerBound ) < .00000001;
            default:
                throw new UnsupportedOperationException( "Unexpected logical condition." );
        }
    }

    @Override
    public boolean isFinite()
    {
        boolean returnMe = true;
        if ( this.hasValues() )
        {
            returnMe = Double.isFinite( this.getValues().first() );
            if ( this.hasBetweenCondition() )
            {
                returnMe = returnMe && Double.isFinite( this.getValues().second() );
            }
        }
        if ( this.hasProbabilities() )
        {
            returnMe = returnMe && Double.isFinite( this.getProbabilities().first() );
            if ( this.hasBetweenCondition() )
            {
                returnMe = returnMe && Double.isFinite( this.getProbabilities().second() );
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
         * The values
         */

        private OneOrTwoDoubles values;

        /**
         * The probabilities.
         */

        private OneOrTwoDoubles probabilities;

        /**
         * The threshold label or null.
         */

        private String label;

        /**
         * The units associated with the threshold.
         */

        private Dimension units;

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
         * Sets the values
         * 
         * @param values the values
         * @return the builder
         */

        ThresholdBuilder setValues( OneOrTwoDoubles values )
        {
            this.values = values;
            return this;
        }

        /**
         * Sets the probability.
         * 
         * @param probabilities the probabilities
         * @return the builder
         */

        ThresholdBuilder setProbabilities( OneOrTwoDoubles probabilities )
        {
            this.probabilities = probabilities;
            return this;
        }

        /**
         * Sets the label for the threshold.
         * 
         * @param label the threshold label
         * @return the builder
         */

        ThresholdBuilder setLabel( String label )
        {
            this.label = label;
            return this;
        }

        /**
         * Sets the units associated with the threshold.
         * 
         * @param units the units
         * @return the builder
         */

        ThresholdBuilder setUnits( Dimension units )
        {
            this.units = units;
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
     */

    private SafeThreshold( ThresholdBuilder builder )
    {
        //Set, then validate
        this.condition = builder.condition;
        this.label = builder.label;
        this.units = builder.units;

        // Set values
        final OneOrTwoDoubles localValues = builder.values;
        if ( Objects.nonNull( localValues ) )
        {
            this.values = SafeOneOrTwoDoubles.of( localValues.first(), localValues.second() );
        }
        else
        {
            this.values = null;
        }

        // Set probabilities
        final OneOrTwoDoubles localProbabilities = builder.probabilities;
        if ( Objects.nonNull( localProbabilities ) )
        {
            this.probabilities = SafeOneOrTwoDoubles.of( localProbabilities.first(), localProbabilities.second() );
        }
        else
        {
            this.probabilities = null;
        }

        //Bounds checks
        Objects.requireNonNull( condition, "Specify a non-null condition." );

        //Do not allow only an upper threshold or all null thresholds
        if ( !this.hasValues() && !this.hasProbabilities() )
        {
            throw new IllegalArgumentException( "Specify one or more values for the threshold." );
        }

        //Check the probability
        if ( this.hasProbabilities()
             && !this.getProbabilities().first().equals( Double.NEGATIVE_INFINITY )
             && ( this.getProbabilities().first() < 0.0 || this.getProbabilities().first() > 1.0 ) )
        {
            throw new IllegalArgumentException( "The threshold probability is out of bounds [0,1]: "
                                                + this.getProbabilities().first() );
        }

        //Check a two-sided threshold
        if ( this.hasBetweenCondition() )
        {
            this.validateTwoSidedThreshold();
        }

        //Check a one-sided threshold
        else
        {
            this.validateOneSidedThreshold();
        }

        //Check for no label when setting threshold as "all data"
        if ( !this.isFinite() && this.hasLabel() )
        {
            throw new IllegalArgumentException( "Cannot set a label for an infinite threshold, as the label is "
                                                + "reserved." );
        }
    }

    /**
     * Validates the parameters for a one-sided threshold.  
     * 
     * @throws IllegalArgumentException if the validation fails
     */

    void validateOneSidedThreshold()
    {

        if ( this.hasValues() && Objects.nonNull( this.getValues().second() ) )
        {
            throw new IllegalArgumentException( "Specify a null upper threshold or define an appropriate "
                                                + "BETWEEN condition." );
        }
        if ( this.hasProbabilities() && Objects.nonNull( this.getProbabilities().second() ) )
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
        if ( ( this.hasProbabilities() && !this.getProbabilities().hasTwo() )
             || ( this.hasValues() && !this.getValues().hasTwo() ) )
        {
            throw new IllegalArgumentException( "When constructing a BETWEEN condition, thresholds must be defined "
                                                + "in pairs." );
        }

        if ( this.hasValues() && Objects.nonNull( values.first() ) && values.second() <= values.first() )
        {
            throw new IllegalArgumentException( "The upper threshold must be greater than the lower threshold: ["
                                                + values.first() + "," + values.second() + "]." );
        }
        // Upper bound is less than or equal to lower bound
        if ( this.hasProbabilities() && Objects.nonNull( probabilities.first() )
             && probabilities.second() <= probabilities.first() )
        {
            throw new IllegalArgumentException( "The upper threshold probability must be greater than the "
                                                + "lower threshold probability: ["
                                                + probabilities.first()
                                                + ","
                                                + probabilities.second()
                                                + "]." );
        }
        // Upper bound is finite and invalid
        if ( this.hasProbabilities() && this.getProbabilities().hasTwo()
             && !probabilities.second().equals( Double.POSITIVE_INFINITY )
             && probabilities.second() > 1.0 )
        {
            throw new IllegalArgumentException( "The upper threshold probability is out of bounds [0,1]: "
                                                + probabilities.second() );
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
     * Compares the input against the current threshold for equivalence in terms of:
     * 
     * <ol>
     * <li>{@link #hasValues()}</li>
     * <li>{@link #hasProbabilities()}</li>
     * <li>{@link #hasLabels()}</li>
     * <li>{@link #hasUnits()}</li>
     * </ol>
     * 
     * @param o the threshold
     * @return a negative, zero, or positive integer if this threshold is less than, equal to, or greater than the 
     *            input, respectively
     */

    private int comparePresenceAbsence( final Threshold o )
    {
        Objects.requireNonNull( o, "Specify a non-null threshold for comparison" );

        //Check for equal status of the values available        
        int returnMe = Boolean.compare( this.hasValues(), o.hasValues() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }
        returnMe = Boolean.compare( this.hasProbabilities(), o.hasProbabilities() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }
        returnMe = Boolean.compare( this.hasLabel(), o.hasLabel() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }
        returnMe = Boolean.compare( this.hasUnits(), o.hasUnits() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }
        return 0;
    }

    /**
     * Returns <code>true</code> if the optional elements of a threshold are equal, otherwise <code>false</code>.
     * 
     * @param in the threshold to test
     * @return true if the optional elements are equal
     */

    private boolean areOptionalStatesEqual( SafeThreshold in )
    {
        boolean returnMe = true;

        if ( hasValues() )
        {
            returnMe = this.getValues().equals( in.getValues() );
        }

        if ( hasProbabilities() )
        {
            returnMe = returnMe && this.getProbabilities().equals( in.getProbabilities() );
        }

        if ( hasLabel() )
        {
            returnMe = returnMe && this.getLabel().equals( in.getLabel() );
        }

        if ( hasUnits() )
        {
            returnMe = returnMe && this.getUnits().equals( in.getUnits() );
        }

        return returnMe;
    }

}
