package wres.datamodel.thresholds;

import java.util.Objects;
import java.util.function.DoublePredicate;

import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.sampledata.MeasurementUnit;
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
 * a threshold may comprise a label and a {@link MeasurementUnit} that describes the units of the real-valued thresholds.</p>
 * 
 * <p>This implementation is immutable.</p>
 * 
 * @author james.brown@hydrosolved.com
 */

public class Threshold implements Comparable<Threshold>, DoublePredicate
{

    /**
     * The real values or null.
     */

    private final OneOrTwoDoubles values;

    /**
     * The probability values or null.
     */

    private final OneOrTwoDoubles probabilities;

    /**
     * The threshold condition.
     */

    private final Operator condition;

    /**
     * The type of data to which the threshold applies.
     */

    private final ThresholdDataType dataType;

    /**
     * The label associated with the threshold.
     */

    private final String label;

    /**
     * The units associated with the threshold.
     */

    private final MeasurementUnit units;

    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param values the values
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @return a threshold
     */

    public static Threshold of( OneOrTwoDoubles values, Operator condition, ThresholdDataType dataType )
    {
        return Threshold.of( values, condition, dataType, null, null );
    }


    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param values the values
     * @param condition the threshold condition
     * @param units the optional units for the threshold values
     * @param dataType the data to which the threshold applies
     * @return a threshold
     */

    public static Threshold of( OneOrTwoDoubles values,
                                Operator condition,
                                ThresholdDataType dataType,
                                MeasurementUnit units )
    {
        return Threshold.of( values, condition, dataType, null, units );
    }

    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param values the threshold values
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @param label an optional label
     * @param units the optional units for the threshold values
     * @return a threshold
     */

    public static Threshold of( OneOrTwoDoubles values,
                                Operator condition,
                                ThresholdDataType dataType,
                                String label,
                                MeasurementUnit units )
    {
        return new ThresholdBuilder().setValues( values )
                                     .setCondition( condition )
                                     .setDataType( dataType )
                                     .setLabel( label )
                                     .setUnits( units )
                                     .build();
    }

    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param values the values
     * @param probabilities the probabilities
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @return a threshold
     */

    public static Threshold ofQuantileThreshold( OneOrTwoDoubles values,
                                                 OneOrTwoDoubles probabilities,
                                                 Operator condition,
                                                 ThresholdDataType dataType )
    {
        return Threshold.ofQuantileThreshold( values, probabilities, condition, dataType, null, null );
    }

    /**
     * Returns a {@link Threshold} from the specified input
     * 
     * @param values the value or null
     * @param probabilities the probabilities or null
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @param label an optional label
     * @param units the optional units for the quantiles
     * @return a quantile
     */

    public static Threshold ofQuantileThreshold( OneOrTwoDoubles values,
                                                 OneOrTwoDoubles probabilities,
                                                 Operator condition,
                                                 ThresholdDataType dataType,
                                                 String label,
                                                 MeasurementUnit units )
    {
        return new ThresholdBuilder().setValues( values )
                                     .setProbabilities( probabilities )
                                     .setCondition( condition )
                                     .setDataType( dataType )
                                     .setLabel( label )
                                     .setUnits( units )
                                     .build();
    }

    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param probabilities the probabilities
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @return a threshold
     */

    public static Threshold ofProbabilityThreshold( OneOrTwoDoubles probabilities,
                                                    Operator condition,
                                                    ThresholdDataType dataType )
    {
        return Threshold.ofProbabilityThreshold( probabilities, condition, dataType, null, null );
    }

    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param probabilities the probabilities
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @param units the optional units for the threshold values
     * @return a threshold
     */

    public static Threshold ofProbabilityThreshold( OneOrTwoDoubles probabilities,
                                                    Operator condition,
                                                    ThresholdDataType dataType,
                                                    MeasurementUnit units )
    {
        return Threshold.ofProbabilityThreshold( probabilities, condition, dataType, null, units );
    }

    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param probabilities the probabilities
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @param label an optional label
     * @return a threshold
     */

    public static Threshold ofProbabilityThreshold( OneOrTwoDoubles probabilities,
                                                    Operator condition,
                                                    ThresholdDataType dataType,
                                                    String label )
    {
        return Threshold.ofProbabilityThreshold( probabilities, condition, dataType, label, null );
    }

    /**
     * Returns {@link Threshold} from the specified input. Both inputs must be in the unit interval, [0,1].
     * 
     * @param probabilities the probabilities
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @param label an optional label
     * @param units an optional set of units to use when deriving quantiles from probability thresholds
     * @return a threshold
     */

    public static Threshold ofProbabilityThreshold( OneOrTwoDoubles probabilities,
                                                    Operator condition,
                                                    ThresholdDataType dataType,
                                                    String label,
                                                    MeasurementUnit units )
    {
        return new ThresholdBuilder().setProbabilities( probabilities )
                                     .setCondition( condition )
                                     .setDataType( dataType )
                                     .setLabel( label )
                                     .setUnits( units )
                                     .build();
    }

    /**
     * Returns <code>true</code> if the threshold contains one or more ordinary (non-probability) values, otherwise
     * <code>false</code>.
     * 
     * @return true if ordinary values are defined, false otherwise
     */

    public boolean hasValues()
    {
        return Objects.nonNull( this.getValues() );
    }

    /**
     * Returns <code>true</code> if the threshold contains one or more probability values, otherwise <code>false</code>.
     * 
     * @return true if probability values are defined, false otherwise
     */

    public boolean hasProbabilities()
    {
        return Objects.nonNull( this.getProbabilities() );
    }

    /**
     * Returns <code>true</code> if {@link #getLabel()} returns a non-null label, otherwise <code>false</code>.
     * 
     * @return true if the threshold has a label, false otherwise
     */

    public boolean hasLabel()
    {
        return Objects.nonNull( this.getLabel() );
    }

    /**
     * Returns <code>true</code> if the threshold is a quantile; that is, when {@link #hasValues()} and 
     * {@link #hasProbabilities()} both return <code>true</code>.
     * 
     * @return true if the threshold is a quantile, false otherwise
     */

    public boolean isQuantile()
    {
        return this.hasValues() && this.hasProbabilities();
    }

    /**
     * Returns <code>true</code> if {@link #getUnits()} returns a non-null {@link MeasurementUnit}, 
     * otherwise <code>false</code>.
     * 
     * @return true if the threshold units are defined, false otherwise
     */

    public boolean hasUnits()
    {
        return Objects.nonNull( this.getUnits() );
    }

    /**
     * Returns the {@link ThresholdType}.
     * 
     * @return the threshold type
     */

    public ThresholdType getType()
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

    public ThresholdDataType getDataType()
    {
        return dataType;
    }

    /**
     * Returns the threshold values or null if no threshold values are defined. If no threshold values are defined,
     * {@link #getProbabilities()} always returns non-null.
     * 
     * @return the threshold values or null
     */

    public OneOrTwoDoubles getValues()
    {
        return values;
    }

    /**
     * Returns the probability values or null if no probability values are defined. If no probability values are 
     * defined, {@link #getValues()} always returns non-null.
     * 
     * @return the threshold values or null
     */

    public OneOrTwoDoubles getProbabilities()
    {
        return probabilities;
    }

    /**
     * Returns the units associated with the {@link Threshold} or null. Always returns null when 
     * {@link #hasValues()} returns <code>false</code>.
     * 
     * @return the units or null
     */

    public MeasurementUnit getUnits()
    {
        return units;
    }

    /**
     * Returns the logical operator associated with the threshold.
     * 
     * @return the logical operator associated with the threshold
     */

    public Operator getOperator()
    {
        return condition;
    }

    /**
     * Returns the label associated with the {@link Threshold} or null if no label is defined.
     * 
     * @return the label or null
     */

    public String getLabel()
    {
        return label;
    }

    /**
     * Returns <code>true</code> if the {@link Threshold} condition corresponds to a {@link Operator#BETWEEN} condition.
     * 
     * @return true if the condition is a {@link Operator#BETWEEN} condition, false otherwise.
     */

    public boolean hasBetweenCondition()
    {
        return this.getOperator().equals( Operator.BETWEEN );
    }

    /**
     * Returns <code>true</code> if {@link Double#isFinite(double)} returns <code>true</code> for all threshold values,
     * otherwise <code>false</code>.
     * 
     * @return true if the threshold is finite, false otherwise
     */

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
     * Returns a string representation of the {@link Threshold} that contains only alphanumeric characters A-Z, a-z, 
     * and 0-9 and, additionally, the underscore character to separate between elements, and the period character as
     * a decimal separator.
     * 
     * @return a safe string representation
     */

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
        
        // Any others, replace with empty
        safe = safe.replaceAll("[^a-zA-Z0-9_.]", "");
        
        return safe;
    }

    /**
     * Returns a string representation of the {@link Threshold} without any units. This is useful when forming string
     * representions of a collection of {@link Threshold} and abstracting the common units to a higher level.
     * 
     * @return a string without any units
     */

    public String toStringWithoutUnits()
    {
        if ( hasUnits() )
        {
            return toString().replaceAll( " " + this.getUnits().toString(), "" );
        }
        
        return toString();
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof Threshold ) )
        {
            return false;
        }

        final Threshold in = (Threshold) o;
        boolean first = this.hasValues() == in.hasValues()
                        && this.hasProbabilities() == in.hasProbabilities();
        boolean second = hasLabel() == in.hasLabel()
                         && this.getOperator() == in.getOperator()
                         && this.getDataType() == in.getDataType()
                         && this.hasUnits() == in.hasUnits();

        return first && second && this.areOptionalStatesEqual( in );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.values, this.probabilities, this.condition, this.dataType, this.label, this.units );
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

        String stringUnits = "";
        if ( this.hasUnits() )
        {
            stringUnits = " " + this.getUnits().toString();
        }

        // Quantile
        if ( this.isQuantile() )
        {
            String common = " [Pr = ";
            if ( this.hasBetweenCondition() )
            {
                return ">= " + this.getValues().first()
                       + stringUnits
                       + common
                       + this.getProbabilities().first()
                       + "] AND < "
                       + this.getValues().second()
                       + stringUnits
                       + common
                       + this.getProbabilities().second()
                       + "]"
                       + append;
            }
            return c + this.getValues().first() + stringUnits + common + this.getProbabilities().first() + "]" + append;
        }
        // Real value only
        else if ( this.hasValues() )
        {
            if ( this.hasBetweenCondition() )
            {
                return ">= " + this.getValues().first()
                       + stringUnits
                       + " AND < "
                       + this.getValues().second()
                       + stringUnits
                       + append;
            }
            return c + this.getValues().first() + stringUnits + append;
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
    public int compareTo( final Threshold o )
    {
        Objects.requireNonNull( o, "Specify a non-null threshold for comparison" );

        //Compare condition
        int returnMe = this.getOperator().compareTo( o.getOperator() );
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

        //Compare data type
        returnMe = this.getDataType().compareTo( o.getDataType() );
        if ( returnMe != 0 )
        {
            return returnMe;
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
    public boolean test( double t )
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

    /**
     * A builder to build the threshold.
     */

    public static class ThresholdBuilder
    {

        /**
         * The threshold condition.
         */

        private Operator condition;

        /**
         * The threshold data type.
         */

        private ThresholdDataType dataType;

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

        private MeasurementUnit units;

        /**
         * Sets the {@link Operator} associated with the threshold
         * 
         * @param condition the threshold condition
         * @return the builder
         */

        public ThresholdBuilder setCondition( Operator condition )
        {
            this.condition = condition;
            return this;
        }

        /**
         * Sets the {@link ThresholdDataType} associated with the threshold
         * 
         * @param dataType the data type
         * @return the builder
         */

        public ThresholdBuilder setDataType( ThresholdDataType dataType )
        {
            this.dataType = dataType;
            return this;
        }

        /**
         * Sets the values
         * 
         * @param values the values
         * @return the builder
         */

        public ThresholdBuilder setValues( OneOrTwoDoubles values )
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

        public ThresholdBuilder setProbabilities( OneOrTwoDoubles probabilities )
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

        public ThresholdBuilder setLabel( String label )
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

        public ThresholdBuilder setUnits( MeasurementUnit units )
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
            return new Threshold( this );
        }
    }

    /**
     * Construct the threshold.
     *
     * @param builder the builder
     */

    private Threshold( ThresholdBuilder builder )
    {
        //Set, then validate
        this.condition = builder.condition;
        this.label = builder.label;
        this.units = builder.units;
        this.dataType = builder.dataType;

        // Set values
        final OneOrTwoDoubles localValues = builder.values;
        if ( Objects.nonNull( localValues ) )
        {
            this.values = OneOrTwoDoubles.of( localValues.first(), localValues.second() );
        }
        else
        {
            this.values = null;
        }

        // Set probabilities
        final OneOrTwoDoubles localProbabilities = builder.probabilities;
        if ( Objects.nonNull( localProbabilities ) )
        {
            this.probabilities = OneOrTwoDoubles.of( localProbabilities.first(), localProbabilities.second() );
        }
        else
        {
            this.probabilities = null;
        }

        //Bounds checks
        Objects.requireNonNull( condition, "Specify a non-null condition." );

        Objects.requireNonNull( dataType, "Specify a non-null data type." );

        //Do not allow only an upper threshold or all null thresholds
        if ( !this.hasValues() && !this.hasProbabilities() )
        {
            throw new IllegalArgumentException( "Specify one or more values for the threshold." );
        }

        //Check the probability
        this.validateProbabilities();

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
     * Validates the probabilities.  
     * 
     * @throws IllegalArgumentException if the validation fails
     */

    void validateProbabilities()
    {
        if ( this.hasProbabilities() )
        {
            if ( !this.getProbabilities().first().equals( Double.NEGATIVE_INFINITY )
                 && ( this.getProbabilities().first() < 0.0 || this.getProbabilities().first() > 1.0 ) )
            {
                throw new IllegalArgumentException( "The threshold probability is out of bounds [0,1]: "
                                                    + this.getProbabilities().first() );
            }

            // Cannot have LESS_THAN on the lower bound
            if ( Math.abs( this.getProbabilities().first() - 0.0 ) < .00000001 && this.getOperator() == Operator.LESS )
            {
                throw new IllegalArgumentException( "Cannot apply a threshold operator of '<' to the lower bound "
                                                    + "probability of 0.0." );
            }
            // Cannot have GREATER_THAN on the upper bound
            if ( Math.abs( this.getProbabilities().first() - 1.0 ) < .00000001
                 && this.getOperator() == Operator.GREATER )
            {
                throw new IllegalArgumentException( "Cannot apply a threshold operator of '>' to the upper bound "
                                                    + "probability of 1.0." );
            }
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
                                                + values.first()
                                                + ","
                                                + values.second()
                                                + "]." );
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

    private boolean areOptionalStatesEqual( Threshold in )
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
