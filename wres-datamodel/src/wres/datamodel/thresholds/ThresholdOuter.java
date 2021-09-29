package wres.datamodel.thresholds;

import java.util.Objects;
import java.util.function.DoublePredicate;

import com.google.protobuf.DoubleValue;

import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdType;
import wres.statistics.generated.Threshold;
import wres.statistics.generated.Threshold.ThresholdOperator;

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
 * <p>The internal data is stored, and accessible, as a {@link Threshold}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class ThresholdOuter implements Comparable<ThresholdOuter>, DoublePredicate
{
    /**
     * Threshold that represents "all data", i.e., no filtering.
     */

    public static final ThresholdOuter ALL_DATA = ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                     ThresholdConstants.Operator.GREATER,
                                                                     ThresholdConstants.ThresholdDataType.LEFT_AND_RIGHT );

    /**
     * The actual threshold.
     */

    private final Threshold threshold;

    /**
     * Returns {@link ThresholdOuter} from the specified input.
     * 
     * @param values the values
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @return a threshold
     */

    public static ThresholdOuter of( OneOrTwoDoubles values, Operator condition, ThresholdDataType dataType )
    {
        return ThresholdOuter.of( values, condition, dataType, null, null );
    }

    /**
     * Returns {@link ThresholdOuter} from the specified input.
     * 
     * @param values the values
     * @param condition the threshold condition
     * @param units the optional units for the threshold values
     * @param dataType the data to which the threshold applies
     * @return a threshold
     */

    public static ThresholdOuter of( OneOrTwoDoubles values,
                                     Operator condition,
                                     ThresholdDataType dataType,
                                     MeasurementUnit units )
    {
        return ThresholdOuter.of( values, condition, dataType, null, units );
    }

    /**
     * Returns {@link ThresholdOuter} from the specified input.
     * 
     * @param values the threshold values
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @param label an optional label
     * @param units the optional units for the threshold values
     * @return a threshold
     */

    public static ThresholdOuter of( OneOrTwoDoubles values,
                                     Operator condition,
                                     ThresholdDataType dataType,
                                     String label,
                                     MeasurementUnit units )
    {
        return new Builder().setValues( values )
                            .setOperator( condition )
                            .setDataType( dataType )
                            .setLabel( label )
                            .setUnits( units )
                            .build();
    }

    /**
     * Returns {@link ThresholdOuter} from the specified input.
     * 
     * @param values the values
     * @param probabilities the probabilities
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @return a threshold
     */

    public static ThresholdOuter ofQuantileThreshold( OneOrTwoDoubles values,
                                                      OneOrTwoDoubles probabilities,
                                                      Operator condition,
                                                      ThresholdDataType dataType )
    {
        return ThresholdOuter.ofQuantileThreshold( values, probabilities, condition, dataType, null, null );
    }

    /**
     * Returns a {@link ThresholdOuter} from the specified input
     * 
     * @param values the value or null
     * @param probabilities the probabilities or null
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @param label an optional label
     * @param units the optional units for the quantiles
     * @return a quantile
     */

    public static ThresholdOuter ofQuantileThreshold( OneOrTwoDoubles values,
                                                      OneOrTwoDoubles probabilities,
                                                      Operator condition,
                                                      ThresholdDataType dataType,
                                                      String label,
                                                      MeasurementUnit units )
    {
        return new Builder().setValues( values )
                            .setProbabilities( probabilities )
                            .setOperator( condition )
                            .setDataType( dataType )
                            .setLabel( label )
                            .setUnits( units )
                            .build();
    }

    /**
     * Returns {@link ThresholdOuter} from the specified input.
     * 
     * @param probabilities the probabilities
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @return a threshold
     */

    public static ThresholdOuter ofProbabilityThreshold( OneOrTwoDoubles probabilities,
                                                         Operator condition,
                                                         ThresholdDataType dataType )
    {
        return ThresholdOuter.ofProbabilityThreshold( probabilities, condition, dataType, null, null );
    }

    /**
     * Returns {@link ThresholdOuter} from the specified input.
     * 
     * @param probabilities the probabilities
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @param units the optional units for the threshold values
     * @return a threshold
     */

    public static ThresholdOuter ofProbabilityThreshold( OneOrTwoDoubles probabilities,
                                                         Operator condition,
                                                         ThresholdDataType dataType,
                                                         MeasurementUnit units )
    {
        return ThresholdOuter.ofProbabilityThreshold( probabilities, condition, dataType, null, units );
    }

    /**
     * Returns {@link ThresholdOuter} from the specified input.
     * 
     * @param probabilities the probabilities
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @param label an optional label
     * @return a threshold
     */

    public static ThresholdOuter ofProbabilityThreshold( OneOrTwoDoubles probabilities,
                                                         Operator condition,
                                                         ThresholdDataType dataType,
                                                         String label )
    {
        return ThresholdOuter.ofProbabilityThreshold( probabilities, condition, dataType, label, null );
    }

    /**
     * Returns {@link ThresholdOuter} from the specified input. Both inputs must be in the unit interval, [0,1].
     * 
     * @param probabilities the probabilities
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @param label an optional label
     * @param units an optional set of units to use when deriving quantiles from probability thresholds
     * @return a threshold
     */

    public static ThresholdOuter ofProbabilityThreshold( OneOrTwoDoubles probabilities,
                                                         Operator condition,
                                                         ThresholdDataType dataType,
                                                         String label,
                                                         MeasurementUnit units )
    {
        return new Builder().setProbabilities( probabilities )
                            .setOperator( condition )
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
        return this.getThreshold().hasLeftThresholdValue();
    }

    /**
     * Returns <code>true</code> if the threshold contains one or more probability values, otherwise <code>false</code>.
     * 
     * @return true if probability values are defined, false otherwise
     */

    public boolean hasProbabilities()
    {
        return this.getThreshold().hasLeftThresholdProbability();
    }

    /**
     * Returns <code>true</code> if {@link #getLabel()} returns a non-null label, otherwise <code>false</code>.
     * 
     * @return true if the threshold has a label, false otherwise
     */

    public boolean hasLabel()
    {
        String label = this.getLabel();
        return Objects.nonNull( label ) && !label.isBlank();
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
        Threshold innerThreshold = this.getThreshold();
        Threshold.ThresholdDataType type = innerThreshold.getDataType();
        return ThresholdDataType.valueOf( type.name() );
    }

    /**
     * Returns the threshold values or null if no threshold values are defined. If no threshold values are defined,
     * {@link #getProbabilities()} always returns non-null.
     * 
     * @return the threshold values or null
     */

    public OneOrTwoDoubles getValues()
    {
        OneOrTwoDoubles returnMe = null;

        Threshold innerThreshold = this.getThreshold();

        if ( innerThreshold.hasLeftThresholdValue() && innerThreshold.hasRightThresholdValue() )
        {
            returnMe = OneOrTwoDoubles.of( innerThreshold.getLeftThresholdValue().getValue(),
                                           innerThreshold.getRightThresholdValue().getValue() );
        }
        else if ( innerThreshold.hasLeftThresholdValue() )
        {
            returnMe = OneOrTwoDoubles.of( innerThreshold.getLeftThresholdValue().getValue() );
        }

        return returnMe;
    }

    /**
     * Returns the probability values or null if no probability values are defined. If no probability values are 
     * defined, {@link #getValues()} always returns non-null.
     * 
     * @return the threshold values or null
     */

    public OneOrTwoDoubles getProbabilities()
    {
        OneOrTwoDoubles returnMe = null;

        Threshold innerThreshold = this.getThreshold();

        if ( innerThreshold.hasLeftThresholdProbability() && innerThreshold.hasRightThresholdProbability() )
        {
            returnMe = OneOrTwoDoubles.of( innerThreshold.getLeftThresholdProbability().getValue(),
                                           innerThreshold.getRightThresholdProbability().getValue() );
        }
        else if ( innerThreshold.hasLeftThresholdProbability() )
        {
            returnMe = OneOrTwoDoubles.of( innerThreshold.getLeftThresholdProbability().getValue() );
        }

        return returnMe;
    }

    /**
     * Returns the units associated with the {@link ThresholdOuter} or null. Always returns null when 
     * {@link #hasValues()} returns <code>false</code>.
     * 
     * @return the units or null
     */

    public MeasurementUnit getUnits()
    {
        MeasurementUnit returnMe = null;
        String unit = this.getThreshold().getThresholdValueUnits();
        if ( Objects.nonNull( unit ) && !unit.isBlank() )
        {
            returnMe = MeasurementUnit.of( unit );
        }

        return returnMe;
    }

    /**
     * Returns the logical operator associated with the threshold.
     * 
     * @return the logical operator associated with the threshold
     */

    public Operator getOperator()
    {
        return Operator.valueOf( this.getThreshold().getOperator().name() );
    }

    /**
     * Returns the label associated with the {@link ThresholdOuter} or null if no label is defined.
     * 
     * @return the label or null
     */

    public String getLabel()
    {
        return this.getThreshold().getName();
    }

    /**
     * Returns the canonical representation of the threshold wrapped by this instance.
     * 
     * @return the canonical representation
     */

    public Threshold getThreshold()
    {
        return this.threshold;
    }

    /**
     * Returns <code>true</code> if the {@link ThresholdOuter} condition corresponds to a {@link Operator#BETWEEN} condition.
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
     * Returns true if this threshold is the "all data" threshold.
     * @return true if this threshold is the all data threshold, otherwise false
     */

    public boolean isAllDataThreshold()
    {
        // Identity equals, followed by content equals
        return ThresholdOuter.ALL_DATA == this || ThresholdOuter.ALL_DATA.equals( this );
    }

    /**
     * Returns a string representation of the {@link ThresholdOuter} that contains only alphanumeric characters A-Z, a-z, 
     * and 0-9 and, additionally, the underscore character to separate between elements, and the period character as
     * a decimal separator.
     * 
     * @return a safe string representation
     */

    public String toStringSafe()
    {
        String safe = toString();

        // Replace spaces and special characters: note the order of application matters
        safe = safe.replace( ">=", "GTE" );
        safe = safe.replace( "<=", "LTE" );
        safe = safe.replace( ">", "GT" );
        safe = safe.replace( "<", "LT" );
        safe = safe.replace( "=", "EQ" );
        safe = safe.replace( "Pr ", "Pr_" );
        safe = safe.replace( " ", "_" );
        safe = safe.replace( "[", "" );
        safe = safe.replace( "]", "" );
        safe = safe.replace( "(", "" );
        safe = safe.replace( ")", "" );

        // Any others, replace with empty
        safe = safe.replaceAll( "[^a-zA-Z0-9_.]", "" );

        return safe;
    }

    /**
     * Returns a string representation of the {@link ThresholdOuter} without any units. This is useful when forming string
     * representions of a collection of {@link ThresholdOuter} and abstracting the common units to a higher level.
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
        if ( o == this )
        {
            return true;
        }

        if ( ! ( o instanceof ThresholdOuter ) )
        {
            return false;
        }

        final ThresholdOuter in = (ThresholdOuter) o;

        return in.getThreshold().equals( this.getThreshold() );
    }

    @Override
    public int hashCode()
    {
        return this.threshold.hashCode();
    }

    @Override
    public String toString()
    {
        // Label not used for all data
        if ( this.isAllDataThreshold() )
        {
            return "All data";
        }

        String append = "";
        if ( this.hasLabel() )
        {
            append = " (" + this.getLabel() + ")";
        }

        final String conditionString = this.getConditionString();

        String stringUnits = "";
        if ( this.hasUnits() )
        {
            stringUnits = " " + this.getUnits().toString();
        }

        // Quantile
        if ( this.isQuantile() )
        {
            return this.getQuantileString( conditionString, stringUnits, append );
        }
        // Real value only
        else if ( this.hasValues() )
        {
            return this.getValueString( conditionString, stringUnits, append );
        }
        // Probability only
        else
        {
            return this.getProbabilityString( conditionString, append );
        }
    }

    @Override
    public int compareTo( final ThresholdOuter o )
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

        Operator operator = this.getOperator();

        switch ( operator )
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

    public static class Builder
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

        public Builder setOperator( Operator condition )
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

        public Builder setDataType( ThresholdDataType dataType )
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

        public Builder setValues( OneOrTwoDoubles values )
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

        public Builder setProbabilities( OneOrTwoDoubles probabilities )
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

        public Builder setLabel( String label )
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

        public Builder setUnits( MeasurementUnit units )
        {
            this.units = units;
            return this;
        }

        /**
         * Return the {@link ThresholdOuter}
         * 
         * @return the {@link ThresholdOuter}
         */
        public ThresholdOuter build()
        {
            return new ThresholdOuter( this );
        }

        /**
         * Default constructor.
         */

        public Builder()
        {
        }

        /**
         * Construct with an existing {@link Threshold}.
         * 
         * @param threshold the threshold.
         * @throws NullPointerException if the threshold is null
         */

        public Builder( Threshold threshold )
        {
            Objects.requireNonNull( threshold );

            if ( !threshold.getThresholdValueUnits().isBlank() )
            {
                this.units = MeasurementUnit.of( threshold.getThresholdValueUnits() );
            }
            if ( !threshold.getName().isBlank() )
            {
                this.label = threshold.getName();
            }

            this.condition = Operator.valueOf( threshold.getOperator().name() );
            this.dataType = ThresholdDataType.valueOf( threshold.getDataType().name() );

            if ( threshold.hasLeftThresholdValue() || threshold.hasRightThresholdValue() )
            {
                if ( !threshold.hasRightThresholdValue() )
                {
                    this.values = OneOrTwoDoubles.of( threshold.getLeftThresholdValue().getValue() );
                }
                else
                {
                    this.values = OneOrTwoDoubles.of( threshold.getLeftThresholdValue().getValue(),
                                                      threshold.getRightThresholdValue().getValue() );
                }
            }

            if ( threshold.hasLeftThresholdProbability() || threshold.hasRightThresholdProbability() )
            {
                if ( !threshold.hasRightThresholdProbability() )
                {
                    this.probabilities = OneOrTwoDoubles.of( threshold.getLeftThresholdProbability().getValue() );
                }
                else
                {
                    this.probabilities = OneOrTwoDoubles.of( threshold.getLeftThresholdProbability().getValue(),
                                                             threshold.getRightThresholdProbability().getValue() );
                }
            }
        }
    }

    /**
     * Construct the threshold.
     *
     * @param builder the builder
     */

    private ThresholdOuter( Builder builder )
    {
        //Set, then validate
        Operator operator = builder.condition;
        String label = builder.label;
        MeasurementUnit units = builder.units;
        ThresholdDataType dataType = builder.dataType;
        OneOrTwoDoubles localValues = builder.values;
        OneOrTwoDoubles localProbabilities = builder.probabilities;

        Threshold.Builder thresholdBuilder = Threshold.newBuilder();

        Objects.requireNonNull( operator, "Cannot build a threshold without an operator." );
        Objects.requireNonNull( dataType, "Cannot build a threshold without a threshold data type." );

        ThresholdOperator anOperator = ThresholdOperator.valueOf( operator.name() );
        thresholdBuilder.setOperator( anOperator );

        Threshold.ThresholdDataType aDataType = Threshold.ThresholdDataType.valueOf( dataType.name() );
        thresholdBuilder.setDataType( aDataType );

        if ( Objects.nonNull( label ) )
        {
            thresholdBuilder.setName( label );
        }

        if ( Objects.nonNull( units ) )
        {
            thresholdBuilder.setThresholdValueUnits( units.getUnit() );
        }

        if ( Objects.nonNull( localValues ) )
        {
            thresholdBuilder.setLeftThresholdValue( DoubleValue.of( localValues.first() ) );
            if ( localValues.hasTwo() )
            {
                thresholdBuilder.setRightThresholdValue( DoubleValue.of( localValues.second() ) );
            }
        }

        if ( Objects.nonNull( localProbabilities ) )
        {
            thresholdBuilder.setLeftThresholdProbability( DoubleValue.of( localProbabilities.first() ) );
            if ( localProbabilities.hasTwo() )
            {
                thresholdBuilder.setRightThresholdProbability( DoubleValue.of( localProbabilities.second() ) );
            }
        }

        this.threshold = thresholdBuilder.build();

        this.validate();
    }

    /**
     * @throws IllegalArgumentException if any inputs are invalid
     */

    private void validate()
    {
        if ( this.threshold.getOperator() == ThresholdOperator.UNRECOGNIZED )
        {
            throw new ThresholdException( "Specify a recognized threshold operator." );
        }

        if ( this.threshold.getDataType() == Threshold.ThresholdDataType.UNRECOGNIZED )
        {
            throw new ThresholdException( "Specify a recognized threshold data type." );
        }

        // Do not allow only an upper threshold or all null thresholds
        if ( !this.hasValues() && !this.hasProbabilities() )
        {
            throw new ThresholdException( "Specify one or more values for the threshold." );
        }

        // Check the probability
        this.validateProbabilities();

        // Check a two-sided threshold
        if ( this.hasBetweenCondition() )
        {
            this.validateTwoSidedThreshold();
        }

        // Check a one-sided threshold
        else
        {
            this.validateOneSidedThreshold();
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
                throw new ThresholdException( "The threshold probability is out of bounds [0,1]: "
                                              + this.getProbabilities().first() );
            }

            // Cannot have LESS_THAN on the lower bound
            if ( Math.abs( this.getProbabilities().first() - 0.0 ) < .00000001 && this.getOperator() == Operator.LESS )
            {
                throw new ThresholdException( "Cannot apply a threshold operator of '<' to the lower bound "
                                              + "probability of 0.0." );
            }
            // Cannot have GREATER_THAN on the upper bound
            if ( Math.abs( this.getProbabilities().first() - 1.0 ) < .00000001
                 && this.getOperator() == Operator.GREATER )
            {
                throw new ThresholdException( "Cannot apply a threshold operator of '>' to the upper bound "
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
            throw new ThresholdException( "Specify a null upper threshold or define an appropriate "
                                          + "BETWEEN condition." );
        }
        if ( this.hasProbabilities() && Objects.nonNull( this.getProbabilities().second() ) )
        {
            throw new ThresholdException( "Specify a non-null upper threshold probability or define an "
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
        OneOrTwoDoubles values = this.getValues();
        OneOrTwoDoubles probabilities = this.getProbabilities();

        //Do not allow a partially defined pair of thresholds/probabilities          
        if ( ( this.hasProbabilities() && !this.getProbabilities().hasTwo() )
             || ( this.hasValues() && !this.getValues().hasTwo() ) )
        {
            throw new ThresholdException( "When constructing a BETWEEN condition, thresholds must be defined "
                                          + "in pairs." );
        }

        if ( this.hasValues() && Objects.nonNull( values.first() ) && values.second() <= values.first() )
        {
            throw new ThresholdException( "The upper threshold must be greater than the lower threshold: ["
                                          + values.first()
                                          + ","
                                          + values.second()
                                          + "]." );
        }
        // Upper bound is less than or equal to lower bound
        if ( this.hasProbabilities() && Objects.nonNull( probabilities.first() )
             && probabilities.second() <= probabilities.first() )
        {
            throw new ThresholdException( "The upper threshold probability must be greater than the "
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
            throw new ThresholdException( "The upper threshold probability is out of bounds [0,1]: "
                                          + probabilities.second() );
        }

    }

    /**
     * Returns a string representation of a condition that is not a {@link Operator#BETWEEN}
     * condition.
     * 
     * @return a string for the elementary condition
     */

    private String getConditionString()
    {
        Operator operator = this.getOperator();

        switch ( operator )
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
                return "";
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

    private int comparePresenceAbsence( final ThresholdOuter o )
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
     * @param conditionString the condition string
     * @param stringUnits the units string
     * @param append a string to append
     * @return a string representation of a quantile threshold
     */

    private String getQuantileString( String conditionString, String stringUnits, String append )
    {
        String common = " [Pr = ";

        if ( this.hasBetweenCondition() )
        {
            if ( !this.getValues().first().isNaN() && !this.getValues().second().isNaN() )
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

            return ">= Pr = " +
                   +this.getProbabilities().first()
                   + " AND < Pr = "
                   + this.getProbabilities().second()
                   + append;
        }

        if ( !this.getValues().first().isNaN() )
        {
            return conditionString + this.getValues().first()
                   + stringUnits
                   + common
                   + this.getProbabilities().first()
                   + "]"
                   + append;
        }

        return conditionString
               + "Pr = "
               + this.getProbabilities().first()
               + append;
    }

    /**
     * @param conditionString the condition string
     * @param stringUnits the units string
     * @param append a string to append
     * @return a string representation of a value threshold
     */

    private String getValueString( String conditionString, String stringUnits, String append )
    {
        if ( this.hasBetweenCondition() && !this.getValues().first().isNaN() && !this.getValues().second().isNaN() )
        {
            return ">= " + this.getValues().first()
                   + stringUnits
                   + " AND < "
                   + this.getValues().second()
                   + stringUnits
                   + append;
        }
        else if ( !this.getValues().first().isNaN() )
        {
            return conditionString + this.getValues().first() + stringUnits + append;
        }

        return conditionString + append.replace( " (", "" ).replace( ")", "" );
    }

    /**
     * @param conditionString the condition string
     * @param append a string to append
     * @return a string representation of a probability threshold
     */

    private String getProbabilityString( String conditionString, String append )
    {
        if ( this.hasBetweenCondition() )
        {
            return "Pr >= " + this.getProbabilities().first()
                   + " AND < "
                   + this.getProbabilities().second()
                   + append;
        }
        return "Pr " + conditionString + this.getProbabilities().first() + append;
    }

}
