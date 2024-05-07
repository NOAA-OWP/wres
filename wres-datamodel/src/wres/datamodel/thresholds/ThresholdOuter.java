package wres.datamodel.thresholds;

import java.util.Comparator;
import java.util.Objects;
import java.util.function.DoublePredicate;

import net.jcip.annotations.Immutable;

import wres.config.yaml.components.ThresholdOrientation;
import wres.datamodel.types.OneOrTwoDoubles;
import wres.datamodel.messages.MessageUtilities;
import wres.datamodel.pools.MeasurementUnit;
import wres.config.yaml.components.ThresholdOperator;
import wres.config.yaml.components.ThresholdType;
import wres.statistics.generated.Threshold;

/**
 * <p>A threshold used to partition or classify data. Wraps a canonical {@link Threshold} and adds behavior. A
 * threshold contains one or both of:
 *
 * <ol>
 * <li>One or two real values, contained in a {@link OneOrTwoDoubles}.</li>
 * <li>One or two probability values, contained in a {@link OneOrTwoDoubles}.</li>
 * </ol>
 *
 * <p>The presence of the former is determined by {@link #hasValues()}. The presence of the latter is determined by 
 * {@link #hasProbabilities()}. If both are present, the threshold is a "quantile", as revealed by 
 * {@link #isQuantile()}. A summary of the threshold type can be obtained from {@link #getType()}.
 *
 * <p>Additionally, a threshold comprises an {@link ThresholdOperator}, denoting the type of threshold condition. Optionally,
 * a threshold may comprise a label and a {@link MeasurementUnit} that describes the units of the real-valued
 * thresholds.
 *
 * @author James Brown
 */
@Immutable
public class ThresholdOuter implements Comparable<ThresholdOuter>, DoublePredicate
{
    /**
     * Threshold that represents "all data", i.e., no filtering.
     */

    public static final ThresholdOuter ALL_DATA =
            ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                               wres.config.yaml.components.ThresholdOperator.GREATER,
                               ThresholdOrientation.LEFT_AND_RIGHT );

    /**
     * The actual threshold.
     */

    private final Threshold threshold;

    /**
     * The type of threshold or context in which it was declared. Note: In principle, it would be simpler to add this
     * to the canonical type, but the application or intention of the threshold is not really relevant in that context.
     * Specifically, it would only help to distinguish between a probability threshold and a probability classifier,
     * which is a probability threshold whose application context is to classify.
     */

    private final ThresholdType thresholdType;

    /**
     * Returns {@link ThresholdOuter} from the specified input.
     *
     * @param threshold the threshold
     * @return a threshold
     */

    public static ThresholdOuter of( Threshold threshold )
    {
        return new ThresholdOuter.Builder( threshold )
                .build();
    }

    /**
     * Returns {@link ThresholdOuter} from the specified input.
     *
     * @param threshold the threshold
     * @param type the threshold type
     * @return a threshold
     */

    public static ThresholdOuter of( Threshold threshold, ThresholdType type )
    {
        return new ThresholdOuter.Builder( threshold ).setThresholdType( type )
                                                      .build();
    }

    /**
     * Returns {@link ThresholdOuter} from the specified input.
     *
     * @param values the values
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @return a threshold
     */

    public static ThresholdOuter of( OneOrTwoDoubles values,
                                     wres.config.yaml.components.ThresholdOperator condition,
                                     ThresholdOrientation dataType )
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
                                     wres.config.yaml.components.ThresholdOperator condition,
                                     ThresholdOrientation dataType,
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
                                     ThresholdOperator condition,
                                     ThresholdOrientation dataType,
                                     String label,
                                     MeasurementUnit units )
    {
        return new Builder().setValues( values )
                            .setOperator( condition )
                            .setOrientation( dataType )
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
                                                      wres.config.yaml.components.ThresholdOperator condition,
                                                      ThresholdOrientation dataType )
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
                                                      ThresholdOperator condition,
                                                      ThresholdOrientation dataType,
                                                      String label,
                                                      MeasurementUnit units )
    {
        return new Builder().setValues( values )
                            .setProbabilities( probabilities )
                            .setOperator( condition )
                            .setOrientation( dataType )
                            .setLabel( label )
                            .setUnits( units )
                            .setThresholdType( ThresholdType.QUANTILE )
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
                                                         wres.config.yaml.components.ThresholdOperator condition,
                                                         ThresholdOrientation dataType )
    {
        return ThresholdOuter.ofProbabilityThreshold( probabilities, condition, dataType, null, null );
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
                                                         wres.config.yaml.components.ThresholdOperator condition,
                                                         ThresholdOrientation dataType,
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
                                                         wres.config.yaml.components.ThresholdOperator condition,
                                                         ThresholdOrientation dataType,
                                                         String label,
                                                         MeasurementUnit units )
    {
        return new Builder().setProbabilities( probabilities )
                            .setOperator( condition )
                            .setOrientation( dataType )
                            .setLabel( label )
                            .setUnits( units )
                            .setThresholdType( ThresholdType.PROBABILITY )
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
        return this.thresholdType;
    }

    /**
     * Returns the {@link ThresholdOrientation} to which the threshold applies.
     *
     * @return the threshold data type
     */

    public ThresholdOrientation getOrientation()
    {
        Threshold innerThreshold = this.getThreshold();
        Threshold.ThresholdDataType type = innerThreshold.getDataType();
        return ThresholdOrientation.valueOf( type.name() );
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

        if ( innerThreshold.hasLeftThresholdValue()
             && innerThreshold.hasRightThresholdValue() )
        {
            returnMe = OneOrTwoDoubles.of( innerThreshold.getLeftThresholdValue(),
                                           innerThreshold.getRightThresholdValue() );
        }
        else if ( innerThreshold.hasLeftThresholdValue() )
        {
            returnMe = OneOrTwoDoubles.of( innerThreshold.getLeftThresholdValue() );
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
            returnMe = OneOrTwoDoubles.of( innerThreshold.getLeftThresholdProbability(),
                                           innerThreshold.getRightThresholdProbability() );
        }
        else if ( innerThreshold.hasLeftThresholdProbability() )
        {
            returnMe = OneOrTwoDoubles.of( innerThreshold.getLeftThresholdProbability() );
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
        String unit = this.getThreshold()
                          .getThresholdValueUnits();
        if ( !unit.isBlank() )
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

    public wres.config.yaml.components.ThresholdOperator getOperator()
    {
        return wres.config.yaml.components.ThresholdOperator.valueOf( this.getThreshold().getOperator().name() );
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
     * Returns <code>true</code> if the {@link ThresholdOuter} condition corresponds to a {@link ThresholdOperator#BETWEEN} condition.
     *
     * @return true if the condition is a {@link wres.config.yaml.components.ThresholdOperator#BETWEEN} condition, false otherwise.
     */

    public boolean hasBetweenCondition()
    {
        return this.getOperator()
                   .equals( wres.config.yaml.components.ThresholdOperator.BETWEEN );
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

    @Override
    public boolean equals( final Object o )
    {
        if ( o == this )
        {
            return true;
        }

        if ( !( o instanceof final ThresholdOuter in ) )
        {
            return false;
        }

        return in.getThreshold().equals( this.getThreshold() )
               && Objects.equals( in.getType(), this.getType() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getThreshold(), this.getType() );
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
    public int compareTo( ThresholdOuter o )
    {
        int result = MessageUtilities.compare( this.getThreshold(), o.getThreshold() );

        if ( result != 0 )
        {
            return result;
        }

        return Comparator.nullsFirst( Comparator.comparing( ThresholdOuter::getType ) )
                         .compare( this, o );
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

        wres.config.yaml.components.ThresholdOperator operator = this.getOperator();

        return switch ( operator )
        {
            case GREATER -> t > lowerBound;
            case LESS -> t < lowerBound;
            case GREATER_EQUAL -> t >= lowerBound;
            case LESS_EQUAL -> t <= lowerBound;
            case BETWEEN -> t >= lowerBound && t < upperBound;
            case EQUAL -> Math.abs( t - lowerBound ) < .00000001;
        };
    }

    /**
     * A builder to build the threshold. Wraps a canonical {@link Threshold.Builder}.
     */

    public static class Builder
    {
        /**
         * The canonical builder.
         */

        private Threshold.Builder innerBuilder = Threshold.newBuilder();

        /**
         * The threshold type or context in which it was declared.
         */

        private ThresholdType thresholdType;

        /**
         * Sets the {@link wres.config.yaml.components.ThresholdOperator} associated with the threshold.
         *
         * @param operator the threshold operator
         * @return the builder
         */

        public Builder setOperator( wres.config.yaml.components.ThresholdOperator operator )
        {
            if ( Objects.nonNull( operator ) )
            {
                wres.statistics.generated.Threshold.ThresholdOperator anOperator = operator.canonical();
                innerBuilder.setOperator( anOperator );
            }
            return this;
        }

        /**
         * Sets the {@link ThresholdOrientation} associated with the threshold
         *
         * @param dataType the data type
         * @return the builder
         */

        public Builder setOrientation( ThresholdOrientation dataType )
        {
            if ( Objects.nonNull( dataType ) )
            {
                Threshold.ThresholdDataType aDataType = Threshold.ThresholdDataType.valueOf( dataType.name() );
                innerBuilder.setDataType( aDataType );
            }
            return this;
        }

        /**
         * Sets the type of threshold or context in which it was declared.
         * @param thresholdType the threshold type
         * @return the builder
         */
        public Builder setThresholdType( ThresholdType thresholdType )
        {
            this.thresholdType = thresholdType;
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
            if ( Objects.nonNull( values ) )
            {
                innerBuilder.setLeftThresholdValue( values.first() );
                if ( values.hasTwo() )
                {
                    innerBuilder.setRightThresholdValue( values.second() );
                }
            }
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
            if ( Objects.nonNull( probabilities ) )
            {
                innerBuilder.setLeftThresholdProbability( probabilities.first() );
                if ( probabilities.hasTwo() )
                {
                    innerBuilder.setRightThresholdProbability( probabilities.second() );
                }
            }
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
            if ( Objects.nonNull( label ) )
            {
                innerBuilder.setName( label );
            }
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
            if ( Objects.nonNull( units ) )
            {
                innerBuilder.setThresholdValueUnits( units.getUnit() );
            }
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
            this.innerBuilder = threshold.toBuilder();
        }
    }

    /**
     * Construct the threshold.
     *
     * @param builder the builder
     */

    private ThresholdOuter( Builder builder )
    {
        // Set, then validate
        this.threshold = builder.innerBuilder.build();

        // Defined?
        if ( Objects.nonNull( builder.thresholdType ) )
        {
            this.thresholdType = builder.thresholdType;
        }
        // Guess it
        else
        {
            if ( this.isQuantile() )
            {
                this.thresholdType = ThresholdType.QUANTILE;
            }
            else if ( this.hasProbabilities() )
            {
                this.thresholdType = ThresholdType.PROBABILITY;
            }
            else
            {
                this.thresholdType = ThresholdType.VALUE;
            }
        }

        this.validate();
    }

    /**
     * @throws IllegalArgumentException if any inputs are invalid
     */

    private void validate()
    {
        if ( this.threshold.getOperator() == wres.statistics.generated.Threshold.ThresholdOperator.UNRECOGNIZED )
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
            if ( Math.abs( this.getProbabilities().first() - 0.0 ) < .00000001
                 && this.getOperator() == wres.config.yaml.components.ThresholdOperator.LESS )
            {
                throw new ThresholdException( "Cannot apply a threshold operator of '<' to the lower bound "
                                              + "probability of 0.0." );
            }
            // Cannot have GREATER_THAN on the upper bound
            if ( Math.abs( this.getProbabilities().first() - 1.0 ) < .00000001
                 && this.getOperator() == ThresholdOperator.GREATER )
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
     * Returns a string representation of a condition that is not a {@link ThresholdOperator#BETWEEN}
     * condition.
     *
     * @return a string for the elementary condition
     */

    private String getConditionString()
    {
        ThresholdOperator operator = this.getOperator();

        return switch ( operator )
        {
            case GREATER -> "> ";
            case LESS -> "< ";
            case GREATER_EQUAL -> ">= ";
            case LESS_EQUAL -> "<= ";
            case EQUAL -> "= ";
            default -> "";
        };
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

            return ">= Pr = "
                   + this.getProbabilities().first()
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
