package wres.datamodel.statistics;

import java.util.Objects;
import java.util.StringJoiner;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.MetricConstants.MetricDimension;

/**
 * An immutable representation of a box plot statistic, which comprises a set of 
 * quantiles and, optionally, a linked value, such as an observed or forecast value.
 * 
 * @author james.brown@hydrosolved.com
 */
public class BoxplotStatistic implements Statistic<VectorOfDoubles>
{

    /**
     * The metadata associated with the output.
     */

    private final StatisticMetadata meta;

    /**
     * Probabilities associated with the whiskers for each box.
     */

    private final VectorOfDoubles probabilities;

    /**
     * The values that correspond to the probabilities.
     */

    private final VectorOfDoubles quantiles;

    /**
     * An optional value against which the box is stored.
     */

    private final double linkedValue;

    /**
     * The dimension associated with the whiskers.
     */

    private final MetricDimension valueType;

    /**
     * The dimension associated with the linked value.
     */

    private final MetricDimension linkedValueType;

    /**
     * Returns an instance from the inputs with a default value type of
     * {@link MetricDimension#FORECAST_ERROR}.
     * 
     * @param probabilities the probabilities
     * @param quantiles the quantiles
     * @param meta the statistic metadata
     * @throws StatisticException if any of the input is invalid
     * @throws NullPointerException if any input is null
     * @return an instance of the output
     */

    public static BoxplotStatistic of( VectorOfDoubles probabilities,
                                       VectorOfDoubles quantiles,
                                       StatisticMetadata meta )
    {
        return new BoxplotStatistic( probabilities, quantiles, MetricDimension.FORECAST_ERROR, meta, Double.NaN, null );
    }
    
    /**
     * Returns an instance from the inputs with a prescribed value type.
     * 
     * @param probabilities the probabilities
     * @param quantiles the quantiles
     * @param valueType the value type
     * @param meta the statistic metadata
     * @throws StatisticException if any of the input is invalid
     * @throws NullPointerException if any input is null
     * @return an instance of the output
     */

    public static BoxplotStatistic of( VectorOfDoubles probabilities,
                                       VectorOfDoubles quantiles,
                                       MetricDimension valueType,
                                       StatisticMetadata meta )
    {
        return new BoxplotStatistic( probabilities, quantiles, valueType, meta, Double.NaN, null );
    }

    /**
     * Returns an instance from the inputs with a default value type of
     * {@link MetricDimension#FORECAST_ERROR}.
     * 
     * @param probabilities the probabilities
     * @param quantiles the quantiles
     * @param meta the statistic metadata
     * @param linkedValue a linked value
     * @param linkedValueType the type of linked value
     * @throws StatisticException if any inputs is invalid
     * @throws NullPointerException if any input is null
     * @return an instance of the output
     */

    public static BoxplotStatistic of( VectorOfDoubles probabilities,
                                       VectorOfDoubles quantiles,
                                       StatisticMetadata meta,
                                       double linkedValue,
                                       MetricDimension linkedValueType )
    {
        return new BoxplotStatistic( probabilities,
                                     quantiles,
                                     MetricDimension.FORECAST_ERROR,
                                     meta,
                                     linkedValue,
                                     linkedValueType );
    }

    /**
     * Returns an instance from the inputs.
     * 
     * @param probabilities the probabilities
     * @param quantiles the quantiles
     * @param valueType the value type
     * @param meta the statistic metadata
     * @param linkedValue a linked value
     * @param linkedValueType the type of linked value
     * @throws StatisticException if any inputs is invalid
     * @throws NullPointerException if any input is null
     * @return an instance of the output
     */

    public static BoxplotStatistic of( VectorOfDoubles probabilities,
                                       VectorOfDoubles quantiles,
                                       MetricDimension valueType,
                                       StatisticMetadata meta,
                                       double linkedValue,
                                       MetricDimension linkedValueType )
    {
        return new BoxplotStatistic( probabilities,
                                     quantiles,
                                     valueType,
                                     meta,
                                     linkedValue,
                                     linkedValueType );
    }

    @Override
    public StatisticMetadata getMetadata()
    {
        return meta;
    }

    @Override
    public VectorOfDoubles getData()
    {
        return quantiles;
    }

    /**
     * Returns a value linked to the box.
     * 
     * @return a value linked to the box
     */

    public double getLinkedValue()
    {
        return this.linkedValue;
    }

    /**
     * Returns the value type of the whiskers.
     * 
     * @return the value type
     */

    public MetricDimension getValueType()
    {
        return this.valueType;
    }

    /**
     * Returns the linked value type, which may be null.
     * 
     * @return the linked value type or null
     */

    public MetricDimension getLinkedValueType()
    {
        return this.linkedValueType;
    }

    /**
     * Returns <code>true</code> if there is a linked value associated
     * with the box that is not a {@link Double#NaN}, otherwise false.
     * 
     * @return true if there is a linked value, false otherwise 
     */

    public boolean hasLinkedValue()
    {
        return !Double.isNaN( linkedValue );
    }

    /**
     * Returns the probabilities associated with the whiskers (quantiles) in each box. 
     * The probabilities are stored in the same order as the quantiles.
     * 
     * @return the probabilities associated with the whiskers of each box
     */

    public VectorOfDoubles getProbabilities()
    {
        return this.probabilities;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof BoxplotStatistic ) )
        {
            return false;
        }
        final BoxplotStatistic v = (BoxplotStatistic) o;

        // Check the value type
        if ( this.getValueType() != v.getValueType() )
        {
            return false;
        }

        // Check the linked value
        if ( Double.compare( this.getLinkedValue(), v.getLinkedValue() ) != 0 )
        {
            return false;
        }

        // Check the linked value type
        if ( this.getLinkedValueType() != v.getLinkedValueType() )
        {
            return false;
        }

        // Check the probabilities
        if ( !this.getProbabilities().equals( v.getProbabilities() ) )
        {
            return false;
        }

        // Check the quantiles
        if ( !this.getData().equals( v.getData() ) )
        {
            return false;
        }

        //Check metadata
        return meta.equals( v.getMetadata() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( meta, probabilities, quantiles, valueType, linkedValue, linkedValueType );
    }

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner( ",", "(", ")" );

        joiner.add( "PROBABILITIES: " + this.getProbabilities() );
        joiner.add( "QUANTILES: " + this.getData() );
        joiner.add( "VALUE TYPE: " + this.getValueType() );

        if ( this.hasLinkedValue() )
        {
            joiner.add( "LINKED VALUE: " + this.getLinkedValue() );
            joiner.add( "LINKED VALUE TYPE: " + this.getLinkedValueType() );
        }

        return joiner.toString();
    }

    /**
     * Hidden constructor.
     * 
     * @param probabilities the probabilities
     * @param quantiles the quantiles
     * @param valueType the type of value
     * @param meta the box plot metadata
     * @param linkedValue a linked value
     * @param linkedValueType the type of linked value
     * @throws StatisticException if any input is invalid
     * @throws NullPointerException if any input is null
     */

    private BoxplotStatistic( VectorOfDoubles probabilities,
                              VectorOfDoubles quantiles,
                              MetricDimension valueType,
                              StatisticMetadata meta,
                              double linkedValue,
                              MetricDimension linkedValueType )
    {
        //Validate
        Objects.requireNonNull( meta, "Specify non-null metadata for the box plot statistic." );

        Objects.requireNonNull( probabilities, "Specify non-null probabilities for the box plot statistic." );

        Objects.requireNonNull( quantiles, "Specify non-null quantiles for the box plot statistic." );

        Objects.requireNonNull( valueType, "Specify a non-null value type for the box plot statistic." );

        if ( !Double.isNaN( linkedValue ) )
        {
            Objects.requireNonNull( linkedValueType,
                                    "Specify a non-null linked value type for the box "
                                                     + "plot statistic." );
        }

        // Probabilities and quantiles of equal size
        if ( quantiles.size() != probabilities.size() )
        {
            throw new StatisticException( "The number of probabilities (" + probabilities.size()
                                          + ") does not match the number of quantiles ("
                                          + quantiles.size()
                                          + ")." );
        }

        // Check the probabilities
        this.checkEachProbability( probabilities );

        //Ensure safe types
        this.quantiles = quantiles;
        this.probabilities = probabilities;
        this.meta = meta;
        this.linkedValue = linkedValue;
        this.valueType = valueType;
        this.linkedValueType = linkedValueType;
    }

    /**
     * Validates each probability.
     * 
     * @param probabilities the probabilities
     */

    private void checkEachProbability( VectorOfDoubles probabilities )
    {
        for ( double next : probabilities.getDoubles() )
        {
            if ( next < 0.0 || next > 1.0 )
            {
                throw new StatisticException( "One or more of the probabilities is out of bounds. "
                                              + "Probabilities must be in [0,1]." );
            }
        }
    }

}
