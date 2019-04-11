package wres.datamodel.statistics;

import java.util.Objects;
import java.util.StringJoiner;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metadata.StatisticMetadata;

/**
 * An immutable representation of a box plot statistic, which comprises a set of 
 * quantiles and, optionally, a linked value, such as an observed or forecast value.
 * 
 * @author james.brown@hydrosolved.com
 */
public class BoxPlotStatistic implements Statistic<VectorOfDoubles>
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
     * Returns an instance from the inputs.
     * 
     * @param probabilities the probabilities
     * @param quantiles the quantiles
     * @param meta the statistic metadata
     * @throws StatisticException if any of the input is invalid
     * @throws NullPointerException if any input is null
     * @return an instance of the output
     */

    public static BoxPlotStatistic of( VectorOfDoubles probabilities,
                                       VectorOfDoubles quantiles,
                                       StatisticMetadata meta )
    {
        return new BoxPlotStatistic( probabilities, quantiles, meta, Double.NaN );
    }

    /**
     * Returns an instance from the inputs.
     * 
     * @param probabilities the probabilities
     * @param quantiles the quantiles
     * @param meta the statistic metadata
     * @param linkedValue a linked value
     * @throws StatisticException if any inputs is invalid
     * @throws NullPointerException if any input is null
     * @return an instance of the output
     */

    public static BoxPlotStatistic of( VectorOfDoubles probabilities,
                                       VectorOfDoubles quantiles,
                                       StatisticMetadata meta,
                                       double linkedValue )
    {
        return new BoxPlotStatistic( probabilities, quantiles, meta, linkedValue );
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
        if ( ! ( o instanceof BoxPlotStatistic ) )
        {
            return false;
        }
        final BoxPlotStatistic v = (BoxPlotStatistic) o;

        // Check the linked value
        if ( this.getLinkedValue() != v.getLinkedValue() )
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
        return Objects.hash( linkedValue, meta, probabilities, quantiles );
    }

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner( ",", "(", ")" );

        joiner.add( "PROBABILITIES: " + probabilities );
        joiner.add( "QUANTILES: " + quantiles );
        joiner.add( "LINKED VALUE: " + linkedValue );

        return joiner.toString();
    }

    /**
     * Hidden constructor.
     * 
     * @param probabilities the probabilities
     * @param quantiles the quantiles
     * @param meta the box plot metadata
     * @param linkedValue a linked value
     * @throws StatisticException if any input is invalid
     * @throws NullPointerException if any input is null
     */

    private BoxPlotStatistic( VectorOfDoubles probabilities,
                              VectorOfDoubles quantiles,
                              StatisticMetadata meta,
                              double linkedValue )
    {
        //Validate
        Objects.requireNonNull( meta, "Specify non-null metadata." );
        
        Objects.requireNonNull( probabilities, "Specify non-null probabilities." );
        
        Objects.requireNonNull( quantiles, "Specify non-null quantiles." );

        // At least two probabilities
        if ( probabilities.size() < 2 )
        {
            throw new StatisticException( "Specify two or more probabilities for the whiskers." );
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
