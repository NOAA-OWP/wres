package wres.datamodel.sampledata.pairs;
import java.util.Objects;

import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.time.TimeSeries;

/**
 * <p>A collection of {@link TimeSeries} of single-valued pairs.
 * 
 * @author james.brown@hydrosolved.com
 * @deprecated This class is deprecated for removal on completion of #56214
 */
@Deprecated(since="1.17", forRemoval=true)
public class TimeSeriesOfSingleValuedPairs extends PoolOfPairs<Double,Double>
{
    
    @Override
    public TimeSeriesOfSingleValuedPairs getBaselineData()
    {
        PoolOfPairs<Double,Double> baseline = super.getBaselineData();

        if( Objects.isNull( baseline ) )
        {
            return null;
        }
        
        TimeSeriesOfSingleValuedPairsBuilder builder = new TimeSeriesOfSingleValuedPairsBuilder();

        builder.addTimeSeries( baseline );
        
        return builder.build();
    }
    
    
    /**
     * Builder.
     * @author james.brown@hydrosolved.com
     */
    @Deprecated(since="1.17", forRemoval=true)
    public static class TimeSeriesOfSingleValuedPairsBuilder extends PoolOfPairsBuilder<Double,Double>
    {
        /**
         * Build.
         */
        
        public TimeSeriesOfSingleValuedPairs build()
        {
            return new TimeSeriesOfSingleValuedPairs( this );
        }
    }
    
    /**
     * Construct the pairs with a builder.
     * 
     * @param b the builder
     * @throws SampleDataException if the pairs are invalid
     */

    TimeSeriesOfSingleValuedPairs( final PoolOfPairsBuilder<Double,Double> b )
    {
        super( b );
    }

}
