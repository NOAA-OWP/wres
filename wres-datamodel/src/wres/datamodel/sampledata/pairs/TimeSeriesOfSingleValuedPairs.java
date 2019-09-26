package wres.datamodel.sampledata.pairs;
import java.util.Objects;

import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.time.TimeSeries;

/**
 * <p>A collection of {@link TimeSeries} of {@link SingleValuedPairs}.</p>
 * 
 * @author james.brown@hydrosolved.com
 */
public class TimeSeriesOfSingleValuedPairs extends TimeSeriesOfPairs<Double,Double>
{
    
    @Override
    public TimeSeriesOfSingleValuedPairs getBaselineData()
    {
        TimeSeriesOfPairs<Double,Double> baseline = super.getBaselineData();

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
    
    public static class TimeSeriesOfSingleValuedPairsBuilder extends TimeSeriesOfPairsBuilder<Double,Double>
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

    TimeSeriesOfSingleValuedPairs( final TimeSeriesOfPairsBuilder<Double,Double> b )
    {
        super( b );
    }

}
