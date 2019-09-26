package wres.datamodel.sampledata.pairs;

import java.util.Objects;

import wres.datamodel.Ensemble;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.time.TimeSeries;

/**
 * A collection of {@link TimeSeries} of {@link EnsemblePairs}.
 * 
 * @author james.brown@hydrosolved.com
 */
public class TimeSeriesOfEnsemblePairs extends TimeSeriesOfPairs<Double,Ensemble>
{
    
    @Override
    public TimeSeriesOfEnsemblePairs getBaselineData()
    {
        TimeSeriesOfPairs<Double,Ensemble> baseline = super.getBaselineData();

        if( Objects.isNull( baseline ) )
        {
            return null;
        }
        
        TimeSeriesOfEnsemblePairsBuilder builder = new TimeSeriesOfEnsemblePairsBuilder();

        builder.addTimeSeries( baseline );
        
        return builder.build();
    }
    
    /**
     * Builder.
     * @author james.brown@hydrosolved.com
     */
    
    public static class TimeSeriesOfEnsemblePairsBuilder extends TimeSeriesOfPairsBuilder<Double,Ensemble>
    {
        /**
         * Build.
         */
        
        public TimeSeriesOfEnsemblePairs build()
        {
            return new TimeSeriesOfEnsemblePairs( this );
        }
    }
    
    /**
     * Construct the pairs with a builder.
     * 
     * @param b the builder
     * @throws SampleDataException if the pairs are invalid
     */

    TimeSeriesOfEnsemblePairs( final TimeSeriesOfPairsBuilder<Double,Ensemble> b )
    {
        super( b );
    }

}
