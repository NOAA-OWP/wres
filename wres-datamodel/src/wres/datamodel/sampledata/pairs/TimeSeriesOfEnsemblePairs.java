package wres.datamodel.sampledata.pairs;

import java.util.Objects;

import wres.datamodel.Ensemble;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.time.TimeSeries;

/**
 * A collection of {@link TimeSeries} of ensemble pairs.
 * 
 * @author james.brown@hydrosolved.com
 * @deprecated This class is deprecated for removal on completion of #56214
 */

@Deprecated(since="1.17", forRemoval=true)
public class TimeSeriesOfEnsemblePairs extends PoolOfPairs<Double,Ensemble>
{
    
    @Override
    public TimeSeriesOfEnsemblePairs getBaselineData()
    {
        PoolOfPairs<Double,Ensemble> baseline = super.getBaselineData();

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
    @Deprecated(since="1.17", forRemoval=true)
    public static class TimeSeriesOfEnsemblePairsBuilder extends PoolOfPairsBuilder<Double,Ensemble>
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

    TimeSeriesOfEnsemblePairs( final PoolOfPairsBuilder<Double,Ensemble> b )
    {
        super( b );
    }

}
