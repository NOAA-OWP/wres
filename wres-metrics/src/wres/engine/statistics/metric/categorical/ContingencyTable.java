package wres.engine.statistics.metric.categorical;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.Metric;

/**
 * <p>
 * Base class for a contingency table. A contingency table compares the number of predictions and observations 
 * associated with each of the N possible outcomes of an N-category variable. The rows of the contingency
 * table store the number of predicted outcomes and the columns store the number of observed outcomes.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 */

public class ContingencyTable implements Metric<SampleData<Pair<Boolean, Boolean>>, DoubleScoreStatistic>,
        Collectable<SampleData<Pair<Boolean, Boolean>>, DoubleScoreStatistic, DoubleScoreStatistic>
{

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static ContingencyTable of()
    {
        return new ContingencyTable();
    }

    @Override
    public DoubleScoreStatistic apply( final SampleData<Pair<Boolean,Boolean>> s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }
        final int outcomes = 2;
        final double[][] returnMe = new double[outcomes][outcomes];
        
        // Function that returns the index within the contingency table to increment
        final Consumer<Pair<Boolean,Boolean>> f = a -> {
            boolean left = a.getLeft();
            boolean right = a.getRight();
            
            // True positives aka hits
            if( left && right )
            {
                returnMe[0][0]+=1;
            }
            // True negatives 
            else if( !left && !right )
            {
                returnMe[1][1]+=1;
            }
            // False positives aka false alarms
            else if( !left && right )
            {
                returnMe[0][1]+=1;
            }
            // False negatives aka misses
            else
            {
                returnMe[1][0]+=1;
            }
        };
        
        // Increment the count in a serial stream as the lambda is stateful
        s.getRawData().stream().forEach( f );
        
        // Name the outcomes for a 2x2 contingency table
        Map<MetricConstants,Double> statistics = new HashMap<>();
        statistics.put( MetricConstants.TRUE_POSITIVES, returnMe[0][0] );
        statistics.put( MetricConstants.FALSE_POSITIVES, returnMe[0][1] );
        statistics.put( MetricConstants.FALSE_NEGATIVES, returnMe[1][0] );
        statistics.put( MetricConstants.TRUE_NEGATIVES, returnMe[1][1] );

        final StatisticMetadata metOut =
                StatisticMetadata.of( s.getMetadata(),
                                    this.getID(),
                                    null,
                                    this.hasRealUnits(),
                                    s.getRawData().size(),
                                    null );
        
        return DoubleScoreStatistic.of( statistics, metOut );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.CONTINGENCY_TABLE;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    @Override
    public String toString()
    {
        return getID().toString();
    }

    @Override
    public DoubleScoreStatistic aggregate( DoubleScoreStatistic output )
    {
        Objects.requireNonNull( output );
        
        return output;
    }

    @Override
    public DoubleScoreStatistic getInputForAggregation( SampleData<Pair<Boolean, Boolean>> input )
    {
        return this.apply( input );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.CONTINGENCY_TABLE;
    }

    /**
     * Hidden constructor.
     */

    ContingencyTable()
    {
    }

}
