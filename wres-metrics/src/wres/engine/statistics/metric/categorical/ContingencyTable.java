package wres.engine.statistics.metric.categorical;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.MatrixStatistic;
import wres.datamodel.statistics.StatisticMetadata;
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

public class ContingencyTable implements Metric<SampleData<Pair<Boolean,Boolean>>, MatrixStatistic>
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
    public MatrixStatistic apply( final SampleData<Pair<Boolean,Boolean>> s )
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
        List<MetricDimension> componentNames = Arrays.asList( MetricDimension.TRUE_POSITIVES,
                                            MetricDimension.FALSE_POSITIVES,
                                            MetricDimension.FALSE_NEGATIVES,
                                            MetricDimension.TRUE_NEGATIVES );

        final StatisticMetadata metOut =
                StatisticMetadata.of( s.getMetadata(),
                                    this.getID(),
                                    MetricConstants.MAIN,
                                    this.hasRealUnits(),
                                    s.getRawData().size(),
                                    null );
        
        return MatrixStatistic.of( returnMe, componentNames, metOut );
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

    /**
     * Hidden constructor.
     */

    ContingencyTable()
    {
    }
}
