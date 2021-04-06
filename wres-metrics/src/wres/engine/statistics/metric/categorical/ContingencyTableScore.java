package wres.engine.statistics.metric.categorical;

import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.OrdinaryScore;

/**
 * A generic implementation of an error score that applies to the components of a {@link ContingencyTable}.
 * 
 * @author james.brown@hydrosolved.com
 */

abstract class ContingencyTableScore extends OrdinaryScore<Pool<Pair<Boolean, Boolean>>, DoubleScoreStatisticOuter>
        implements Collectable<Pool<Pair<Boolean, Boolean>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
{

    /**
     * A {@link ContingencyTable} to compute.
     */

    private final ContingencyTable table;

    /**
     * Null string warning, used in several places.
     */

    private final String nullString = "Specify non-null input to the '" + toString() + "'.";

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.CONTINGENCY_TABLE;
    }

    @Override
    public DoubleScoreStatisticOuter getInputForAggregation( final Pool<Pair<Boolean, Boolean>> s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new PoolException( nullString );
        }
        return table.apply( s );
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    @Override
    public MetricGroup getScoreOutputGroup()
    {
        return MetricGroup.NONE;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * Convenience method that checks whether the output is compatible with a contingency table. Throws an exception if
     * the output is incompatible.
     * 
     * @param output the output to check
     * @param metric the metric to use when throwing an informative exception
     * @throws PoolException if the output is not a valid input for an intermediate calculation
     */

    void isContingencyTable( final DoubleScoreStatisticOuter output, final Metric<?, ?> metric )
    {
        if ( Objects.isNull( output ) )
        {
            throw new PoolException( nullString );
        }
        if ( Objects.isNull( metric ) )
        {
            throw new PoolException( nullString );
        }
        int count = output.getComponents().size();

        double sr = Math.sqrt( count );

        // If square root is an integer 
        boolean square = sr - Math.floor( sr ) == 0;

        if ( !square )
        {
            throw new PoolException( "Expected an intermediate result with a square number of elements when "
                                           + "computing the '"
                                           + metric
                                           + "': ["
                                           + count
                                           + "]." );
        }
    }

    /**
     * Convenience method that checks whether the output is compatible with a 2x2 contingency table. Throws an exception
     * if the output is incompatible.
     * 
     * @param output the output to check
     * @param metric the metric to use when throwing an informative exception
     * @throws PoolException if the output is not a valid input for an intermediate calculation
     */

    void is2x2ContingencyTable( final DoubleScoreStatisticOuter output, final Metric<?, ?> metric )
    {
        if ( Objects.isNull( output ) )
        {
            throw new PoolException( nullString );
        }
        if ( Objects.isNull( metric ) )
        {
            throw new PoolException( nullString );
        }

        Set<MetricConstants> expected = Set.of( MetricConstants.TRUE_POSITIVES,
                                                MetricConstants.TRUE_NEGATIVES,
                                                MetricConstants.FALSE_POSITIVES,
                                                MetricConstants.FALSE_NEGATIVES );

        if ( !expected.equals( output.getComponents() ) )
        {
            throw new PoolException( "Expected an intermediate result with elements "
                                           + expected
                                           + " but found elements "
                                           + output.getComponents()
                                           + "." );
        }

    }

    /**
     * Hidden constructor.
     */

    ContingencyTableScore()
    {
        super();
        table = new ContingencyTable();
    }

}
