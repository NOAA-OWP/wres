package wres.engine.statistics.metric.categorical;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MatrixOfDoubles;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreGroup;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.MatrixStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.OrdinaryScore;

/**
 * A generic implementation of an error score that applies to the components of a {@link ContingencyTable}.
 * 
 * @author james.brown@hydrosolved.com
 */

abstract class ContingencyTableScore extends OrdinaryScore<SampleData<Pair<Boolean,Boolean>>, DoubleScoreStatistic>
        implements Collectable<SampleData<Pair<Boolean,Boolean>>, MatrixStatistic, DoubleScoreStatistic>
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
    public MatrixStatistic getInputForAggregation( final SampleData<Pair<Boolean,Boolean>> s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( nullString );
        }
        return table.apply( s );
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    @Override
    public ScoreGroup getScoreOutputGroup()
    {
        return ScoreGroup.NONE;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * Returns the {@link StatisticMetadata} for a {ContingencyTableScore}.
     * 
     * @param output the output from which the {@link StatisticMetadata} is built
     * @return the {@link StatisticMetadata}
     */

    StatisticMetadata getMetadata( final MatrixStatistic output )
    {    
        return StatisticMetadata.of( output.getMetadata().getSampleMetadata(),
                                     this.getID(),
                                     MetricConstants.MAIN,
                                     this.hasRealUnits(),
                                     output.getMetadata().getSampleSize(),
                                     null );
    }

    /**
     * Convenience method that checks whether the output is compatible with a contingency table. Throws an exception if
     * the output is incompatible.
     * 
     * @param output the output to check
     * @param metric the metric to use when throwing an informative exception
     * @throws SampleDataException if the output is not a valid input for an intermediate calculation
     */

    void isContingencyTable( final MatrixStatistic output, final Metric<?, ?> metric )
    {
        if ( Objects.isNull( output ) )
        {
            throw new SampleDataException( nullString );
        }
        if ( Objects.isNull( metric ) )
        {
            throw new SampleDataException( nullString );
        }
        final MatrixOfDoubles v = output.getData();
        if ( !v.isSquare() )
        {
            throw new SampleDataException( "Expected an intermediate result with a square matrix when "
                                            + "computing the '"
                                            + metric
                                            + "': ["
                                            + v.rows()
                                            + ", "
                                            + v.columns()
                                            + "]." );
        }
    }

    /**
     * Convenience method that checks whether the output is compatible with a 2x2 contingency table. Throws an exception
     * if the output is incompatible.
     * 
     * @param output the output to check
     * @param metric the metric to use when throwing an informative exception
     * @throws SampleDataException if the output is not a valid input for an intermediate calculation
     */

    void is2x2ContingencyTable( final MatrixStatistic output, final Metric<?, ?> metric )
    {
        if ( Objects.isNull( output ) )
        {
            throw new SampleDataException( nullString );
        }
        if ( Objects.isNull( metric ) )
        {
            throw new SampleDataException( nullString );
        }
        final MatrixOfDoubles v = output.getData();
        if ( v.rows() != 2 || v.columns() != 2 )
        {
            throw new SampleDataException( "Expected an intermediate result with a 2x2 square matrix when computing the '"
                                            + metric
                                            + "': ["
                                            + v.rows()
                                            + ", "
                                            + v.columns()
                                            + "]." );
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
