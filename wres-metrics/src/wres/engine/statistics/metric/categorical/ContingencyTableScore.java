package wres.engine.statistics.metric.categorical;

import java.util.Objects;

import wres.datamodel.MatrixOfDoubles;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.sampledata.MetricInputException;
import wres.datamodel.sampledata.pairs.MulticategoryPairs;
import wres.datamodel.statistics.DoubleScoreOutput;
import wres.datamodel.statistics.MatrixOutput;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.OrdinaryScore;

/**
 * A generic implementation of an error score that applies to the components of a {@link ContingencyTable}.
 * 
 * @author james.brown@hydrosolved.com
 */

abstract class ContingencyTableScore<S extends MulticategoryPairs> extends OrdinaryScore<S, DoubleScoreOutput>
        implements Collectable<S, MatrixOutput, DoubleScoreOutput>
{

    /**
     * A {@link ContingencyTable} to compute.
     */

    private final ContingencyTable<S> table;

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
    public MatrixOutput getInputForAggregation( final S s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( nullString );
        }
        return table.apply( s );
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    @Override
    public ScoreOutputGroup getScoreOutputGroup()
    {
        return ScoreOutputGroup.NONE;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * Returns the {@link MetricOutputMetadata} for a {ContingencyTableScore}.
     * 
     * @param output the output from which the {@link MetricOutputMetadata} is built
     * @return the {@link MetricOutputMetadata}
     */

    MetricOutputMetadata getMetadata( final MatrixOutput output )
    {    
        final MetricOutputMetadata metIn = output.getMetadata();

        return MetricOutputMetadata.of( output.getMetadata(),
                                        this.getID(),
                                        MetricConstants.MAIN,
                                        this.hasRealUnits(),
                                        metIn.getSampleSize(),
                                        null );
    }

    /**
     * Convenience method that checks whether the output is compatible with a contingency table. Throws an exception if
     * the output is incompatible.
     * 
     * @param output the output to check
     * @param metric the metric to use when throwing an informative exception
     * @throws MetricInputException if the output is not a valid input for an intermediate calculation
     */

    void isContingencyTable( final MatrixOutput output, final Metric<?, ?> metric )
    {
        if ( Objects.isNull( output ) )
        {
            throw new MetricInputException( nullString );
        }
        if ( Objects.isNull( metric ) )
        {
            throw new MetricInputException( nullString );
        }
        final MatrixOfDoubles v = output.getData();
        if ( !v.isSquare() )
        {
            throw new MetricInputException( "Expected an intermediate result with a square matrix when "
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
     * @throws MetricInputException if the output is not a valid input for an intermediate calculation
     */

    void is2x2ContingencyTable( final MatrixOutput output, final Metric<?, ?> metric )
    {
        if ( Objects.isNull( output ) )
        {
            throw new MetricInputException( nullString );
        }
        if ( Objects.isNull( metric ) )
        {
            throw new MetricInputException( nullString );
        }
        final MatrixOfDoubles v = output.getData();
        if ( v.rows() != 2 || v.columns() != 2 )
        {
            throw new MetricInputException( "Expected an intermediate result with a 2x2 square matrix when computing the '"
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
        table = new ContingencyTable<>();
    }

}
