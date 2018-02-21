package wres.engine.statistics.metric.categorical;

import java.util.Objects;

import wres.datamodel.MatrixOfDoubles;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.MulticategoryPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MatrixOutput;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.OrdinaryScore;
import wres.engine.statistics.metric.categorical.ContingencyTable.ContingencyTableBuilder;

/**
 * A generic implementation of an error score that applies to the components of a {@link ContingencyTable}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
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

    private final String nullString = "Specify non-null input for the '" + toString() + "'.";

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.CONTINGENCY_TABLE;
    }

    @Override
    public MatrixOutput getCollectionInput( final S s )
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
        final MetadataFactory f = getDataFactory().getMetadataFactory();
        return f.getOutputMetadata( metIn.getSampleSize(),
                                    f.getDimension(),
                                    metIn.getInputDimension(),
                                    getID(),
                                    MetricConstants.MAIN,
                                    metIn.getIdentifier() );
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
                                            + metric + "': [" + v.rows() + ", " + v.columns() + "]." );
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    ContingencyTableScore( final OrdinaryScoreBuilder<S, DoubleScoreOutput> builder ) throws MetricParameterException
    {
        super( builder );
        ContingencyTableBuilder<S> ct = new ContingencyTableBuilder<>();
        ct.setOutputFactory( getDataFactory() );
        table = ct.build();
    }

}
