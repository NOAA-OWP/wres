package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.MatrixOfDoubles;
import wres.datamodel.MatrixOutput;
import wres.datamodel.MetadataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricInputException;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.MulticategoryPairs;
import wres.datamodel.ScalarOutput;
import wres.datamodel.MetricConstants.MetricDecompositionGroup;
import wres.engine.statistics.metric.ContingencyTable.ContingencyTableBuilder;

/**
 * A generic implementation of an error score that applies to the components of a {@link ContingencyTable}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

abstract class ContingencyTableScore<S extends MulticategoryPairs> extends Metric<S, ScalarOutput>
implements Score, Collectable<S, MatrixOutput, ScalarOutput>
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
    public MatrixOutput getCollectionInput(final S s)
    {
        if(Objects.isNull(s))
        {
            throw new MetricInputException(nullString);
        }
        return table.apply(s);
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    @Override
    public MetricDecompositionGroup getDecompositionID()
    {
        return MetricDecompositionGroup.NONE;
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

    MetricOutputMetadata getMetadata(final MatrixOutput output)
    {
        final MetricOutputMetadata metIn = output.getMetadata();
        final MetadataFactory f = getDataFactory().getMetadataFactory();
        return f.getOutputMetadata(metIn.getSampleSize(),
                                   f.getDimension(),
                                   metIn.getDimension(),
                                   getID(),
                                   MetricConstants.MAIN,
                                   metIn.getIdentifier());
    }

    /**
     * Convenience method that checks whether the output is compatible with a contingency table. Throws an exception if
     * the output is incompatible.
     * 
     * @param output the output to check
     * @param metric the metric to use when throwing an informative exception
     * @throws MetricInputException if the output is not a valid input for an intermediate calculation
     */

    void isContingencyTable(final MatrixOutput output, final Metric<?, ?> metric)
    {
        if(Objects.isNull(output))
        {
            throw new MetricInputException(nullString);
        }
        final MatrixOfDoubles v = output.getData();
        if(!v.isSquare())
        {
            throw new MetricInputException("Expected an intermediate result with a square matrix when "
                + "computing the '" + metric + "': [" + v.rows() + ", " + v.columns() + "].");
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

    void is2x2ContingencyTable(final MatrixOutput output, final Metric<?, ?> metric)
    {
        if(Objects.isNull(output))
        {
            throw new MetricInputException(nullString);
        }
        final MatrixOfDoubles v = output.getData();
        if(v.rows() != 2 || v.columns() != 2)
        {
            throw new MetricInputException("Expected an intermediate result with a 2x2 square matrix when computing the '"
                + metric + "': [" + v.rows() + ", " + v.columns() + "].");
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    ContingencyTableScore(final MetricBuilder<S, ScalarOutput> builder)
    {
        super(builder);
        ContingencyTableBuilder<S> ct = new ContingencyTableBuilder<>();
        ct.setOutputFactory(builder.dataFactory);
        table = ct.build();
    }

}
