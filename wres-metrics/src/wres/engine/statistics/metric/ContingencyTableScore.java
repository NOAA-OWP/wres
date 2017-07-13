package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.MatrixOfDoubles;
import wres.datamodel.metric.MatrixOutput;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricInputException;
import wres.datamodel.metric.MetricOutputFactory;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.MulticategoryPairs;
import wres.datamodel.metric.ScalarOutput;

/**
 * A generic implementation of an error score that applies to the components of a {@link ContingencyTable}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public abstract class ContingencyTableScore<S extends MulticategoryPairs> extends Metric<S, ScalarOutput>
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
        Objects.requireNonNull(s, "Specify non-null input for the '" + toString() + "'.");
        return table.apply(s);
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    @Override
    public MetricConstants getDecompositionID()
    {
        return MetricConstants.NONE;
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

    protected MetricOutputMetadata getMetadata(final MatrixOutput output)
    {
        final MetricOutputMetadata metIn = output.getMetadata();
        final MetadataFactory f = getOutputFactory().getMetadataFactory();
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

    protected void isContingencyTable(final MatrixOutput output, final Metric<?, ?> metric)
    {
        Objects.requireNonNull(output, nullString);
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

    protected void is2x2ContingencyTable(final MatrixOutput output, final Metric<?, ?> metric)
    {
        Objects.requireNonNull(output, nullString);
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
     * @param outputFactory the {@link MetricOutputFactory}
     */

    protected ContingencyTableScore(final MetricOutputFactory outputFactory)
    {
        super(outputFactory);
        table = new ContingencyTable<>(outputFactory);
    }

}
