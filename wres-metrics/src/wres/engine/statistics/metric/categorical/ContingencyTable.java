package wres.engine.statistics.metric.categorical;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.VectorOfBooleans;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.MulticategoryPairs;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.MetricOutput;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * <p>
 * Base class for a contingency table. A contingency table compares the number of predictions and observations 
 * associated with each of the N possible outcomes of an N-category variable. The rows of the contingency
 * table store the number of predicted outcomes and the columns store the number of observed outcomes.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 */

public class ContingencyTable<S extends MulticategoryPairs> implements Metric<S, MatrixOutput>
{

    /**
     * The data factory.
     */

    private final DataFactory dataFactory;

    @Override
    public DataFactory getDataFactory()
    {
        return dataFactory;
    }

    @Override
    public MatrixOutput apply( final MulticategoryPairs s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }
        final int outcomes = s.getCategoryCount();
        final double[][] returnMe = new double[outcomes][outcomes];
        // Function that returns the index within the contingency table to increment
        final Consumer<VectorOfBooleans> f = a -> {
            boolean[] b = a.getBooleans();
            // Dichotomous event represented as a single outcome: expand
            if ( b.length == 2 )
            {
                b = new boolean[] { b[0], !b[0], b[1], !b[1] };
            }
            final boolean[] c = b;
            final int[] index = IntStream.range( 0, c.length ).filter( i -> c[i] ).toArray();
            returnMe[index[1] - outcomes][index[0]] += 1;
        };
        // Increment the count in a serial stream as the lambda is stateful
        s.getRawData().stream().forEach( f );
        // Name the outcomes for a 2x2 contingency table
        List<MetricDimension> componentNames = null;
        if ( outcomes == 2 )
        {
            componentNames = Arrays.asList( MetricDimension.TRUE_POSITIVES,
                                            MetricDimension.FALSE_POSITIVES,
                                            MetricDimension.FALSE_NEGATIVES,
                                            MetricDimension.TRUE_NEGATIVES );
        }
        final MetricOutputMetadata metOut = getMetadata( s, s.getRawData().size(), MetricConstants.MAIN, null );
        return getDataFactory().ofMatrixOutput( returnMe, componentNames, metOut );
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
     * A {@link MetricBuilder} to build the metric.
     */

    public static class ContingencyTableBuilder<S extends MulticategoryPairs> implements MetricBuilder<S, MatrixOutput>
    {

        @Override
        public ContingencyTable<S> build() throws MetricParameterException
        {
            return new ContingencyTable<>( this );
        }

        /**
         * The data factory.
         */

        private DataFactory dataFactory;

        /**
         * Sets the {@link DataFactory} for constructing a {@link MetricOutput}.
         * 
         * @param dataFactory the {@link DataFactory}
         * @return the builder
         */

        @Override
        public ContingencyTableBuilder<S> setOutputFactory( final DataFactory dataFactory )
        {
            this.dataFactory = dataFactory;
            return this;
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder.
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    ContingencyTable( final ContingencyTableBuilder<S> builder ) throws MetricParameterException
    {
        if ( Objects.isNull( builder ) )
        {
            throw new MetricParameterException( "Cannot construct the metric with a null builder." );
        }
        this.dataFactory = builder.dataFactory;
        if ( Objects.isNull( this.dataFactory ) )
        {
            throw new MetricParameterException( "Specify a data factory with which to build the metric." );
        }
    }
}
