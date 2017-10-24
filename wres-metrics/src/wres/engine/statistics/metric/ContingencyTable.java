package wres.engine.statistics.metric;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import wres.datamodel.MatrixOutput;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricInputException;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.MulticategoryPairs;
import wres.datamodel.VectorOfBooleans;

/**
 * <p>
 * Base class for a contingency matrix with N*N elements. A contingency matrix compares the number of predictions and
 * observations associated with each of the N possible outcomes of an N-category variable. The rows of the contingency
 * table store the number of predicted outcomes and the columns store the number of observed outcomes.
 * </p>
 * <p>
 * The elements of the contingency table are unpacked into a vector. The elements are unpacked from left to right and
 * top to bottom. Thus, for a 2x2 contingency table, the "true positives" (hits), "false positives" (false alarms),
 * "false negatives" (misses), and "true negatives" are contained in the first, second, third and fourth positions of
 * the returned vector, respectively.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

class ContingencyTable<S extends MulticategoryPairs> extends Metric<S, MatrixOutput>
{

    @Override
    public MatrixOutput apply(final MulticategoryPairs s)
    {
        if(Objects.isNull(s))
        {
            throw new MetricInputException("Specify non-null input to the '"+this+"'.");
        }
        final int outcomes = s.getCategoryCount();
        final double[][] returnMe = new double[outcomes][outcomes];
        //Function that returns the index within the contingency matrix to increment
        final Consumer<VectorOfBooleans> f = a -> {
            boolean[] b = a.getBooleans();
            //Dichotomous event represented as a single outcome: expand
            if(b.length == 2)
            {
                b = new boolean[]{b[0], !b[0], b[1], !b[1]};
            }
            final boolean[] c = b;
            final int[] index = IntStream.range(0, c.length).filter(i -> c[i]).toArray();
            returnMe[index[1] - outcomes][index[0]] += 1;
        };
        //Increment the count in a serial stream as the lambda is stateful
        s.getData().stream().forEach(f);
        final MetricOutputMetadata metOut = getMetadata(s, s.getData().size(), MetricConstants.MAIN, null);
        return getDataFactory().ofMatrixOutput(returnMe, metOut);
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

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    static class ContingencyTableBuilder<S extends MulticategoryPairs> extends MetricBuilder<S, MatrixOutput>
    {

        @Override
        protected ContingencyTable<S> build() throws MetricParameterException
        {
            return new ContingencyTable<>(this);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder.
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    ContingencyTable(final ContingencyTableBuilder<S> builder) throws MetricParameterException
    {
        super(builder);
    }
}
