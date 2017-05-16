package wres.engine.statistics.metric;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import wres.datamodel.VectorOfBooleans;
import wres.engine.statistics.metric.inputs.MetricInputException;
import wres.engine.statistics.metric.inputs.MulticategoryPairs;
import wres.engine.statistics.metric.outputs.MatrixOutput;
import wres.engine.statistics.metric.outputs.MetricOutput;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;
import wres.engine.statistics.metric.parameters.MetricParameter;

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

public class ContingencyTable<S extends MulticategoryPairs, T extends MetricOutput<?, ?>> extends Metric<S, T>
{

    @Override
    public T apply(final S s)
    {
        Objects.requireNonNull(s, "Specify non-null input for the '" + toString() + "'.");
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
        return MetricOutputFactory.getMatrixExtendsMetricOutput(returnMe, s.size(), null);
    }

    @Override
    public void checkParameters(final MetricParameter... par)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public String getName()
    {
        return "Contingency Table";
    }

    /**
     * Convenience method that checks whether the output is compatible with a 2x2 contingency table. Throws an exception
     * if the output is incompatible.
     * 
     * @param output the output to check
     * @param metric the metric to use when throwing an informative exception
     * @throws MetricInputException if the output is not a valid input for an intermediate calculation
     */

    protected void is2x2ContingencyTable(final MetricOutput<?, ?> output, final Metric<?, ?> metric)
    {
        Objects.requireNonNull(output, "Specify non-null input for the '" + toString() + "'.");
        final String message = "Expected an intermediate result with the 2x2 Contingency Table when "
            + "computing the '" + metric + "'.";
        if(!(output instanceof MatrixOutput))
        {
            throw new MetricInputException(message);
        }
        final MatrixOutput v = (MatrixOutput)output;
        if(v.getData().size() != 4)
        {
            throw new MetricInputException(message);
        }
    }

    /**
     * Protected constructor.
     */

    protected ContingencyTable()
    {
        super();
    }

}
