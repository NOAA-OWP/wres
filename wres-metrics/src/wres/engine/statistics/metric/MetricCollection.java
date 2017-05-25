package wres.engine.statistics.metric;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import wres.datamodel.metric.MetricInput;
import wres.datamodel.metric.MetricOutput;
import wres.engine.statistics.metric.inputs.DichotomousPairs;
import wres.engine.statistics.metric.inputs.DiscreteProbabilityPairs;
import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.MetricOutputCollection;
import wres.engine.statistics.metric.outputs.ScalarOutput;
import wres.engine.statistics.metric.outputs.VectorOutput;

/**
 * <p>
 * An abstract base class for a collection of {@link Metric} that apply to a common class of {@link MetricInput} and
 * return a common class of {@link MetricOutput}. Multiple instances of the same metric are allowed (e.g. with different
 * parameter values).
 * </p>
 * <p>
 * For metrics that implement {@link Collectable} and whose method {@link Collectable#getCollectionOf()} returns a
 * common superclass (by name), the intermediate output is computed once and applied to all subclasses within the
 * collection. For example, if the {@link MetricCollection} contains several {@link Score} that extend
 * {@link ContingencyTable} and implement {@link Collectable}, the contingency table will be computed once, with all
 * dependent scores using this result.
 * </p>
 * <p>
 * Factory methods are included for constructing concrete collections. For example, {@link #ofDichotomousScalar()} will
 * return a concrete collection for storing instances of {@link Metric} that consume {@link DichotomousPairs} and
 * produce {@link ScalarOutput}.
 * </p>
 * <p>
 * Currently, each metric within a {@link MetricCollection} is computed sequentially.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public abstract class MetricCollection<S extends MetricInput<?>, T extends MetricOutput<?>>
extends
    ArrayList<Metric<S, T>>
implements Function<S, MetricOutputCollection<T>>
{

    /**
     * Serial ID.
     */

    private static final long serialVersionUID = -1302714449019049419L;

    /**
     * Construct a collection of metrics that consume {@link DichotomousPairs} and return {@link ScalarOutput}.
     * 
     * @return a metric collection
     */

    public static MetricCollection<DichotomousPairs, ScalarOutput> ofDichotomousScalar()
    {
        final class MetricCollectionImpl extends MetricCollection<DichotomousPairs, ScalarOutput>
        {
            private static final long serialVersionUID = -8001239422255786923L;

            @Override
            public MetricOutputCollection<ScalarOutput> apply(final DichotomousPairs s)
            {
                return super.applyInternal(s);
            }
        }
        return new MetricCollectionImpl();
    }

    /**
     * Construct a collection of metrics that consume {@link SingleValuedPairs} and return {@link VectorOutput}.
     * 
     * @return a metric collection
     */

    public static MetricCollection<SingleValuedPairs, VectorOutput> ofSingleValuedVector()
    {
        final class MetricCollectionImpl extends MetricCollection<SingleValuedPairs, VectorOutput>
        {

            private static final long serialVersionUID = -2462430150463352477L;

            @Override
            public MetricOutputCollection<VectorOutput> apply(final SingleValuedPairs s)
            {
                return super.applyInternal(s);
            }
        }
        return new MetricCollectionImpl();
    }

    /**
     * Construct a collection of metrics that consume {@link SingleValuedPairs} and return {@link VectorOutput}.
     * 
     * @return a metric collection
     */

    public static MetricCollection<SingleValuedPairs, ScalarOutput> ofSingleValuedScalar()
    {
        final class MetricCollectionImpl extends MetricCollection<SingleValuedPairs, ScalarOutput>
        {

            private static final long serialVersionUID = -7002084211172866874L;

            @Override
            public MetricOutputCollection<ScalarOutput> apply(final SingleValuedPairs s)
            {
                return super.applyInternal(s);
            }
        }
        return new MetricCollectionImpl();
    }

    /**
     * Construct a collection of metrics that consume {@link DiscreteProbabilityPairs} and return {@link MetricOutput}.
     * 
     * @return a metric collection
     */

    public static MetricCollection<DiscreteProbabilityPairs, ScalarOutput> ofDiscreteProbabilityScalarOutput()
    {
        final class MetricCollectionImpl extends MetricCollection<DiscreteProbabilityPairs, ScalarOutput>
        {

            private static final long serialVersionUID = 9034583833774457407L;

            @Override
            public MetricOutputCollection<ScalarOutput> apply(final DiscreteProbabilityPairs s)
            {
                return super.applyInternal(s);
            }
        }
        return new MetricCollectionImpl();
    }

    /**
     * Default method for computing the metric results and returning them in a collection with a prescribed type.
     * Collects instances of {@link Collectable} by {@link Collectable#getCollectionOf()} and computes their common
     * (intermediate) input once. Each metric is computed sequentially.
     * 
     * @param s the metric input
     * @return the output for each metric, contained in a collection
     */

    private MetricOutputCollection<T> applyInternal(final S s)
    {
        final MetricOutputCollection<T> m = new MetricOutputCollection<>(size());
        //Collect the instances of Collectable by their getCollectionOf string, which denotes the superclass that
        //provides the intermediate result for all metrics of that superclass
        @SuppressWarnings("unchecked")
        final Map<String, List<Collectable<S, MetricOutput<?>, T>>> collectable =
                                                                                stream().filter(Collectable.class::isInstance)
                                                                                        .map(p -> (Collectable<S, MetricOutput<?>, T>)p)
                                                                                        .collect(Collectors.groupingBy(Collectable::getCollectionOf));
        //Consumer that computes the intermediate output once and applies it to all grouped instances of Collectable
        final Consumer<List<Collectable<S, MetricOutput<?>, T>>> c = x -> {
            final MetricOutput<?> intermediate = x.get(0).getCollectionInput(s); //Compute intermediate output
            x.forEach(y -> m.add(indexOf(y), y.apply(intermediate))); //Use intermediate output to compute all measures
        };

        //Compute the collectable metrics    
        collectable.values().forEach(c);
        //collectable.values().parallelStream().forEach(c); //Parallel

        //Compute the non-collectable metrics: must be sequential at present as the lambda is stateful
        stream().filter(p -> !(p instanceof Collectable)).forEach(y -> m.add(indexOf(y), y.apply(s)));
        return m;
    }

    /**
     * Default constructor. Use the factory methods.
     */

    private MetricCollection()
    {

    }

}
