package wres.datamodel.outputs;

import java.util.Set;
import java.util.concurrent.Future;

import wres.datamodel.MetricConstants.MetricOutputGroup;

/**
 * <p>
 * A high-level store of {@link MetricOutput} associated with a verification project. The outputs are stored in a
 * {@link MetricOutputMultiMap}. The {@link MetricOutputMultiMap} are further grouped by {@link MetricOutputGroup},
 * which denotes the atomic type of output stored by the container. For example, the 
 * {@link MetricOutputGroup#DOUBLE_SCORE} maps to {@link DoubleScoreOutput}.
 * </p>
 * <p>
 * Retrieve the outputs using the instance methods for particular {@link MetricOutputGroup}. If no outputs exist, the
 * instance methods return null. The store is built with {@link Future} of the {@link MetricOutput} and the instance
 * methods call {@link Future#get()}.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricOutputForProject<T extends MetricOutputMultiMap<?>>
{

    /**
     * Returns true if results are available for the input type, false otherwise.
     * 
     * @param outGroup the {@link MetricOutputGroup} to test
     * @return true if results are available for the input, false otherwise
     */

    boolean hasOutput( MetricOutputGroup outGroup );

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link DoubleScoreOutput} or null if no output exists.
     * 
     * @return the scalar output or null
     * @throws MetricOutputException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    MetricOutputMultiMap<DoubleScoreOutput> getDoubleScoreOutput() throws InterruptedException;

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link MultiVectorOutput} or null if no output exists.
     * 
     * @return the multi-vector output or null
     * @throws MetricOutputException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    MetricOutputMultiMap<MultiVectorOutput> getMultiVectorOutput() throws InterruptedException;

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link MatrixOutput} or null if no output exists.
     * 
     * @return the matrix output or null
     * @throws MetricOutputException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    MetricOutputMultiMap<MatrixOutput> getMatrixOutput() throws InterruptedException;

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link BoxPlotOutput} or null if no output exists.
     * 
     * @return the matrix output or null
     * @throws MetricOutputException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    MetricOutputMultiMap<BoxPlotOutput> getBoxPlotOutput() throws InterruptedException;

    /**
     * Returns a {@link MetricOutputMultiMap} for a prescribed array of {@link MetricOutputGroup} or null if no output
     * exists. To return all available outputs, use {@link #getOutputTypes()} as input to this method.
     * 
     * @param outGroup the array of {@link MetricOutputGroup}
     * @return the metric output or null
     * @throws MetricOutputException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    T getOutput( MetricOutputGroup... outGroup ) throws InterruptedException;

    /**
     * Returns all {@link MetricOutputGroup} for which outputs are available.
     * 
     * @return all {@link MetricOutputGroup} for which outputs are available
     */

    Set<MetricOutputGroup> getOutputTypes();

}
