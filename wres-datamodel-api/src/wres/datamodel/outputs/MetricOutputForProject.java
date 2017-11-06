package wres.datamodel.outputs;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricOutputGroup;

/**
 * <p>
 * A high-level store of {@link MetricOutput} associated with a verification project. The outputs are stored in a
 * {@link MetricOutputMultiMap}. The {@link MetricOutputMultiMap} are further grouped by {@link MetricOutputGroup},
 * which denotes the atomic type of output stored by the container. For example, the {@link MetricOutputGroup#SCALAR}
 * maps to {@link ScalarOutput}.
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

    boolean hasOutput(MetricOutputGroup outGroup);

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link ScalarOutput} or null if no output exists.
     * 
     * @return the scalar output or null
     * @throws InterruptedException if the retrieval of {@link MetricOutput} is cancelled
     * @throws ExecutionException if the retrieval of {@link MetricOutput} fails
     */

    MetricOutputMultiMap<ScalarOutput> getScalarOutput() throws InterruptedException, ExecutionException;

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link VectorOutput} or null if no output exists.
     * 
     * @return the vector output or null
     * @throws InterruptedException if the retrieval of {@link MetricOutput} is cancelled
     * @throws ExecutionException if the retrieval of {@link MetricOutput} fails
     */

    MetricOutputMultiMap<VectorOutput> getVectorOutput() throws InterruptedException, ExecutionException;

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link MultiVectorOutput} or null if no output exists.
     * 
     * @return the multi-vector output or null
     * @throws InterruptedException if the retrieval of {@link MetricOutput} is cancelled
     * @throws ExecutionException if the retrieval of {@link MetricOutput} fails
     */

    MetricOutputMultiMap<MultiVectorOutput> getMultiVectorOutput() throws InterruptedException, ExecutionException;

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link MatrixOutput} or null if no output exists.
     * 
     * @return the matrix output or null
     * @throws InterruptedException if the retrieval of {@link MetricOutput} is cancelled
     * @throws ExecutionException if the retrieval of {@link MetricOutput} fails
     */

    MetricOutputMultiMap<MatrixOutput> getMatrixOutput() throws InterruptedException, ExecutionException;

    /**
     * Returns a {@link MetricOutputMultiMap} for a prescribed array of {@link MetricOutputGroup} or null if no output
     * exists. To return all available outputs, use {@link #getOutputTypes()} as input to this method.
     * 
     * @param outGroup the array of {@link MetricOutputGroup}
     * @return the metric output or null
     * @throws InterruptedException if the retrieval of {@link MetricOutput} is cancelled
     * @throws ExecutionException if the retrieval of {@link MetricOutput} fails
     */

    T getOutput(MetricOutputGroup... outGroup) throws InterruptedException, ExecutionException;

    /**
     * Returns all {@link MetricOutputGroup} for which outputs are available.
     * 
     * @return all {@link MetricOutputGroup} for which outputs are available
     */

    MetricOutputGroup[] getOutputTypes();

}
