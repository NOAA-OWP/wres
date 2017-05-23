package wres.engine.statistics.metric;

import wres.datamodel.metric.MetricOutput;
import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.parameters.MetricParameter;

/**
 * The mean square error (MSE) measures the accuracy of a single-valued predictand. It comprises the average square
 * difference between the predictand and verifying observation. Optionally, the MSE may be factored into two-component
 * or three-component decompositions.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class MeanSquareError<S extends SingleValuedPairs, T extends MetricOutput<?>> extends DoubleErrorScore<S, T>
{

    @Override
    public T apply(final S s)
    {
        //TODO: implement any required decompositions, based on the instance parameters  
        return super.apply(s);
    }

    @Override
    public void checkParameters(final MetricParameter... par)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public String getName()
    {
        return "Mean Square Error";
    }

    @Override
    public boolean isDecomposable()
    {
        return true;
    }

    /**
     * Protected constructor.
     */

    protected MeanSquareError()
    {
        super(FunctionFactory.squareError());
    }

}
