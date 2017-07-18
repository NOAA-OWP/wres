package wres.datamodel.metric;

import java.util.List;
import java.util.Objects;

import wres.datamodel.PairOfDoubles;

/**
 * Default implementation of a utility class for slicing and dicing datasets associated with verification metrics. TODO:
 * reconcile this class with the Slicer in wres.datamodel.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class DefaultSlicer implements Slicer
{

    /**
     * Instance of the slicer.
     */

    private static DefaultSlicer instance = null;

    /**
     * Returns an instance of a {@link Slicer}.
     * 
     * @return a {@link MetricOutputFactory}
     */

    public static Slicer getInstance()
    {
        if(Objects.isNull(instance))
        {
            instance = new DefaultSlicer();
        }
        return instance;
    }    
       
    /**
     * Null error message.
     */
    private static final String NULL_ERROR = "Specify a non-null input to slice.";

    @Override
    public double[] getLeftSide(List<PairOfDoubles> input)
    {
        Objects.requireNonNull(input, NULL_ERROR);
        return input.stream().mapToDouble(a -> a.getItemOne()).toArray();
    }

    @Override
    public double[] getRightSide(List<PairOfDoubles> input)
    {
        Objects.requireNonNull(input, NULL_ERROR);
        return input.stream().mapToDouble(a -> a.getItemTwo()).toArray();
    }

    /**
     * Hidden constructor.
     */

    private DefaultSlicer()
    {
    }

}
