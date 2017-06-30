package wres.datamodel.metric;

/**
 * Immutable store of verification pairs associated with a dichotomous input, i.e. a single event whose outcome is
 * recorded as occurring (true) or not occurring (false). The event is not defined as part of the input. A dichotomous
 * pair is be encoded with a single indicator.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class DichotomousPairs extends MulticategoryPairs
{

    /**
     * A {@link MetricInputBuilder} to build the metric input.
     */

    public static class DichotomousPairsBuilder extends MulticategoryPairsBuilder
    {

        @Override
        public DichotomousPairs build()
        {
            return new DichotomousPairs(this);
        }

    }

    @Override
    public int getCategoryCount()
    {
        return 2;
    }

    /**
     * Construct the single-valued pairs with a builder.
     * 
     * @param b the builder
     * @throws MetricInputException if the pairs are invalid
     */

    private DichotomousPairs(final DichotomousPairsBuilder b)
    {
        super(b);
        final int check = b.mainInput.get(0).size();
        if(check != 2 && check != 4) //Allow for shorthand and longhand construction of a dichotomous event  
        {
            throw new MetricInputException("Expected one outcome in the dichotomous input (two or four columns).");
        }
    }

}
