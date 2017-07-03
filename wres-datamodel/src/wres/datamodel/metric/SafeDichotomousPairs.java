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
final class SafeDichotomousPairs extends SafeMulticategoryPairs implements DichotomousPairs
{

    @Override
    public DichotomousPairs getBaselineData()
    {
        final MetricInputFactory metIn = DefaultMetricInputFactory.of();
        return metIn.ofDichotomousPairs(getDataForBaseline(),getMetadataForBaseline());
    }        

    @Override
    public int getCategoryCount()
    {
        return 2;
    }
    
    /**
     * A {@link MetricInputBuilder} to build the metric input.
     */

    protected static class DichotomousPairsBuilder extends MulticategoryPairsBuilder
    {

        @Override
        public DichotomousPairs build()
        {
            return new SafeDichotomousPairs(this);
        }

    }    

    /**
     * Construct the single-valued pairs with a builder.
     * 
     * @param b the builder
     * @throws MetricInputException if the pairs are invalid
     */

    private SafeDichotomousPairs(final DichotomousPairsBuilder b)
    {
        super(b);
        if(super.getCategoryCount() != 2)
        {
            throw new MetricInputException("Expected one outcome in the dichotomous input (two or four columns).");
        }
    }

}
