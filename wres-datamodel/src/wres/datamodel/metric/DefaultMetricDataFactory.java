package wres.datamodel.metric;

/**
 * A default factory class for producing datasets associated with verification metrics.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public abstract class DefaultMetricDataFactory implements MetricDataFactory
{

    @Override
    public MetadataFactory getMetadataFactory() {
        return DefaultMetadataFactory.getInstance();  
    }

    @Override
    public Slicer getSlicer()
    {
        return DefaultSlicer.getInstance();  
    }    
       
    /**
     * Hidden constructor.
     */
    
    protected DefaultMetricDataFactory() {
    }    
    
}
