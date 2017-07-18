package wres.datamodel.metric;

/**
 * A factory class for producing datasets associated with verification metrics.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricDataFactory
{
  
    /**
     * Returns a {@link MetadataFactory} for building {@link Metadata}.
     * 
     * @return an instance of {@link MetadataFactory} 
     */

    MetadataFactory getMetadataFactory();
    
    /**
     * Returns a {@link Slicer} for slicing data.
     * 
     * @return a {@link Slicer}
     */

    Slicer getSlicer();
}
