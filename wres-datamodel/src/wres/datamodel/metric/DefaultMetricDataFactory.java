package wres.datamodel.metric;

import java.util.Objects;

/**
 * A default factory class for producing datasets associated with verification metrics.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public abstract class DefaultMetricDataFactory
{

    /**
     * Instance of the factory.
     */
    
    private MetadataFactory instance = null;
    
    /**
     * Returns a {@link MetadataFactory} for building {@link Metadata}.
     * 
     * @return an instance of {@link MetadataFactory} 
     */

    public MetadataFactory getMetadataFactory() {
        if(Objects.isNull(instance)) {
            instance = DefaultMetadataFactory.of();
        }
        return instance;        
    }
       
    /**
     * Hidden constructor.
     */
    
    protected DefaultMetricDataFactory() {
    }    
    
}
