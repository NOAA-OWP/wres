package wres.control;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.sampledata.SampleData;
import wres.engine.statistics.metric.processing.MetricProcessorException;

/**
 * <p>A processor that consumes {@link Future} of {@link SampleData} and then returns the {@link SampleData} 
 * for further processing.</p>.
 * 
 * @author james.brown@hydrosolved.com
 */

class SupplySampleData implements Supplier<SampleData<?>>
{

    /**
     * Logger.
     */
    
    private static final Logger LOGGER =
            LoggerFactory.getLogger( SupplySampleData.class );

    /**
     * The future sample data.
     */
    
    private final Future<SampleData<?>> futureSamples;

    /**
     * Is true to return the baseline data only.
     */
    
    private final boolean baselineOnly;
    
    /**
     * Returns an instance.
     * 
     * @param futureSamples the future sample data
     * @return an instance of the supplier
     */
    
    static SupplySampleData of( Future<SampleData<?>> futureSamples )
    {
        return SupplySampleData.of( futureSamples, false );
    }

    /**
     * When the <code>baselineOnly</code> is <code>true</code>, returns a supplier for the baseline data only, 
     * otherwise the fully composed sample data. 
     * 
     * @param futureSamples the future sample data
     * @param baselineOnly is true to return the baseline data only
     * @return an instance of the supplier
     */
    
    static SupplySampleData of( Future<SampleData<?>> futureSamples, boolean baselineOnly )
    {
        return new SupplySampleData( futureSamples, baselineOnly );
    }    

    @Override
    public SampleData<?> get()
    {
        SampleData<?> returnMe = null;
        
        try
        {
            if( this.baselineOnly )
            {
                returnMe = this.futureSamples.get().getBaselineData();
            }
            else
            {
                returnMe = this.futureSamples.get();
            }           
            
            LOGGER.debug( "Acquired pairs for feature '{}' and time window {}.",
                          returnMe.getMetadata().getIdentifier().getGeospatialID(),
                          returnMe.getMetadata().getTimeWindow() );
        }
        catch ( InterruptedException e )
        {
            LOGGER.warn( "Interrupted while acquiring pairs:", e );
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException | MetricProcessorException e )
        {
            throw new WresProcessingException( "While acquiring pairs:", e );
        }
        
        return returnMe;
    }    
    
    /**
     * Construct.
     * 
     * @param futureSamples the future sample data
     * @param baselineOnly is true to supply the baseline data only 
     * @throws NullPointerException if either input is null
     */

    private SupplySampleData( Future<SampleData<?>> futureSamples, boolean baselineOnly )
    {
        Objects.requireNonNull( futureSamples, "Specify a non-null sample data future." );

        this.futureSamples = futureSamples;
        this.baselineOnly = baselineOnly;
    }    
    
}
