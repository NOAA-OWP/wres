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

class SampleDataSupplier implements Supplier<SampleData<?>>
{

    /**
     * Logger.
     */
    
    private static final Logger LOGGER =
            LoggerFactory.getLogger( SampleDataSupplier.class );

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
    
    static SampleDataSupplier of( Future<SampleData<?>> futureSamples )
    {
        return SampleDataSupplier.of( futureSamples, false );
    }

    /**
     * When the <code>baselineOnly</code> is <code>true</code>, returns a supplier for the baseline data only, 
     * otherwise the fully composed sample data. 
     * 
     * @param futureSamples the future sample data
     * @param baselineOnly is true to return the baseline data only
     * @return an instance of the supplier
     */
    
    static SampleDataSupplier of( Future<SampleData<?>> futureSamples, boolean baselineOnly )
    {
        return new SampleDataSupplier( futureSamples, baselineOnly );
    }    

    @Override
    public SampleData<?> get()
    {
        SampleData<?> returnMe = null;
        
        try
        {
            returnMe = this.futureSamples.get();
            
            if( this.baselineOnly )
            {
                returnMe = returnMe.getBaselineData();
            }      
            
            // #67532
            if( Objects.isNull( returnMe ) )
            {
                throw new IllegalStateException( "Failed to retrieve non-null sample data, which is not an "
                        + "expected state." );
            }
            
            LOGGER.debug( "Acquired pairs for {}.", returnMe.getMetadata() );
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

    private SampleDataSupplier( Future<SampleData<?>> futureSamples, boolean baselineOnly )
    {
        Objects.requireNonNull( futureSamples, "Specify a non-null sample data future." );

        this.futureSamples = futureSamples;
        this.baselineOnly = baselineOnly;
    }    
    
}
