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
     * Construct.
     * 
     * @param futureSamples the future sample data
     * @throws NullPointerException if either input is null
     */

    SupplySampleData( final Future<SampleData<?>> futureSamples )
    {
        Objects.requireNonNull( futureSamples, "Specify a non-null sample data future." );

        this.futureSamples = futureSamples;
    }

    @Override
    public SampleData<?> get()
    {
        SampleData<?> returnMe = null;
        
        try
        {
            returnMe = futureSamples.get();

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
    
}
