package wres.io.concurrency;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ctubbs on 7/19/17.
 */
public abstract class WRESCallable<V> extends WRESTask implements Callable<V>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WRESCallable.class );
    
    @Override
    public final V call()
    {
        try
        {
            this.executeOnRun();
            V result = this.execute();
            this.executeOnComplete();
            return result;
        }
        catch (RuntimeException e)
        {
            // If a runtime error is thrown, we want to catch it and package it up
            this.executeOnError( e );
            LOGGER.error( "Callable task failed", e);
            throw new WRESRunnableException( "Callable task failed", e );
        }
        catch(Exception e)
        {
            this.executeOnError( e );
            throw new WRESRunnableException( "Callable task failed", e );
        }
    }

    protected abstract V execute() throws IOException, SQLException;
}
