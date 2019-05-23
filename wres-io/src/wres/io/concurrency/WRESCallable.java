package wres.io.concurrency;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Callable;

/**
 * Created by ctubbs on 7/19/17.
 */
public abstract class WRESCallable<V> extends WRESTask implements Callable<V>
{
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
            this.getLogger().error( "Callable task failed", e);
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
