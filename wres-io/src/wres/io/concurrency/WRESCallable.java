package wres.io.concurrency;

import java.util.concurrent.Callable;

/**
 * Created by ctubbs on 7/19/17.
 */
public abstract class WRESCallable<V> extends WRESTask implements Callable<V>
{
    @Override
    public final V call () throws Exception
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
            this.getLogger().error( "Callable task failed: {}", e);
            throw new WRESRunnableException( "Callable task failed", e );
        }
    }

    protected abstract V execute() throws Exception;
}
