package wres.io.concurrency;

import wres.util.Internal;

import java.util.concurrent.Callable;

/**
 * Created by ctubbs on 7/19/17.
 */
@Internal(exclusivePackage = "wres.io")
public abstract class WRESCallable<V> extends WRESTask implements Callable<V>
{
    @Override
    public final V call () throws Exception {
        this.executeOnRun();
        V result = this.execute();
        this.executeOnComplete();
        return result;
    }

    protected abstract V execute() throws Exception;
}
