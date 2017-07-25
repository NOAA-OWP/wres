package wres.io.concurrency;

import wres.util.Internal;

/**
 * Created by ctubbs on 7/19/17.
 */
@Internal(exclusivePackage = "wres.io")
public abstract class WRESRunnable extends WRESTask implements Runnable
{
    @Override
    public final void run ()
    {
        this.executeOnRun();
        this.execute();
        this.executeOnComplete();
    }

    protected abstract void execute();
}
