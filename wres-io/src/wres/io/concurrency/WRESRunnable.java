package wres.io.concurrency;

/**
 * Created by ctubbs on 7/19/17.
 */
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
