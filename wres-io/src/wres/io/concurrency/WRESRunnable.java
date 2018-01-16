package wres.io.concurrency;

import java.sql.SQLException;

/**
 * Created by ctubbs on 7/19/17.
 */
public abstract class WRESRunnable extends WRESTask implements Runnable
{
    @Override
    public final void run()
    {
        this.executeOnRun();

        try
        {
            this.execute();
        }
        catch ( SQLException se )
        {
            throw new WRESRunnableException( "Task failed:", se );
        }

        this.executeOnComplete();
    }

    protected abstract void execute() throws SQLException;
}
