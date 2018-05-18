package wres.io.concurrency;

import java.io.IOException;
import java.sql.SQLException;

import org.omg.SendingContext.RunTime;

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
        catch (RuntimeException re)
        {
            this.getLogger().error("Task failed: {}", re);
            throw new WRESRunnableException( "Task failed:", re );
        }
        catch ( IOException | SQLException se )
        {
            throw new WRESRunnableException( "Task failed:", se );
        }

        this.executeOnComplete();
    }

    protected abstract void execute() throws SQLException, IOException;
}
