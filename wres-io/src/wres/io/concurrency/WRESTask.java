package wres.io.concurrency;

import java.util.function.Consumer;

import org.slf4j.Logger;

import wres.util.Strings;

/**
 * @author Christopher Tubbs
 *
 */
public abstract class WRESTask
{
    protected static final String NEWLINE = System.lineSeparator();

    public void setOnComplete(Consumer<Object> onComplete) {
        this.onComplete = onComplete;
    }
    
    public void setOnRun(Consumer<Object> onRun) {
        this.onRun = onRun;
    }

    protected abstract Logger getLogger ();

    protected boolean validate()
    {
        if (this.getLogger() != null)
        {
            this.getLogger().trace( "{} in: '{}' has been validated.",
                                    this.getClass().getName(),
                                    this.getThreadName()  );
        }
        return true;
    }

    protected void executeOnComplete() {
        if (this.onComplete != null) {
            this.onComplete.accept(this);
        }
    }
    
    protected void executeOnRun() {
        this.setThreadName();
        this.validate();
        if (this.onRun != null) {
            this.onRun.accept(this);
        }
    }

    private void setThreadName()
    {
        Thread.currentThread().setName(this.getThreadName());
    }

    private String getThreadName()
    {
        if (this.threadName == null)
        {
            this.threadName = Thread.currentThread().getName();
            String newName =
                    " -> #" + Thread.currentThread().getId();

            if ( Strings.contains( threadName, "\\s->\\s#\\d+" ) )
            {
                threadName = threadName.replace( "\\s->\\s#\\d+", newName );
            }
            else
            {
                threadName += newName;
            }
        }
        return this.threadName;
    }

    private String threadName;
    private Consumer<Object> onComplete;
    private Consumer<Object> onRun;
}
