package wres.io.concurrency;

import org.slf4j.Logger;

import java.util.function.Consumer;

/**
 * @author Christopher Tubbs
 *
 */
public abstract class WRESTask
{
    protected final static String NEWLINE = System.lineSeparator();

    public void setOnComplete(Consumer<Object> onComplete) {
        this.onComplete = onComplete;
    }
    
    public void setOnRun(Consumer<Object> onRun) {
        this.onRun = onRun;
    }

    protected abstract String getTaskName();
    protected abstract Logger getLogger ();

    protected void executeOnComplete() {
        if (this.onComplete != null) {
            this.onComplete.accept(null);
        }
    }
    
    protected void executeOnRun() {
        this.setThreadName();
        if (this.onRun != null) {
            this.onRun.accept(null);
        }
    }

    private void setThreadName()
    {
        String threadName = this.getTaskName();
        threadName += " - ";
        threadName += this.getThreadID();

        Thread.currentThread().setName(threadName);
    }

    private String getThreadID()
    {
        return String.valueOf(Thread.currentThread().getId());
    }

    private Consumer<Object> onComplete;
    private Consumer<Object> onRun;
}
