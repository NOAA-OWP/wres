package wres.io.concurrency;

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

    protected void executeOnComplete() {
        Thread.currentThread().setName(this.getTaskName());
        if (this.onComplete != null) {
            this.onComplete.accept(null);
        }
    }
    
    protected void executeOnRun() {
        if (this.onRun != null) {
            this.onRun.accept(null);
        }
    }

    private Consumer<Object> onComplete;
    private Consumer<Object> onRun;
}
