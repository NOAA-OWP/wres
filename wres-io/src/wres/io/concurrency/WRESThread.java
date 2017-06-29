package wres.io.concurrency;

import java.util.function.Consumer;

/**
 * @author Christopher Tubbs
 *
 */
public abstract class WRESThread
{
    protected final static String NEWLINE = System.lineSeparator();

    public void setOnComplete(Consumer<Object> onComplete) {
        this.onComplete = onComplete;
    }
    
    public void setOnRun(Consumer<Object> onRun) {
        this.onRun = onRun;
    }
    
    protected void executeOnComplete() {
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
