package wres.io.concurrency;

import java.util.function.Consumer;

/**
 * @author Christopher Tubbs
 *
 */
public abstract class WRESThread
{
    public void setOnComplete(Consumer<Object> onComplete) {
        this.onComplete = onComplete;
    }
    
    public void setOnRun(Consumer<Object> onRun) {
        this.onRun = onRun;
    }
    
    protected void exectureOnComplete() {
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
