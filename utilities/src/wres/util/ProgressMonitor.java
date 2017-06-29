package wres.util;

import java.io.PrintStream;
import java.text.DecimalFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author Christopher Tubbs
 *
 */
public class ProgressMonitor
{

    private final ExecutorService ASYNC_UPDATER = Executors.newSingleThreadExecutor();

    private static ProgressMonitor MONITOR = new ProgressMonitor();

    public static void deactivate()
    {
        MONITOR.shutdown();
    }

    public static void activate()
    {
        if (MONITOR != null)
        {
            MONITOR.shutdown();
        }
        MONITOR = new ProgressMonitor();
    }

    public static void increment()
    {
        synchronized (MONITOR) {
            MONITOR.addStep();
        }
    }

    public static void completeStep()
    {
        synchronized (MONITOR) {
            MONITOR.UpdateMonitor();
        }
    }

    public static void setUpdateFrequency(Long frequency)
    {
        synchronized (MONITOR) {
            MONITOR.updateFrequency = frequency;
        }
    }

    public static Consumer<Object> onThreadStartHandler() {
        return (Object t) -> {
            ProgressMonitor.increment();
        };
    }
    
    public static Consumer<Object> onThreadCompleteHandler() {
        return (Object t) -> {
            ProgressMonitor.completeStep();
        };
    }

    public static void setOutput(Consumer<ProgressMonitor> outputFunction)
    {
        synchronized (MONITOR) {
            MONITOR.setOuputFunction(outputFunction);
        }
    }
    
    public ProgressMonitor(PrintStream printer) {
        this.totalSteps = 0L;
        this.completedSteps = 0L;
        this.percentFormat = new DecimalFormat();
        this.percentFormat.setMaximumFractionDigits(2);
        this.mainFormat = new DecimalFormat("###,###");
        if (printer == null)
        {
            printer = System.out;
        }
        
        this.printer = printer;
        this.outputFunction = (ProgressMonitor monitor) -> {
            ASYNC_UPDATER.execute(()-> {
                this.printer.print(getProgressMessage());
            });
        };
        this.startTime = System.currentTimeMillis();
    }
    
    public ProgressMonitor()
    {
        this.totalSteps = 0L;
        this.completedSteps = 0L;
        this.percentFormat = new DecimalFormat();

        this.percentFormat.setMaximumFractionDigits(2);
        this.startTime = System.currentTimeMillis();
        this.printer = System.out;
        this.outputFunction = (ProgressMonitor monitor) -> {
            ASYNC_UPDATER.execute(()-> {
                this.printer.print(getProgressMessage());
            });
        };
        this.mainFormat = new DecimalFormat("###,###");
    }
    
    public ProgressMonitor(Consumer<ProgressMonitor> outputFunction)
    {
        this.totalSteps = 0L;
        this.completedSteps = 0L;
        this.percentFormat = new DecimalFormat();
        this.percentFormat.setMaximumFractionDigits(2);
        this.printer = System.out;
        this.outputFunction = outputFunction;
        this.startTime = System.currentTimeMillis();
        this.mainFormat = new DecimalFormat("###,###");
    }
    
    public static void resetMonitor()
    {
        synchronized (MONITOR) {
            MONITOR.reset();
        }
    }
    
    public void UpdateMonitor() {
        
        if (completedSteps < totalSteps) {
            completedSteps++;
            executeOutput();
        }
        
        if (this.autoReset && totalSteps.equals(completedSteps)) {
            reset();
        }
    }
    
    public void addStep() {
        this.totalSteps++;
        executeOutput();
    }
    
    public void setSteps(Long steps) {
            this.totalSteps = steps;
            executeOutput();
    }

    private void executeOutput()
    {
        if (this.shouldUpdate())
        {
            this.outputFunction.accept(this);
            this.lastUpdate = System.currentTimeMillis();
        }
    }
    
    public void setAutoReset(boolean reset) {
        this.autoReset = reset;
    }

    public void setShowStepDescription(boolean showStepDescription)
    {
        this.showStepDescription = showStepDescription;
    }
    
    public void reset() {
        this.totalSteps = 0L;
        this.completedSteps = 0L;
        this.printer.println();
    }
    
    private String getProgressMessage() {
        String message = "COULDN'T CREATE UPDATE MESSAGE";
        String builder = "\r";
        try
        {
            if (this.showStepDescription)
            {
                builder += mainFormat.format(completedSteps);
                builder += " steps out of ";
                builder += mainFormat.format(totalSteps);
                builder += " completed";
            }
            
            builder += " at an average speed of ";
            builder += getCompletionSpeed();
            message = builder;
        }
        catch (Exception e) {
            System.err.println();
            System.err.println("Could not print: ");
            System.err.println(builder);
            e.printStackTrace();
        }
        return message;
    }

    private boolean shouldUpdate()
    {
        boolean update = true;

        if (this.updateFrequency != null &&
                lastUpdate != null &&
                (System.currentTimeMillis() - this.lastUpdate) < (this.updateFrequency * 1000))
        {
                update = false;
        }

        return update;
    }

    private String getCompletionSpeed()
    {
        Long elapsed = System.currentTimeMillis() - startTime;
        Double seconds = elapsed / 1000.0;
        return String.format("%.2f TPS", this.completedSteps/seconds);
    }
    
    public void changePrinter(PrintStream printer)
    {
        this.printer = printer;
    }

    @Override
    protected void finalize () throws Throwable {
        this.shutdown();
    }

    private void shutdown()
    {
        this.ASYNC_UPDATER.shutdownNow();
    }

    public void setOuputFunction (Consumer<ProgressMonitor> outputFunction)
    {
        this.outputFunction = outputFunction;
    }

    private Long totalSteps;
    private Long completedSteps;
    private boolean autoReset;
    private boolean showStepDescription = true;
    private Long lastUpdate;
    private Long updateFrequency;
    private final Long startTime;
    private final DecimalFormat percentFormat;
    private final DecimalFormat mainFormat;
    private PrintStream printer;
    private Consumer<ProgressMonitor> outputFunction;
}
