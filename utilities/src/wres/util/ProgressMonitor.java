package wres.util;

import java.io.PrintStream;
import java.text.DecimalFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Christopher Tubbs
 *
 */
public class ProgressMonitor
{

    private ExecutorService ASYNC_UPDATER = Executors.newSingleThreadExecutor();

    private static ProgressMonitor MONITOR = new ProgressMonitor();
    private final Logger logger = LoggerFactory.getLogger( this.getClass() );
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
        if (ProgressMonitor.UPDATE_MONITOR)
        {
            synchronized (MONITOR)
            {
                MONITOR.addStep();
            }
        }
    }

    public static void completeStep()
    {
        if (ProgressMonitor.UPDATE_MONITOR)
        {
            synchronized (MONITOR)
            {
                MONITOR.UpdateMonitor();
            }
        }
    }

    public static void setUpdateFrequency(Long frequency)
    {
        synchronized (MONITOR)
        {
            MONITOR.updateFrequency = frequency;
        }
    }

    public static Consumer<Object> onThreadStartHandler()
    {
        Consumer<Object> handler = (Object obj) -> {};

        if (ProgressMonitor.UPDATE_MONITOR)
        {
            handler = (Object t) -> ProgressMonitor.increment();
        }

        return handler;
    }
    
    public static Consumer<Object> onThreadCompleteHandler() {

        Consumer<Object> handler = (Object obj) -> {};

        if (ProgressMonitor.UPDATE_MONITOR)
        {
            handler = (Object t) -> ProgressMonitor.completeStep();
        }

        return handler;
    }

    public static void setOutput(Consumer<ProgressMonitor> outputFunction)
    {
        synchronized (MONITOR)
        {
            MONITOR.setOutputFunction( outputFunction);
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
            if (this.shouldUpdate())
            {
                ASYNC_UPDATER.execute(()-> this.printMessage(getProgressMessage()));
            }
        };
        this.startTime = System.currentTimeMillis();
    }
    
    public ProgressMonitor()
    {
        this.startTime = System.currentTimeMillis();
        this.printer = System.out;
        this.outputFunction = (ProgressMonitor monitor) -> {
            if (this.shouldUpdate())
            {
                ASYNC_UPDATER.execute(()-> this.printMessage(getProgressMessage()));
            }
        };

        this.totalSteps = 0L;
        this.completedSteps = 0L;

        this.percentFormat = new DecimalFormat();
        this.percentFormat.setMaximumFractionDigits(2);
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
        synchronized (MONITOR)
        {
            MONITOR.reset();
        }
    }
    
    public void UpdateMonitor()
    {

        if (completedSteps <= totalSteps)
        {
            completedSteps++;
            executeOutput();
        }
    }
    
    public void addStep()
    {
        if (this.totalSteps == 0)
        {
            this.startTime = System.currentTimeMillis();
        }

        this.totalSteps++;
        executeOutput();
    }
    
    public static void setSteps(Long steps)
    {
        if (!MONITOR.shouldUpdate())
        {
            return;
        }

        synchronized ( MONITOR )
        {
            MONITOR.setTotalSteps( steps );
        }
    }

    public void setTotalSteps(Long steps)
    {
        this.completedSteps = 0L;
        this.totalSteps = steps;
        this.executeOutput();
    }

    private void executeOutput()
    {
        if (this.shouldUpdate())
        {
            this.outputFunction.accept(this);
            this.lastUpdate = System.currentTimeMillis();
        }
    }

    public static void setShowStepDescription(boolean showStepDescription)
    {
        MONITOR.showStepDescription = showStepDescription;
    }
    
    public void reset()
    {
        this.totalSteps = 0L;
        this.completedSteps = 0L;
        this.startTime = System.currentTimeMillis();
    }
    
    private String getProgressMessage() {
        String message = "COULDN'T CREATE UPDATE MESSAGE";
        String builder = "\r  ";
        try
        {
            if (this.showStepDescription)
            {
                builder += mainFormat.format(completedSteps);
                builder += " steps out of ";
                builder += mainFormat.format(totalSteps);
                builder += " completed";

                builder += " at an average speed of ";
                builder += getCompletionSpeed();
            }
            else
            {
                float completion = ((completedSteps * 1.0F) / (totalSteps * 1.0F)) * 100.0f;
                if (Float.isNaN( completion ))
                {
                    completion = 0.0F;
                }
                else if (completion > 100.0)
                {
                    completion = 100.0F;
                }
                builder += String.format("%.2f", completion);
                builder += "% Completed ";

                for (int i = 0; i < completion; i += 2)
                {
                    builder += "=";
                }

                builder += ">";
            }
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
        if (!ProgressMonitor.UPDATE_MONITOR ||
            this.ASYNC_UPDATER == null ||
            this.ASYNC_UPDATER.isTerminated() ||
            this.ASYNC_UPDATER.isShutdown())
        {
            return false;
        }

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
        Double speed = 0.0;

        if (!seconds.isNaN() && seconds != 0 && this.completedSteps != 0)
        {
            speed = this.completedSteps/seconds;
        }

        return String.format("%.2f TPS", speed);
    }

    public void printMessage(String message)
    {
        if (this.printer != null)
        {
            this.printer.print(message);
        }

        if (logger.isTraceEnabled())
        {
            logger.trace( message );
        }
    }
    
    public void changePrinter(PrintStream printer)
    {
        this.printer = printer;
    }

    @Override
    protected void finalize () throws Throwable {
        this.shutdown();
    }

    public static void setShouldUpdate(boolean shouldUpdate)
    {
        ProgressMonitor.UPDATE_MONITOR = shouldUpdate;

        if (shouldUpdate)
        {
            MONITOR.ASYNC_UPDATER = Executors.newSingleThreadExecutor(
                    runnable -> new Thread( runnable, "Progress Monitor Thread")
            );
        }
        else
        {
            MONITOR.ASYNC_UPDATER = null;
        }
    }

    private void shutdown()
    {
        if (this.ASYNC_UPDATER != null)
        {
            this.ASYNC_UPDATER.shutdown();
            while ( !this.ASYNC_UPDATER.isTerminated() )
            {
            }
        }
    }

    public void setOutputFunction( Consumer<ProgressMonitor> outputFunction)
    {
        this.outputFunction = outputFunction;
    }

    private Long totalSteps;
    private Long completedSteps;
    private boolean showStepDescription = true;
    private Long lastUpdate;
    private Long updateFrequency;
    private Long startTime;
    private final DecimalFormat percentFormat;
    private final DecimalFormat mainFormat;
    private PrintStream printer;
    private Consumer<ProgressMonitor> outputFunction;
    private static boolean UPDATE_MONITOR = true;
}
