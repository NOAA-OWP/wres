package wres.system;

import java.io.PrintStream;
import java.text.DecimalFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.util.Strings;

/**
 * TODO: remove this class.
 * @author Christopher Tubbs
 * @deprecated not currently used and needs to be replaced with something more effective
 */
@Deprecated( since = "6.12" )
public class ProgressMonitor
{
    private ExecutorService asyncUpdater = Executors.newSingleThreadExecutor();

    private static final ProgressMonitor MONITOR = new ProgressMonitor();
    private static final Object MONITOR_LOCK = new Object();
    private static final Logger LOGGER = LoggerFactory.getLogger( ProgressMonitor.class );
    private static boolean updateMonitor = true;
    private final AtomicBoolean reachedCompletion = new AtomicBoolean( false );
    private final DecimalFormat percentFormat;
    private final DecimalFormat mainFormat;
    private final PrintStream printer;
    private Long totalSteps;
    private Long completedSteps;
    private boolean showStepDescription = true;
    private Long lastUpdate;
    private Long updateFrequency;
    private Long startTime;
    private Consumer<ProgressMonitor> outputFunction;

    /**
     * Deactivates the monitor.
     */
    public static void deactivate()
    {
        MONITOR.shutdown();
    }

    /**
     * Increments the monitor.
     */
    public static void increment()
    {
        if ( ProgressMonitor.updateMonitor )
        {
            synchronized ( MONITOR_LOCK )
            {
                MONITOR.addStep();
            }
        }
    }

    /**
     * Sets the update frequency.
     * @param frequency the update frequency
     */
    public static void setUpdateFrequency( Long frequency )
    {
        synchronized ( MONITOR_LOCK )
        {
            MONITOR.updateFrequency = frequency;
        }
    }

    /**
     * Sets the consumer.
     * @param outputFunction the consumer
     */
    public static void setOutput( Consumer<ProgressMonitor> outputFunction )
    {
        synchronized ( MONITOR_LOCK )
        {
            MONITOR.setOutputFunction( outputFunction );
        }
    }

    /**
     * Creates an instance.
     */
    public ProgressMonitor()
    {
        this.startTime = System.currentTimeMillis();
        this.printer = System.out;
        this.outputFunction = monitor -> {
            if ( this.shouldUpdate() )
            {
                asyncUpdater.execute( () -> this.printMessage( getProgressMessage() ) );
            }
        };

        this.totalSteps = 0L;
        this.completedSteps = 0L;

        this.percentFormat = new DecimalFormat();
        this.percentFormat.setMaximumFractionDigits( 2 );
        this.mainFormat = new DecimalFormat( "###,###" );
    }

    /**
     * Resets the monitor.
     */
    public static void resetMonitor()
    {
        synchronized ( MONITOR_LOCK )
        {
            MONITOR.reset();
        }
    }

    /**
     * Sets the step description state.
     * @param showStepDescription is true to show the step description, false to hide
     */
    public static void setShowStepDescription( boolean showStepDescription )
    {
        MONITOR.showStepDescription = showStepDescription;
    }

    /**
     * Sets the update state.
     * @param shouldUpdate is true to update, false to not update
     */
    public static void setShouldUpdate( boolean shouldUpdate )
    {
        ProgressMonitor.updateMonitor = shouldUpdate;

        if ( shouldUpdate )
        {
            MONITOR.asyncUpdater = Executors.newSingleThreadExecutor(
                    runnable -> new Thread( runnable, "Progress Monitor Thread" )
            );
        }
        else
        {
            MONITOR.asyncUpdater = null;
        }
    }

    /**
     * Adds a step.
     */
    private void addStep()
    {
        synchronized ( MONITOR_LOCK )
        {
            if ( this.totalSteps == 0 )
            {
                this.startTime = System.currentTimeMillis();
            }

            this.totalSteps++;
            executeOutput();
        }
    }

    private void executeOutput()
    {
        if ( this.shouldUpdate() )
        {
            this.outputFunction.accept( this );
            this.lastUpdate = System.currentTimeMillis();
        }
    }

    private void reset()
    {
        this.totalSteps = 0L;
        this.completedSteps = 0L;
        this.reachedCompletion.set( false );
        this.startTime = System.currentTimeMillis();
    }

    private float getCompletion()
    {
        float completion = ( ( completedSteps * 1.0F ) / ( totalSteps * 1.0F ) ) * 100.0f;

        if ( Float.isNaN( completion ) )
        {
            completion = 0.0F;
        }
        else if ( completion > 100.0 )
        {
            completion = 100.0F;
        }
        return completion;
    }

    private String getProgressMessage()
    {
        if ( reachedCompletion.get() )
        {
            return "";
        }

        String builder = "\r  ";
        if ( this.showStepDescription )
        {
            builder += mainFormat.format( completedSteps );
            builder += " steps out of ";
            builder += mainFormat.format( totalSteps );
            builder += " completed";
            builder += " at an average speed of ";
            builder += getCompletionSpeed();
        }
        else
        {
            float completion = this.getCompletion();

            builder += String.format( "%.2f", completion );
            builder += "% Completed ";

            for ( int i = 0; i < completion; i += 2 )
            {
                builder += "=";
            }

            builder += ">";

            builder = Strings.formatForLine( builder );

            if ( completion == 100.0 )
            {
                builder += System.lineSeparator() + System.lineSeparator();
                reachedCompletion.set( true );
            }
        }

        return builder;
    }

    private boolean shouldUpdate()
    {
        if ( !ProgressMonitor.updateMonitor ||
             this.asyncUpdater == null ||
             this.asyncUpdater.isTerminated() ||
             this.asyncUpdater.isShutdown() ||
             this.reachedCompletion.get() )
        {
            return false;
        }

        boolean update = true;

        if ( this.updateFrequency != null &&
             lastUpdate != null &&
             ( ( System.currentTimeMillis() - this.lastUpdate ) < ( this.updateFrequency * 1000 ) ) )
        {
            update = false;
        }


        update = update || ( !this.showStepDescription && this.getCompletion() > 99.0 );

        return update;
    }

    private String getCompletionSpeed()
    {
        long elapsed = System.currentTimeMillis() - startTime;
        double seconds = elapsed / 1000.0;
        double speed = 0.0;

        if ( !Double.isNaN( seconds ) && seconds != 0 && this.completedSteps != 0 )
        {
            speed = this.completedSteps / seconds;
        }

        return String.format( "%.2f TPS", speed );
    }

    private void printMessage( String message )
    {
        if ( this.printer != null )
        {
            this.printer.print( message );
        }

        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( message );
        }
    }

    private void shutdown()
    {
        if ( this.asyncUpdater != null )
        {
            this.asyncUpdater.shutdown();
            while ( !this.asyncUpdater.isTerminated() )
            {
            }
        }
    }

    private void setOutputFunction( Consumer<ProgressMonitor> outputFunction )
    {
        this.outputFunction = outputFunction;
    }

}
