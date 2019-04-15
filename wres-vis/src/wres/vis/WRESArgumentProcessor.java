package wres.vis;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.SortedSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ohd.hseb.hefs.utils.arguments.Argument;
import ohd.hseb.hefs.utils.arguments.DefaultArgumentsProcessor;
import ohd.hseb.hefs.utils.plugins.UniqueGenericParameterList;
import ohd.hseb.util.misc.HString;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.StatisticGroup;
import wres.datamodel.Slicer;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.statistics.BoxPlotStatistics;
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.statistics.Statistic;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.util.TimeHelper;
import wres.vis.ChartEngineFactory.ChartType;

/**
 * WRES implementation of {@link DefaultArgumentsProcessor}.  This is intended as a one-stop shop for setting all arguments necessary
 * for wres-vis plot chart generation.  
 * 
 * @author Hank.Herr
 */
public class WRESArgumentProcessor extends DefaultArgumentsProcessor
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ChartEngineFactory.class );
    private static final String EARLIEST_DATE_TO_TEXT = "earliestDateToText";
    private static final String LATEST_DATE_TO_TEXT = "latestDateToText";
    private static final String POOLING_WINDOW = "dataPoolingWindow";

    /**
     * Basis for earliestDateToText function.
     */
    private Instant earliestInstant = null;

    /**
     * Basis for latestDateToText function.
     */
    private Instant latestInstant = null;
    
    /**
     * The duration units.
     */

    private final ChronoUnit durationUnits;

    /**
     * An arguments processor intended for use in displaying single-valued pairs. It is assumed that there is 
     * only a single time window associated with the data, specified in the meta data for the displayed plot input.
     * @param meta The metadata corresponding to a SingleValuedPairs instance.
     * @param durationUnits the time units for durations
     * @throws NullPointerException if either input is null
     */
    public WRESArgumentProcessor( final SampleMetadata meta, final ChronoUnit durationUnits )
    {
        super();

        Objects.requireNonNull( meta, "Specify non-null metadata." );
        
        Objects.requireNonNull( durationUnits, "Specify non-null duration units." );
        
        this.durationUnits = durationUnits;
        
        //Setup fixed arguments.  This uses a special set since it is not metric output.
        addArgument( "rangeAxisLabelPrefix", "Forecast" );
        addArgument( "domainAxisLabelPrefix", "Observed" );
        addArgument( "inputUnitsLabelSuffix", " [" + meta.getMeasurementUnit() + "]" );
        addArgument( "inputUnits", meta.getMeasurementUnit().toString() );

        recordIdentifierArguments( meta );

        recordWindowingArguments( meta.getTimeWindow() );

        initializeFunctionInformation();
    }

    /**
     * An arguments processor intended for use in displaying a box-plot of errors.  It is assumed that there is 
     * only a single time window associated with the data, specified in the meta data for the displayed plot input.
     * 
     * @param displayPlotInput the input data
     * @param durationUnits the time units for durations
     * @throws NullPointerException if either input is null
     */
    public WRESArgumentProcessor( final BoxPlotStatistics displayPlotInput, final ChronoUnit durationUnits )
    {
        super();

        Objects.requireNonNull( displayPlotInput, "Specify a non-null box plot statistic." );
        
        Objects.requireNonNull( durationUnits, "Specify non-null duration units." );
        
        this.durationUnits = durationUnits;
        
        StatisticMetadata meta = displayPlotInput.getMetadata();
        extractStandardArgumentsFromMetadata( meta );

        recordWindowingArguments( meta.getSampleMetadata().getTimeWindow() );

        String durationString = Long.toString( TimeHelper.durationToLongUnits( meta.getSampleMetadata()
                                                                                   .getTimeWindow()
                                                                                   .getLatestLeadDuration(),
                                                                               this.getDurationUnits() ) )
                                + " "
                                + this.getDurationUnits().name().toUpperCase();

        // Plot per pool? See: #62374
        if ( displayPlotInput.getMetadata().getMetricID().isInGroup( StatisticGroup.BOXPLOT_PER_POOL ) )
        {
            addArgument( "diagramInstanceDescription",
                         "for "
                                                       + meta.getSampleMetadata().getThresholds() );
        }
        else
        {
            addArgument( "diagramInstanceDescription",
                         "at Lead Time " + durationString
                                                       + " for "
                                                       + meta.getSampleMetadata().getThresholds() );            
        }

        addArgument( "probabilities",
                     HString.buildStringFromArray( displayPlotInput.getData().get( 0 ).getProbabilities().getDoubles(),
                                                   ", " )
                            .replaceAll( "0.0,", "min," )
                            .replaceAll( "1.0", "max" ) );

        initializeFunctionInformation();
    }

    /**
     * An arguments processor intended for use in displaying metric output FOR POOLING WINDOWS, whether scalar or vector.
     * 
     * @param <T> the output type
     * @param displayedPlotInput the plot input
     * @param plotType the plot type; null is allowed, which will trigger recording arguments as if this were anything but
     *     a pooling window plot.
     * @param durationUnits the time units for durations
     * @throws NullPointerException if the displayedPlotInput or the durationUnits are null
     */
    public <T extends Statistic<?>> WRESArgumentProcessor( final ListOfStatistics<T> displayedPlotInput,
                                                           final ChartType plotType,
                                                           final ChronoUnit durationUnits )
    {
        super();
        
        Objects.requireNonNull( displayedPlotInput, "Specify a non-null list of statistics." );
        
        Objects.requireNonNull( durationUnits, "Specify non-null duration units." );

        this.durationUnits = durationUnits;

        StatisticMetadata meta = displayedPlotInput.getData().get( 0 ).getMetadata();
        extractStandardArgumentsFromMetadata( meta );

        // Assemble a collection of smaller time windows where necessary
        if ( plotType == ChartType.POOLING_WINDOW || meta.getMetricID().isInGroup( StatisticGroup.PAIRED ) )
        {
            SortedSet<TimeWindow> timeWindows =
                    Slicer.discover( displayedPlotInput,
                                     next -> next.getMetadata().getSampleMetadata().getTimeWindow() );
            TimeWindow timeWindow = TimeWindow.of( timeWindows.first().getEarliestReferenceTime(),
                                                   timeWindows.last().getLatestReferenceTime(),
                                                   timeWindows.first().getEarliestValidTime(),
                                                   timeWindows.last().getLatestValidTime(),
                                                   timeWindows.first().getEarliestLeadDuration(),
                                                   timeWindows.last().getLatestLeadDuration() );
            recordWindowingArguments( timeWindow );
        }
        else
        {
            recordWindowingArguments( meta.getSampleMetadata().getTimeWindow() );
        }

        initializeFunctionInformation();
    }
    
    /**
     * Returns the duration units.
     * 
     * @return the duration units
     */

    private ChronoUnit getDurationUnits()
    {
        return this.durationUnits;
    }
    
    /**
     * Extracts the standard arguments that can be pulled from and interpreted consistently for any output meta data. 
     * @param meta the output metadata 
     */
    private void extractStandardArgumentsFromMetadata( StatisticMetadata meta )
    {
        //Setup fixed arguments.
        addArgument( "metricName", meta.getMetricID().toString() );
        addArgument( "metricShortName", meta.getMetricID().toString() );
        addArgument( "outputUnits", meta.getMeasurementUnit().toString() );
        addArgument( "inputUnits", meta.getSampleMetadata().getMeasurementUnit().toString() );
        addArgument( "outputUnitsLabelSuffix", " [" + meta.getMeasurementUnit() + "]" );
        addArgument( "inputUnitsLabelSuffix", " [" + meta.getSampleMetadata().getMeasurementUnit() + "]" );

        recordIdentifierArguments( meta.getSampleMetadata() );

        // Add conditional arguments
        
        //I could create a helper method to handle this wrapping, but I don't think this will be used outside of this context,
        //so why bother?  (This relates to an email James wrote.)
        if ( ! meta.hasMetricComponentID() || meta.getMetricComponentID().equals( MetricConstants.MAIN ) )
        {
            addArgument( "metricComponentNameSuffix", "" );
        }
        else
        {
            addArgument( "metricComponentNameSuffix", meta.getMetricComponentID().toString() );
        }
        
        // Time scale arguments, where defined
        this.addTimeScaleArguments( meta );       
    }

    /**
     * Record the identifier arguments based on the metadata.  This will do nothing if the metadata provides no identifier.
     * @param meta
     */
    private void recordIdentifierArguments( final SampleMetadata meta )
    {
        if ( meta.hasIdentifier() )
        {
            final DatasetIdentifier identifier = meta.getIdentifier();
            addArgument( "locationName", identifier.getGeospatialID().toString() );
            addArgument( "variableName", identifier.getVariableID() );
            if ( identifier.getScenarioID() == null )
            {
                addArgument( "primaryScenario", "" );
            }
            else
            {
                addArgument( "primaryScenario", identifier.getScenarioID() );
            }
        }
    }

    private void recordWindowingArguments( final TimeWindow timeWindow )
    {
        // Check for unbounded times and do not display this unconstrained condition: #46772
        if ( Objects.nonNull( timeWindow ) && ! timeWindow.hasUnboundedReferenceTimes() )
        {
            earliestInstant = timeWindow.getEarliestReferenceTime();
            latestInstant = timeWindow.getLatestReferenceTime();
            addArgument( "earliestLeadTime",
                         Long.toString( TimeHelper.durationToLongUnits( timeWindow.getEarliestLeadDuration(),
                                                                        this.getDurationUnits() ) ) );
            addArgument( "latestLeadTime",
                         Long.toString( TimeHelper.durationToLongUnits( timeWindow.getLatestLeadDuration(),
                                                         this.getDurationUnits() ) ) );
            
            // Jbr: Qualify the reference time with "evaluation period" in code rather than the graphics template
            // This allows for the time window to be ommited when unconstrained. See #46772.
            String referenceTime = "ISSUE TIME Evaluation Period: ";
            
            addArgument( "referenceTime", referenceTime ); //#44873, #46772
        }
        else
        {
            addArgument( "referenceTime", "" );
        }
    }

    /**
     * Adds arguments for plots where the lead time is on the domain axis and threshold is in the legend.
     * @param <T> the output type
     * @param displayedPlotInput the plot input
     * @param plotTimeWindow the time window
     */
    public <T extends Statistic<?>> void addLeadThresholdArguments( ListOfStatistics<T> displayedPlotInput,
                                           TimeWindow plotTimeWindow )
    {
        final StatisticMetadata meta = displayedPlotInput.getData().get( 0 ).getMetadata();

        final String legendTitle = "Threshold";
        String legendUnitsText = "";
        // Display real values for all real-valued thresholds, not just quantiles: #56955
        SortedSet<Boolean> thresholds =
                Slicer.discover( displayedPlotInput,
                                 next -> next.getMetadata().getSampleMetadata().getThresholds().first().hasValues() );

        if ( thresholds.contains( true ) )
        {
            legendUnitsText += " [" + meta.getSampleMetadata().getMeasurementUnit() + "]";
        }

        addArgument( "legendTitle", legendTitle );
        addArgument( "legendUnitsText", legendUnitsText );
        if ( plotTimeWindow != null )
        {
            String durationString =
                    TimeHelper.durationToLongUnits( plotTimeWindow.getLatestLeadDuration(), this.getDurationUnits() ) + " "
                              + this.getDurationUnits().name().toUpperCase();
            addArgument( "diagramInstanceDescription", "at Lead Time " + durationString );
            addArgument( "plotTitleVariable", "Thresholds" );
        }
    }


    /**
     * Adds arguments for plots where the threshold value is on the domain axis and lead time is in the legend.
     * @param <T> the output type
     * @param displayedPlotInput the plot input
     * @param threshold the threshold
     */
    public <T extends Statistic<?>> void addThresholdLeadArguments( ListOfStatistics<T> displayedPlotInput,
                                           OneOrTwoThresholds threshold )
    {

        // Augment the plot title when the input dataset contains a secondary threshold/classifier
        // Create a string from the set of secondary thresholds
        String supplementary = "";
        SortedSet<Threshold> secondThresholds =
                Slicer.discover( displayedPlotInput,
                                 next -> next.getMetadata().getSampleMetadata().getThresholds().second() );
        if ( !secondThresholds.isEmpty() )
        {
            String set = secondThresholds.toString();
            supplementary = " with occurrences defined as " + set;
        }
        
        addArgument( "plotTitleSupplementary", supplementary );
        addArgument( "legendTitle",  "Lead Time" );
        addArgument( "legendUnitsText", " [" + this.getDurationUnits().name().toUpperCase() + "]" );
        
        if ( threshold != null )
        {
            addArgument( "diagramInstanceDescription",
                         "for Threshold " + threshold.toString() );
            addArgument( "plotTitleVariable", "Lead Times" );
        }
    }

    /**
     * Adds arguments for plots where the pooling window (as in rolling window) is displayed along the domain axis 
     * and the legend includes both lead time and threshold.
     * @param <T> the output type
     * @param displayedPlotInput the plot input
     */
    public <T extends Statistic<?>> void addPoolingWindowArguments(ListOfStatistics<T> displayedPlotInput )
    {
        final StatisticMetadata meta = displayedPlotInput.getData().get( 0 ).getMetadata();
        
        String durationUnitsString = "[" + this.getDurationUnits().name().toUpperCase() + "]";
        
        if ( !meta.getSampleMetadata()
                  .getTimeWindow()
                  .getEarliestLeadDuration()
                  .equals( meta.getSampleMetadata().getTimeWindow().getLatestLeadDuration() ) )
        {
            addArgument( "legendTitle", "Lead time window "+durationUnitsString+", Threshold " );
        }
        else
        {

            addArgument( "legendTitle", "Lead time "+durationUnitsString+", Threshold " );
        }
        addArgument( "legendUnitsText", "[" + meta.getSampleMetadata().getMeasurementUnit() + "]" );
    }

    /**
     * Adds the arguments for metrics expressed with durations.
     */
    
    public void addDurationMetricArguments()
    {
        addArgument( "outputUnitsLabelSuffix", " [HOURS]" );
    }
    
    /**
     * Custom method created for the time-to-peak plots.
     * @param <T> the output type
     * @param displayedPlotInput the plot input
     */
    public <T extends Statistic<?>> void addTimeToPeakArguments(ListOfStatistics<T> displayedPlotInput )
    {
        final StatisticMetadata meta = displayedPlotInput.getData().get( 0 ).getMetadata();
        addArgument( "legendTitle", "Threshold " );
        addArgument( "legendUnitsText", "[" + meta.getSampleMetadata().getMeasurementUnit() + "]" );
    }
    
    /**
     * Adds arguments related to the baseline forecasts for skill scores.
     * @param meta the output metadata
     */
    public void addBaselineArguments( StatisticMetadata meta )
    {
        final DatasetIdentifier identifier = meta.getSampleMetadata().getIdentifier();
        String baselineSuffix = "";
        if ( !Objects.isNull( identifier.getScenarioIDForBaseline() ) )
        {
            baselineSuffix = " Against Predictions From " + identifier.getScenarioIDForBaseline();
        }
        addArgument( "baselineLabelSuffix", baselineSuffix );
    }

    /**
     * Adds the arguments relating to the {@timeScale} of the pairs from which the verification metrics were
     * computed.
     * 
     * @param meta the statistics metadata
     */
    
    private void addTimeScaleArguments( StatisticMetadata meta )
    {
        Objects.requireNonNull( "Specify non-null metadata from which to obtain the time scale." );

        String timeScale = "";
        if ( meta.getSampleMetadata().hasTimeScale() )
        {
            String period =
                    Long.toString( TimeHelper.durationToLongUnits( meta.getSampleMetadata().getTimeScale().getPeriod(),
                                                                   this.getDurationUnits() ) )
                            + " "
                            + this.getDurationUnits().name().toUpperCase();

            timeScale =
                    " with time scale [" + period + ", " + meta.getSampleMetadata().getTimeScale().getFunction() + "] ";
        }

        addArgument( "timeScale", timeScale );
    }

    private void initializeFunctionInformation()
    {
        this.addFunctionName( EARLIEST_DATE_TO_TEXT );
        this.addFunctionName( LATEST_DATE_TO_TEXT );
    }

    /**
     * Called to process a date-to-text argument function.
     * @param argument the argument
     * @param dateInstant the date to format
     * @return Date function processed value
     */
    private String processDateFunction( final Argument argument, Instant dateInstant )
    {
        if ( dateInstant == null )
        {
            LOGGER.warn( "Date for argument function " + argument.getArgumentName()
                         + " is not provided with plotting meta data." );
            return null;
        }
        if ( argument.getFunctionParameterValues().size() == 2 )
        {
            final String dateFormat = argument.getFunctionParameterValues().get( 0 );
            final String timeZoneStr = argument.getFunctionParameterValues().get( 1 );

            DateTimeFormatter formatter;

            try
            {
                 formatter =
                         DateTimeFormatter.ofPattern( dateFormat )
                                          .withZone( ZoneId.of( timeZoneStr ) );
            }
            catch ( IllegalArgumentException e )
            {
                LOGGER.warn( "Date format '{}' is invalid for argument {}.",
                             dateFormat, argument, e );
                return null;
            }

            return formatter.format( dateInstant );
        }
        else
        {
            LOGGER.warn( "Incorrect number of parameters specified for argument {}; requires 2 arguments, "
                         + "the date format and time zone identifier.",
                         argument );
            return null;
        }
    }

    private String processPoolingWindowFunction( final Argument argument )
    {
        if ( ( ( earliestInstant == null ) || ( !earliestInstant.isAfter( Instant.MIN ) ) )
             && ( ( latestInstant == null ) || ( !latestInstant.isBefore( Instant.MAX ) ) ) )
        {
            return ""; //Jbr. See #46772. Return an empty string, as this will now be unqualified
        }
        else if ( ( earliestInstant == null ) || ( !earliestInstant.isAfter( Instant.MIN ) ) )
        {
            return "Before " + this.processDateFunction( argument, latestInstant );
        }
        else if ( ( latestInstant == null ) || ( !latestInstant.isBefore( Instant.MAX ) ) )
        {
            return "After " + this.processDateFunction( argument, earliestInstant );
        }
        else
        {
            return this.processDateFunction( argument, earliestInstant ) + " - "
                   + this.processDateFunction( argument, latestInstant );
        }
    }

    @Override
    protected String evaluateFunctionValue( final Argument argument )
    {
        if ( argument.getArgumentName().equals( EARLIEST_DATE_TO_TEXT ) )
        {
            return this.processDateFunction( argument, earliestInstant );
        }
        else if ( argument.getArgumentName().equals( LATEST_DATE_TO_TEXT ) )
        {
            return this.processDateFunction( argument, latestInstant );
        }
        else if ( argument.getArgumentName().equals( POOLING_WINDOW ) )
        {
            return this.processPoolingWindowFunction( argument );
        }
        return null;
    }

    @Override
    protected String[] getFunctionParameterNames( final String name )
    {
        if ( name.equals( EARLIEST_DATE_TO_TEXT ) || name.equals( LATEST_DATE_TO_TEXT ) )
        {
            return new String[] { "date format", "time zone" };
        }
        return null;
    }


    /**
     * Convenience wrapper on {@link UniqueGenericParameterList#addParameter(String, String)} for the return of
     * {@link #getArguments()}.
     * 
     * @param key the argument key
     * @param value the argument value
     */
    public void addArgument( final String key, final String value )
    {
        getArguments().addParameter( key, value );
    }
}
