package wres.vis;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ohd.hseb.hefs.utils.arguments.Argument;
import ohd.hseb.hefs.utils.arguments.DefaultArgumentsProcessor;
import ohd.hseb.hefs.utils.plugins.UniqueGenericParameterList;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.Slicer;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.MetricGroup;
import wres.datamodel.metrics.MetricConstants.SampleDataGroup;
import wres.datamodel.metrics.MetricConstants.StatisticType;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.Statistic;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Outputs.GraphicFormat.GraphicShape;
import wres.vis.ChartEngineFactory.ChartType;

/**
 * WRES implementation of {@link DefaultArgumentsProcessor}.  This is intended as a one-stop shop for setting all arguments necessary
 * for wres-vis plot chart generation.  
 * 
 * @author Hank.Herr
 */
class ArgumentProcessor extends DefaultArgumentsProcessor
{
    private static final String THRESHOLD = "Threshold ";
    private static final String VARIABLE_NAME = "variableName";
    private static final String LEGEND_TITLE = "legendTitle";
    private static final String LEGEND_UNITS_TEXT = "legendUnitsText";
    private static final String DIAGRAM_INSTANCE_DESCRIPTION = "diagramInstanceDescription";
    private static final String SPECIFY_NON_NULL_DURATION_UNITS = "Specify non-null duration units.";
    private static final Logger LOGGER = LoggerFactory.getLogger( ArgumentProcessor.class );
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
     * @param metricUnits the measurement units of the metric
     * @param durationUnits the time units for durations
     * @throws NullPointerException if either input is null
     */
    ArgumentProcessor( final PoolMetadata meta, String metricUnits, final ChronoUnit durationUnits )
    {
        super();

        Objects.requireNonNull( meta, "Specify non-null metadata." );

        Objects.requireNonNull( durationUnits, SPECIFY_NON_NULL_DURATION_UNITS );

        this.durationUnits = durationUnits;

        //Setup fixed arguments.  This uses a special set since it is not metric output.
        addArgument( "rangeAxisLabelPrefix", "Forecast" );
        addArgument( "domainAxisLabelPrefix", "Observed" );
        addArgument( "inputUnitsLabelSuffix", " [" + metricUnits + "]" );
        addArgument( "inputUnits", meta.getMeasurementUnit().toString() );

        recordIdentifierArguments( meta, null, null );

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
    ArgumentProcessor( final BoxplotStatisticOuter displayPlotInput, final ChronoUnit durationUnits )
    {
        super();

        Objects.requireNonNull( displayPlotInput, "Specify a non-null box plot statistic." );

        Objects.requireNonNull( durationUnits, SPECIFY_NON_NULL_DURATION_UNITS );

        this.durationUnits = durationUnits;

        String metricUnits = displayPlotInput.getMetadata().getEvaluation().getMeasurementUnit();

        if ( !displayPlotInput.getData().getMetric().getUnits().isBlank() )
        {
            metricUnits = displayPlotInput.getData().getMetric().getUnits();
        }

        PoolMetadata meta = displayPlotInput.getMetadata();
        MetricConstants metricName = displayPlotInput.getMetricName();
        extractStandardArgumentsFromMetadata( meta, metricUnits, metricName, null );

        recordWindowingArguments( meta.getTimeWindow() );

        String durationString = Long.toString( GraphicsUtils.durationToLongUnits( meta.getTimeWindow()
                                                                                  .getLatestLeadDuration(),
                                                                              this.getDurationUnits() ) )
                                + " "
                                + this.getDurationUnits().name().toUpperCase();

        // Plot per pool? See: #62374
        if ( metricName.isInGroup( StatisticType.BOXPLOT_PER_POOL ) )
        {
            addArgument( DIAGRAM_INSTANCE_DESCRIPTION,
                         "and for Threshold " + meta.getThresholds() );
        }
        else
        {
            addArgument( DIAGRAM_INSTANCE_DESCRIPTION,
                         "and at Lead Time "
                                                       + durationString
                                                       + " and for Threshold "
                                                       + meta.getThresholds() );
        }


        // See #65503
        String probabilities = "none defined";
        if ( displayPlotInput.getData().getStatisticsCount() != 0 )
        {
            List<Double> probs = displayPlotInput.getData().getMetric().getQuantilesList();
            probabilities = Arrays.toString( probs.toArray() );

            // Pretty print
            probabilities = probabilities.replaceAll( "0.0,", "min," );
            probabilities = probabilities.replaceAll( "1.0", "max" );
            probabilities = probabilities.replace( "[", "" );
            probabilities = probabilities.replace( "]", "" );
        }

        this.addArgument( "probabilities", probabilities );

        initializeFunctionInformation();
    }

    /**
     * An arguments processor intended for use in displaying metric output FOR POOLING WINDOWS, whether scalar or vector.
     * 
     * @param <T> the output type
     * @param metricName the metric name
     * @param metricComponentName the optional metric component name
     * @param metricUnits the metric units
     * @param displayedPlotInput the plot input
     * @param plotType the plot type; null is allowed, which will trigger recording arguments as if this were anything but
     *     a pooling window plot.
     * @param durationUnits the time units for durations
     * @throws NullPointerException if the displayedPlotInput or the durationUnits are null
     */
    <T extends Statistic<?>> ArgumentProcessor( final MetricConstants metricName,
                                                    final MetricConstants metricComponentName,
                                                    final String metricUnits,
                                                    final List<T> displayedPlotInput,
                                                    final ChartType plotType,
                                                    final ChronoUnit durationUnits )
    {
        super();

        Objects.requireNonNull( displayedPlotInput, "Specify a non-null list of statistics." );

        Objects.requireNonNull( durationUnits, SPECIFY_NON_NULL_DURATION_UNITS );

        this.durationUnits = durationUnits;

        PoolMetadata meta = displayedPlotInput.get( 0 ).getMetadata();
        extractStandardArgumentsFromMetadata( meta, metricUnits, metricName, metricComponentName );

        // Assemble a collection of smaller time windows where necessary
        if ( plotType == ChartType.POOLING_WINDOW || metricName.isInGroup( StatisticType.DURATION_DIAGRAM ) )
        {
            SortedSet<TimeWindowOuter> timeWindows =
                    Slicer.discover( displayedPlotInput,
                                     next -> next.getMetadata().getTimeWindow() );
            TimeWindowOuter timeWindow = TimeWindowOuter.of( timeWindows.first().getEarliestReferenceTime(),
                                                             timeWindows.last().getLatestReferenceTime(),
                                                             timeWindows.first().getEarliestValidTime(),
                                                             timeWindows.last().getLatestValidTime(),
                                                             timeWindows.first().getEarliestLeadDuration(),
                                                             timeWindows.last().getLatestLeadDuration() );
            recordWindowingArguments( timeWindow );
        }
        else
        {
            recordWindowingArguments( meta.getTimeWindow() );
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
     * @param metricUnits the metric units
     * @param metricName the metric name
     * @param metricComponentName the optional metric component name
     */
    private void extractStandardArgumentsFromMetadata( PoolMetadata meta,
                                                       String metricUnits,
                                                       MetricConstants metricName,
                                                       MetricConstants metricComponentName )
    {
        //Setup fixed arguments.
        addArgument( "metricName", metricName.toString() );
        addArgument( "metricShortName", metricName.toString() );
        addArgument( "outputUnits", metricUnits );
        addArgument( "inputUnits", meta.getMeasurementUnit().toString() );
        addArgument( "outputUnitsLabelSuffix", " [" + metricUnits + "]" );
        addArgument( "inputUnitsLabelSuffix", " [" + meta.getMeasurementUnit() + "]" );

        recordIdentifierArguments( meta, metricName, metricComponentName );

        // Add conditional arguments

        //I could create a helper method to handle this wrapping, but I don't think this will be used outside of this context,
        //so why bother?  (This relates to an email James wrote.)
        if ( Objects.isNull( metricComponentName ) || metricComponentName == MetricConstants.MAIN )
        {
            addArgument( "metricComponentNameSuffix", "" );
        }
        else
        {
            addArgument( "metricComponentNameSuffix", " " + metricComponentName.toString() );
        }

        // Time scale arguments, where defined
        this.addTimeScaleArguments( meta );
    }

    /**
     * Record the identifier arguments based on the metadata.  This will do nothing if the metadata provides no identifier.
     * @param meta the metadata
     * @param metric the metric context
     * @param component the optional metric component name
     */
    private void recordIdentifierArguments( PoolMetadata meta, MetricConstants metric, MetricConstants component )
    {

        String regionName = meta.getPool().getRegionName();

        if ( Objects.isNull( regionName ) )
        {
            throw new IllegalArgumentException( "Failed to create parameters for graphics generation: the region "
                                                + "name was missing from the pool metadata, which is not allowed." );
        }

        addArgument( "locationName", regionName );

        // Addition to the primary scenario based on metric context. See #81790
        String primaryScenario = this.getScenarioName( meta, metric, component );

        addArgument( "primaryScenario", primaryScenario );
    }
    
    /**
     * Uncovers the scenario name for the plot.
     * 
     * @param metadata the sample metadata
     * @param metric the metric name
     * @param component the metric component name
     * @return the scenario name
     */

    private String getScenarioName( PoolMetadata metadata, MetricConstants metric, MetricConstants component )
    {
        String scenarioName = "";

        Evaluation evaluation = metadata.getEvaluation();

        if ( Objects.nonNull( metric ) )
        {
            // Not univariate statistics, except the sample size
            if ( !metric.isInGroup( MetricGroup.UNIVARIATE_STATISTIC ) || metric == MetricConstants.SAMPLE_SIZE )
            {
                // Use the left name for paired statistics. Should probably use the triple of variable names for 
                // accuracy. See #81790.
                addArgument( VARIABLE_NAME, evaluation.getLeftVariableName() );

                String name = "";
                if ( metadata.getPool().getIsBaselinePool() )
                {
                    name = " " + evaluation.getBaselineDataName();
                }
                else if ( !evaluation.getRightDataName().isBlank() )
                {
                    name = " " + evaluation.getRightDataName();
                }

                scenarioName = name + " predictions of ";
            }
            else if ( Objects.nonNull( component ) )
            {
                // Get the name that corresponds to the side of the component. Again, should probably use the triple.
                switch ( component )
                {
                    case LEFT:
                        addArgument( VARIABLE_NAME, evaluation.getLeftVariableName() );
                        break;
                    case RIGHT:
                        addArgument( VARIABLE_NAME, evaluation.getRightVariableName() );
                        break;
                    case BASELINE:
                        addArgument( VARIABLE_NAME, evaluation.getBaselineVariableName() );
                        break;
                    default:
                        break;
                }
                // Add a space to the name
                scenarioName = " ";
            }
        }

        return scenarioName;
    }

    private void recordWindowingArguments( final TimeWindowOuter timeWindow )
    {
        // Check for unbounded times and do not display this unconstrained condition: #46772
        if ( Objects.nonNull( timeWindow ) && !timeWindow.hasUnboundedReferenceTimes() )
        {
            earliestInstant = timeWindow.getEarliestReferenceTime();
            latestInstant = timeWindow.getLatestReferenceTime();
            addArgument( "earliestLeadTime",
                         Long.toString( GraphicsUtils.durationToLongUnits( timeWindow.getEarliestLeadDuration(),
                                                                       this.getDurationUnits() ) ) );
            addArgument( "latestLeadTime",
                         Long.toString( GraphicsUtils.durationToLongUnits( timeWindow.getLatestLeadDuration(),
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
     * @param hasRepeatedComponents is true for diagrams that have repeated components, false otherwise
     */
    public <T extends Statistic<?>> void addLeadThresholdArguments( List<T> displayedPlotInput,
                                                                    TimeWindowOuter plotTimeWindow,
                                                                    boolean hasRepeatedComponents )
    {
        PoolMetadata meta = displayedPlotInput.get( 0 ).getMetadata();

        // One dataset for every qualifier, so one threshold only
        if ( !hasRepeatedComponents )
        {
            String legendTitle = "Threshold";
            String legendUnitsText = "";
            // Display real values for all real-valued thresholds, not just quantiles: #56955
            SortedSet<Boolean> thresholds =
                    Slicer.discover( displayedPlotInput,
                                     next -> next.getMetadata().getThresholds().first().hasValues() );

            if ( thresholds.contains( true ) )
            {
                legendUnitsText += " [" + meta.getMeasurementUnit() + "]";
            }

            addArgument( LEGEND_TITLE, legendTitle );
            addArgument( LEGEND_UNITS_TEXT, legendUnitsText );
        }
        else
        {
            addArgument( LEGEND_TITLE, "Name" );
            addArgument( LEGEND_UNITS_TEXT, "" );
        }
    
        if ( plotTimeWindow != null )
        {
            String durationString =
                    GraphicsUtils.durationToLongUnits( plotTimeWindow.getLatestLeadDuration(), this.getDurationUnits() )
                                    + " "
                                    + this.getDurationUnits().name().toUpperCase();
            if ( hasRepeatedComponents )
            {
                addArgument( DIAGRAM_INSTANCE_DESCRIPTION,
                             "at Lead Time " + durationString + " and for Threshold All Data" );
            }
            else
            {
                addArgument( DIAGRAM_INSTANCE_DESCRIPTION, "at Lead Time " + durationString );
            }
            
            addArgument( "plotTitleVariable", "Thresholds" );
        }
    }


    /**
     * Adds arguments for plots where the threshold value is on the domain axis and lead time is in the legend.
     * @param <T> the output type
     * @param displayedPlotInput the plot input
     * @param threshold the threshold
     * @param hasRepeatedComponents is true for diagrams that have repeated components, false otherwise
     */
    public <T extends Statistic<?>> void addThresholdLeadArguments( List<T> displayedPlotInput,
                                                                    OneOrTwoThresholds threshold,
                                                                    boolean hasRepeatedComponents )
    {

        // Augment the plot title when the input dataset contains a secondary threshold/classifier
        // Create a string from the set of secondary thresholds
        String supplementary = "";
        SortedSet<ThresholdOuter> secondThresholds =
                Slicer.discover( displayedPlotInput,
                                 next -> next.getMetadata().getThresholds().second() );
        if ( !secondThresholds.isEmpty() )
        {
            String set = secondThresholds.toString();
            supplementary = " with occurrences defined as " + set;
        }

        String supplementaryLegend = "";
        if ( hasRepeatedComponents )
        {
            supplementaryLegend = ", Name";
        }
        
        addArgument( "plotTitleSupplementary", supplementary );
        addArgument( LEGEND_TITLE, "Lead Time" );
        addArgument( LEGEND_UNITS_TEXT, " [" + this.getDurationUnits().name().toUpperCase() + "]" + supplementaryLegend );

        if ( threshold != null )
        {
            addArgument( DIAGRAM_INSTANCE_DESCRIPTION,
                         "and for Threshold " + threshold.toString() );
            addArgument( "plotTitleVariable", "Lead Times" );
        }
    }

    /**
     * Adds arguments for plots where the pooling window (as in rolling window) is displayed along the domain axis 
     * and the legend includes both lead time and threshold.
     * @param <T> the output type
     * @param displayedPlotInput the plot input
     * @param graphicShape the graphic shape
     */
    public <T extends Statistic<?>> void addPoolingWindowArguments( List<T> displayedPlotInput,
                                                                    GraphicShape graphicShape )
    {
        PoolMetadata meta = displayedPlotInput.get( 0 )
                                              .getMetadata();

        String durationUnitsString = "[" + this.getDurationUnits().name().toUpperCase() + "]";

        String legendTitle = "";

        Duration earliest = meta.getTimeWindow().getEarliestLeadDuration();
        Duration latest = meta.getTimeWindow().getLatestLeadDuration();
        Instant earliestValidTime = meta.getTimeWindow().getEarliestValidTime();
        Instant latestValidTime = meta.getTimeWindow().getLatestValidTime();

        // If the lead durations are unbounded, do not qualify them, else qualify them
        if ( !earliest.equals( TimeWindowOuter.DURATION_MIN ) || !latest.equals( TimeWindowOuter.DURATION_MAX ) )
        {
            legendTitle = legendTitle + "Lead time window " + durationUnitsString + ", ";
        }

        // If the valid times are unbounded, do not qualify them, else qualify them
        if ( graphicShape != GraphicShape.VALID_DATE_POOLS
             && ( !earliestValidTime.equals( Instant.MIN ) || !latestValidTime.equals( Instant.MAX ) ) )
        {
            legendTitle = legendTitle + "Valid time window (UTC), ";
        }
        
        legendTitle =  legendTitle + THRESHOLD;
        
        addArgument( LEGEND_TITLE, legendTitle );
        addArgument( LEGEND_UNITS_TEXT, "[" + meta.getMeasurementUnit() + "]" );
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
    public <T extends Statistic<?>> void addTimeToPeakArguments( List<T> displayedPlotInput )
    {
        PoolMetadata meta = displayedPlotInput.get( 0 ).getMetadata();
        addArgument( LEGEND_TITLE, THRESHOLD );
        addArgument( LEGEND_UNITS_TEXT, "[" + meta.getMeasurementUnit() + "]" );
    }

    /**
     * Adds arguments related to the baseline forecasts for skill scores.
     * @param meta the output metadata
     * @param metric the metric
     */
    public void addBaselineArguments( PoolMetadata meta, MetricConstants metric )
    {
        Objects.requireNonNull( meta );

        // TODO: need a less brittle way to identify skill measures that have used a default baseline vs. an explicit 
        // one because a pool that includes an explicit baseline may have been used or not used for specific measures.
        if ( metric.isSkillMetric()
             && !metric.isInGroup( SampleDataGroup.DICHOTOMOUS )
             && metric != MetricConstants.KLING_GUPTA_EFFICIENCY )
        {

            String baselineSuffix = meta.getEvaluation()
                                        .getBaselineDataName();

            // Skill scores for baseline use a default reference, which is climatology
            // This is also potentially brittle, so consider a better way, such as adding the default baseline
            // name into the evaluation description
            if ( meta.getPool().getIsBaselinePool() )
            {
                baselineSuffix = meta.getEvaluation()
                                     .getDefaultBaseline()
                                     .name()
                                     .replace( "_", " " );
            }

            baselineSuffix = " Against Predictions From " + baselineSuffix;

            addArgument( "baselineLabelSuffix", baselineSuffix );
        }
        else
        {
            addArgument( "baselineLabelSuffix", "" );
        }
    }

    /**
     * Adds the arguments relating to the {@timeScale} of the pairs from which the verification metrics were
     * computed.
     * 
     * @param meta the statistics metadata
     */

    private void addTimeScaleArguments( PoolMetadata meta )
    {
        Objects.requireNonNull( "Specify non-null metadata from which to obtain the time scale." );

        String timeScale = "";
        if ( meta.hasTimeScale() )
        {
            // Use the default string representation of an instantaneous time scale
            // See #62867
            if ( meta.getTimeScale().isInstantaneous() )
            {
                timeScale = meta.getTimeScale().toString() + " ";
            }
            else
            {
                String period =
                        Long.toString( GraphicsUtils.durationToLongUnits( meta.getTimeScale()
                                                                          .getPeriod(),
                                                                      this.getDurationUnits() ) )
                                + " "
                                + this.getDurationUnits().name().toUpperCase();

                timeScale = "[" + period + ", " + meta.getTimeScale().getFunction() + "] ";
            }
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
            LOGGER.warn( "Date for argument function {} is not provided with plotting meta data.",
                         argument.getArgumentName() );
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
                             dateFormat,
                             argument,
                             e );
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
        return new String[] {};
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
