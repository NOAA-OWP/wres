package wres.vis;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ohd.hseb.hefs.utils.arguments.Argument;
import ohd.hseb.hefs.utils.arguments.DefaultArgumentsProcessor;
import ohd.hseb.hefs.utils.datetime.HEFSTimeZoneTools;
import ohd.hseb.hefs.utils.plugins.UniqueGenericParameterList;
import ohd.hseb.util.misc.HString;
import wres.config.generated.PlotTypeSelection;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.Threshold;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;

/**
 * WRES implementation of {@link DefaultArgumentsProcessor}.  This is intended as a one-stop shop for setting all arguments necessary
 * for wres-vis plot chart generation.  
 * 
 * @author Hank.Herr
 */
public class WRESArgumentProcessor extends DefaultArgumentsProcessor
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ChartEngineFactory.class );
    private final static String EARLIEST_DATE_TO_TEXT = "earliestDateToText";
    private final static String LATEST_DATE_TO_TEXT = "latestDateToText";
    private final static String POOLING_WINDOW = "dataPoolingWindow";

    /**
     * Basis for earliestDateToText function.
     */
    private Instant earliestInstant = null;

    /**
     * Basis for latestDateToText function.
     */
    private Instant latestInstant = null;

    /**
     * An arguments processor intended for use in displaying single-valued pairs. It is assumed that there is 
     * only a single time window associated with the data, specified in the meta data for the displayed plot input.
     * @param meta The metadata corresponding to a SingleValuedPairs instance.
     */
    public WRESArgumentProcessor( final Metadata meta )
    {
        super();

        //Setup fixed arguments.  This uses a special set since it is not metric output.
        addArgument( "rangeAxisLabelPrefix", "Forecast" );
        addArgument( "domainAxisLabelPrefix", "Observed" );
        addArgument( "inputUnitsLabelSuffix", " [" + meta.getDimension() + "]" );
        addArgument( "inputUnits", meta.getDimension().toString() );

        if ( meta.hasIdentifier() )
        {
            final DatasetIdentifier identifier = meta.getIdentifier();
            addArgument( "locationName", identifier.getGeospatialID() );
            addArgument( "variableName", identifier.getVariableID() );
            addArgument( "primaryScenario", identifier.getScenarioID() );
        }

        if ( meta.hasTimeWindow() )
        {
            earliestInstant = meta.getTimeWindow().getEarliestTime();
            latestInstant = meta.getTimeWindow().getLatestTime();
            addArgument( "earliestLeadTimeHours", "" + meta.getTimeWindow().getEarliestLeadTimeInHours() );
            addArgument( "latestLeadTimeHours", "" + meta.getTimeWindow().getLatestLeadTimeInHours() );
            addArgument( "referenceTime", meta.getTimeWindow().getReferenceTime().toString() ); //#44873
        }
        else
        {
            addArgument( "referenceTime", "" );
        }
    }

    /**
     * An arguments processor intended for use in displaying a box-plot of errors.  It is assumed that there is 
     * only a single time window associated with the data, specified in the meta data for the displayed plot input.
     * 
     * @param inputKeyInstance the input key
     * @param displayPlotInput the input data
     */
    public WRESArgumentProcessor( Pair<TimeWindow, Threshold> inputKeyInstance, BoxPlotOutput displayPlotInput )
    {
        super();
        MetricOutputMetadata meta = displayPlotInput.getMetadata();
        extractStandardArgumentsFromMetadata( meta );
        if ( meta.hasTimeWindow() )
        {
            earliestInstant = meta.getTimeWindow().getEarliestTime();
            latestInstant = meta.getTimeWindow().getLatestTime();
            addArgument( "earliestLeadTimeHours", "" + meta.getTimeWindow().getEarliestLeadTimeInHours() );
            addArgument( "latestLeadTimeHours", "" + meta.getTimeWindow().getLatestLeadTimeInHours() );
            addArgument( "referenceTime", meta.getTimeWindow().getReferenceTime().toString() ); //#44873
        }
        else
        {
            addArgument( "referenceTime", "" );
        }
        addArgument( "diagramInstanceDescription",
                     "at Lead Hour " + inputKeyInstance.getLeft().getLatestLeadTimeInHours()
                                                   + " for "
                                                   + inputKeyInstance.getRight() );
        addArgument( "probabilities",
                     HString.buildStringFromArray( displayPlotInput.getProbabilities().getDoubles(), ", " )
                            .replaceAll( "0.0,", "min," )
                            .replaceAll( "1.0", "max" ) );
        initializeFunctionInformation();
    }

    /**
     * An arguments processor intended for use in displaying metric output FOR POOLING WINDOWS, whether scalar or vector.
     * 
     * @param displayedPlotInput the plot input
     * @param plotType the plot type
     */
    public WRESArgumentProcessor( MetricOutputMapByTimeAndThreshold<?> displayedPlotInput, PlotTypeSelection plotType )
    {
        super();

        MetricOutputMetadata meta = displayedPlotInput.getMetadata();
        extractStandardArgumentsFromMetadata( meta );

        if ( plotType.equals( PlotTypeSelection.POOLING_WINDOW ) )
        {
            earliestInstant = displayedPlotInput.firstKey().getLeft().getEarliestTime();
            latestInstant = displayedPlotInput.lastKey().getLeft().getLatestTime();
            addArgument( "earliestLeadTimeHours",
                         "" + displayedPlotInput.firstKey().getLeft().getEarliestLeadTimeInHours() );
            addArgument( "latestLeadTimeHours",
                         "" + displayedPlotInput.lastKey().getLeft().getLatestLeadTimeInHours() );
            addArgument( "referenceTime", displayedPlotInput.firstKey().getLeft().getReferenceTime().toString() ); //#44873
        }
        else if ( meta.hasTimeWindow() )
        {
            earliestInstant = meta.getTimeWindow().getEarliestTime();
            latestInstant = meta.getTimeWindow().getLatestTime();
            addArgument( "earliestLeadTimeHours", "" + meta.getTimeWindow().getEarliestLeadTimeInHours() );
            addArgument( "latestLeadTimeHours", "" + meta.getTimeWindow().getLatestLeadTimeInHours() );
            addArgument( "referenceTime", meta.getTimeWindow().getReferenceTime().toString() ); //#44873
        }
        else
        {
            addArgument( "referenceTime", "" );
        }

        initializeFunctionInformation();
    }

    /**
     * Extracts the standard arguments that can be pulled from and interpreted consistently for any output meta data. 
     * @param meta the output metadata 
     */
    private void extractStandardArgumentsFromMetadata( MetricOutputMetadata meta )
    {
        //Setup fixed arguments.
        addArgument( "metricName", meta.getMetricID().toString() );
        addArgument( "metricShortName", meta.getMetricID().toString() );
        addArgument( "outputUnits", meta.getDimension().toString() );
        addArgument( "inputUnits", meta.getInputDimension().toString() );
        addArgument( "outputUnitsLabelSuffix", " [" + meta.getDimension() + "]" );
        addArgument( "inputUnitsLabelSuffix", " [" + meta.getInputDimension() + "]" );

        if ( meta.hasIdentifier() )
        {
            final DatasetIdentifier identifier = meta.getIdentifier();
            addArgument( "locationName", identifier.getGeospatialID() );
            addArgument( "variableName", identifier.getVariableID() );
            addArgument( "primaryScenario", identifier.getScenarioID() );
        }

        //I could create a helper method to handle this wrapping, but I don't think this will be used outside of this context,
        //so why bother?  (This relates to an email James wrote.)
        if ( meta.getMetricComponentID().equals( MetricConstants.MAIN ) )
        {
            addArgument( "metricComponentNameSuffix", "" );
        }
        else
        {
            addArgument( "metricComponentNameSuffix", meta.getMetricComponentID().toString() );
        }
    }

    /**
     * Adds arguments for plots where the lead time is on the domain axis and threshold is in the legend.
     * @param displayedPlotInput the plot input
     * @param plotTimeWindow the time window
     */
    public void addLeadThresholdArguments(
                                           MetricOutputMapByTimeAndThreshold<?> displayedPlotInput,
                                           TimeWindow plotTimeWindow )
    {
        final MetricOutputMetadata meta = displayedPlotInput.getMetadata();

        final String legendTitle = "Threshold";
        String legendUnitsText = "";
        if ( ( displayedPlotInput.hasQuantileThresholds() ) || ( displayedPlotInput.keySetByThreshold().size() > 1 ) )
        {
            legendUnitsText += " [" + meta.getInputDimension() + "]";
        }

        addArgument( "legendTitle", legendTitle );
        addArgument( "legendUnitsText", legendUnitsText );
        if ( plotTimeWindow != null )
        {
            Object key = plotTimeWindow.getLatestLeadTimeInHours();
            addArgument( "diagramInstanceDescription", "at Lead Hour " + key );
            addArgument( "plotTitleVariable", "Thresholds" );
        }
    }


    /**
     * Adds arguments for plots where the threshold value is on the domain axis and lead time is in the legend.
     * @param displayedPlotInput the plot input
     * @param threshold the threshold
     */
    public void addThresholdLeadArguments( MetricOutputMapByTimeAndThreshold<?> displayedPlotInput,
                                           Threshold threshold )
    {
        final MetricOutputMetadata meta = displayedPlotInput.getMetadata();

        addArgument( "legendTitle", "Lead Time" );
        addArgument( "legendUnitsText", " [hours]" );
        if ( threshold != null )
        {
            addArgument( "diagramInstanceDescription",
                         "for Threshold " + threshold.toString()
                                                       + " ("
                                                       + meta.getInputDimension()
                                                       + ")" );
            addArgument( "plotTitleVariable", "Lead Times" );
        }
    }

    /**
     * Adds arguments for plots where the pooling window (as in rolling window) is displayed along the domain axis 
     * and the legend includes both lead time and threshold.
     * @param displayedPlotInput the plot input
     */
    public void addPoolingWindowArguments( //MetricOutputMapByTimeAndThreshold<?> input,
                                           MetricOutputMapByTimeAndThreshold<?> displayedPlotInput )
    {
        final MetricOutputMetadata meta = displayedPlotInput.getMetadata();

        addArgument( "legendTitle", "Lead time [HOUR], Threshold " );
        addArgument( "legendUnitsText", "[" + meta.getInputDimension() + "]" );
    }

    /**
     * Adds arguments related to the baseline forecasts for skill scores.
     * @param meta the output metadata
     */
    public void addBaselineArguments( MetricOutputMetadata meta )
    {
        final DatasetIdentifier identifier = meta.getIdentifier();
        String baselineSuffix = "";
        if ( !Objects.isNull( identifier.getScenarioIDForBaseline() ) )
        {
            baselineSuffix = " Against Predictions From " + identifier.getScenarioIDForBaseline();
        }
        addArgument( "baselineLabelSuffix", baselineSuffix );
    }

    private void initializeFunctionInformation()
    {
        this.addFunctionName( EARLIEST_DATE_TO_TEXT );
        this.addFunctionName( LATEST_DATE_TO_TEXT );
    }

    /**
     * Called to process a date-to-text argument function.
     * @param argument the argument
     * @param dateInMillis the time in milliseconds from the epoch
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
        try
        {
            if ( argument.getFunctionParameterValues().size() == 2 )
            {
                final String dateFormat = argument.getFunctionParameterValues().get( 0 );
                final String timeZoneStr = argument.getFunctionParameterValues().get( 1 );
                HEFSTimeZoneTools.retrieveTimeZone( timeZoneStr );
                DateTimeFormatter formatter =
                        DateTimeFormatter.ofPattern( dateFormat )
                                         .withZone( HEFSTimeZoneTools.retrieveTimeZone( timeZoneStr ).toZoneId() );
                return formatter.format( dateInstant );
            }
            else
            {
                LOGGER.warn( "Incorrect number of parameters specified for " + argument.getArgumentName()
                             + " function; requires 2 arguments, the date format and time zone identifier." );
                return null;
            }
        }
        catch ( final Exception e )
        {
            LOGGER.warn( "Date format is invalid for " + argument.getArgumentName()
                         + " function; message: "
                         + e.getMessage() );
            return null;
        }
    }

    private String processPoolingWindowFunction( final Argument argument )
    {
        if ( ( earliestInstant == null ) || ( !earliestInstant.isAfter( Instant.MIN ) )
                && ( ( latestInstant == null ) || ( !latestInstant.isBefore( Instant.MAX ) ) ) )
        {
            return "Window is Unconstrained";
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
