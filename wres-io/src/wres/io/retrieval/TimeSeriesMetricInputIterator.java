package wres.io.retrieval;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.Feature;
import wres.datamodel.sampledata.SampleData;
import wres.io.concurrency.Executor;
import wres.io.config.ConfigHelper;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.DataScripter;
import wres.io.utilities.ScriptBuilder;
import wres.io.writing.pair.SharedWriterManager;
import wres.util.CalculationException;

public class TimeSeriesMetricInputIterator extends MetricInputIterator
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(TimeSeriesMetricInputIterator.class);

    TimeSeriesMetricInputIterator( Feature feature,
                                   ProjectDetails projectDetails,
                                   SharedWriterManager sharedWriterManager )
            throws IOException
    {
        super( feature, projectDetails, sharedWriterManager );
    }

    @Override
    int calculateWindowCount() throws CalculationException
    {
        // If we're going to end up with 100 MetricInputs due to the size of
        // the data, we want to ensure that we have a step for each.
        return this.getFinalPoolingStep();
    }

    @Override
    protected Future<SampleData<?>> submitForRetrieval() throws IOException
    {
        // We override because we want to add these to the more limited
        // Executor to ensure that less memory is used.

        // TODO: This is set to return ~1MB at a time. If there is only going
        // to be ~30 database threads, is this saving a lot?
        return Executor.submit( this.createRetriever() );
    }

    @Override
    protected int calculateFinalPoolingStep() throws CalculationException
    {
        String minimumDate = null;
        try
        {
            minimumDate = this.getProjectDetails().getInitialForecastDate( this.getRight(), this.getFeature() );
        }
        catch ( SQLException e )
        {
            throw new CalculationException( "The final pooling step could not be calculated because "
                                            + "the system could not determine when to start.", e );
        }

        // This will give an estimate of the amount of hours between each Time Series
        Integer lag = this.getProjectDetails().getForecastLag( this.getRight(), this.getFeature() );

        if (lag == 0)
        {
            return 1;
        }

        Integer seriesToRetrieve = this.getProjectDetails().getNumberOfSeriesToRetrieve();
        String variablePosition = null;
        try
        {
            variablePosition = ConfigHelper.getVariableFeatureClause(
                    this.getFeature(),
                    getProjectDetails().getRightVariableID(),
                    "TS"
            );
        }
        catch ( SQLException e )
        {
            throw new CalculationException( "The final pooling step could not be "
                                            + "calculated because the identifier for "
                                            + "the variable could not be determined.", e );
        }

        // If we know that we want to retrieve 851 time series at a time, we
        // want to determine how many 851 time series pulls to execute
        // lag * seriesToRetrieve = duration of each set of 851 time series
        //      If each time series is at most 24 hours after its prior and
        //      we want to pull 851 at a time, we want to cast a net over
        //      20,424 hours.
        // Since we know the earliest issue date to retrieve, we want to find the
        // upper bound of the number of times that our "lag * seriesToRetrieve"
        // fits within the amount of time between our initial date and the
        // absolute last time series
        DataScripter script = new DataScripter(  );
        script.addLine("SELECT CEILING(");
        script.addTab().addLine("EXTRACT( epoch FROM AGE(MAX(TS.initialization_date), ", minimumDate, "::timestamp without time zone)) /");
        script.addTab().addLine("EXTRACT( epoch FROM (INTERVAL '", lag * seriesToRetrieve, " MINUTE'))");
        script.addLine(") AS total_steps");
        script.addLine("FROM (");
        script.addTab().addLine("SELECT TS.initialization_date");
        script.addTab().addLine("FROM wres.TimeSeries TS");
        script.addTab().addLine( "WHERE ", variablePosition);
        script.addTab(  2  ).addLine("AND EXISTS (");
        script.addTab(   3   ).addLine("SELECT 1");
        script.addTab(   3   ).addLine("FROM wres.ProjectSource PS");
        script.addTab(   3   ).addLine("INNER JOIN wres.TimeSeriesSource TSS");
        script.addTab(    4    ).addLine("ON TSS.source_id = PS.source_id");
        script.addTab(   3   ).addLine("WHERE PS.project_id = ", this.getProjectDetails().getId());
        script.addTab(    4    ).addLine("AND PS.member = ", ProjectDetails.RIGHT_MEMBER);
        script.addTab(    4    ).addLine("AND TSS.timeseries_id = TS.timeseries_id");
        script.addTab(  2  ).addLine(")");
        script.addLine(") AS TS;");

        Double steps = null;
        try
        {
            steps = script.retrieve( "total_steps" );
        }
        catch ( SQLException e )
        {
            throw new CalculationException( "The calculation used to determine the "
                                            + "number of groups of time series to "
                                            + "evaluate failed.", e );
        }

        if (steps == null)
        {
            throw new CalculationException( "The calculation used to determine the "
                                            + "number of windows to evaluate returned "
                                            + "nothing.");
        }

        return steps.intValue();
    }

    @Override
    Logger getLogger()
    {
        return LOGGER;
    }
}