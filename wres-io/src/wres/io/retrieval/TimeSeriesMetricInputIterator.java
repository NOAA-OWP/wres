package wres.io.retrieval;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.Feature;
import wres.datamodel.inputs.MetricInput;
import wres.io.concurrency.Executor;
import wres.io.config.ConfigHelper;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.ScriptBuilder;

public class TimeSeriesMetricInputIterator extends MetricInputIterator
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(TimeSeriesMetricInputIterator.class);

    TimeSeriesMetricInputIterator( Feature feature,
                                   ProjectDetails projectDetails )
            throws SQLException, IOException
    {
        super( feature, projectDetails );
    }

    @Override
    int calculateWindowCount() throws SQLException
    {
        return 1;
    }

    @Override
    protected Future<MetricInput<?>> submitForRetrieval() throws IOException
    {
        return Executor.submit( this.createRetriever() );
    }

    @Override
    protected int getFinalPoolingStep() throws SQLException
    {
        String minimumDate = this.getProjectDetails().getInitialForecastDate( this.getRight(), this.getFeature() );
        Integer lag = this.getProjectDetails().getForecastLag( this.getRight(), this.getFeature() );
        Integer seriesToRetrieve = this.getProjectDetails().getNumberOfSeriesToRetrieve();
        String variablePosition = ConfigHelper.getVariablePositionClause(
                this.getFeature(),
                getProjectDetails().getRightVariableID(),
                "TS"
        );

        ScriptBuilder script = new ScriptBuilder(  );
        script.addLine("SELECT CEILING(");
        script.addTab().addLine("EXTRACT( epoch FROM AGE(MAX(TS.initialization_date), ", minimumDate, "::timestamp without time zone)) /");
        script.addTab().addLine("EXTRACT( epoch FROM (INTERVAL '", lag * seriesToRetrieve, " HOUR'))");
        script.addLine(") AS total_steps");
        script.addLine("FROM (");
        script.addTab().addLine("SELECT TS.initialization_date");
        script.addTab().addLine("FROM wres.TimeSeries TS");
        script.addTab().addLine( "WHERE ", variablePosition);
        script.addTab(  2  ).addLine("AND EXISTS (");
        script.addTab(   3   ).addLine("SELECT 1");
        script.addTab(   3   ).addLine("FROM wres.ProjectSource PS");
        script.addTab(   3   ).addLine("INNER JOIN wres.ForecastSource FS");
        script.addTab(    4    ).addLine("ON FS.source_id = PS.source_id");
        script.addTab(   3   ).addLine("WHERE PS.project_id = ", this.getProjectDetails().getId());
        script.addTab(    4    ).addLine("AND PS.member = ", ProjectDetails.RIGHT_MEMBER);
        script.addTab(    4    ).addLine("AND FS.forecast_id = TS.timeseries_id");
        script.addTab(  2  ).addLine(")");
        script.addLine(") AS TS;");

        Double steps = script.retrieve( "total_steps" );

        if (steps == null)
        {
            throw new SQLException( "The number of windows within which to pull "
                                    + "data could not be determined by the data "
                                    + "in the database." );
        }

        return steps.intValue();
    }

    @Override
    Logger getLogger()
    {
        return LOGGER;
    }
}