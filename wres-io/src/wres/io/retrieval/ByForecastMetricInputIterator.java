package wres.io.retrieval;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.EnsembleCondition;
import wres.config.generated.Feature;
import wres.datamodel.sampledata.SampleData;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.DataScripter;
import wres.io.utilities.ScriptBuilder;
import wres.io.writing.pair.SharedWriterManager;
import wres.util.CalculationException;

public class ByForecastMetricInputIterator extends MetricInputIterator
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ByForecastMetricInputIterator.class );

    ByForecastMetricInputIterator( Feature feature,
                                   ProjectDetails projectDetails,
                                   SharedWriterManager sharedWriterManager )
            throws IOException
    {
        super( feature, projectDetails, sharedWriterManager );
    }

    @Override
    int getFinalPoolingStep()
    {
        return 0;
    }

    @Override
    int calculateWindowCount() throws CalculationException
    {
        if (this.windowCount == 0)
        {
            int variableFeatureID;
            try
            {
                variableFeatureID = Features.getVariableFeatureID(
                        this.getFeature(),
                        this.getProjectDetails().getRightVariableID()
                );
            }
            catch ( SQLException e )
            {
                throw new CalculationException( "The id of the variable is required to "
                                                + "calculate the number of time series "
                                                + "to evaluate but could not be found.", e );
            }

            DataScripter script = new DataScripter(  );
            script.addLine("SELECT TS.timeseries_id");
            script.addLine("FROM wres.TimeSeries TS");
            script.addLine("WHERE EXISTS (");
            script.addTab().addLine("SELECT 1");
            script.addTab().addLine("FROM wres.ProjectSource PS");
            script.addTab().addLine("INNER JOIN wres.TimeSeriesSource TSS");
            script.addTab(  2  ).addLine("ON TSS.source_id = PS.source_id");
            script.addTab().addLine("WHERE PS.project_id = ", this.getProjectDetails().getId());
            script.addTab(  2  ).addLine("AND PS.member = ", ProjectDetails.RIGHT_MEMBER);
            script.addTab(  2  ).addLine("AND TSS.timeseries_id = TS.timeseries_id");
            script.addTab().addLine(")");
            script.addTab(  2  ).addLine("AND TS.variablefeature_id = ", variableFeatureID);

            if (this.getProjectDetails().getLatestIssueDate() != null)
            {
                script.addTab(  2  ).addLine("AND TS.initialization_date <= '", this.getProjectDetails().getLatestIssueDate(), "'");
            }

            if (this.getProjectDetails().getEarliestIssueDate() != null)
            {
                script.addTab(  2  ).addLine("AND TS.initialization_date >= '", this.getProjectDetails().getEarliestIssueDate(), "'");
            }

            if (this.getProjectDetails().getRight().getEnsemble() != null &&
                !this.getProjectDetails().getRight().getEnsemble().isEmpty())
            {
                Set<Integer> includes = new HashSet<>();
                Set<Integer> excludes = new HashSet<>();

                try
                {
                    for ( EnsembleCondition ensembleCondition : this.getProjectDetails().getRight().getEnsemble() )
                    {
                        List<Integer> ids = Ensembles.getEnsembleIDs( ensembleCondition );

                        if ( ensembleCondition.isExclude() )
                        {
                            excludes.addAll( ids );
                        }
                        else
                        {
                            includes.addAll( ids );
                        }
                    }
                }
                catch ( SQLException e )
                {
                    throw new CalculationException( "Could not calculate what "
                                                    + "ensembles to include or "
                                                    + "exclude within the evaluation.", e );
                }

                if (!includes.isEmpty())
                {
                    StringJoiner includeJoiner = new StringJoiner( ",", "ensemble_id = ANY('{", "}')" );
                    includes.forEach( id -> includeJoiner.add( id.toString() ) );
                    script.addTab(  2  ).addLine("AND ", includeJoiner.toString());
                }

                if (!excludes.isEmpty())
                {
                    StringJoiner excludeJoiner = new StringJoiner( ",", "ensemble_id != ANY('{", "}')" );
                    excludes.forEach( id -> excludeJoiner.add(id.toString()) );
                    script.addTab(  2  ).addLine("AND ", excludeJoiner.toString());
                }
            }

            script.addLine("ORDER BY TS.initialization_date;");

            // TODO: Add handling for season - should probably be in the thing that finds time series

            try
            {
                this.timeSeries = script.interpret( results -> results.getInt( "timeseries_id" ) );
            }
            catch ( SQLException e )
            {
                throw new CalculationException( "The collection of time series to "
                                                + "evaluate could not be calculated.", e );
            }

            this.windowCount = this.timeSeries.size();
        }

        return this.windowCount;
    }

    @Override
    Logger getLogger()
    {
        return ByForecastMetricInputIterator.LOGGER;
    }

    @Override
    public boolean hasNext()
    {
        return this.getWindowNumber() + 1 < this.windowCount;
    }

    @Override
    Callable<SampleData<?>> createRetriever() throws IOException
    {
        Retriever retriever = new TimeSeriesRetriever(
                this.getProjectDetails(),
                this.leftCache::getLeftValues,
                this.timeSeries.get(this.getWindowNumber()),
                this.getSharedWriterManager()
        );
        retriever.setFeature( this.getFeature() );
        retriever.setClimatology( this.getClimatology() );
        retriever.setLeadIteration( this.getWindowNumber() );

        return retriever;
    }

    private List<Integer> timeSeries;
    private int windowCount;
}
