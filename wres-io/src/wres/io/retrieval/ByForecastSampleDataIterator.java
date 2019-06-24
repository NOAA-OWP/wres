package wres.io.retrieval;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.Feature;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.time.TimeWindow;
import wres.io.config.ConfigHelper;
import wres.io.config.OrderedSampleMetadata;
import wres.io.data.caching.Features;
import wres.io.project.Project;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.util.CalculationException;
import wres.util.TimeHelper;

public class ByForecastSampleDataIterator extends SampleDataIterator
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ByForecastSampleDataIterator.class );


    ByForecastSampleDataIterator( Feature feature, Project project, Path outputDirectoryForPairs)
            throws IOException
    {
        super( feature, project, outputDirectoryForPairs);
    }

    @Override
    int getFinalPoolingStep()
    {
        return 0;
    }

    @Override
    protected void calculateSamples() throws CalculationException
    {
        DataScripter script;

        if ( ConfigHelper.usesNetCDFData( this.getProject().getProjectConfig() ) ||
             ConfigHelper.usesS3Data( this.getProject().getProjectConfig() ))
        {
            script = this.getNetcdfWindowScript();
        }
        else
        {
            script = this.getWindowScript();
        }

        try ( DataProvider provider = script.getData())
        {
            OrderedSampleMetadata.Builder sampleMetadataBuilder = new OrderedSampleMetadata.Builder();
            sampleMetadataBuilder.setProject( this.getProject() ).setFeature( this.getFeature() );

            while (provider.next())
            {
                TimeWindow window = TimeWindow.of(
                        provider.getInstant( "initialization_date" ),
                        provider.getInstant( "initialization_date" ),
                        provider.getDuration( "earliest_lead" ),
                        provider.getDuration( "latest_lead" )
                );

                this.addSample(
                        sampleMetadataBuilder.setSampleNumber( provider.getInt( "timeseries_id" ) )
                                             .setTimeWindow( window )
                                             .build()
                );
            }
        }
        catch ( SQLException e )
        {
            throw new CalculationException( "Data could not be retrieved from the database. "
                                            + "Constraints for data evaluation groups could not be formed.",
                                            e );
        }
    }

    private DataScripter getNetcdfWindowScript() throws CalculationException
    {
        DataScripter script = new DataScripter();

        script.addLine("SELECT TS.timeseries_id,");
        script.addTab().addLine("TS.initialization_date,");
        script.addTab().addLine("LEAST(", this.getLeastLead(), ", MIN(TSS.lead)) AS earliest_lead,");
        script.addTab().addLine("MAX(TSS.lead) AS latest_lead");
        script.addLine("FROM wres.TimeSeries TS");
        script.addLine("INNER JOIN wres.TimeSeriesSource TSS");
        script.addTab().addLine("ON TSS.timeseries_id = TS.timeseries_id");
        script.addLine("INNER JOIN wres.ProjectSource PS");
        script.addTab().addLine("ON TSS.source_id = PS.source_id");
        script.addLine("WHERE PS.project_id = ", this.getProject().getId());
        script.addTab().addLine( "AND PS.member = ", Project.RIGHT_MEMBER);

        try
        {
            script.addTab().addLine(
                    "AND TS.variablefeature_id = ", Features.getVariableFeatureID(
                            this.getFeature(),
                            this.getProject().getRightVariableID()
                    )
            );
        }
        catch ( SQLException e )
        {
            throw new CalculationException( "The variable and location for the evaluation of " +
                                            ConfigHelper.getFeatureDescription( this.getFeature() ) +
                                            " could not be calculated.", e );
        }

        script.addTab().addLine("AND TSS.lead >= ", this.getLeastLead());

        if ( this.getProject().getMaximumLead() != Integer.MAX_VALUE)
        {
            script.addTab().addLine("AND TSS.lead <= ", this.getProject().getMaximumLead());
        }

        script.add(
                ConfigHelper.getSeasonQualifier( this.getProject(),
                                                 "TS.initialization_date",
                                                 this.getProject().getRightTimeShift()
                )
        );

        script.addLine("GROUP BY TS.timeseries_id, TS.initialization_date");
        script.addLine("ORDER BY TS.initialization_date, TS.ensemble_id;");

        return script;
    }

    private DataScripter getWindowScript() throws CalculationException
    {
        DataScripter script = new DataScripter(  );

        script.addLine("SELECT TS.timeseries_id,");
        script.addTab().addLine("TS.initialization_date,");
        script.addTab().addLine("LEAST(", this.getLeastLead(), ", MIN(TSV.lead)) AS earliest_lead,");
        script.addTab().addLine("MAX(TSV.lead) AS latest_lead");
        script.addLine("FROM wres.TimeSeries TS");
        script.addLine("INNER JOIN wres.TimeSeriesSource TSS");
        script.addTab().addLine("ON TSS.timeseries_id = TS.timeseries_id");
        script.addLine("INNER JOIN wres.ProjectSource PS");
        script.addTab().addLine("ON PS.source_id = TSS.source_id");
        script.addLine("INNER JOIN wres.TimeSeriesValue TSV");
        script.addTab().addLine("ON TSV.timeseries_id = TS.timeseries_id");
        script.addLine("WHERE PS.project_id = ", this.getProject().getId());
        script.addTab().addLine( "AND PS.member = ", Project.RIGHT_MEMBER);

        try
        {
            script.addTab().addLine(
                    "AND TS.variablefeature_id = ", Features.getVariableFeatureID(
                            this.getFeature(),
                            this.getProject().getRightVariableID()
                    )
            );
        }
        catch ( SQLException e )
        {
            throw new CalculationException( "The variable and location for the evaluation of " +
                                            ConfigHelper.getFeatureDescription( this.getFeature() ) +
                                            " could not be calculated.", e );
        }

        script.addTab().addLine("AND TSV.lead >= ", this.getLeastLead());

        if ( this.getProject().getMaximumLead() != Integer.MAX_VALUE)
        {
            script.addTab().addLine("AND TSV.lead <= ", this.getProject().getMaximumLead());
        }

        script.add(
                ConfigHelper.getSeasonQualifier( this.getProject(),
                                                 "TS.initialization_date",
                                                 this.getProject().getRightTimeShift()
                )
        );

        if ( this.getProject().getEarliestIssueDate() != null)
        {
            script.addTab().addLine("AND TS.initialization_date >= '", this.getProject().getEarliestIssueDate(), "'");
        }

        if (this.getProject().getLatestIssueDate() != null)
        {
            script.addTab().addLine("AND TS.initialization_date <= '", this.getProject().getLatestIssueDate(), "'");
        }

        if (this.getProject().getEarliestDate() != null)
        {
            script.addTab().add( "AND TS.initialization_date ")
                  .add("+ INTERVAL '1 ", TimeHelper.LEAD_RESOLUTION, "' * TSV.lead >= ")
                  .addLine("'", this.getProject().getEarliestDate(), "'");
        }

        if (this.getProject().getLatestDate() != null)
        {
            script.addTab().add( "AND TS.initialization_date ")
                  .add("+ INTERVAL '1 ", TimeHelper.LEAD_RESOLUTION, "' * TSV.lead <= ")
                  .addLine("'", this.getProject().getLatestDate(), "'");
        }

        script.addLine("GROUP BY TS.timeseries_id, TS.initialization_date");
        script.addLine("ORDER BY TS.initialization_date, TS.ensemble_id;");


        return script;
    }

    private int getLeastLead()
    {
        int least = this.getProject().getMinimumLead();

        if (least == Integer.MIN_VALUE)
        {
            least = 0;
        }

        return least;
    }

    @Override
    Logger getLogger()
    {
        return ByForecastSampleDataIterator.LOGGER;
    }

    @Override
    Callable<SampleData<?>> createRetriever(final OrderedSampleMetadata sampleMetadata) throws IOException
    {
        Retriever retriever = new TimeSeriesRetriever(
                sampleMetadata,
                this.leftCache::getLeftValues,
                this.getOutputPathForPairs()
        );

        retriever.setClimatology( this.getClimatology() );

        return retriever;
    }
}
