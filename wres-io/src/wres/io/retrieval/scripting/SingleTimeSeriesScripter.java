package wres.io.retrieval.scripting;

import java.io.IOException;
import java.sql.SQLException;

import wres.config.generated.DataSourceConfig;
import wres.io.config.OrderedSampleMetadata;

class SingleTimeSeriesScripter extends Scripter
{
    SingleTimeSeriesScripter( OrderedSampleMetadata sampleMetadata,
                              DataSourceConfig dataSourceConfig )
    {
        super(sampleMetadata, dataSourceConfig);
    }
    @Override
    String formScript() throws SQLException, IOException
    {
        this.addLine("-- ", this.getSampleMetadata());
        this.add("SELECT ");
        this.applyValueDate();
        this.addTab().addLine("TSV.lead,");
        this.addTab( ).add("(EXTRACT( epoch FROM TS.initialization_date)");

        if (this.getTimeShift() != null)
        {
            this.add(" + ", this.getTimeShift().getSeconds());
        }

        this.addLine(")::bigint AS basis_epoch_time,");
        this.addTab().addLine("ARRAY[TSV.series_value] AS measurements,");
        this.addTab().addLine("ARRAY[TS.ensemble_id] AS members,");
        this.addTab().addLine("TS.measurementunit_id");
        this.addLine("FROM (");
        this.addTab().addLine("SELECT TS.initialization_date, TS.timeseries_id, TS.measurementunit_id, TS.ensemble_id");
        this.addTab().addLine("FROM wres.TimeSeries TS");
        this.addTab().addLine("WHERE TS.timeseries_id = ", this.getSampleMetadata().getSampleNumber());
        this.addLine(") AS TS");
        this.addLine("INNER JOIN wres.TimeSeriesValue TSV");
        this.addTab().addLine("ON TS.timeseries_id = TSV.timeseries_id");

        boolean whereAdded = false;

        if (this.getProjectDetails().getMaximumLead() < Integer.MAX_VALUE)
        {
            this.addLine("WHERE TSV.lead <= ", this.getProjectDetails().getMaximumLead());

            whereAdded = true;
        }

        if (this.getProjectDetails().getMinimumLead() > Integer.MIN_VALUE)
        {
            if (whereAdded)
            {
                this.addTab().add("AND ");
            }
            else
            {
                this.add("WHERE ");
                whereAdded = true;
            }

            this.addLine("TSV.lead >= ", this.getProjectDetails().getMinimumLead());
        }

        if (this.getProjectDetails().getEarliestDate() != null)
        {
            if (whereAdded)
            {
                this.addTab().add("AND ");
            }
            else
            {
                whereAdded = true;
                this.add( "WHERE " );
            }

            this.addLine("TS.initialization_date + INTERVAL '1 MINUTE' * TSV.lead >= '", this.getProjectDetails().getEarliestDate(), "'");
        }

        if (this.getProjectDetails().getLatestDate() != null)
        {
            if (whereAdded)
            {
                this.addTab().add("AND ");
            }
            else
            {
                this.add("WHERE ");
            }

            this.addLine("TS.initialization_date + INTERVAL '1 MINUTE' * TSV.lead <= '", this.getProjectDetails().getLatestDate(), "'");
        }

        this.addTab().add("ORDER BY TSV.lead;");

        return this.toString();
    }

    @Override
    String getBaseDateName()
    {
        return "TS.initialization_date";
    }

    @Override
    String getValueDate()
    {
        return this.getBaseDateName() + " + INTERVAL '1 MINUTE' * TSV.lead";
    }
}
