package wres.io.retrieval;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.datamodel.VectorOfDoubles;
import wres.io.concurrency.WRESCallable;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.UnitConversions;
import wres.io.data.caching.Variables;
import wres.io.data.details.ProjectDetails;
import wres.io.reading.usgs.USGSReader;
import wres.io.utilities.DataProvider;
import wres.io.utilities.Database;
import wres.io.utilities.ScriptBuilder;
import wres.util.CalculationException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ClimatologyBuilder
{
    private static final Logger
            LOGGER = LoggerFactory.getLogger( ClimatologyBuilder.class );
    private static final String NEWLINE = System.lineSeparator();

    // TODO: Put into its own file
    /**
     * Serves as a fuzzy key for date values. The idea is that, given a date,
     * the value should match if it is contained within the range
     */
    private static class DateRange implements Comparable<DateRange>
    {
        DateRange(String beginning, String end)
        {
            this.startDate = beginning;
            this.endDate = end;
        }

        DateRange(DataProvider data) throws SQLException
        {
            this.startDate = data.getString( "start_date" );
            this.endDate = data.getString( "end_date" );
        }

        private final String startDate;
        private final String endDate;

        public boolean contains(String date)
        {
            // start <= date && end > date
            return this.startDate.compareTo(date) <= 0 &&
                    this.endDate.compareTo(date) > 0;
        }

        public boolean contains(OffsetDateTime date)
        {
            OffsetDateTime beginning = OffsetDateTime.parse(this.startDate);
            OffsetDateTime end = OffsetDateTime.parse(this.endDate);

            // start <= date && date < end
            return (beginning.isBefore(date) || beginning.isEqual(date)) &&
                    end.isAfter(date);
        }

        @Override
        public int compareTo(DateRange dateRange)
        {

            // The default assumption is that this encompasses an earlier
            // range than dateRange, even if there is some sort of overlap
            int comparison = -1;

            if (dateRange.startDate.equals( dateRange.endDate ))
            {
                String instantDate = dateRange.startDate;

                if (this.contains(instantDate))
                {
                    comparison = 0;
                }
                else if (instantDate.compareTo( this.endDate ) >= 0)
                {
                    comparison = 1;
                }

                return comparison;
            }
            else if (this.startDate.equals( this.endDate ))
            {
                String instantDate = this.startDate;

                if (dateRange.contains( instantDate ))
                {
                    comparison = 0;
                }
                else if (instantDate.compareTo( dateRange.endDate ) >= 0)
                {
                    comparison = 1;
                }

                return comparison;
            }

            int thisToOtherStart = this.startDate.compareTo(dateRange.startDate);
            int otherToThisStart = dateRange.startDate.compareTo(this.startDate);
            int thisToOtherEnd = this.endDate.compareTo(dateRange.endDate);
            int otherToThisEnd = dateRange.endDate.compareTo(this.endDate);

            /*When this is set up as single hours (which shouldn't follow this model anyways),
                dates like '1980-10-01 23:00:00' and '1980-10-02 00:00:00' clash

            The solution is to test two other cases:
                1) this has a single value for the start and end, but dateRange doesn't
                2) dateRange has a single value, but this doesn't

            In both cases, the values for the single valued need to be greater than
            or equal to the multi-valued start and less than (but not equal) to the
            multi-valued end.*/

            if ((thisToOtherStart >= 0 && thisToOtherEnd <= 0) ||
                (otherToThisStart >= 0 && otherToThisEnd <= 0))
            {
                // Either this is a subset of dateRange or dateRange is a subset of this
                comparison = 0;
            }
            else if (thisToOtherStart > 0 && thisToOtherEnd > 0)
            {
                // This encompasses an later range than dateRange, even if there is overlap
                comparison = 1;
            }

            return comparison;
        }

        @Override
        public boolean equals(Object obj)
        {
            boolean equal = false;

            if (obj != null)
            {
                if (obj instanceof OffsetDateTime)
                {
                    OffsetDateTime dateTime = (OffsetDateTime)obj;
                    equal = this.contains(dateTime);
                }
                else if (obj instanceof String)
                {
                    String dateTime = (String)obj;
                    equal = this.contains(dateTime);
                }
                else if (obj instanceof DateRange)
                {
                    equal = this.compareTo((DateRange)obj) == 0;
                }
            }

            return equal;
        }

        @Override
        public String toString()
        {
            String description = "";

            if (this.endDate.equals( this.startDate ))
            {
                description = String.valueOf(this.startDate);
            }
            else
            {
                description = "From '" +
                              String.valueOf(this.startDate) +
                              "' up to '" +
                              String.valueOf(this.endDate) +
                              "'";
            }

            return description;
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException( "Cannot call hashCode" );
        }
    }

    // Collection of values mapped to their fuzzy date definitions
    private final SortedMap<DateRange, List<Double>> values;

    private Future<SortedMap<DateRange, List<Double>>> futureClimatologicalDates;
    private VectorOfDoubles climatology;
    private Map<Integer, UnitConversions.Conversion> conversions;
    private final ProjectDetails projectDetails;
    private final DataSourceConfig dataSourceConfig;
    private final Feature feature;

    ClimatologyBuilder(ProjectDetails projectDetails, DataSourceConfig dataSourceConfig, Feature feature)
            throws IOException
    {
        this.projectDetails = projectDetails;
        this.dataSourceConfig = dataSourceConfig;
        this.feature = feature;
        this.values = new TreeMap<>();

        LOGGER.debug( "ClimatologyBuilder constructed with args {}, {}, {}",
                      projectDetails, dataSourceConfig, feature );
        try
        {
            String earliestDate =
                    this.projectDetails.getInitialObservationDate( this.dataSourceConfig, this.feature );
            this.prepareDateLoad( earliestDate );
        }
        catch (SQLException e)
        {
            throw new IOException( "The earliest date from which to retrieve " +
                                   "climatological data could not be " +
                                   "determined.",
                                   e );
        }
    }

    private void prepareDateLoad(String earliestDate)
    {
        LOGGER.debug( "prepareDateLoad called with earliestDate {}",
                      earliestDate );
        PrepareDateLoad loadTask = new PrepareDateLoad( this.feature,
                                                        this.projectDetails,
                                                        this.dataSourceConfig,
                                                        earliestDate );
        this.futureClimatologicalDates = Database.submit( loadTask );
    }

    // TODO: put into its own file
    private static final class PrepareDateLoad
            extends WRESCallable<SortedMap<DateRange, List<Double>>>
    {
        private final Feature feature;
        private final ProjectDetails projectDetails;
        private final DataSourceConfig dataSourceConfig;
        private final String earliestDate;

        PrepareDateLoad( Feature feature,
                         ProjectDetails projectDetails,
                         DataSourceConfig dataSourceConfig,
                         String earliestDate )
        {
            this.feature = feature;
            this.projectDetails = projectDetails;
            this.dataSourceConfig = dataSourceConfig;
            this.earliestDate = earliestDate;
        }

        @Override
        protected SortedMap<DateRange, List<Double>> execute()
                throws CalculationException
        {
            ScriptBuilder script = new ScriptBuilder();
            script.addLine("SELECT");

            script.addTab().add("(", this.earliestDate, "::timestamp without time zone");

            if (this.dataSourceConfig.getTimeShift() != null)
            {
                script.add(" + '",
                           this.dataSourceConfig.getTimeShift().getWidth(),
                           " ",
                           this.dataSourceConfig.getTimeShift().getUnit(),
                           "'");
            }


            script.addLine(" + ( member_number || ' ",
                           this.projectDetails.getScale().getUnit(),
                           "')::INTERVAL)::TEXT AS start_date,");

            script.addTab().addLine("(",
                                    this.earliestDate,
                                    "::timestamp without time zone");

            if (this.dataSourceConfig.getTimeShift() != null)
            {
                script.addLine(" + '",
                               this.dataSourceConfig.getTimeShift().getWidth(),
                               " ",
                               this.dataSourceConfig.getTimeShift().getUnit(),
                               "'");
            }

            script.addLine(" + ( ( member_number + ",
                           this.projectDetails.getScale().getPeriod(),
                           " ) || ' ",
                           this.projectDetails.getScale().getUnit(),
                           "')::INTERVAL)::TEXT AS end_date");

            try
            {
                script.add("FROM generate_series(0, ",
                           ConfigHelper.getValueCount(this.projectDetails,
                                                      this.dataSourceConfig,
                                                      this.feature),
                           " * ",
                           this.projectDetails.getScale().getPeriod(),
                           ", ",
                           this.projectDetails.getScale().getPeriod(),
                           ") AS member_number;");
            }
            catch ( SQLException e )
            {
                throw new CalculationException( "The number of values to load could not be calculated.", e );
            }

            SortedMap<DateRange, List<Double>> returnValues = new TreeMap<>();

            try
            {
                script.consume(
                        range -> returnValues.put( new DateRange(range), new ArrayList<>())
                );
            }
            catch ( SQLException e )
            {
                throw new CalculationException( "The dates that contain values could not be calculated.", e );
            }

            return Collections.unmodifiableSortedMap( returnValues );
        }

        @Override
        protected Logger getLogger()
        {
            return ClimatologyBuilder.LOGGER;
        }
    }

    VectorOfDoubles getClimatology() throws CalculationException
    {
        if (this.climatology == null)
        {
            try
            {
                this.setupDates();
                this.addValues();
            }
            catch ( IOException e )
            {
                throw new CalculationException(
                        "The dates that will contain values and "
                        + "the values themselves could not be calculated.",
                        e );
            }

            List<Double> aggregatedValues = new ArrayList<>();

            for ( List<Double> valuesToAggregate : this.getValues()
                                                       .values() )
            {
                if ( this.projectDetails.shouldScale() )
                {
                    Double aggregation = wres.util.Collections.aggregate(
                            valuesToAggregate,
                            this.projectDetails.getScale()
                                               .getFunction()
                                               .value() );
                    if ( !Double.isNaN( aggregation ) )
                    {
                        aggregatedValues.add( aggregation );
                    }
                }
                else
                {
                    aggregatedValues.addAll( valuesToAggregate );
                }
            }

            this.climatology = VectorOfDoubles.of( aggregatedValues.toArray( new Double[aggregatedValues.size()] ) );
        }
        return this.climatology;
    }

    private void setupDates() throws IOException
    {
        LOGGER.debug( "setupDates called" );

        final String errorPrepend = "The process of determining dates within "
                                    + "which to aggregate ";
        try
        {
            this.values.putAll( this.futureClimatologicalDates.get() );
        }
        catch ( InterruptedException e )
        {
            if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( errorPrepend +
                             "was interrupted and could not be completed.",
                             e );
            }

            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException e )
        {
            throw new IOException( errorPrepend + "encountered an error.", e );
        }
    }

    private void addValues() throws IOException
    {
        // TODO: Convert to using a ScriptBuilder
        StringBuilder script = new StringBuilder();

        script.append("SELECT O.observed_value,").append(NEWLINE);
        script.append("    O.measurementunit_id,").append(NEWLINE);
        script.append("    (O.observation_time");

        if (this.dataSourceConfig.getTimeShift() != null)
        {
            script.append(" + '")
                  .append(this.dataSourceConfig.getTimeShift().getWidth())
                  .append(" ")
                  .append(this.dataSourceConfig.getTimeShift().getUnit())
                  .append("'");
        }

        script.append(")::text AS observation_time").append(NEWLINE);
        script.append("FROM wres.Observation O").append(NEWLINE);
        try
        {
            script.append("WHERE ")
                  .append(ConfigHelper.getVariableFeatureClause( this.feature,
                                                                 Variables.getVariableID( this.dataSourceConfig ),
                                                                 "O"))
                  .append(NEWLINE);
        }
        catch ( SQLException e )
        {
            String message = "The proper variable used to generate climatology ";
            message += "data could not be gleaned from the ";

            message += this.projectDetails.getInputName( this.dataSourceConfig );

            message += " side of the data source specification.";
            throw new IOException( message, e );
        }

        // Impose date limitations to keep a consistent climatology for USGS projects
        if (ConfigHelper.usesUSGSData( this.projectDetails.getProjectConfig() ))
        {
            Instant date = ConfigHelper.getEarliestDateTimeFromDataSources( this.projectDetails.getProjectConfig() );

            String earliest = "'";
            String latest = "'";

            if (date == null)
            {
                earliest += USGSReader.EARLIEST_DATE;
            }
            else
            {
                earliest += date.toString();
            }

            earliest += "'";

            date = ConfigHelper.getLatestDateTimeFromDataSources( this.projectDetails.getProjectConfig() );

            if (date == null)
            {
                latest += USGSReader.LATEST_DATE;
            }
            else
            {
                latest += date.toString();
            }

            latest += "'";

            script.append("    AND O.observation_time");

            if (this.dataSourceConfig.getTimeShift() != null)
            {
                script.append(" + '")
                      .append(this.dataSourceConfig.getTimeShift().getWidth())
                      .append(" ")
                      .append(this.dataSourceConfig.getTimeShift().getUnit())
                      .append("'")
                      .append(NEWLINE);
            }

            script.append(" >= ").append(earliest).append(NEWLINE);
            script.append("    AND O.observation_time");

            if (this.dataSourceConfig.getTimeShift() != null)
            {
                script.append(" + '")
                      .append(this.dataSourceConfig.getTimeShift().getWidth())
                      .append(" ")
                      .append(this.dataSourceConfig.getTimeShift().getUnit())
                      .append("'")
                      .append(NEWLINE);
            }

            script.append(" <= ").append(latest).append(NEWLINE);
        }

        script.append("    AND EXISTS (").append(NEWLINE);
        script.append("        SELECT 1").append(NEWLINE);
        script.append("        FROM wres.ProjectSource PS").append(NEWLINE);
        script.append("        WHERE PS.project_id = ").append(this.projectDetails.getId()).append(NEWLINE);
        script.append("            AND PS.member = ")
              .append(this.projectDetails.getInputName( this.dataSourceConfig ))
              .append(NEWLINE);
        script.append("        AND PS.source_id = O.source_id").append(NEWLINE);
        script.append(");");

        Connection connection = null;

        try
        {
            connection = Database.getConnection();
            try (DataProvider data = Database.getResults( connection, script.toString() ))
            {

                // Add and convert all retrieved values
                while ( data.next() )
                {
                    Double value = data.getValue( "observed_value" );
                    if ( value == null )
                    {
                        continue;
                    }

                    value = this.getConversion( data.getInt(
                            "measurementunit_id" ) ).convert( value );

                    if ( value < this.projectDetails.getMinimumValue() &&
                         this.projectDetails.getDefaultMinimumValue() != null )
                    {

                        this.addValue( data.getString( "observation_time" ),
                                       this.projectDetails.getDefaultMinimumValue() );
                    }
                    else if ( value > this.projectDetails.getMaximumValue() &&
                              this.projectDetails.getDefaultMaximumValue() != null )
                    {
                        this.addValue( data.getString( "observation_time" ),
                                       this.projectDetails.getDefaultMaximumValue() );
                    }
                    else if ( value >= this.projectDetails.getMinimumValue()
                              && value <= this.projectDetails.getMaximumValue() )
                    {
                        this.addValue( data.getString( "observation_time" ),
                                       value );
                    }
                    else
                    {
                        LOGGER.debug( "The value {} was not added for the date '{}'",
                                      value,
                                      data.getString( "observation_time" ) );
                    }
                }
            }
        }
        catch ( SQLException e )
        {
            throw new IOException( "Values could not be retrieved to add to the climatology data.", e );
        }
        finally
        {
            if (connection != null)
            {
                Database.returnConnection( connection );
            }
        }

        // Determine how many entries in a time slot indicates a full range of values
        int expectedNumberToAggregate = 0;
        for (Entry<DateRange, List<Double>> convertedValues : this.values.entrySet())
        {
            expectedNumberToAggregate = Math.max( expectedNumberToAggregate, convertedValues.getValue().size() );
        }

        // Find all ranges that don't have all of their necessary values
        List<DateRange> partialRanges = new ArrayList<>(  );

        for (Entry<DateRange, List<Double>> entry : this.values.entrySet())
        {
            if (entry.getValue().size() < expectedNumberToAggregate)
            {
                partialRanges.add( entry.getKey() );
            }
        }

        // Remove all partial ranges
        for (DateRange partialRange : partialRanges)
        {
            this.values.remove( partialRange );
        }
    }

    private UnitConversions.Conversion getConversion(int measurementUnitID)
    {
        if (this.conversions == null)
        {
            this.conversions = new TreeMap<>(  );
        }

        if (!this.conversions.containsKey( measurementUnitID ))
        {
            this.conversions.put(measurementUnitID,
                                 UnitConversions.getConversion( measurementUnitID,
                                                                this.projectDetails.getDesiredMeasurementUnit() ));
        }

        return this.conversions.get(measurementUnitID);
    }

    private SortedMap<DateRange, List<Double>> getValues()
    {
        return this.values;
    }

    private void addValue(final String date, final Double value)
    {
        DateRange key = new DateRange(date, date);

        if (this.getValues().containsKey( key ))
        {
            this.getValues().get( key ).add(value);
        }
        else
        {
            LOGGER.debug("The value {} could not be added for the date of '{}'",
                         String.valueOf(value),
                         date);
        }
    }
}
