package wres.io.retrieval;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.TimeAggregationFunction;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.VectorOfDoubles;
import wres.io.concurrency.WRESRunnable;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.UnitConversions;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.Database;
import wres.util.Collections;
import wres.util.Strings;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ClimatologyBuilder
{
    private static final Logger
            LOGGER = LoggerFactory.getLogger( ClimatologyBuilder.class );
    private final static String NEWLINE = System.lineSeparator();
    /**
     * Serves as a fuzzy key for date values. The idea is that, given a date
     * of either an offsetdatetime or in a string, the value should
     * match if it is contained within the range
     */
    private static class DateRange implements Comparable<DateRange>
    {
        public DateRange(String beginning, String end)
        {
            /*if (!TimeHelper.isTimestamp( beginning ))
            {
                throw new InvalidParameterException (
                        "The value '" +
                        String.valueOf(beginning) +
                        "' must be formatted as a timestamp to build a date range."
                );
            }
            else if (!TimeHelper.isTimestamp( end ))
            {
                throw new InvalidParameterException (
                        "The value '" +
                        String.valueOf(end) +
                        "' must be formatted as a timestamp to build a date range."
                );
            }*/


            this.startDate = beginning;
            this.endDate = end;
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
            // The hash code is a constant to force all equality checks to
            // bypass the hash check and instead
            // force use of #equals
            return 1268475648;
        }
    }

    // Collection of values mapped to their fuzzy date definitions
    private Map<DateRange, List<Double>> values;

    private Future<?> futureClimatologicalDates;
    //private Future<DataSet> futureValues;
    private VectorOfDoubles climatology;
    private Map<Integer, UnitConversions.Conversion> conversions;
    private final ProjectDetails projectDetails;
    private final DataSourceConfig dataSourceConfig;
    private final Feature feature;

    public ClimatologyBuilder(ProjectDetails projectDetails, DataSourceConfig dataSourceConfig, Feature feature)
            throws IOException
    {
        this.projectDetails = projectDetails;
        this.dataSourceConfig = dataSourceConfig;
        this.feature = feature;

        try
        {
            String earliestDate =
                    this.projectDetails.getZeroDate( this.dataSourceConfig, this.feature );
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
        this.futureClimatologicalDates = Database.execute( new WRESRunnable() {
            @Override
            protected Logger getLogger()
            {
                return LOGGER;
            }

            @Override
            protected void execute()
            {

                Connection connection = null;
                ResultSet results = null;

                try
                {
                    StringBuilder script = new StringBuilder("SELECT").append(NEWLINE);
                    script.append("    (")
                          .append(earliestDate).append("::timestamp without time zone")
                          .append(" + ( member_number || ' ")
                          .append(projectDetails.getAggregationUnit())
                          .append("')::INTERVAL)::TEXT AS start_date,").append(NEWLINE);
                    script.append("    (")
                          .append(earliestDate).append("::timestamp without time zone")
                          .append(" + ( ( member_number + ").append(projectDetails.getAggregationPeriod())
                          .append(" ) || ' ").append(projectDetails.getAggregationUnit()).append("')::INTERVAL)::TEXT AS end_date")
                          .append(NEWLINE);
                    script.append("FROM generate_series(0, ")
                          .append( ConfigHelper.getValueCount(projectDetails,
                                                              dataSourceConfig,
                                                              feature))
                          .append(" * ")
                          .append(projectDetails.getAggregationPeriod())
                          .append(", ")
                          .append(projectDetails.getAggregationPeriod())
                          .append(") AS member_number;");
                    connection = Database.getConnection();
                    results = Database.getResults( connection, script.toString() );

                    while (results.next())
                    {
                        if (values == null)
                        {
                            values = new TreeMap<>();
                        }

                        values.put(
                                new DateRange(results.getString("start_date"),
                                              results.getString("end_date")),
                                new ArrayList<>(  )
                        );
                    }
                }
                catch ( SQLException e )
                {
                    LOGGER.error( Strings.getStackTrace(e));
                }
                finally
                {
                    if (results != null)
                    {
                        try
                        {
                            results.close();
                        }
                        catch ( SQLException e )
                        {
                            LOGGER.error( Strings.getStackTrace(e));
                        }
                    }

                    if (connection != null)
                    {
                        Database.returnConnection( connection );
                    }
                }
            }
        } );
    }

    public VectorOfDoubles getClimatology() throws IOException
    {
        if (this.climatology == null)
        {
            this.setupDates();
            this.addValues();

            List<Double> aggregatedValues = new ArrayList<>();

            for (List<Double> valuesToAggregate : this.getValues().values())
            {
                if (this.projectDetails.shouldAggregate())
                {
                    Double aggregation = Collections.aggregate(
                            valuesToAggregate,
                            this.projectDetails.getAggregationFunction() );
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

            DataFactory factory = DefaultDataFactory.getInstance();
            this.climatology = factory.vectorOf(
                    aggregatedValues.toArray( new Double[aggregatedValues.size()] )
            );
        }
        return this.climatology;
    }

    private void setupDates() throws IOException
    {
        String errorPrepend = "The process of determining dates within which to aggregate ";
        try
        {
            this.futureClimatologicalDates.get();
        }
        catch ( InterruptedException e )
        {
            throw new IOException( errorPrepend +
                                   " was interupted and could not be completed.",
                                   e );
        }
        catch ( ExecutionException e )
        {
            throw new IOException( errorPrepend + "encountered an error.", e );
        }
    }

    private void addValues() throws IOException
    {

        StringBuilder script = new StringBuilder();

        script.append("SELECT O.observed_value,").append(NEWLINE);
        script.append("    O.measurementunit_id,").append(NEWLINE);
        script.append("    O.observation_time::text").append(NEWLINE);
        script.append("FROM wres.Observation O").append(NEWLINE);
        try
        {
            script.append("WHERE ")
                  .append(ConfigHelper.getVariablePositionClause( this.feature,
                                                                  ConfigHelper.getVariableID( this.dataSourceConfig ),
                                                                  "O"))
                  .append(NEWLINE);
        }
        catch ( SQLException e )
        {
            String message = "The proper variable used to generate climatology ";
            message += "data could not be gleaned from the ";

            if (this.projectDetails.getLeft() == this.dataSourceConfig)
            {
                message += ProjectDetails.LEFT_MEMBER;
            }
            else if (this.projectDetails.getRight() == this.dataSourceConfig)
            {
                message += ProjectDetails.RIGHT_MEMBER;
            }
            else
            {
                message += ProjectDetails.BASELINE_MEMBER;
            }

            message += " side of the data source specification.";
            throw new IOException( message, e );
        }
        script.append("    AND EXISTS (").append(NEWLINE);
        script.append("        SELECT 1").append(NEWLINE);
        script.append("        FROM wres.ProjectSource PS").append(NEWLINE);
        script.append("        WHERE PS.project_id = ").append(this.projectDetails.getId()).append(NEWLINE);
        script.append("            AND PS.member = ");

        if (this.projectDetails.getLeft().equals(this.dataSourceConfig))
        {
            script.append(ProjectDetails.LEFT_MEMBER);
        }
        else if (this.projectDetails.getRight().equals(this.dataSourceConfig))
        {
            script.append(ProjectDetails.RIGHT_MEMBER);
        }
        else
        {
            script.append(ProjectDetails.BASELINE_MEMBER);
        }

        script.append(NEWLINE);
        script.append("        AND PS.source_id = O.source_id").append(NEWLINE);
        script.append("        AND PS.inactive_time IS NULL").append(NEWLINE);
        script.append(");");

        Connection connection = null;
        ResultSet results = null;

        try
        {
            connection = Database.getConnection();
            results = Database.getResults( connection, script.toString() );

            // Add and convert all retrieved values
            while ( results.next() )
            {
                Double value = Database.getValue( results, "observed_value" );
                if ( value == null )
                {
                    continue;
                }

                value = this.getConversion( results.getInt(
                        "measurementunit_id" ) ).convert( value );

                if ( value >= this.projectDetails.getMinimumValue()
                     && value <= this.projectDetails.getMaximumValue() )
                {
                    this.addValue( results.getString( "observation_time" ),
                                   value );
                }
                else
                {
                    LOGGER.debug( "The value {} was not added for the date '{}'",
                                  value,
                                  results.getString( "observation_time" ) );
                }
            }
        }
        catch ( SQLException e )
        {
            throw new IOException( "Values could not be retrieved to add to the climatology data.", e );
        }
        finally
        {
            if (results != null)
            {
                try
                {
                    results.close();
                }
                catch ( SQLException e )
                {
                    LOGGER.debug( "ClimatologyBuilder#addValues: the result set could not be closed." );
                }
            }

            if (connection != null)
            {
                Database.returnConnection( connection );
            }
        }

        // Determine how many entries in a time slot indicates a full range of values
        int expectedNumberToAggregate = 0;
        for (Map.Entry<DateRange, List<Double>> convertedValues : this.values.entrySet())
        {
            expectedNumberToAggregate = Math.max( expectedNumberToAggregate, convertedValues.getValue().size() );
        }

        // Find all ranges that don't have all of their necessary values
        List<DateRange> partialRanges = new ArrayList<>(  );

        for (Map.Entry<DateRange, List<Double>> entry : this.values.entrySet())
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

    private Map<DateRange, List<Double>> getValues()
    {
        if (this.values == null)
        {
            this.values = new HashMap<>(  );
        }

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
