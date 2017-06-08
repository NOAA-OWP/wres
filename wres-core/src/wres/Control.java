package wres;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.datamodel.DataFactory;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.io.data.caching.MeasurementCache;
import wres.io.data.caching.VariableCache;
import wres.io.utilities.Database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * Another Main. The reason for creating this class separately from Main
 * is to defer conflict with the existing Main.java code to a later date,
 * at the request of a teammate.
 *
 * It is expected that eventually one of the two will become Main and the other
 * will be merged into it. In order to make something like MainFunctions where
 * a closed loop request/response is created, a separate Main seemed needed.
 *
 * Has (too many?) private static classes that will need to be split out if they
 * are deemed useful.
 */
public class Control
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Control.class);

    private static final String NEWLINE = System.lineSeparator();

    private static final String SQL_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final DateTimeFormatter SQL_FORMATTER = DateTimeFormatter.ofPattern(SQL_FORMAT);

    public static void main(String[] args)
    {
        PairConfig config = PairConfig.of(
                LocalDateTime.of(1980,1,1,0,0),
                LocalDateTime.of(1999, 12, 31, 23, 59),
                "SQIN",
                "QINE",
                "CFS");

        PairGetterByLeadTime pairFunc = new PairGetterByLeadTime(config);

        List<PairOfDoubleAndVectorOfDoubles> pairsOneDay = pairFunc.apply(24);

        List<PairOfDoubleAndVectorOfDoubles> pairsTwoDays = pairFunc.apply(48);

        pairsOneDay.stream()
                   .forEach(p ->
                            LOGGER.info("With lead time 24: {}", p));
        pairsTwoDays.stream()
                    .forEach(p ->
                             LOGGER.info("With lead time 48: {}", p));
    }

    private static class PairGetterByLeadTime implements Function<Integer, List<PairOfDoubleAndVectorOfDoubles>>
    {
        private final PairConfig config;

        public PairGetterByLeadTime(PairConfig config)
        {
            this.config = config;
        }

        @Override
        public List<PairOfDoubleAndVectorOfDoubles> apply(Integer lead)
        {
            List<PairOfDoubleAndVectorOfDoubles> result = new ArrayList<>();
            String sql;

            try
            {
                sql = getPairSqlFromConfigForLead(this.config, lead);
            }
            catch (IOException ioe)
            {
                LOGGER.error("When trying to build sql for pairs:", ioe);
                return result;
            }

            DataFactory valueFactory = wres.datamodel.DataFactory.instance();

            try (Connection con = Database.getConnection();
                 Statement statement = con.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql))
            {
                while (resultSet.next())
                {
                    double observationValue = (double)resultSet.getFloat("observation");
                    Double[] forecastValues = (Double[])resultSet.getArray("forecasts").getArray();
                    PairOfDoubleAndVectorOfDoubles pair = valueFactory.pairOf(observationValue, forecastValues);

                    LOGGER.trace("Adding a pair with observationValue {} and forecastValues {}",
                            pair.getItemOne(), pair.getItemTwo());

                    result.add(pair);
                }
            }
            catch (SQLException se)
            {
                LOGGER.error("When getting pair results from database:", se);
                return result;
            }
            finally
            {
                LOGGER.debug("Query: ");
                LOGGER.debug(sql);
            }

            return result;
        }
    }

    /**
     * Builds a pairing query based on values already present in the database.
     *
     * Side-effects include reaching out to the database for values, and
     * inserting values to generate ids if they are not already present.
     *
     * @param config configuration information for pairing
     * @param lead the lead time to build this query for.
     * @return a SQL string that will retrieve pairs for the given lead time
     * @throws IOException when any checked (aka non-RuntimeException) exception occurs
     */
    private static String getPairSqlFromConfigForLead(PairConfig config, int lead) throws IOException
    {
        if (config.getForecastVariable() == null
            || config.getObservationVariable() == null
            || config.getTargetUnit() == null)
        {
            throw new IllegalArgumentException("Forecast and obs variables as well as target unit must be specified");
        }

        long startTime = Long.MAX_VALUE; // used during debug
        if (LOGGER.isDebugEnabled())
        {
            startTime = System.currentTimeMillis();
        }

        int targetUnitID = Control.getMeasurementUnitID(config.getTargetUnit());
        int observationVariableID = Control.getVariableID(config.getObservationVariable(), targetUnitID);
        int forecastVariableID = Control.getVariableID(config.getForecastVariable(), targetUnitID);

        if (LOGGER.isDebugEnabled())
        {
            long duration = System.currentTimeMillis() - startTime;
            LOGGER.debug("Retrieving meas unit, fc var, obs var IDs took {}ms",
                         duration);
            startTime = System.currentTimeMillis();
        }

        Integer observationVariablePositionID;
        Integer forecastVariablePositionID;

        String obsVarPosSql = "SELECT variableposition_id FROM wres.VariablePosition WHERE variable_id = " + observationVariableID + ";";
        try
        {
            observationVariablePositionID = Database.getResult(obsVarPosSql, "variableposition_id");
        }
        catch (SQLException se)
        {
            String message = "Couldn't retrieve variableposition_id for observation variable "
                             + config.getObservationVariable()
                             + " with obs id " + observationVariableID;
            throw new IOException(message, se);
        }

        if (LOGGER.isDebugEnabled())
        {
            long duration = System.currentTimeMillis() - startTime;
            LOGGER.debug("Retrieving variableposition_id for obs id {} took {}ms",
                         observationVariableID, duration);
            startTime = System.currentTimeMillis();
        }

        String fcVarPosSql = "SELECT variableposition_id FROM wres.VariablePosition WHERE variable_id = " + forecastVariableID + ";";
        try
        {
            forecastVariablePositionID = Database.getResult(fcVarPosSql, "variableposition_id");
        }
        catch (SQLException se)
        {
            String message = "Couldn't retrieve variableposition_id for forecast variable "
                             + config.getForecastVariable() + " with fc id " + forecastVariableID;
            throw new IOException(message, se);
        }

        if (LOGGER.isDebugEnabled())
        {
            long duration = System.currentTimeMillis() - startTime;
            LOGGER.debug("Retrieving variableposition_id for fc id {} took {}ms",
                         forecastVariableID, duration);
        }

        String innerWhere = getPairInnerWhereClauseFromConfig(config);

        return
              "WITH forecast_measurements AS (" + NEWLINE
            + "    SELECT F.forecast_date + INTERVAL '1 hour' * lead AS forecasted_date," + NEWLINE
            + "        array_agg(FV.forecasted_value * UC.factor) AS forecasts" + NEWLINE
            + "    FROM wres.Forecast F" + NEWLINE
            + "    INNER JOIN wres.ForecastEnsemble FE" + NEWLINE
            + "        ON F.forecast_id = FE.forecast_id" + NEWLINE
            + "    INNER JOIN wres.ForecastValue FV" + NEWLINE
            + "        ON FV.forecastensemble_id = FE.forecastensemble_id" + NEWLINE
            + "    INNER JOIN wres.UnitConversion UC" + NEWLINE
            + "        ON UC.from_unit = FE.measurementunit_id" + NEWLINE
            + "    WHERE lead = " + lead + NEWLINE
            + "        AND FE.variableposition_id = "
            + forecastVariablePositionID + NEWLINE
            + "        AND UC.to_unit = " + targetUnitID + NEWLINE
            + "        AND " + innerWhere + NEWLINE
            + "    GROUP BY forecasted_date" + NEWLINE
            + ")" + NEWLINE
            + "SELECT O.observed_value * UC.factor AS observation, FM.forecasts" + NEWLINE
            + "FROM forecast_measurements FM" + NEWLINE
            + "INNER JOIN wres.Observation O" + NEWLINE
            + "    ON O.observation_time = FM.forecasted_date" + NEWLINE
            + "INNER JOIN wres.UnitConversion UC" + NEWLINE
            + "    ON UC.from_unit = O.measurementunit_id" + NEWLINE
            + "WHERE O.variableposition_id = " + observationVariablePositionID + NEWLINE
            + "    AND UC.to_unit = " + targetUnitID + NEWLINE
            + "ORDER BY FM.forecasted_date;";
    }

    /**
     * The following method is obsolete after refactoring
     * MeasurementCache.getMeasurementUnitID to only throw checked exceptions
     * such as IOException or SQLException, also when
     * MeasurementCache.getMeasurementUnitID gives a meaningful error message.
     * Those are the only two justifications for this wrapper method.
     * @param unitName the unit we want the ID for
     * @return the result of successful MeasurementCache.getMeasurementUnitID
     * @throws IOException when any checked (aka non-RuntimeException) exception occurs
     */
    // TODO: refactor MeasurementCache.getMeasurementUnitID to declare it throws only checked exceptions, not Exception, then remove:
    private static int getMeasurementUnitID(String unitName) throws IOException
    {
        int result;
        try
        {
            result = MeasurementCache.getMeasurementUnitID(unitName);
        }
        catch (Exception e) // see comment below re: Exception
        {
            if (e instanceof RuntimeException)
            {
                throw (RuntimeException) e;
            }

            String message = "Couldn't retrieve/set measurement id for unit id " + unitName;
            throw new IOException(message, e);
        }
        return result;
    }

    /**
     * The following method becomes obsolete after
     * refactoring VariableCache.getVariableID to only throw checked exceptions
     * such as IOException or SQLException, also when
     * VariableCache.getVariableID gives a meaningful error message. Those are
     * the only two justifications for this wrapper method.
     * @param variableName the variable name to look for
     * @param measurementUnitId the targeted measurement unit id
     * @return the result of successful VariableCache.getVariableID
     * @throws IOException when any checked (aka non-RuntimeException) exception occurs
     */
    // TODO: refactor MeasurementCache.getMeasurementUnitID to declare it throws only checked exceptions, not Exception, then remove:
    private static int getVariableID(String variableName, int measurementUnitId) throws IOException
    {
        int result;
        try
        {
            result = VariableCache.getVariableID(variableName, measurementUnitId);
        }
        catch (Exception e) // see comment below re: Exception
        {
            if (e instanceof RuntimeException)
            {
                throw (RuntimeException) e;
            }

            String message = "Couldn't retrieve/set variableid for " + variableName;
            throw new IOException(message, e);
        }
        return result;
    }

    private static String getPairInnerWhereClauseFromConfig(PairConfig config)
    {
        if (config.getFromTime() == null && config.getToTime() == null)
        {
            return "";
        }
        else if (config.getFromTime() == null)
        {
            return "(F.forecast_date + INTERVAL '1 hour' * lead) <= '"
                    + config.getToTime().format(SQL_FORMATTER) + "'";
        }
        else if (config.getToTime() == null)
        {
            return "(F.forecast_date + INTERVAL '1 hour' * lead) >= '"
                    + config.getFromTime().format(SQL_FORMATTER) + "'";
        }
        else
        {
            return "((F.forecast_date + INTERVAL '1 hour' * lead) >= '"
                    + config.getFromTime().format(SQL_FORMATTER) + "'" + NEWLINE
                    + "             AND (F.forecast_date + INTERVAL '1 hour' * lead) <= '"
                    + config.getToTime().format(SQL_FORMATTER) + "')";
        }
    }

    /**
     * All properties optional, any/each can be null.
     */
    private static class PairConfig
    {
        private final LocalDateTime fromTime;
        private final LocalDateTime toTime;
        private final String forecastVariable;
        private final String observationVariable;
        private final String targetUnit;

        private PairConfig(LocalDateTime fromTime,
                           LocalDateTime toTime,
                           String forecastVariable,
                           String observationVariable,
                           String targetUnit)
        {
            this.fromTime = fromTime;
            this.toTime = toTime;
            this.forecastVariable = forecastVariable;
            this.observationVariable = observationVariable;
            this.targetUnit = targetUnit;
        }

        public static PairConfig of(LocalDateTime fromTime,
                                    LocalDateTime toTime,
                                    String forecastVariable,
                                    String observationVariable,
                                    String targetUnit)
        {
            return new PairConfig(fromTime,
                                  toTime,
                                  forecastVariable,
                                  observationVariable,
                                  targetUnit);
        }

        LocalDateTime getFromTime()
        {
            return this.fromTime;
        }

        LocalDateTime getToTime()
        {
            return this.toTime;
        }

        String getForecastVariable()
        {
            return this.forecastVariable;
        }

        String getObservationVariable()
        {
            return this.observationVariable;
        }

        String getTargetUnit()
        {
            return this.targetUnit;
        }
    }
}
