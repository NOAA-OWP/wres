package wres.io.writing.commaseparated;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.Format;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import wres.config.ProjectConfigException;
import wres.config.generated.ProjectConfig;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.io.config.ConfigHelper;

/**
 * Helps write files of Comma Separated Values (CSV).
 * 
 * @author jesse
 * @author james.brown@hydrosolved.com
 * @version 0.3
 * @since 0.1
 */
abstract class CommaSeparatedWriter
{

    /**
     * Delimiter for the header.
     */

    static final String HEADER_DELIMITER = " ";

    /**
     * Earliest possible time window to index the header.
     */

    static final TimeWindow HEADER_INDEX = TimeWindow.of( Instant.MIN,
                                                          Instant.MIN,
                                                          ReferenceTime.VALID_TIME,
                                                          Duration.ofSeconds( Long.MIN_VALUE ) );

    /**
     * Default information for the header.
     */

    static final StringJoiner HEADER_DEFAULT =
            new StringJoiner( "," ).add( "EARLIEST" + HEADER_DELIMITER + "TIME" )
                                   .add( "LATEST" + HEADER_DELIMITER + "TIME" )
                                   .add( "EARLIEST" + HEADER_DELIMITER
                                         + "LEAD"
                                         + HEADER_DELIMITER
                                         + "HOUR" )
                                   .add( "LATEST" + HEADER_DELIMITER
                                         + "LEAD"
                                         + HEADER_DELIMITER
                                         + "HOUR" );

    /**
     * The project configuration to write.
     */

    final ProjectConfig projectConfig;

    /**
     * Validates the input configuration for writing. Throws an exception if the configuration is invalid.
     * 
     * @param projectConfig the project configuration
     * @throws NullPointerException if the input is null
     * @throws ProjectConfigException if the project is not correctly configured for writing numerical output
     */

    static void validateProjectForWriting( ProjectConfig projectConfig ) throws ProjectConfigException
    {
        Objects.requireNonNull( projectConfig, "Specify non-null project configuration when writing outputs." );

        if ( Objects.isNull( projectConfig.getOutputs() )
             || Objects.isNull( projectConfig.getOutputs().getDestination() )
             || projectConfig.getOutputs().getDestination().isEmpty() )
        {
            throw new ProjectConfigException( projectConfig.getOutputs(),
                                              ConfigHelper.OUTPUT_CLAUSE_BOILERPLATE );
        }
    }

    /**
     * Writes the raw tabular output to file.
     * 
     * @param rows the tabular data to write
     * @param path the path to which the file should be written
     * @throws IOException if the output cannot be written
     */

    static void writeTabularOutputToFile( List<RowCompareByLeft> rows,
                                          Path outputPath )
            throws IOException
    {
        // Sort the rows before writing them
        Collections.sort( rows );

        try ( BufferedWriter w = Files.newBufferedWriter( outputPath,
                                                          StandardCharsets.UTF_8,
                                                          StandardOpenOption.CREATE,
                                                          StandardOpenOption.TRUNCATE_EXISTING ) )
        {
            for ( RowCompareByLeft row : rows )
            {
                w.write( row.getRight().toString() );
                w.write( System.lineSeparator() );
            }
        }
    }

    /**
     * Mutates the input, adding a new row.
     * 
     * @param <T> the type of values to add
     * @param rows the map of rows to mutate
     * @param timeWindow the time window
     * @param values the values to add, one for each column
     * @param formatter an optional formatter
     * @param append is true to add the values to an existing row with the same time window, false otherwise
     */

    static <T> void addRowToInput( List<RowCompareByLeft> rows,
                                   TimeWindow timeWindow,
                                   List<T> values,
                                   Format formatter,
                                   boolean append )
    {
        StringJoiner row = null;
        int rowIndex = rows.indexOf( RowCompareByLeft.of( timeWindow, null ) );
        // Set the row to append if it exists and appending is required
        if ( rowIndex > -1 && append )
        {
            row = rows.get( rowIndex ).getRight();
        }
        // Otherwise, start a new row 
        else
        {
            row = new StringJoiner( "," );
            row.add( timeWindow.getEarliestTime().toString() );
            row.add( timeWindow.getLatestTime().toString() );
            row.add( Long.toString( timeWindow.getEarliestLeadTimeInHours() ) );
            row.add( Long.toString( timeWindow.getLatestLeadTimeInHours() ) );
            rows.add( RowCompareByLeft.of( timeWindow, row ) );
        }

        for ( T nextColumn : values )
        {

            String toWrite = "NA";

            // Write the current score component at the current window and threshold
            if ( nextColumn != null && !Double.valueOf( Double.NaN ).equals( nextColumn ) )
            {
                if ( formatter != null )
                {
                    toWrite = formatter.format( nextColumn );
                }
                else
                {
                    toWrite = nextColumn.toString();
                }
            }
            row.add( toWrite );
        }
    }

    /**
     * A helper class that contains a single row whose natural order is based on the {@link TimeWindow} of the row
     * and not the contents. All comparisons are based on the left value only.
     * 
     * @author james.brown@hydrosolved.com
     * @version 0.1
     * @since version 0.4
     */

    static class RowCompareByLeft implements Comparable<RowCompareByLeft>
    {
        /**
         * The row time window.
         */
        private final TimeWindow left;

        /**
         * The row value.
         */

        private final StringJoiner right;

        /**
         * Returns an instance for the given input.
         * 
         * @param timeWindow the time window
         * @param value the row value
         * @return an instance 
         */

        static RowCompareByLeft of( TimeWindow timeWindow, StringJoiner value )
        {
            return new RowCompareByLeft( timeWindow, value );
        }

        /**
         * Returns the left value.
         * 
         * @return the left value
         */

        TimeWindow getLeft()
        {
            return left;
        }

        /**
         * Returns the right value
         * 
         * @return the right value
         */

        StringJoiner getRight()
        {
            return right;
        }

        @Override
        public int compareTo( RowCompareByLeft compareTo )
        {
            Objects.requireNonNull( compareTo, "Specify a non-null input row for comparison." );
            return getLeft().compareTo( compareTo.getLeft() );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( ! ( o instanceof RowCompareByLeft ) )
            {
                return false;
            }
            RowCompareByLeft in = (RowCompareByLeft) o;
            return Objects.equals( in.getLeft(), getLeft() );
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode( left );
        }

        /**
         * Constructor.
         * 
         * @param timeWindow the time window
         * @param value the row value
         */

        private RowCompareByLeft( TimeWindow timeWindow, StringJoiner value )
        {
            Objects.requireNonNull( timeWindow, "Specify a non-null time window for the row." );
            left = timeWindow;
            right = value;
        }
    }

    /**
     * Constructor.
     * 
     * @param projectConfig the project configuration
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    CommaSeparatedWriter( ProjectConfig projectConfig ) throws ProjectConfigException
    {
        // Validate project for writing
        CommaSeparatedWriter.validateProjectForWriting( projectConfig );
        this.projectConfig = projectConfig;
    }

}
