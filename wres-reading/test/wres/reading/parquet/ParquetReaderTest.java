package wres.reading.parquet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.config.components.DatasetBuilder;
import wres.config.components.SourceBuilder;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.reading.DataSource;
import wres.reading.TimeSeriesTuple;
import wres.statistics.MessageUtilities;

/**
 * Test reading of parquet files.
 *
 * @author James Brown
 */

class ParquetReaderTest
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ParquetReaderTest.class );

    @Test
    void testReadParquetFile() throws IOException
    {
        // Unfortunately, writing to an in-memory file system will not work when using DuckDB, which requires a native
        // file path.

        Path parquetPath = Files.createTempFile( "wres_test_", ".parquet" );
        LOGGER.debug( "Wrote a temporary Parquet file to {}.", parquetPath );

        // Write the test bytes
        byte[] parquetBytes = this.getTestParquetBytes();
        Files.write( parquetPath, parquetBytes );

        DataSource dataSource = DataSource.builder()
                                          .context( DatasetBuilder.builder()
                                                                  .build() )
                                          .source( SourceBuilder.builder()
                                                                .build() )
                                          .links( Collections.emptyList() )
                                          .uri( parquetPath.toUri() )
                                          .disposition( DataSource.DataDisposition.PARQUET )
                                          .build();

        ParquetReader reader = ParquetReader.of();
        List<TimeSeries<Double>> actual = reader.read( dataSource )
                                                .map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                .toList();

        TimeSeriesMetadata expectedMetadata =
                TimeSeriesMetadata.of( Collections.emptyMap(),
                                       null,
                                       "streamflow",
                                       Feature.of( MessageUtilities.getGeometry( "50147800" ) ),
                                       "m3/s" );

        TimeSeries<Double> expectedOne =
                new TimeSeries.Builder<Double>().setMetadata( expectedMetadata )
                                                .addEvent( Event.of( Instant.parse( "2015-10-01T01:00:00Z" ),
                                                                     0.43953895568847656 ) )
                                                .addEvent( Event.of( Instant.parse( "2015-10-01T02:00:00Z" ),
                                                                     0.8975731730461121 ) )
                                                .addEvent( Event.of( Instant.parse( "2015-10-01T03:00:00Z" ),
                                                                     1.2245831489562988 ) )
                                                .build();

        List<TimeSeries<Double>> expected = List.of( expectedOne );

        assertEquals( expected, actual );

        // Clean up
        if ( Files.exists( parquetPath ) )
        {
            LOGGER.debug( "Deleted the temporary parquet file at {}.", parquetPath );
            Files.delete( parquetPath );
        }
    }

    /**
     * @return a parquet byte stream
     */

    private byte[] getTestParquetBytes()
    {
        String base64String = """
                UEFSMRUEFTAVNEwVBhUAEgAAGFwAoANqNuoIFABAvJp87QgUAOB0y8LwCBQVABUUFRgsFQYVEBUGFQYcGAgA4HTLwvAIFBgIAKADajbqCBQWACgIAOB0y8Lw
                CBQYCACgA2o26ggUEREAAAAKJAIAAAAGAQIDJAAVBBUYFRxMFQYVABIAAAwsQAvhPlvHZT8kv5w/FQAVFBUYLBUGFRAVBhUGHBgEJL+cPxgEQAvhPhYAKAQk
                v5w/GARAC+E+EREAAAAKJAIAAAAGAQIDJAAVBBk8NQAYBnNjaGVtYRUEABUEJQIYCnZhbHVlX3RpbWVsjBIcPAAAAAAAFQglAhgIc2ltX2Zsb3cAFgYZHBks
                JgAcFQQZNQAGEBkYCnZhbHVlX3RpbWUVAhYGFt4BFuYBJlgmCBwYCADgdMvC8AgUGAgAoANqNuoIFBYAKAgA4HTLwvAIFBgIAKADajbqCBQREQAZLBUEFQAV
                AgAVABUQFQIAPCkGGSYABgAAACYAHBUIGTUABhAZGAhzaW1fZmxvdxUCFgYWpgEWrgEmpgIm7gEcGAQkv5w/GARAC+E+FgAoBCS/nD8YBEAL4T4REQAZLBUE
                FQAVAgAVABUQFQIAPCkGGSYABgAAABaEAxYGJggWlAMAGVwYBnBhbmRhcxiTA3siaW5kZXhfY29sdW1ucyI6IFtdLCAiY29sdW1uX2luZGV4ZXMiOiBbXSwg
                ImNvbHVtbnMiOiBbeyJuYW1lIjogInZhbHVlX3RpbWUiLCAiZmllbGRfbmFtZSI6ICJ2YWx1ZV90aW1lIiwgInBhbmRhc190eXBlIjogImRhdGV0aW1lIiwg
                Im51bXB5X3R5cGUiOiAiZGF0ZXRpbWU2NFtuc10iLCAibWV0YWRhdGEiOiBudWxsfSwgeyJuYW1lIjogInNpbV9mbG93IiwgImZpZWxkX25hbWUiOiAic2lt
                X2Zsb3ciLCAicGFuZGFzX3R5cGUiOiAiZmxvYXQzMiIsICJudW1weV90eXBlIjogImZsb2F0MzIiLCAibWV0YWRhdGEiOiBudWxsfV0sICJhdHRyaWJ1dGVz
                Ijoge30sICJjcmVhdG9yIjogeyJsaWJyYXJ5IjogInB5YXJyb3ciLCAidmVyc2lvbiI6ICIyMy4wLjEifSwgInBhbmRhc192ZXJzaW9uIjogIjMuMC4xIn0A
                GApmZWF0dXJlX2lkGAg1MDE0NzgwMAAYDXZhcmlhYmxlX25hbWUYCnN0cmVhbWZsb3cAGBBtZWFzdXJlbWVudF91bml0GARtMy9zABgMQVJST1c6c2NoZW1h
                GKAILy8vLy94QURBQUFRQUFBQUFBQUtBQTRBQmdBRkFBZ0FDZ0FBQUFBQkJBQVFBQUFBQUFBS0FBd0FBQUFFQUFnQUNnQUFBR0FDQUFBRUFBQUFCQUFBQUtR
                QUFBQnNBQUFBT0FBQUFBUUFBQUIwLy8vL0ZBQUFBQVFBQUFBRUFBQUFiVE12Y3dBQUFBQVFBQUFBYldWaGMzVnlaVzFsYm5SZmRXNXBkQUFBQUFDay8vLy9H
                QUFBQUFRQUFBQUtBQUFBYzNSeVpXRnRabXh2ZHdBQURRQUFBSFpoY21saFlteGxYMjVoYldVQUFBRFUvLy8vR0FBQUFBUUFBQUFJQUFBQU5UQXhORGM0TURB
                QUFBQUFDZ0FBQUdabFlYUjFjbVZmYVdRQUFBZ0FEQUFFQUFnQUNBQUFBS0FCQUFBRUFBQUFrd0VBQUhzaWFXNWtaWGhmWTI5c2RXMXVjeUk2SUZ0ZExDQWlZ
                MjlzZFcxdVgybHVaR1Y0WlhNaU9pQmJYU3dnSW1OdmJIVnRibk1pT2lCYmV5SnVZVzFsSWpvZ0luWmhiSFZsWDNScGJXVWlMQ0FpWm1sbGJHUmZibUZ0WlNJ
                NklDSjJZV3gxWlY5MGFXMWxJaXdnSW5CaGJtUmhjMTkwZVhCbElqb2dJbVJoZEdWMGFXMWxJaXdnSW01MWJYQjVYM1I1Y0dVaU9pQWlaR0YwWlhScGJXVTJO
                RnR1YzEwaUxDQWliV1YwWVdSaGRHRWlPaUJ1ZFd4c2ZTd2dleUp1WVcxbElqb2dJbk5wYlY5bWJHOTNJaXdnSW1acFpXeGtYMjVoYldVaU9pQWljMmx0WDJa
                c2IzY2lMQ0FpY0dGdVpHRnpYM1I1Y0dVaU9pQWlabXh2WVhRek1pSXNJQ0p1ZFcxd2VWOTBlWEJsSWpvZ0ltWnNiMkYwTXpJaUxDQWliV1YwWVdSaGRHRWlP
                aUJ1ZFd4c2ZWMHNJQ0poZEhSeWFXSjFkR1Z6SWpvZ2UzMHNJQ0pqY21WaGRHOXlJam9nZXlKc2FXSnlZWEo1SWpvZ0luQjVZWEp5YjNjaUxDQWlkbVZ5YzJs
                dmJpSTZJQ0l5TXk0d0xqRWlmU3dnSW5CaGJtUmhjMTkyWlhKemFXOXVJam9nSWpNdU1DNHhJbjBBQmdBQUFIQmhibVJoY3dBQUFnQUFBRWdBQUFBRUFBQUEw
                UC8vL3dBQUFRTVFBQUFBSEFBQUFBUUFBQUFBQUFBQUNBQUFBSE5wYlY5bWJHOTNBQUFBQUw3Ly8vOEFBQUVBRUFBVUFBZ0FCZ0FIQUF3QUFBQVFBQkFBQUFB
                QUFBRUtFQUFBQUNRQUFBQUVBQUFBQUFBQUFBb0FBQUIyWVd4MVpWOTBhVzFsQUFBQUFBWUFDQUFHQUFZQUFBQUFBQU1BABggcGFycXVldC1jcHAtYXJyb3cg
                dmVyc2lvbiAyMy4wLjEZLBwAABwAAABfBwAAUEFSMQ==
                """;
        return Base64.getMimeDecoder()
                     .decode( base64String.trim() );
    }
}
