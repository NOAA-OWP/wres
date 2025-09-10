package wres.reading;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

import wres.config.MetricConstants;
import wres.config.components.DataType;
import wres.config.components.Dataset;
import wres.config.components.DatasetBuilder;
import wres.config.components.DatasetOrientation;
import wres.config.components.EvaluationDeclaration;
import wres.config.components.EvaluationDeclarationBuilder;
import wres.config.components.FeatureAuthority;
import wres.config.components.FeatureGroups;
import wres.config.components.FeatureGroupsBuilder;
import wres.config.components.Features;
import wres.config.components.FeaturesBuilder;
import wres.config.components.Metric;
import wres.config.components.MetricBuilder;
import wres.config.components.MetricParametersBuilder;
import wres.config.components.Source;
import wres.config.components.SourceBuilder;
import wres.config.components.SourceInterface;
import wres.config.components.ThresholdBuilder;
import wres.config.components.ThresholdSource;
import wres.config.components.ThresholdSourceBuilder;
import wres.config.components.ThresholdType;
import wres.config.components.TimeInterval;
import wres.config.components.TimeIntervalBuilder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.types.Ensemble;
import wres.http.WebClient;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.ReferenceTime;
import wres.statistics.generated.Threshold;
import wres.statistics.generated.TimeScale;

/**
 * Tests the {@link ReaderUtilities}.
 * @author James Brown
 */

class ReaderUtilitiesTest
{
    /** Test file name. */
    private static final String TEST_JSON = "test.json";

    /** Re-used string. */
    private static final String TEST = "test";

    /** Mocker server instance. */
    private ClientAndServer mockServer;

    /** Unit string. */
    private static final String FT = "FT";

    /**
     * <p>The response used is created from this URL:
     * <a href="https://redacted/api/location/v3.0/nws_threshold/nws_lid/PTSA1,MNTG1,BLOF1,CEDG1,SMAF1/">https://redacted/api/location/v3.0/nws_threshold/nws_lid/PTSA1,MNTG1,BLOF1,CEDG1,SMAF1/</a>
     *
     * <p>Executed on 5/20/2021 at 10:15am.
     */
    private static final String RESPONSE = """
            {
                "_metrics": {
                    "threshold_count": 10,
                    "total_request_time": 0.39240050315856934
                },
                "_warnings": [],
                "_documentation": {
                    "swagger URL": "redacted/docs/location/v3.0/swagger/"
                },
                "deployment": {
                    "api_url": "https://redacted/api/location/v3.0/nws_threshold/nws_lid/PTSA1,MNTG1,BLOF1,CEDG1,SMAF1/",
                    "stack": "prod",
                    "version": "v3.1.0"
                },
                "data_sources": {
                    "metadata_sources": [
                        "NWS data: NRLDB - Last updated: 2021-05-04 17:44:31 UTC",
                        "USGS data: USGS NWIS - Last updated: 2021-05-04 17:15:04 UTC"
                    ],
                    "crosswalk_datasets": {
                        "location_nwm_crosswalk_dataset": {
                            "location_nwm_crosswalk_dataset_id": "1.1",
                            "name": "Location NWM Crosswalk v1.1",
                            "description": "Created 20201106.  Source 1) NWM Routelink File v2.1   2) NHDPlus v2.1   3) GID"
                        },
                        "nws_usgs_crosswalk_dataset": {
                            "nws_usgs_crosswalk_dataset_id": "1.0",
                            "name": "NWS Station to USGS Gages 1.0",
                            "description": "Authoritative 1.0 dataset mapping NWS Stations to USGS Gages"
                        }
                    }
                },
                "value_set": [
                    {
                        "metadata": {
                            "data_type": "NWS Stream Thresholds",
                            "nws_lid": "PTSA1",
                            "usgs_site_code": "02372250",
                            "nwm_feature_id": "2323396",
                            "threshold_type": "all (stage,flow)",
                            "stage_units": "FT",
                            "flow_units": "CFS",
                            "calc_flow_units": "CFS",
                            "threshold_source": "NWS-NRLDB",
                            "threshold_source_description": "National Weather Service - National River Location Database"
                        },
                        "stage_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": null
                        },
                        "flow_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": null
                        },
                        "calc_flow_values": {
                            "low": null,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": null,
                            "rating_curve": {
                                "location_id": "PTSA1",
                                "id_type": "NWS Station",
                                "source": "NRLDB",
                                "description": "National Weather Service - National River Location Database",
                                "interpolation_method": null,
                                "interpolation_description": null
                            }
                        }
                    },
                    {
                        "metadata": {
                            "data_type": "NWS Stream Thresholds",
                            "nws_lid": "PTSA1",
                            "usgs_site_code": "02372250",
                            "nwm_feature_id": "2323396",
                            "threshold_type": "all (stage,flow)",
                            "stage_units": "FT",
                            "flow_units": "CFS",
                            "calc_flow_units": "CFS",
                            "threshold_source": "NWS-NRLDB",
                            "threshold_source_description": "National Weather Service - National River Location Database"
                        },
                        "stage_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": null
                        },
                        "flow_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": null
                        },
                        "calc_flow_values": {
                            "low": null,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": null,
                            "rating_curve": {
                                "location_id": "02372250",
                                "id_type": "USGS Gage",
                                "source": "USGS Rating Depot",
                                "description": "The EXSA rating curves provided by USGS",
                                "interpolation_method": "logarithmic",
                                "interpolation_description": "logarithmic"
                            }
                        }
                    },
                    {
                        "metadata": {
                            "data_type": "NWS Stream Thresholds",
                            "nws_lid": "MNTG1",
                            "usgs_site_code": "02349605",
                            "nwm_feature_id": "6444276",
                            "threshold_type": "all (stage,flow)",
                            "stage_units": "FT",
                            "flow_units": "CFS",
                            "calc_flow_units": "CFS",
                            "threshold_source": "NWS-NRLDB",
                            "threshold_source_description": "National Weather Service - National River Location Database"
                        },
                        "stage_values": {
                            "low": -0.16,
                            "bankfull": 11.0,
                            "action": 11.0,
                            "flood": 20.0,
                            "minor": 20.0,
                            "moderate": 28.0,
                            "major": 31.0,
                            "record": 34.11
                        },
                        "flow_values": {
                            "low": null,
                            "bankfull": null,
                            "action": 11900.0,
                            "flood": 31500.0,
                            "minor": 31500.0,
                            "moderate": 77929.0,
                            "major": 105100.0,
                            "record": 136000.0
                        },
                        "calc_flow_values": {
                            "low": 557.8,
                            "bankfull": 9379.0,
                            "action": 9379.0,
                            "flood": 35331.0,
                            "minor": 35331.0,
                            "moderate": 102042.0,
                            "major": 142870.0,
                            "record": null,
                            "rating_curve": {
                                "location_id": "MNTG1",
                                "id_type": "NWS Station",
                                "source": "NRLDB",
                                "description": "National Weather Service - National River Location Database",
                                "interpolation_method": null,
                                "interpolation_description": null
                            }
                        }
                    },
                    {
                        "metadata": {
                            "data_type": "NWS Stream Thresholds",
                            "nws_lid": "MNTG1",
                            "usgs_site_code": "02349605",
                            "nwm_feature_id": "6444276",
                            "threshold_type": "all (stage,flow)",
                            "stage_units": "FT",
                            "flow_units": "CFS",
                            "calc_flow_units": "CFS",
                            "threshold_source": "NWS-NRLDB",
                            "threshold_source_description": "National Weather Service - National River Location Database"
                        },
                        "stage_values": {
                            "low": -0.16,
                            "bankfull": 11.0,
                            "action": 11.0,
                            "flood": 20.0,
                            "minor": 20.0,
                            "moderate": 28.0,
                            "major": 31.0,
                            "record": 34.11
                        },
                        "flow_values": {
                            "low": null,
                            "bankfull": null,
                            "action": 11900.0,
                            "flood": 31500.0,
                            "minor": 31500.0,
                            "moderate": 77929.0,
                            "major": 105100.0,
                            "record": 136000.0
                        },
                        "calc_flow_values": {
                            "low": 554.06,
                            "bankfull": 9390.0,
                            "action": 9390.0,
                            "flood": 35329.0,
                            "minor": 35329.0,
                            "moderate": 102040.6,
                            "major": 142867.9,
                            "record": null,
                            "rating_curve": {
                                "location_id": "02349605",
                                "id_type": "USGS Gage",
                                "source": "USGS Rating Depot",
                                "description": "The EXSA rating curves provided by USGS",
                                "interpolation_method": "logarithmic",
                                "interpolation_description": "logarithmic"
                            }
                        }
                    },
                    {
                        "metadata": {
                            "data_type": "NWS Stream Thresholds",
                            "nws_lid": "BLOF1",
                            "usgs_site_code": "02358700",
                            "nwm_feature_id": "2297254",
                            "threshold_type": "all (stage,flow)",
                            "stage_units": "FT",
                            "flow_units": "CFS",
                            "calc_flow_units": "CFS",
                            "threshold_source": "NWS-NRLDB",
                            "threshold_source_description": "National Weather Service - National River Location Database"
                        },
                        "stage_values": {
                            "low": null,
                            "bankfull": 15.0,
                            "action": 13.0,
                            "flood": 17.0,
                            "minor": 17.0,
                            "moderate": 23.5,
                            "major": 26.0,
                            "record": 28.6
                        },
                        "flow_values": {
                            "low": null,
                            "bankfull": null,
                            "action": null,
                            "flood": 36900.0,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": 209000.0
                        },
                        "calc_flow_values": {
                            "low": null,
                            "bankfull": 38633.0,
                            "action": 31313.0,
                            "flood": 48628.0,
                            "minor": 48628.0,
                            "moderate": 144077.0,
                            "major": 216266.0,
                            "record": null,
                            "rating_curve": {
                                "location_id": "BLOF1",
                                "id_type": "NWS Station",
                                "source": "NRLDB",
                                "description": "National Weather Service - National River Location Database",
                                "interpolation_method": null,
                                "interpolation_description": null
                            }
                        }
                    },
                    {
                        "metadata": {
                            "data_type": "NWS Stream Thresholds",
                            "nws_lid": "BLOF1",
                            "usgs_site_code": "02358700",
                            "nwm_feature_id": "2297254",
                            "threshold_type": "all (stage,flow)",
                            "stage_units": "FT",
                            "flow_units": "CFS",
                            "calc_flow_units": "CFS",
                            "threshold_source": "NWS-NRLDB",
                            "threshold_source_description": "National Weather Service - National River Location Database"
                        },
                        "stage_values": {
                            "low": null,
                            "bankfull": 15.0,
                            "action": 13.0,
                            "flood": 17.0,
                            "minor": 17.0,
                            "moderate": 23.5,
                            "major": 26.0,
                            "record": 28.6
                        },
                        "flow_values": {
                            "low": null,
                            "bankfull": null,
                            "action": null,
                            "flood": 36900.0,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": 209000.0
                        },
                        "calc_flow_values": {
                            "low": null,
                            "bankfull": 36928.1,
                            "action": 30031.5,
                            "flood": 46234.6,
                            "minor": 46234.6,
                            "moderate": 133995.6,
                            "major": 205562.6,
                            "record": null,
                            "rating_curve": {
                                "location_id": "02358700",
                                "id_type": "USGS Gage",
                                "source": "USGS Rating Depot",
                                "description": "The EXSA rating curves provided by USGS",
                                "interpolation_method": "logarithmic",
                                "interpolation_description": "logarithmic"
                            }
                        }
                    },
                    {
                        "metadata": {
                            "data_type": "NWS Stream Thresholds",
                            "nws_lid": "CEDG1",
                            "usgs_site_code": "02343940",
                            "nwm_feature_id": "2310009",
                            "threshold_type": "all (stage,flow)",
                            "stage_units": "FT",
                            "flow_units": "CFS",
                            "calc_flow_units": "CFS",
                            "threshold_source": "NWS-NRLDB",
                            "threshold_source_description": "National Weather Service - National River Location Database"
                        },
                        "stage_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": 14.29
                        },
                        "flow_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": null
                        },
                        "calc_flow_values": {
                            "low": null,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": null,
                            "rating_curve": {
                                "location_id": "CEDG1",
                                "id_type": "NWS Station",
                                "source": "NRLDB",
                                "description": "National Weather Service - National River Location Database",
                                "interpolation_method": null,
                                "interpolation_description": null
                            }
                        }
                    },
                    {
                        "metadata": {
                            "data_type": "NWS Stream Thresholds",
                            "nws_lid": "CEDG1",
                            "usgs_site_code": "02343940",
                            "nwm_feature_id": "2310009",
                            "threshold_type": "all (stage,flow)",
                            "stage_units": "FT",
                            "flow_units": "CFS",
                            "calc_flow_units": "CFS",
                            "threshold_source": "NWS-NRLDB",
                            "threshold_source_description": "National Weather Service - National River Location Database"
                        },
                        "stage_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": 14.29
                        },
                        "flow_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": null
                        },
                        "calc_flow_values": {
                            "low": null,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": null,
                            "rating_curve": {
                                "location_id": "02343940",
                                "id_type": "USGS Gage",
                                "source": "USGS Rating Depot",
                                "description": "The EXSA rating curves provided by USGS",
                                "interpolation_method": "logarithmic",
                                "interpolation_description": "logarithmic"
                            }
                        }
                    },
                    {
                        "metadata": {
                            "data_type": "NWS Stream Thresholds",
                            "nws_lid": "SMAF1",
                            "usgs_site_code": "02359170",
                            "nwm_feature_id": "2298964",
                            "threshold_type": "all (stage,flow)",
                            "stage_units": "FT",
                            "flow_units": "CFS",
                            "calc_flow_units": "CFS",
                            "threshold_source": "NWS-NRLDB",
                            "threshold_source_description": "National Weather Service - National River Location Database"
                        },
                        "stage_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": 8.0,
                            "flood": 9.5,
                            "minor": 9.5,
                            "moderate": 11.5,
                            "major": 13.5,
                            "record": 15.36
                        },
                        "flow_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": 179000.0
                        },
                        "calc_flow_values": {
                            "low": null,
                            "bankfull": null,
                            "action": 45700.0,
                            "flood": 67700.0,
                            "minor": 67700.0,
                            "moderate": 107000.0,
                            "major": 159000.0,
                            "record": 221000.0,
                            "rating_curve": {
                                "location_id": "SMAF1",
                                "id_type": "NWS Station",
                                "source": "NRLDB",
                                "description": "National Weather Service - National River Location Database",
                                "interpolation_method": null,
                                "interpolation_description": null
                            }
                        }
                    },
                    {
                        "metadata": {
                            "data_type": "NWS Stream Thresholds",
                            "nws_lid": "SMAF1",
                            "usgs_site_code": "02359170",
                            "nwm_feature_id": "2298964",
                            "threshold_type": "all (stage,flow)",
                            "stage_units": "FT",
                            "flow_units": "CFS",
                            "calc_flow_units": "CFS",
                            "threshold_source": "NWS-NRLDB",
                            "threshold_source_description": "National Weather Service - National River Location Database"
                        },
                        "stage_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": 8.0,
                            "flood": 9.5,
                            "minor": 9.5,
                            "moderate": 11.5,
                            "major": 13.5,
                            "record": 15.36
                        },
                        "flow_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": 179000.0
                        },
                        "calc_flow_values": {
                            "low": null,
                            "bankfull": null,
                            "action": 47355.4,
                            "flood": 69570.6,
                            "minor": 69570.6,
                            "moderate": 108505.8,
                            "major": 159463.2,
                            "record": 218963.05,
                            "rating_curve": {
                                "location_id": "02359170",
                                "id_type": "USGS Gage",
                                "source": "USGS Rating Depot",
                                "description": "The EXSA rating curves provided by USGS",
                                "interpolation_method": "logarithmic",
                                "interpolation_description": "logarithmic"
                            }
                        }
                    }
                ]
            }
            """;

    @BeforeEach
    void startServer()
    {
        this.mockServer = ClientAndServer.startClientAndServer( 0 );
    }

    @AfterEach
    void stopServer()
    {
        this.mockServer.stop();
    }

    @Test
    void testSplitByDelimiter()
    {
        String csvToSplit = "\"foo, bar\",\"baz\",qux,";
        String[] actual = ReaderUtilities.splitByDelimiter( csvToSplit, ',' );
        assertArrayEquals( new String[] { "foo, bar", "baz", "qux", "" }, actual );
    }

    @Test
    void testSplitByDelimiterIssue100674()
    {
        String csvToSplit = "\"Seboeis River near Shin Pond, Maine\"";
        String[] actual = ReaderUtilities.splitByDelimiter( csvToSplit, ',' );
        assertArrayEquals( new String[] { "Seboeis River near Shin Pond, Maine" }, actual );
    }

    @Test
    void testReadThresholdsFromFileSystemAndFillDeclaration() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new WRDS JSON service response to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, TEST_JSON );
            Path jsonPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( jsonPath ) )
            {
                writer.append( RESPONSE );
            }

            ThresholdSource service
                    = ThresholdSourceBuilder.builder()
                                            .uri( jsonPath.toUri() )
                                            .featureNameFrom( DatasetOrientation.LEFT )
                                            .type( wres.config.components.ThresholdType.VALUE )
                                            .parameter( "stage" )
                                            .provider( "NWS-NRLDB" )
                                            .operator( wres.config.components.ThresholdOperator.GREATER )
                                            .build();

            Dataset left = DatasetBuilder.builder()
                                         .featureAuthority( FeatureAuthority.NWS_LID )
                                         .build();

            GeometryTuple first = GeometryTuple.newBuilder()
                                               .setLeft( Geometry.newBuilder()
                                                                 .setName( "PTSA1" ) )
                                               .build();
            GeometryTuple second = GeometryTuple.newBuilder()
                                                .setLeft( Geometry.newBuilder()
                                                                  .setName( "MNTG1" ) )
                                                .build();
            GeometryTuple third = GeometryTuple.newBuilder()
                                               .setLeft( Geometry.newBuilder()
                                                                 .setName( "BLOF1" ) )
                                               .build();
            GeometryTuple fourth = GeometryTuple.newBuilder()
                                                .setLeft( Geometry.newBuilder()
                                                                  .setName( "CEDG1" ) )
                                                .build();
            GeometryTuple fifth = GeometryTuple.newBuilder()
                                               .setLeft( Geometry.newBuilder()
                                                                 .setName( "SMAF1" ) )
                                               .build();

            Set<GeometryTuple> geometries = Set.of( first, second, third, fourth, fifth );
            Features features = FeaturesBuilder.builder()
                                               .geometries( geometries )
                                               .build();

            Set<Metric> metrics = Set.of( new Metric( MetricConstants.MEAN_ABSOLUTE_ERROR, null ) );

            EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                            .left( left )
                                                                            .features( features )
                                                                            .thresholdSources( Set.of( service ) )
                                                                            .metrics( metrics )
                                                                            .build();

            EvaluationDeclaration actual = ReaderUtilities.readAndFillThresholds( declaration );

            // Build the expected declaration
            Set<wres.config.components.Threshold> expectedThresholds = new HashSet<>();
            Geometry firstGeometry = Geometry.newBuilder()
                                             .setName( "SMAF1" )
                                             .build();

            Threshold firstOne = Threshold.newBuilder()
                                          .setOperator( Threshold.ThresholdOperator.GREATER )
                                          .setObservedThresholdValue( 0.0 )
                                          .setThresholdValueUnits( FT )
                                          .setName( "low" )
                                          .build();
            Threshold firstTwo = Threshold.newBuilder()
                                          .setOperator( Threshold.ThresholdOperator.GREATER )
                                          .setObservedThresholdValue( 8.0 )
                                          .setThresholdValueUnits( FT )
                                          .setName( "action" )
                                          .build();
            Threshold firstThree = Threshold.newBuilder()
                                            .setOperator( Threshold.ThresholdOperator.GREATER )
                                            .setObservedThresholdValue( 9.5 )
                                            .setThresholdValueUnits( FT )
                                            .setName( "flood" )
                                            .build();
            Threshold firstFour = Threshold.newBuilder()
                                           .setOperator( Threshold.ThresholdOperator.GREATER )
                                           .setObservedThresholdValue( 9.5 )
                                           .setThresholdValueUnits( FT )
                                           .setName( "minor" )
                                           .build();
            Threshold firstFive = Threshold.newBuilder()
                                           .setOperator( Threshold.ThresholdOperator.GREATER )
                                           .setObservedThresholdValue( 11.5 )
                                           .setThresholdValueUnits( FT )
                                           .setName( "moderate" )
                                           .build();
            Threshold firstSix = Threshold.newBuilder()
                                          .setOperator( Threshold.ThresholdOperator.GREATER )
                                          .setObservedThresholdValue( 13.5 )
                                          .setThresholdValueUnits( FT )
                                          .setName( "major" )
                                          .build();
            Threshold firstSeven = Threshold.newBuilder()
                                            .setOperator( Threshold.ThresholdOperator.GREATER )
                                            .setObservedThresholdValue( 15.36 )
                                            .setThresholdValueUnits( FT )
                                            .setName( "record" )
                                            .build();

            wres.config.components.Threshold firstOneW
                    = ThresholdBuilder.builder()
                                      .threshold( firstOne )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( firstGeometry )
                                      .build();
            wres.config.components.Threshold firstTwoW
                    = ThresholdBuilder.builder()
                                      .threshold( firstTwo )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( firstGeometry )
                                      .build();
            wres.config.components.Threshold firstThreeW
                    = ThresholdBuilder.builder()
                                      .threshold( firstThree )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( firstGeometry )
                                      .build();
            wres.config.components.Threshold firstFourW
                    = ThresholdBuilder.builder()
                                      .threshold( firstFour )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( firstGeometry )
                                      .build();
            wres.config.components.Threshold firstFiveW
                    = ThresholdBuilder.builder()
                                      .threshold( firstFive )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( firstGeometry )
                                      .build();
            wres.config.components.Threshold firstSixW
                    = ThresholdBuilder.builder()
                                      .threshold( firstSix )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( firstGeometry )
                                      .build();
            wres.config.components.Threshold firstSevenW
                    = ThresholdBuilder.builder()
                                      .threshold( firstSeven )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( firstGeometry )
                                      .build();

            expectedThresholds.add( firstOneW );
            expectedThresholds.add( firstTwoW );
            expectedThresholds.add( firstThreeW );
            expectedThresholds.add( firstFourW );
            expectedThresholds.add( firstFiveW );
            expectedThresholds.add( firstSixW );
            expectedThresholds.add( firstSevenW );

            Geometry secondGeometry = Geometry.newBuilder()
                                              .setName( "CEDG1" )
                                              .build();

            Threshold secondOne = Threshold.newBuilder()
                                           .setOperator( Threshold.ThresholdOperator.GREATER )
                                           .setObservedThresholdValue( 0.0 )
                                           .setThresholdValueUnits( FT )
                                           .setName( "low" )
                                           .build();
            Threshold secondTwo = Threshold.newBuilder()
                                           .setOperator( Threshold.ThresholdOperator.GREATER )
                                           .setObservedThresholdValue( 14.29 )
                                           .setThresholdValueUnits( FT )
                                           .setName( "record" )
                                           .build();

            wres.config.components.Threshold secondOneW
                    = ThresholdBuilder.builder()
                                      .threshold( secondOne )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( secondGeometry )
                                      .build();
            wres.config.components.Threshold secondTwoW
                    = ThresholdBuilder.builder()
                                      .threshold( secondTwo )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( secondGeometry )
                                      .build();

            expectedThresholds.add( secondOneW );
            expectedThresholds.add( secondTwoW );

            Geometry thirdGeometry = Geometry.newBuilder()
                                             .setName( "PTSA1" )
                                             .build();

            Threshold thirdOne = Threshold.newBuilder()
                                          .setOperator( Threshold.ThresholdOperator.GREATER )
                                          .setObservedThresholdValue( 0.0 )
                                          .setThresholdValueUnits( FT )
                                          .setName( "low" )
                                          .build();

            wres.config.components.Threshold thirdOneW
                    = ThresholdBuilder.builder()
                                      .threshold( thirdOne )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( thirdGeometry )
                                      .build();

            expectedThresholds.add( thirdOneW );

            Geometry fourthGeometry = Geometry.newBuilder()
                                              .setName( "BLOF1" )
                                              .build();

            Threshold fourthOne = Threshold.newBuilder()
                                           .setOperator( Threshold.ThresholdOperator.GREATER )
                                           .setObservedThresholdValue( 13.0 )
                                           .setThresholdValueUnits( FT )
                                           .setName( "action" )
                                           .build();
            Threshold fourthTwo = Threshold.newBuilder()
                                           .setOperator( Threshold.ThresholdOperator.GREATER )
                                           .setObservedThresholdValue( 15.0 )
                                           .setThresholdValueUnits( FT )
                                           .setName( "bankfull" )
                                           .build();
            Threshold fourthThree = Threshold.newBuilder()
                                             .setOperator( Threshold.ThresholdOperator.GREATER )
                                             .setObservedThresholdValue( 17.0 )
                                             .setThresholdValueUnits( FT )
                                             .setName( "flood" )
                                             .build();
            Threshold fourthFour = Threshold.newBuilder()
                                            .setOperator( Threshold.ThresholdOperator.GREATER )
                                            .setObservedThresholdValue( 17.0 )
                                            .setThresholdValueUnits( FT )
                                            .setName( "minor" )
                                            .build();
            Threshold fourthFive = Threshold.newBuilder()
                                            .setOperator( Threshold.ThresholdOperator.GREATER )
                                            .setObservedThresholdValue( 23.5 )
                                            .setThresholdValueUnits( FT )
                                            .setName( "moderate" )
                                            .build();
            Threshold fourthSix = Threshold.newBuilder()
                                           .setOperator( Threshold.ThresholdOperator.GREATER )
                                           .setObservedThresholdValue( 26.0 )
                                           .setThresholdValueUnits( FT )
                                           .setName( "major" )
                                           .build();
            Threshold fourthSeven = Threshold.newBuilder()
                                             .setOperator( Threshold.ThresholdOperator.GREATER )
                                             .setObservedThresholdValue( 28.6 )
                                             .setThresholdValueUnits( FT )
                                             .setName( "record" )
                                             .build();

            wres.config.components.Threshold fourthOneW
                    = ThresholdBuilder.builder()
                                      .threshold( fourthOne )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fourthGeometry )
                                      .build();
            wres.config.components.Threshold fourthTwoW
                    = ThresholdBuilder.builder()
                                      .threshold( fourthTwo )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fourthGeometry )
                                      .build();
            wres.config.components.Threshold fourthThreeW
                    = ThresholdBuilder.builder()
                                      .threshold( fourthThree )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fourthGeometry )
                                      .build();
            wres.config.components.Threshold fourthFourW
                    = ThresholdBuilder.builder()
                                      .threshold( fourthFour )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fourthGeometry )
                                      .build();
            wres.config.components.Threshold fourthFiveW
                    = ThresholdBuilder.builder()
                                      .threshold( fourthFive )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fourthGeometry )
                                      .build();
            wres.config.components.Threshold fourthSixW
                    = ThresholdBuilder.builder()
                                      .threshold( fourthSix )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fourthGeometry )
                                      .build();
            wres.config.components.Threshold fourthSevenW
                    = ThresholdBuilder.builder()
                                      .threshold( fourthSeven )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fourthGeometry )
                                      .build();

            expectedThresholds.add( fourthOneW );
            expectedThresholds.add( fourthTwoW );
            expectedThresholds.add( fourthThreeW );
            expectedThresholds.add( fourthFourW );
            expectedThresholds.add( fourthFiveW );
            expectedThresholds.add( fourthSixW );
            expectedThresholds.add( fourthSevenW );

            Geometry fifthGeometry = Geometry.newBuilder()
                                             .setName( "MNTG1" )
                                             .build();

            Threshold fifthOne = Threshold.newBuilder()
                                          .setOperator( Threshold.ThresholdOperator.GREATER )
                                          .setObservedThresholdValue( -0.16 )
                                          .setThresholdValueUnits( FT )
                                          .setName( "low" )
                                          .build();
            Threshold fifthTwo = Threshold.newBuilder()
                                          .setOperator( Threshold.ThresholdOperator.GREATER )
                                          .setObservedThresholdValue( 11.0 )
                                          .setThresholdValueUnits( FT )
                                          .setName( "action" )
                                          .build();
            Threshold fifthThree = Threshold.newBuilder()
                                            .setOperator( Threshold.ThresholdOperator.GREATER )
                                            .setObservedThresholdValue( 11.0 )
                                            .setThresholdValueUnits( FT )
                                            .setName( "bankfull" )
                                            .build();
            Threshold fifthFour = Threshold.newBuilder()
                                           .setOperator( Threshold.ThresholdOperator.GREATER )
                                           .setObservedThresholdValue( 20.0 )
                                           .setThresholdValueUnits( FT )
                                           .setName( "flood" )
                                           .build();
            Threshold fifthFive = Threshold.newBuilder()
                                           .setOperator( Threshold.ThresholdOperator.GREATER )
                                           .setObservedThresholdValue( 20.0 )
                                           .setThresholdValueUnits( FT )
                                           .setName( "minor" )
                                           .build();
            Threshold fifthSix = Threshold.newBuilder()
                                          .setOperator( Threshold.ThresholdOperator.GREATER )
                                          .setObservedThresholdValue( 28.0 )
                                          .setThresholdValueUnits( FT )
                                          .setName( "moderate" )
                                          .build();
            Threshold fifthSeven = Threshold.newBuilder()
                                            .setOperator( Threshold.ThresholdOperator.GREATER )
                                            .setObservedThresholdValue( 31.0 )
                                            .setThresholdValueUnits( FT )
                                            .setName( "major" )
                                            .build();
            Threshold fifthEighth = Threshold.newBuilder()
                                             .setOperator( Threshold.ThresholdOperator.GREATER )
                                             .setObservedThresholdValue( 34.11 )
                                             .setThresholdValueUnits( FT )
                                             .setName( "record" )
                                             .build();

            wres.config.components.Threshold fifthOneW
                    = ThresholdBuilder.builder()
                                      .threshold( fifthOne )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fifthGeometry )
                                      .build();
            wres.config.components.Threshold fifthTwoW
                    = ThresholdBuilder.builder()
                                      .threshold( fifthTwo )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fifthGeometry )
                                      .build();
            wres.config.components.Threshold fifthThreeW
                    = ThresholdBuilder.builder()
                                      .threshold( fifthThree )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fifthGeometry )
                                      .build();
            wres.config.components.Threshold fifthFourW
                    = ThresholdBuilder.builder()
                                      .threshold( fifthFour )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fifthGeometry )
                                      .build();
            wres.config.components.Threshold fifthFiveW
                    = ThresholdBuilder.builder()
                                      .threshold( fifthFive )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fifthGeometry )
                                      .build();
            wres.config.components.Threshold fifthSixW
                    = ThresholdBuilder.builder()
                                      .threshold( fifthSix )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fifthGeometry )
                                      .build();
            wres.config.components.Threshold fifthSevenW
                    = ThresholdBuilder.builder()
                                      .threshold( fifthSeven )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fifthGeometry )
                                      .build();

            wres.config.components.Threshold fifthEighthW
                    = ThresholdBuilder.builder()
                                      .threshold( fifthEighth )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fifthGeometry )
                                      .build();

            expectedThresholds.add( fifthOneW );
            expectedThresholds.add( fifthTwoW );
            expectedThresholds.add( fifthThreeW );
            expectedThresholds.add( fifthFourW );
            expectedThresholds.add( fifthFiveW );
            expectedThresholds.add( fifthSixW );
            expectedThresholds.add( fifthSevenW );
            expectedThresholds.add( fifthEighthW );

            Metric expectedMetric = MetricBuilder.builder()
                                                 .name( MetricConstants.MEAN_ABSOLUTE_ERROR )
                                                 .parameters( MetricParametersBuilder.builder()
                                                                                     .thresholds(
                                                                                             expectedThresholds )
                                                                                     .build() )
                                                 .build();
            Set<Metric> expectedMetrics = Set.of( expectedMetric );

            EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                         .left( left )
                                                                         .features( features )
                                                                         .thresholdSources( Set.of( service ) )
                                                                         .thresholds( expectedThresholds )
                                                                         .metrics( expectedMetrics )
                                                                         .build();
            // Assert equality
            assertEquals( expected, actual );

            // Clean up
            if ( Files.exists( jsonPath ) )
            {
                Files.delete( jsonPath );
            }
        }
    }

    @Test
    void testFillThresholdsRemovesFeaturesWithoutThresholds() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new WRDS JSON service response to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, TEST_JSON );
            Path jsonPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( jsonPath ) )
            {
                writer.append( RESPONSE );
            }

            ThresholdSource service
                    = ThresholdSourceBuilder.builder()
                                            .uri( jsonPath.toUri() )
                                            .featureNameFrom( DatasetOrientation.LEFT )
                                            .type( wres.config.components.ThresholdType.VALUE )
                                            .parameter( "stage" )
                                            .provider( "NWS-NRLDB" )
                                            .operator( wres.config.components.ThresholdOperator.GREATER )
                                            .build();

            Dataset left = DatasetBuilder.builder()
                                         .featureAuthority( FeatureAuthority.NWS_LID )
                                         .build();

            GeometryTuple first = GeometryTuple.newBuilder()
                                               .setLeft( Geometry.newBuilder()
                                                                 .setName( "PTSA1" ) )
                                               .build();
            GeometryTuple second = GeometryTuple.newBuilder()
                                                .setLeft( Geometry.newBuilder()
                                                                  .setName( "MNTG1" ) )
                                                .build();
            GeometryTuple third = GeometryTuple.newBuilder()
                                               .setLeft( Geometry.newBuilder()
                                                                 .setName( "BLOF1" ) )
                                               .build();
            GeometryTuple fourth = GeometryTuple.newBuilder()
                                                .setLeft( Geometry.newBuilder()
                                                                  .setName( "CEDG1" ) )
                                                .build();
            GeometryTuple fifth = GeometryTuple.newBuilder()
                                               .setLeft( Geometry.newBuilder()
                                                                 .setName( "SMAF1" ) )
                                               .build();
            GeometryTuple sixth = GeometryTuple.newBuilder()
                                               .setLeft( Geometry.newBuilder()
                                                                 .setName( "FAKE FOO" ) )
                                               .build();

            Set<GeometryTuple> geometries = Set.of( first, second, third, fourth, fifth, sixth );
            Features features = FeaturesBuilder.builder()
                                               .geometries( geometries )
                                               .build();
            GeometryGroup group = GeometryGroup.newBuilder()
                                               .addAllGeometryTuples( geometries )
                                               .setRegionName( "FOO REGION" )
                                               .build();
            FeatureGroups featureGroups = FeatureGroupsBuilder.builder()
                                                              .geometryGroups( Collections.singleton( group ) )
                                                              .build();
            Set<Metric> metrics = Set.of( new Metric( MetricConstants.MEAN_ABSOLUTE_ERROR, null ) );

            EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                            .left( left )
                                                                            .features( features )
                                                                            .featureGroups( featureGroups )
                                                                            .thresholdSources( Set.of( service ) )
                                                                            .metrics( metrics )
                                                                            .build();

            EvaluationDeclaration actual = ReaderUtilities.readAndFillThresholds( declaration );

            Set<GeometryTuple> actualSingletons = actual.features()
                                                        .geometries();

            Set<GeometryTuple> actualGroup = actual.featureGroups()
                                                   .geometryGroups()
                                                   .stream()
                                                   .map( GeometryGroup::getGeometryTuplesList )
                                                   .map( Set::copyOf )
                                                   .findFirst()
                                                   .orElse( Set.of() );

            Set<GeometryTuple> expected = Set.of( first, second, third, fourth, fifth );

            // Assert equality
            assertAll( () -> assertEquals( expected, actualSingletons ),
                       () -> assertEquals( expected, actualGroup ) );

            // Clean up
            if ( Files.exists( jsonPath ) )
            {
                Files.delete( jsonPath );
            }
        }
    }

    @Test
    void testFillThresholdsUsesHandbook5IdentifiersForNwsFeatureAuthority()
    {
        // Path expectation includes handbook 5 identifiers
        this.mockServer.when( HttpRequest.request()
                                         .withPath( "/nws_lid/BLOF1,CEDG1,MNTG1,PTSA1,SMAF1/" )
                                         .withMethod( "GET" ) )
                       .respond( HttpResponse.response( RESPONSE ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort() );

        ThresholdSource service
                = ThresholdSourceBuilder.builder()
                                        .uri( fakeUri )
                                        .featureNameFrom( DatasetOrientation.LEFT )
                                        .type( wres.config.components.ThresholdType.VALUE )
                                        .parameter( "stage" )
                                        .provider( "NWS-NRLDB" )
                                        .operator( wres.config.components.ThresholdOperator.GREATER )
                                        .build();

        Dataset left = DatasetBuilder.builder()
                                     .featureAuthority( FeatureAuthority.NWS_LID )
                                     .build();

        GeometryTuple first = GeometryTuple.newBuilder()
                                           .setLeft( Geometry.newBuilder()
                                                             .setName( "PTSA1FOO" ) )
                                           .build();
        GeometryTuple second = GeometryTuple.newBuilder()
                                            .setLeft( Geometry.newBuilder()
                                                              .setName( "MNTG1FOO" ) )
                                            .build();
        GeometryTuple third = GeometryTuple.newBuilder()
                                           .setLeft( Geometry.newBuilder()
                                                             .setName( "BLOF1FOO" ) )
                                           .build();
        GeometryTuple fourth = GeometryTuple.newBuilder()
                                            .setLeft( Geometry.newBuilder()
                                                              .setName( "CEDG1FOO" ) )
                                            .build();
        GeometryTuple fifth = GeometryTuple.newBuilder()
                                           .setLeft( Geometry.newBuilder()
                                                             .setName( "SMAF1FOO" ) )
                                           .build();
        Set<GeometryTuple> geometries = Set.of( first, second, third, fourth, fifth );
        Features features = FeaturesBuilder.builder()
                                           .geometries( geometries )
                                           .build();

        Set<Metric> metrics = Set.of( new Metric( MetricConstants.MEAN_ABSOLUTE_ERROR, null ) );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .features( features )
                                                                        .thresholdSources( Set.of( service ) )
                                                                        .metrics( metrics )
                                                                        .build();

        EvaluationDeclaration actual = ReaderUtilities.readAndFillThresholds( declaration );

        Set<GeometryTuple> actualSingletons = actual.features()
                                                    .geometries();

        Set<GeometryTuple> expected = Set.of( first, second, third, fourth, fifth );

        // Assert equality
        assertEquals( expected, actualSingletons );
    }

    @Test
    void testReadFromWebSource() throws IOException
    {
        // Path expectation includes handbook 5 identifiers
        this.mockServer.when( HttpRequest.request()
                                         .withMethod( "GET" ) )
                       .respond( HttpResponse.response( RESPONSE ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort() );

        try ( InputStream stream =
                      ReaderUtilities.getByteStreamFromWebSource( fakeUri,
                                                                  c -> c == 404,
                                                                  c -> c >= 400,
                                                                  null,
                                                                  null ) )
        {
            assert stream != null;

            assertEquals( RESPONSE, new String( stream.readAllBytes() ) );
        }
    }

    @Test
    void testReadFromWebSourceSkipsMissingWhenEncountering404() throws IOException
    {
        // Path expectation includes handbook 5 identifiers
        this.mockServer.when( HttpRequest.request()
                                         .withMethod( "GET" ) )
                       .respond( HttpResponse.notFoundResponse() );

        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort() );

        try ( InputStream stream =
                      ReaderUtilities.getByteStreamFromWebSource( fakeUri,
                                                                  c -> c == 404,
                                                                  c -> c >= 400,
                                                                  null,
                                                                  null ) )
        {
            assertNull( stream ); // Missing data
        }
    }

    @Test
    void testReadFromWebSourceThrowsExceptionWhenEncountering404AssignedAsException()
    {
        // Path expectation includes handbook 5 identifiers
        this.mockServer.when( HttpRequest.request()
                                         .withMethod( "GET" ) )
                       .respond( HttpResponse.notFoundResponse() );

        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort() );

        AtomicInteger actualErrorCode = new AtomicInteger();
        Function<WebClient.ClientResponse, String> unpacker =
                r ->
                {
                    actualErrorCode.set( r.getStatusCode() );
                    return "";
                };

        assertThrows( ReadException.class,
                      () -> ReaderUtilities.getByteStreamFromWebSource( fakeUri,
                                                                        c -> c == 400,
                                                                        c -> c == 404,
                                                                        unpacker,
                                                                        null ) );

        assertEquals( 404, actualErrorCode.get() );
    }

    @Test
    void testReadFromWebSourceThrowsExpectedExceptionWhenGetFromWebExcepts() throws IOException
    {
        // Path expectation includes handbook 5 identifiers
        this.mockServer.when( HttpRequest.request()
                                         .withMethod( "GET" ) )
                       .respond( HttpResponse.response( RESPONSE ) );

        String uri = "http://localhost:"
                     + this.mockServer.getLocalPort();
        URI fakeUri = URI.create( uri );

        WebClient client = Mockito.mock( WebClient.class );
        Mockito.when( client.getFromWeb( Mockito.any(), Mockito.anyList() ) )
               .thenThrow( new IOException() );

        ReadException actual = assertThrows( ReadException.class,
                                             () -> ReaderUtilities.getByteStreamFromWebSource( fakeUri,
                                                                                               c -> c == 404,
                                                                                               c -> c >= 400,
                                                                                               null,
                                                                                               client ) );

        assertEquals( "Failed to acquire a byte stream from " + uri + ".", actual.getMessage() );
    }

    @Test
    void testReadFromWebSourceThrowsExpectedExceptionWhenEncounteringFileScheme()
    {
        URI fakeUri = URI.create( "file://foo" );

        ReadException exception = assertThrows( ReadException.class,
                                                () -> ReaderUtilities.getByteStreamFromWebSource( fakeUri,
                                                                                                  c -> c == 400,
                                                                                                  c -> c == 404,
                                                                                                  null,
                                                                                                  null ) );

        assertTrue( exception.getMessage()
                             .contains( "it is not a web source" ) );
    }

    @Test
    void testParseInstant()
    {
        String date = "2025-04-21";
        String time = "12:00:00";
        Instant actual = ReaderUtilities.parseInstant( date, time, ZoneOffset.UTC );
        Instant expected = Instant.parse( "2025-04-21T12:00:00Z" );
        assertEquals( expected, actual );
    }

    @Test
    void testGetTimeSeriesMetadataFromHeader()
    {
        TimeSeriesHeader header = new TimeSeriesHeader.TimeSeriesHeaderBuilder()
                .forecastDateDate( "2025-04-21" )
                .forecastDateTime( "12:00:00" )
                .parameterId( "foo" )
                .locationId( "bar" )
                .units( "qux" )
                .locationDescription( "baz" )
                .type( "instantaneous" )
                .x( "41.26" )
                .y( "23.79" )
                .z( "13.5" )
                .build();

        TimeSeriesMetadata actual = ReaderUtilities.getTimeSeriesMetadataFromHeader( header, ZoneOffset.UTC );

        Geometry geometry = MessageUtilities.getGeometry( "bar", "baz", null, "POINT ( 41.26 23.79 13.5 )" );
        TimeSeriesMetadata expected = new TimeSeriesMetadata.Builder()
                .setVariableName( "foo" )
                .setTimeScale( TimeScaleOuter.of() )
                .setUnit( "qux" )
                .setFeature( Feature.of( geometry ) )
                .setReferenceTimes( Map.of( ReferenceTime.ReferenceTimeType.T0,
                                            Instant.parse( "2025-04-21T12:00:00Z" ) ) )
                .build();

        assertEquals( expected, actual );
    }

    @Test
    void testGetTimeSeriesMetadataFromHeaderWithAggregatedTimeScale()
    {
        TimeSeriesHeader header = new TimeSeriesHeader.TimeSeriesHeaderBuilder()
                .forecastDateDate( "2025-04-21" )
                .forecastDateTime( "12:00:00" )
                .parameterId( "foo" )
                .locationId( "bar" )
                .locationDescription( "baz" )
                .type( "accumulation" )
                .units( "qux" )
                .timeStepUnit( "second" )
                .timeStepMultiplier( "21600" )
                .latitude( "-43.2789" )
                .longitude( "48.4523" )
                .build();

        TimeSeriesMetadata actual = ReaderUtilities.getTimeSeriesMetadataFromHeader( header, ZoneOffset.UTC );

        Geometry geometry = MessageUtilities.getGeometry( "bar", "baz", null, "POINT ( 48.4523 -43.2789 )" );
        TimeSeriesMetadata expected = new TimeSeriesMetadata.Builder()
                .setVariableName( "foo" )
                .setTimeScale( TimeScaleOuter.of( Duration.ofSeconds( 21600 ), TimeScale.TimeScaleFunction.UNKNOWN ) )
                .setUnit( "qux" )
                .setFeature( Feature.of( geometry ) )
                .setReferenceTimes( Map.of( ReferenceTime.ReferenceTimeType.T0,
                                            Instant.parse( "2025-04-21T12:00:00Z" ) ) )
                .build();

        assertEquals( expected, actual );
    }

    @Test
    void testGetTimeSeriesMetadataFromHeaderWithNonEquidistantTimeScale()
    {
        TimeSeriesHeader header = new TimeSeriesHeader.TimeSeriesHeaderBuilder()
                .forecastDateDate( "2025-04-21" )
                .forecastDateTime( "12:00:00" )
                .parameterId( "foo" )
                .locationId( "bar" )
                .locationDescription( "baz" )
                .units( "qux" )
                .timeStepUnit( "nonequidistant" )
                .build();

        TimeSeriesMetadata actual = ReaderUtilities.getTimeSeriesMetadataFromHeader( header, ZoneOffset.UTC );

        Geometry geometry = MessageUtilities.getGeometry( "bar", "baz", null, null );
        TimeSeriesMetadata expected = new TimeSeriesMetadata.Builder()
                .setVariableName( "foo" )
                .setUnit( "qux" )
                .setFeature( Feature.of( geometry ) )
                .setReferenceTimes( Map.of( ReferenceTime.ReferenceTimeType.T0,
                                            Instant.parse( "2025-04-21T12:00:00Z" ) ) )
                .build();

        assertEquals( expected, actual );
    }

    @Test
    void testGetTimeSeriesMetadataFromHeaderThrowsExceptedExceptionForNonNumericMultiplier()
    {
        TimeSeriesHeader header = new TimeSeriesHeader.TimeSeriesHeaderBuilder()
                .forecastDateDate( "2025-04-21" )
                .forecastDateTime( "12:00:00" )
                .parameterId( "foo" )
                .locationId( "bar" )
                .locationDescription( "baz" )
                .type( "accumulation" )
                .units( "qux" )
                .timeStepUnit( "second" )
                .timeStepMultiplier( "fooNotANumber" )
                .latitude( "-43.2789" )
                .longitude( "48.4523" )
                .build();

        ReadException actual = assertThrows( ReadException.class,
                                             () -> ReaderUtilities.getTimeSeriesMetadataFromHeader( header,
                                                                                                    ZoneOffset.UTC ) );

        assertTrue( actual.getMessage()
                          .contains( "fooNotANumber" ) );
    }

    @Test
    void testGetTimeSeriesMetadataFromHeaderThrowsExceptedExceptionForInvalidTimeStepUnit()
    {
        TimeSeriesHeader header = new TimeSeriesHeader.TimeSeriesHeaderBuilder()
                .forecastDateDate( "2025-04-21" )
                .forecastDateTime( "12:00:00" )
                .parameterId( "foo" )
                .locationId( "bar" )
                .locationDescription( "baz" )
                .type( "accumulation" )
                .units( "qux" )
                .timeStepUnit( "fooNotAValidUnit" )
                .timeStepMultiplier( "21600" )
                .latitude( "-43.2789" )
                .longitude( "48.4523" )
                .build();

        ReadException actual = assertThrows( ReadException.class,
                                             () -> ReaderUtilities.getTimeSeriesMetadataFromHeader( header,
                                                                                                    ZoneOffset.UTC ) );

        assertTrue( actual.getMessage()
                          .contains( "fooNotAValidUnit" ) );
    }

    @Test
    void testTransform()
    {
        Geometry geometry = MessageUtilities.getGeometry( "bar", "baz", null, null );
        TimeSeriesMetadata metadata = new TimeSeriesMetadata.Builder()
                .setVariableName( "foo" )
                .setUnit( "qux" )
                .setFeature( Feature.of( geometry ) )
                .setReferenceTimes( Map.of( ReferenceTime.ReferenceTimeType.T0,
                                            Instant.parse( "2025-04-21T12:00:00Z" ) ) )
                .build();

        SortedMap<Instant, Double> trace = new TreeMap<>();
        Instant instant = Instant.parse( "2025-04-25T12:00:00Z" );
        trace.put( instant, 34.2 );

        TimeSeries<Double> actual = ReaderUtilities.transform( metadata,
                                                               trace,
                                                               0,
                                                               URI.create( "https://foo.bar" ) );

        SortedSet<Event<Double>> events = new TreeSet<>();
        events.add( Event.of( instant, 34.2 ) );
        TimeSeries<Double> expected = TimeSeries.of( metadata, events );
        assertEquals( expected, actual );
    }

    @Test
    void testTransformThrowsExpectedExceptionForEmptyTrace()
    {
        Geometry geometry = MessageUtilities.getGeometry( "bar", "baz", null, null );
        TimeSeriesMetadata metadata = new TimeSeriesMetadata.Builder()
                .setVariableName( "foo" )
                .setUnit( "qux" )
                .setFeature( Feature.of( geometry ) )
                .setReferenceTimes( Map.of( ReferenceTime.ReferenceTimeType.T0,
                                            Instant.parse( "2025-04-21T12:00:00Z" ) ) )
                .build();

        SortedMap<Instant, Double> trace = new TreeMap<>();
        URI uri = URI.create( "https://foo.bar" );
        ReadException actual = assertThrows( ReadException.class,
                                             () -> ReaderUtilities.transform( metadata,
                                                                              trace,
                                                                              0,
                                                                              uri ) );

        assertTrue( actual.getMessage()
                          .contains( "there are no values in the trace" ) );

    }

    @Test
    void testTransformEnsemble()
    {
        Geometry geometry = MessageUtilities.getGeometry( "bar", "baz", null, null );
        TimeSeriesMetadata metadata = new TimeSeriesMetadata.Builder()
                .setVariableName( "foo" )
                .setUnit( "qux" )
                .setFeature( Feature.of( geometry ) )
                .setReferenceTimes( Map.of( ReferenceTime.ReferenceTimeType.T0,
                                            Instant.parse( "2025-04-21T12:00:00Z" ) ) )
                .build();

        SortedMap<String, SortedMap<Instant, Double>> traces = new TreeMap<>();
        Instant instant = Instant.parse( "2025-04-25T12:00:00Z" );
        SortedMap<Instant, Double> trace = new TreeMap<>();
        trace.put( instant, 34.2 );
        traces.put( "quux", trace );
        TimeSeries<Ensemble> actual = ReaderUtilities.transformEnsemble( metadata,
                                                                         traces,
                                                                         0,
                                                                         URI.create( "https://foo.bar" ) );

        SortedSet<Event<Ensemble>> events = new TreeSet<>();
        events.add( Event.of( instant, Ensemble.of( new double[] { 34.2 }, Ensemble.Labels.of( "quux" ) ) ) );
        TimeSeries<Ensemble> expected = TimeSeries.of( metadata, events );
        assertEquals( expected, actual );
    }

    @Test
    void testTransformEnsembleThrowsExpectedExceptionWhenValidDatetimesAreInconsistent()
    {
        Geometry geometry = MessageUtilities.getGeometry( "bar", "baz", null, null );
        TimeSeriesMetadata metadata = new TimeSeriesMetadata.Builder()
                .setVariableName( "foo" )
                .setUnit( "qux" )
                .setFeature( Feature.of( geometry ) )
                .setReferenceTimes( Map.of( ReferenceTime.ReferenceTimeType.T0,
                                            Instant.parse( "2025-04-21T12:00:00Z" ) ) )
                .build();

        SortedMap<String, SortedMap<Instant, Double>> traces = new TreeMap<>();
        Instant instant = Instant.parse( "2025-04-25T12:00:00Z" );
        SortedMap<Instant, Double> trace = new TreeMap<>();
        trace.put( instant, 34.2 );
        SortedMap<Instant, Double> anotherTrace = new TreeMap<>();
        anotherTrace.put( instant.plus( Duration.ofMinutes( 1 ) ), 34.3 );
        traces.put( "quux", trace );
        traces.put( "garply", anotherTrace );
        URI uri = URI.create( "https://foo.bar" );
        ReadException actual = assertThrows( ReadException.class,
                                             () -> ReaderUtilities.transformEnsemble( metadata,
                                                                                      traces,
                                                                                      0,
                                                                                      uri ) );

        assertTrue( actual.getMessage()
                          .contains( "All traces must be dense and have matching valid datetimes." ) );
    }

    @Test
    void TestGetTimeScaleFromUri()
    {
        URI uri = URI.create( "https://foo.bar/nwis/iv" );
        URI unknown = URI.create( "https://foo.bar" );

        assertAll( () -> assertEquals( TimeScaleOuter.of(), ReaderUtilities.getTimeScaleFromUri( uri ) ),
                   () -> assertNull( ReaderUtilities.getTimeScaleFromUri( unknown ) ),
                   () -> assertNull( ReaderUtilities.getTimeScaleFromUri( null ) ) );
    }

    @Test
    void getSimpleRange()
    {
        Source source = SourceBuilder.builder()
                                     .build();
        List<Source> sources = List.of( source );
        Dataset observedDataset = DatasetBuilder.builder()
                                                .sources( sources )
                                                .type( DataType.OBSERVATIONS )
                                                .build();
        Dataset forecastDataset = DatasetBuilder.builder()
                                                .sources( sources )
                                                .type( DataType.ENSEMBLE_FORECASTS )
                                                .build();

        TimeInterval interval = TimeIntervalBuilder.builder()
                                                   .minimum( Instant.parse( "2023-02-01T00:00:00Z" ) )
                                                   .maximum( Instant.parse( "2023-03-01T00:00:00Z" ) )
                                                   .build();
        EvaluationDeclaration forecastDeclaration = EvaluationDeclarationBuilder.builder()
                                                                                .left( observedDataset )
                                                                                .right( forecastDataset )
                                                                                .referenceDates( interval )
                                                                                .build();
        EvaluationDeclaration observedDeclaration = EvaluationDeclarationBuilder.builder()
                                                                                .left( observedDataset )
                                                                                .right( forecastDataset )
                                                                                .validDates( interval )
                                                                                .build();
        DataSource observedDataSource = DataSource.of( DataSource.DataDisposition.XML_PI_TIMESERIES,
                                                       source,
                                                       observedDataset,
                                                       List.of(),
                                                       URI.create( "http://foo.bar" ),
                                                       DatasetOrientation.LEFT,
                                                       null );
        DataSource forecastDataSource = DataSource.of( DataSource.DataDisposition.XML_PI_TIMESERIES,
                                                       source,
                                                       forecastDataset,
                                                       List.of(),
                                                       URI.create( "http://foo.bar" ),
                                                       DatasetOrientation.RIGHT,
                                                       null );
        Pair<Instant, Instant> expected = Pair.of( Instant.parse( "2023-02-01T00:00:00Z" ),
                                                   Instant.parse( "2023-03-01T00:00:00Z" ) );
        assertAll( () -> assertEquals( expected,
                                       ReaderUtilities.getSimpleRange( observedDeclaration, observedDataSource ) ),
                   () -> assertEquals( expected,
                                       ReaderUtilities.getSimpleRange( forecastDeclaration, forecastDataSource ) ) );
    }

    @Test
    void getSimpleRangeThrowsReadExceptionWhenReferenceDatesMissing()
    {
        Source source = SourceBuilder.builder()
                                     .build();
        List<Source> sources = List.of( source );
        Dataset forecastDataset = DatasetBuilder.builder()
                                                .sources( sources )
                                                .type( DataType.ENSEMBLE_FORECASTS )
                                                .build();
        EvaluationDeclaration forecastDeclaration = EvaluationDeclarationBuilder.builder()
                                                                                .left( forecastDataset )
                                                                                .right( forecastDataset )
                                                                                .build();
        DataSource forecastDataSource = DataSource.of( DataSource.DataDisposition.XML_PI_TIMESERIES,
                                                       source,
                                                       forecastDataset,
                                                       List.of(),
                                                       URI.create( "http://foo.bar" ),
                                                       DatasetOrientation.RIGHT,
                                                       null );

        ReadException actual = assertThrows( ReadException.class,
                                             () -> ReaderUtilities.getSimpleRange( forecastDeclaration,
                                                                                   forecastDataSource ) );

        assertTrue( actual.getMessage()
                          .contains( "missing 'reference_dates', which is not allowed" ) );
    }

    @Test
    void getSimpleRangeThrowsReadExceptionWhenValidDatesMissing()
    {
        Source source = SourceBuilder.builder()
                                     .build();
        List<Source> sources = List.of( source );
        Dataset observedDataset = DatasetBuilder.builder()
                                                .sources( sources )
                                                .type( DataType.OBSERVATIONS )
                                                .build();
        EvaluationDeclaration observedDeclaration = EvaluationDeclarationBuilder.builder()
                                                                                .left( observedDataset )
                                                                                .right( observedDataset )
                                                                                .build();
        DataSource observedDataSource = DataSource.of( DataSource.DataDisposition.XML_PI_TIMESERIES,
                                                       source,
                                                       observedDataset,
                                                       List.of(),
                                                       URI.create( "http://foo.bar" ),
                                                       DatasetOrientation.LEFT,
                                                       null );

        ReadException actual = assertThrows( ReadException.class,
                                             () -> ReaderUtilities.getSimpleRange( observedDeclaration,
                                                                                   observedDataSource ) );

        assertTrue( actual.getMessage()
                          .contains( "missing 'valid_dates', which is not allowed" ) );
    }

    @Test
    void testIsWrdsHefsSourceWithExplicitInterface()
    {
        Source source = SourceBuilder.builder()
                                     .sourceInterface( SourceInterface.WRDS_HEFS )
                                     .build();
        List<Source> sources = List.of( source );
        Dataset dataset = DatasetBuilder.builder()
                                        .sources( sources )
                                        .type( DataType.OBSERVATIONS )
                                        .build();
        DataSource dataSource = DataSource.of( DataSource.DataDisposition.XML_PI_TIMESERIES,
                                               source,
                                               dataset,
                                               List.of(),
                                               URI.create( "http://foo.bar" ),
                                               DatasetOrientation.RIGHT,
                                               null );

        Source sourceTwo = SourceBuilder.builder()
                                        .sourceInterface( SourceInterface.WRDS_AHPS )
                                        .build();
        DataSource dataSourceTwo = DataSource.of( DataSource.DataDisposition.XML_PI_TIMESERIES,
                                                  sourceTwo,
                                                  dataset,
                                                  List.of(),
                                                  URI.create( "http://foo.bar" ),
                                                  DatasetOrientation.RIGHT,
                                                  null );

        assertAll( () -> assertTrue( ReaderUtilities.isWrdsHefsSource( dataSource ) ),
                   () -> assertFalse( ReaderUtilities.isWrdsHefsSource( dataSourceTwo ) ) );
    }

    @Test
    void testIsWrdsHefsSourceWithImplicitInterface()
    {
        Source source = SourceBuilder.builder()
                                     .build();
        List<Source> sources = List.of( source );
        Dataset dataset = DatasetBuilder.builder()
                                        .sources( sources )
                                        .type( DataType.OBSERVATIONS )
                                        .build();
        DataSource dataSource = DataSource.of( DataSource.DataDisposition.XML_PI_TIMESERIES,
                                               source,
                                               dataset,
                                               List.of(),
                                               URI.create( "http://foo.bar/hefs/" ),
                                               DatasetOrientation.RIGHT,
                                               null );

        assertTrue( ReaderUtilities.isWrdsHefsSource( dataSource ) );
    }

    @Test
    void testGetMissingValueDouble()
    {
        TimeSeriesHeader one = TimeSeriesHeader.builder().missingValue( "null" )
                                               .build();
        TimeSeriesHeader two = TimeSeriesHeader.builder().missingValue( "NULL" )
                                               .build();
        TimeSeriesHeader three = TimeSeriesHeader.builder()
                                                 .build();
        TimeSeriesHeader four = TimeSeriesHeader.builder()
                                                .missingValue( "NaN" )
                                                .build();
        TimeSeriesHeader five = TimeSeriesHeader.builder().missingValue( "23.5" )
                                                .build();

        assertAll( () -> assertEquals( Double.NaN, ReaderUtilities.getMissingValueDouble( one ) ),
                   () -> assertEquals( Double.NaN, ReaderUtilities.getMissingValueDouble( two ) ),
                   () -> assertEquals( Double.NaN, ReaderUtilities.getMissingValueDouble( three ) ),
                   () -> assertEquals( Double.NaN, ReaderUtilities.getMissingValueDouble( four ) ),
                   () -> assertEquals( 23.5, ReaderUtilities.getMissingValueDouble( five ) ) );
    }

}
