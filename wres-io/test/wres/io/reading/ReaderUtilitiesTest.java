package wres.io.reading;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.protobuf.DoubleValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

import wres.config.MetricConstants;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.FeatureAuthority;
import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.MetricBuilder;
import wres.config.yaml.components.MetricParametersBuilder;
import wres.config.yaml.components.ThresholdBuilder;
import wres.config.yaml.components.ThresholdSource;
import wres.config.yaml.components.ThresholdSourceBuilder;
import wres.config.yaml.components.ThresholdType;
import wres.datamodel.units.UnitMapper;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Threshold;

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
                                            .type( wres.config.yaml.components.ThresholdType.VALUE )
                                            .parameter( "stage" )
                                            .provider( "NWS-NRLDB" )
                                            .operator( wres.config.yaml.components.ThresholdOperator.GREATER )
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
            Features features = new Features( geometries );

            Set<Metric> metrics = Set.of( new Metric( MetricConstants.MEAN_ABSOLUTE_ERROR, null ) );

            EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                            .left( left )
                                                                            .features( features )
                                                                            .thresholdSources( Set.of( service ) )
                                                                            .metrics( metrics )
                                                                            .build();

            UnitMapper unitMapper = UnitMapper.of( FT );

            EvaluationDeclaration actual = ReaderUtilities.readAndFillThresholds( declaration, unitMapper );

            // Build the expected declaration
            Set<wres.config.yaml.components.Threshold> expectedThresholds = new HashSet<>();
            Geometry firstGeometry = Geometry.newBuilder()
                                             .setName( "SMAF1" )
                                             .build();

            Threshold firstOne = Threshold.newBuilder()
                                          .setOperator( Threshold.ThresholdOperator.GREATER )
                                          .setLeftThresholdValue( DoubleValue.of( 0.0 ) )
                                          .setThresholdValueUnits( FT )
                                          .setName( "low" )
                                          .build();
            Threshold firstTwo = Threshold.newBuilder()
                                          .setOperator( Threshold.ThresholdOperator.GREATER )
                                          .setLeftThresholdValue( DoubleValue.of( 8.0 ) )
                                          .setThresholdValueUnits( FT )
                                          .setName( "action" )
                                          .build();
            Threshold firstThree = Threshold.newBuilder()
                                            .setOperator( Threshold.ThresholdOperator.GREATER )
                                            .setLeftThresholdValue( DoubleValue.of( 9.5 ) )
                                            .setThresholdValueUnits( FT )
                                            .setName( "flood" )
                                            .build();
            Threshold firstFour = Threshold.newBuilder()
                                           .setOperator( Threshold.ThresholdOperator.GREATER )
                                           .setLeftThresholdValue( DoubleValue.of( 9.5 ) )
                                           .setThresholdValueUnits( FT )
                                           .setName( "minor" )
                                           .build();
            Threshold firstFive = Threshold.newBuilder()
                                           .setOperator( Threshold.ThresholdOperator.GREATER )
                                           .setLeftThresholdValue( DoubleValue.of( 11.5 ) )
                                           .setThresholdValueUnits( FT )
                                           .setName( "moderate" )
                                           .build();
            Threshold firstSix = Threshold.newBuilder()
                                          .setOperator( Threshold.ThresholdOperator.GREATER )
                                          .setLeftThresholdValue( DoubleValue.of( 13.5 ) )
                                          .setThresholdValueUnits( FT )
                                          .setName( "major" )
                                          .build();
            Threshold firstSeven = Threshold.newBuilder()
                                            .setOperator( Threshold.ThresholdOperator.GREATER )
                                            .setLeftThresholdValue( DoubleValue.of( 15.36 ) )
                                            .setThresholdValueUnits( FT )
                                            .setName( "record" )
                                            .build();

            wres.config.yaml.components.Threshold firstOneW
                    = ThresholdBuilder.builder()
                                      .threshold( firstOne )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( firstGeometry )
                                      .build();
            wres.config.yaml.components.Threshold firstTwoW
                    = ThresholdBuilder.builder()
                                      .threshold( firstTwo )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( firstGeometry )
                                      .build();
            wres.config.yaml.components.Threshold firstThreeW
                    = ThresholdBuilder.builder()
                                      .threshold( firstThree )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( firstGeometry )
                                      .build();
            wres.config.yaml.components.Threshold firstFourW
                    = ThresholdBuilder.builder()
                                      .threshold( firstFour )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( firstGeometry )
                                      .build();
            wres.config.yaml.components.Threshold firstFiveW
                    = ThresholdBuilder.builder()
                                      .threshold( firstFive )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( firstGeometry )
                                      .build();
            wres.config.yaml.components.Threshold firstSixW
                    = ThresholdBuilder.builder()
                                      .threshold( firstSix )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( firstGeometry )
                                      .build();
            wres.config.yaml.components.Threshold firstSevenW
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
                                           .setLeftThresholdValue( DoubleValue.of( 0.0 ) )
                                           .setThresholdValueUnits( FT )
                                           .setName( "low" )
                                           .build();
            Threshold secondTwo = Threshold.newBuilder()
                                           .setOperator( Threshold.ThresholdOperator.GREATER )
                                           .setLeftThresholdValue( DoubleValue.of( 14.29 ) )
                                           .setThresholdValueUnits( FT )
                                           .setName( "record" )
                                           .build();

            wres.config.yaml.components.Threshold secondOneW
                    = ThresholdBuilder.builder()
                                      .threshold( secondOne )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( secondGeometry )
                                      .build();
            wres.config.yaml.components.Threshold secondTwoW
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
                                          .setLeftThresholdValue( DoubleValue.of( 0.0 ) )
                                          .setThresholdValueUnits( FT )
                                          .setName( "low" )
                                          .build();

            wres.config.yaml.components.Threshold thirdOneW
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
                                           .setLeftThresholdValue( DoubleValue.of( 13.0 ) )
                                           .setThresholdValueUnits( FT )
                                           .setName( "action" )
                                           .build();
            Threshold fourthTwo = Threshold.newBuilder()
                                           .setOperator( Threshold.ThresholdOperator.GREATER )
                                           .setLeftThresholdValue( DoubleValue.of( 15.0 ) )
                                           .setThresholdValueUnits( FT )
                                           .setName( "bankfull" )
                                           .build();
            Threshold fourthThree = Threshold.newBuilder()
                                             .setOperator( Threshold.ThresholdOperator.GREATER )
                                             .setLeftThresholdValue( DoubleValue.of( 17.0 ) )
                                             .setThresholdValueUnits( FT )
                                             .setName( "flood" )
                                             .build();
            Threshold fourthFour = Threshold.newBuilder()
                                            .setOperator( Threshold.ThresholdOperator.GREATER )
                                            .setLeftThresholdValue( DoubleValue.of( 17.0 ) )
                                            .setThresholdValueUnits( FT )
                                            .setName( "minor" )
                                            .build();
            Threshold fourthFive = Threshold.newBuilder()
                                            .setOperator( Threshold.ThresholdOperator.GREATER )
                                            .setLeftThresholdValue( DoubleValue.of( 23.5 ) )
                                            .setThresholdValueUnits( FT )
                                            .setName( "moderate" )
                                            .build();
            Threshold fourthSix = Threshold.newBuilder()
                                           .setOperator( Threshold.ThresholdOperator.GREATER )
                                           .setLeftThresholdValue( DoubleValue.of( 26.0 ) )
                                           .setThresholdValueUnits( FT )
                                           .setName( "major" )
                                           .build();
            Threshold fourthSeven = Threshold.newBuilder()
                                             .setOperator( Threshold.ThresholdOperator.GREATER )
                                             .setLeftThresholdValue( DoubleValue.of( 28.6 ) )
                                             .setThresholdValueUnits( FT )
                                             .setName( "record" )
                                             .build();

            wres.config.yaml.components.Threshold fourthOneW
                    = ThresholdBuilder.builder()
                                      .threshold( fourthOne )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fourthGeometry )
                                      .build();
            wres.config.yaml.components.Threshold fourthTwoW
                    = ThresholdBuilder.builder()
                                      .threshold( fourthTwo )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fourthGeometry )
                                      .build();
            wres.config.yaml.components.Threshold fourthThreeW
                    = ThresholdBuilder.builder()
                                      .threshold( fourthThree )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fourthGeometry )
                                      .build();
            wres.config.yaml.components.Threshold fourthFourW
                    = ThresholdBuilder.builder()
                                      .threshold( fourthFour )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fourthGeometry )
                                      .build();
            wres.config.yaml.components.Threshold fourthFiveW
                    = ThresholdBuilder.builder()
                                      .threshold( fourthFive )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fourthGeometry )
                                      .build();
            wres.config.yaml.components.Threshold fourthSixW
                    = ThresholdBuilder.builder()
                                      .threshold( fourthSix )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fourthGeometry )
                                      .build();
            wres.config.yaml.components.Threshold fourthSevenW
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
                                          .setLeftThresholdValue( DoubleValue.of( -0.16 ) )
                                          .setThresholdValueUnits( FT )
                                          .setName( "low" )
                                          .build();
            Threshold fifthTwo = Threshold.newBuilder()
                                          .setOperator( Threshold.ThresholdOperator.GREATER )
                                          .setLeftThresholdValue( DoubleValue.of( 11.0 ) )
                                          .setThresholdValueUnits( FT )
                                          .setName( "action" )
                                          .build();
            Threshold fifthThree = Threshold.newBuilder()
                                            .setOperator( Threshold.ThresholdOperator.GREATER )
                                            .setLeftThresholdValue( DoubleValue.of( 11.0 ) )
                                            .setThresholdValueUnits( FT )
                                            .setName( "bankfull" )
                                            .build();
            Threshold fifthFour = Threshold.newBuilder()
                                           .setOperator( Threshold.ThresholdOperator.GREATER )
                                           .setLeftThresholdValue( DoubleValue.of( 20.0 ) )
                                           .setThresholdValueUnits( FT )
                                           .setName( "flood" )
                                           .build();
            Threshold fifthFive = Threshold.newBuilder()
                                           .setOperator( Threshold.ThresholdOperator.GREATER )
                                           .setLeftThresholdValue( DoubleValue.of( 20.0 ) )
                                           .setThresholdValueUnits( FT )
                                           .setName( "minor" )
                                           .build();
            Threshold fifthSix = Threshold.newBuilder()
                                          .setOperator( Threshold.ThresholdOperator.GREATER )
                                          .setLeftThresholdValue( DoubleValue.of( 28.0 ) )
                                          .setThresholdValueUnits( FT )
                                          .setName( "moderate" )
                                          .build();
            Threshold fifthSeven = Threshold.newBuilder()
                                            .setOperator( Threshold.ThresholdOperator.GREATER )
                                            .setLeftThresholdValue( DoubleValue.of( 31.0 ) )
                                            .setThresholdValueUnits( FT )
                                            .setName( "major" )
                                            .build();
            Threshold fifthEighth = Threshold.newBuilder()
                                             .setOperator( Threshold.ThresholdOperator.GREATER )
                                             .setLeftThresholdValue( DoubleValue.of( 34.11 ) )
                                             .setThresholdValueUnits( FT )
                                             .setName( "record" )
                                             .build();

            wres.config.yaml.components.Threshold fifthOneW
                    = ThresholdBuilder.builder()
                                      .threshold( fifthOne )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fifthGeometry )
                                      .build();
            wres.config.yaml.components.Threshold fifthTwoW
                    = ThresholdBuilder.builder()
                                      .threshold( fifthTwo )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fifthGeometry )
                                      .build();
            wres.config.yaml.components.Threshold fifthThreeW
                    = ThresholdBuilder.builder()
                                      .threshold( fifthThree )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fifthGeometry )
                                      .build();
            wres.config.yaml.components.Threshold fifthFourW
                    = ThresholdBuilder.builder()
                                      .threshold( fifthFour )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fifthGeometry )
                                      .build();
            wres.config.yaml.components.Threshold fifthFiveW
                    = ThresholdBuilder.builder()
                                      .threshold( fifthFive )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fifthGeometry )
                                      .build();
            wres.config.yaml.components.Threshold fifthSixW
                    = ThresholdBuilder.builder()
                                      .threshold( fifthSix )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fifthGeometry )
                                      .build();
            wres.config.yaml.components.Threshold fifthSevenW
                    = ThresholdBuilder.builder()
                                      .threshold( fifthSeven )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .type( ThresholdType.VALUE )
                                      .feature( fifthGeometry )
                                      .build();

            wres.config.yaml.components.Threshold fifthEighthW
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
                                            .type( wres.config.yaml.components.ThresholdType.VALUE )
                                            .parameter( "stage" )
                                            .provider( "NWS-NRLDB" )
                                            .operator( wres.config.yaml.components.ThresholdOperator.GREATER )
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
            Features features = new Features( geometries );
            GeometryGroup group = GeometryGroup.newBuilder()
                                               .addAllGeometryTuples( geometries )
                                               .setRegionName( "FOO REGION" )
                                               .build();
            FeatureGroups featureGroups = new FeatureGroups( Set.of( group ) );
            Set<Metric> metrics = Set.of( new Metric( MetricConstants.MEAN_ABSOLUTE_ERROR, null ) );

            EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                            .left( left )
                                                                            .features( features )
                                                                            .featureGroups( featureGroups )
                                                                            .thresholdSources( Set.of( service ) )
                                                                            .metrics( metrics )
                                                                            .build();

            UnitMapper unitMapper = UnitMapper.of( FT );

            EvaluationDeclaration actual = ReaderUtilities.readAndFillThresholds( declaration, unitMapper );

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
                                        .type( wres.config.yaml.components.ThresholdType.VALUE )
                                        .parameter( "stage" )
                                        .provider( "NWS-NRLDB" )
                                        .operator( wres.config.yaml.components.ThresholdOperator.GREATER )
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
        Features features = new Features( geometries );

        Set<Metric> metrics = Set.of( new Metric( MetricConstants.MEAN_ABSOLUTE_ERROR, null ) );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .features( features )
                                                                        .thresholdSources( Set.of( service ) )
                                                                        .metrics( metrics )
                                                                        .build();

        UnitMapper unitMapper = UnitMapper.of( FT );

        EvaluationDeclaration actual = ReaderUtilities.readAndFillThresholds( declaration, unitMapper );

        Set<GeometryTuple> actualSingletons = actual.features()
                                                    .geometries();

        Set<GeometryTuple> expected = Set.of( first, second, third, fourth, fifth );

        // Assert equality
        assertEquals( expected, actualSingletons );
    }
}
