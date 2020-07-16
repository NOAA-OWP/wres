package wres.io.thresholds.wrds;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.commons.compress.utils.IOUtils;
import wres.config.generated.ThresholdsConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.io.thresholds.exceptions.StreamIOException;
import wres.io.thresholds.wrds.response.ThresholdExtractor;
import wres.io.utilities.WebClient;
import wres.io.retrieval.UnitMapper;
import wres.io.thresholds.wrds.response.ThresholdResponse;
import wres.system.SystemSettings;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static wres.io.config.ConfigHelper.LOGGER;

public final class WRDSReader {
    private static final String CERT_NAME = "dod_sw_ca-54_expires_2022-11.pem";
    private static final WebClient WEB_CLIENT = new WebClient(WebClient.createSSLContext(CERT_NAME), true);
    private static final ObjectMapper JSON_OBJECT_MAPPER =
            new ObjectMapper().registerModule( new JavaTimeModule() )
                    .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true );
    private static final int LOCATION_REQUEST_COUNT = 20;

    public static Map<String, Set<ThresholdOuter>> readThresholds(
            final SystemSettings systemSettings,
            final ThresholdsConfig threshold,
            final UnitMapper unitMapper,
            final Set<String> features
    ) throws IOException
    {
        ThresholdsConfig.Source source = (ThresholdsConfig.Source) threshold.getCommaSeparatedValuesOrSource();

        List<String> addresses = new ArrayList<>();

        if (source.getValue().getScheme() == null || source.getValue().getScheme().toLowerCase().equals("file")) {
            Path resolvedPath = systemSettings.getDataDirectory().resolve(source.getValue().getPath());
            addresses.add(resolvedPath.toString());
        }
        else {
            Set<String> locationGroups = groupLocations( features );
            final String coreAddress;

            if (!source.getValue().toString().endsWith("/")) {
                coreAddress = source.getValue().toString() + "/";
            } else {
                coreAddress = source.getValue().toString();
            }

            for (String group : locationGroups) {
                addresses.add(coreAddress + group + "/");
            }
        }

        try {
            return addresses.parallelStream()
                    .map(WRDSReader::getResponse)
                    .map(thresholdResponse -> extract(thresholdResponse, threshold, unitMapper))
                    .flatMap(featurePlusSetMap -> featurePlusSetMap.entrySet().stream())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        } catch (StreamIOException streamReadingException) {
            throw new IOException(streamReadingException.getCause());
        }
    }

    static Map<String, Set<ThresholdOuter>> extract(ThresholdResponse response, ThresholdsConfig config, UnitMapper desiredUnitMapper)
    {
        ThresholdsConfig.Source source = (ThresholdsConfig.Source)config.getCommaSeparatedValuesOrSource();
        ThresholdConstants.ThresholdDataType side = ThresholdConstants.ThresholdDataType.LEFT;

        if (Objects.nonNull(config.getApplyTo())) {
            side = ThresholdConstants.ThresholdDataType.valueOf(config.getApplyTo().name());
        }

        ThresholdConstants.Operator operator = ThresholdConstants.Operator.GREATER;

        if (Objects.nonNull(config.getOperator())) {
            operator = DataFactory.getThresholdOperator(config);
        }

        ThresholdExtractor extractor = new ThresholdExtractor(response)
                .from(source.getProvider())
                .operatesBy(operator)
                .onSide(side);

        if (source.getRatingProvider() != null) {
            extractor.ratingFrom(source.getRatingProvider());
        }

        if (source.getParameterToMeasure().toLowerCase().equals("stage")) {
            extractor.readStage();
        }
        else {
            extractor.readFlow();
        }

        extractor.convertTo(desiredUnitMapper);

        return extractor.extract();
    }

    static Set<String> groupLocations( Set<String> features ) {
        Set<String> locationGroups = new HashSet<>();
        StringJoiner locationJoiner = new StringJoiner(",");
        int counter = 0;

        for ( String feature : features ) {
            if (counter % LOCATION_REQUEST_COUNT == 0 && locationJoiner.length() > 0) {
                locationGroups.add(locationJoiner.toString());
                locationJoiner = new StringJoiner(",");
                counter = 0;
            }

            locationJoiner.add( feature );
            counter++;
        }

        if (locationJoiner.length() > 0) {
            locationGroups.add(locationJoiner.toString());
        }

        return locationGroups;
    }

    private static ThresholdResponse getResponse(final String inputAddress) throws StreamIOException {
        URI address = URI.create(inputAddress);
        try {
            if (address.getScheme() == null || address.getScheme().toLowerCase().equals("file")) {
                Path thresholdPath;

                if (address.getScheme() == null) {
                    thresholdPath = Paths.get(inputAddress);
                }
                else {
                    thresholdPath = Paths.get(address);
                }
                try (InputStream data = new FileInputStream(thresholdPath.toFile())) {
                    byte[] rawForecast = IOUtils.toByteArray(data);
                    return JSON_OBJECT_MAPPER.readValue(rawForecast, ThresholdResponse.class);
                }
            } else if (address.getScheme().toLowerCase().startsWith("http")) {
                return getRemoteResponse(inputAddress);
            }
        }
        catch (IOException ioe) {
            throw new StreamIOException("Error encountered while requesting WRDS threshold data", ioe);
        }

        throw new IllegalArgumentException("Only files or web addresses may be used to retrieve thresholds");
    }

    private static ThresholdResponse getRemoteResponse(String inputAddress) throws IOException {
        try (WebClient.ClientResponse response = WEB_CLIENT.getFromWeb(URI.create(inputAddress))) {

            if (response.getStatusCode() >= 400 && response.getStatusCode() < 500) {
                LOGGER.warn("Treating HTTP response code {} as no data found from URI {}",
                        response.getStatusCode(),
                        inputAddress);
                return null;
            }

            return JSON_OBJECT_MAPPER.readValue(response.getResponse(), ThresholdResponse.class);
        }
    }

}
