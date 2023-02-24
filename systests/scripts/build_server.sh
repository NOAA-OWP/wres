# Copy of the Jenkins build server configuration for the OWP_WRES_Tests project as of 20230224T12:00:00Z.

echo "Here are artifacts copied from the upstream Verify_OWP_WRES job:"
sha256sum *.zip

# Get the wres version and wres-vis version from their respective zips
wres_version=$( ls -1tf wres-[0-9]*.zip | tail -1 | cut -d '-' -f 2-3 | cut -d'.' -f 1 )
vis_version=$( ls -1tf wres-vis-[0-9]*.zip | tail -1 | cut -d '-' -f 3-4 | cut -d'.' -f 1 )

# Unzip the system tests and store the directory name in systests_dir
unzip systests-*[a-f0-9].zip
systests_dir=$( ls -1d systests-*[a-f0-9] )

echo "Starting a test using a separate graphics process..."

# Configure the parameters sent to the test JVM in the settings file (avoids some quoting/escaping issues). 
# TODO: replace with -P gradle property overrides in the test command once the quoting/escaping issues can be worked out
sed -i "s/testJvmSystemProperties =.*/testJvmSystemProperties = -Dwres.useSSL=false -Dwres.useDatabase=true -Dwres.databaseJdbcUrl=jdbc:h2:mem:test;MODE=REGULAR;TRACE_LEVEL_FILE=4;DB_CLOSE_DELAY=-1;INIT=create schema if not exists wres\\; -Djava.awt.headless=true -Dwres.dataDirectory=. -Djava.io.tmpdir=.. -Ducar.unidata.io.http.maxReadCacheSize=200000 -Ducar.unidata.io.http.httpBufferSize=200000 -Dwres.attemptToMigrate=true -Duser.timezone=UTC -Dwres.externalGraphics=true -Dwres.startBroker=false -Dwres.eventsBrokerAddress=localhost -Dwres.eventsBrokerPort=5673/g" $systests_dir/gradle.properties
sed -i "s/testJvmSystemPropertiesGraphics =.*/testJvmSystemPropertiesGraphics = -Djava.io.tmpdir=. -Dwres.startBroker=true -Dwres.eventsBrokerAddress=localhost -Dwres.eventsBrokerPort=5673/g" $systests_dir/gradle.properties

# Run the tests using a separate graphics process
$systests_dir/gradlew --debug -p $systests_dir cleanTest test -PwresZipDirectory=.. -PwresGraphicsZipDirectory=.. -PversionToTest=$wres_version -PgraphicsVersionToTest=$vis_version --tests=Scenario052 --tests=Scenario053 --tests=Scenario1000 --tests=Scenario1001 --tests=Scenario500 --tests=Scenario501 --tests=Scenario502 --tests=Scenario504 --tests=Scenario505 --tests=Scenario506 --tests=Scenario507 --tests=Scenario508 --tests=Scenario509 --tests=Scenario510 --tests=Scenario511 --tests=Scenario512 --tests=Scenario720 --tests=Scenario721

# Save the test results
mkdir -p test_results_1
mv systests-*/build/reports/tests/test/* test_results_1
mv systests-*/build/*.log test_results_1

echo "Starting a second test using a self-contained process..."

# Configure the parameters sent to the test JVM in the settings file (avoids some quoting/escaping issues). 
# TODO: replace with -P gradle property overrides in the test command once the quoting/escaping issues can be worked out
sed -i "s/testJvmSystemProperties =.*/testJvmSystemProperties = -Dwres.useSSL=false -Dwres.databaseJdbcUrl=jdbc:h2:mem:test;MODE=REGULAR;TRACE_LEVEL_FILE=4;DB_CLOSE_DELAY=-1;INIT=create schema if not exists wres\\; -Djava.awt.headless=true -Dwres.dataDirectory=. -Djava.io.tmpdir=.. -Ducar.unidata.io.http.maxReadCacheSize=200000 -Ducar.unidata.io.http.httpBufferSize=200000 -Dwres.attemptToMigrate=true -Duser.timezone=UTC -Dwres.externalGraphics=false/g" $systests_dir/gradle.properties

# Run the tests again using a self-contained graphics/eventsbroker process
$systests_dir/gradlew --debug -p $systests_dir cleanTest test -PwresZipDirectory=.. -PwresGraphicsZipDirectory=.. -PversionToTest=$wres_version -PgraphicsVersionToTest=$vis_version --tests=Scenario052 --tests=Scenario053 --tests=Scenario1000 --tests=Scenario1001 --tests=Scenario500 --tests=Scenario501 --tests=Scenario502 --tests=Scenario504 --tests=Scenario505 --tests=Scenario506 --tests=Scenario507 --tests=Scenario508 --tests=Scenario509 --tests=Scenario510 --tests=Scenario511 --tests=Scenario512 --tests=Scenario720 --tests=Scenario721

# Save the test results
mkdir -p test_results_2
mv systests-*/build/reports/tests/test/* test_results_2
