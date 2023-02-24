# Copy of the Jenkins build server configuration for the Verify_OWP_WRES project as of 20230224T12:00:00Z.

# In order to have exactly one zip per distribution in the workspace,
# delete all the zips prior to building. Jars will remain, allowing
# decent speed incremental builds. If these zips differ in a few bits
# that means we have work to do toward reproducible builds. The goal
# here is to have the short-list of artifacts formally listed within
# the jenkins builds, accessible via API calls from test machines.
find build/distributions wres-*/build/distributions systests/build/distributions scripts/build/distributions -type f \( -name "wres*.zip" -o -name "systests*.zip" \) -delete

# Run the build using gradle
# Temporarily disable aggregateJavadocs, see #62732
./gradlew distZip testCodeCoverageReport javadoc

# view list of jars, zips and their identities
find build wres-*/build \( -name "wres*.zip" -o -name "wres*.jar" \) -exec sha256sum "{}" \+

# Confirm the test code inside the wres-external-services-tests zip compiles.
pushd wres-external-services-tests
../gradlew installDist
pushd build/install/wres-external-services-tests
./gradlew testClasses
popd
popd

# create the system tests zip using java 11 from above
pushd systests
git status
../gradlew distZip installDist
popd

# Confirm that the test code inside the system tests zip compiles.
# To make this work in recent builds, need to set the -PgraphicsVersionToTest too
# Get the directory and version of the above-built WRES ZIP:
#WRES_ZIP_DIR=$( readlink -f build/distributions )
#WRES_VERSION=$( ./gradlew properties | grep "^version:" | cut -d' ' -f 2 )
#pushd systests build/install/systests
#./gradlew -PwresZipDirectory=$WRES_ZIP_DIR -PversionToTest=$WRES_VERSION testClasses
#popd

# Create the admin scripts zip
pushd scripts
git status
../gradlew distZip
popd

# Stop the gradle daemon(s) used above. Bad news: this can kill other running tests.
# ./gradlew --stop

# look at free space (helps us decide when to clean out workspace)
df -h

# Remove any/all build data older than 28 days
find build */build -type f -mtime +28 -delete