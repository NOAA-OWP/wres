package wres.io.data.caching;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;
import wres.io.config.SystemSettings;
import wres.io.utilities.Database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static ru.yandex.qatools.embed.postgresql.distribution.Version.Main.V9_6;
import static ru.yandex.qatools.embed.postgresql.EmbeddedPostgres.cachedRuntimeConfig;

import java.beans.PropertyVetoException;
import java.io.*;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

@Ignore
@RunWith(PowerMockRunner.class)
@PrepareForTest({SystemSettings.class, Database.class})
@PowerMockIgnore("javax.management.*") // thanks https://stackoverflow.com/questions/16520699/mockito-powermock-linkageerror-while-mocking-system-class#21268013
public class SourceCacheTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceCacheTest.class);

    /* failed attempt with pure mockito, throws NPE because the SystemSettings
       are using static methods during construction? Or static methods?
       or private methods? Any combination, maybe.

    public static final String NEWLINE = System.lineSeparator();

    public static final String SYS_CONFIG =
              "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + NEWLINE
            + "<wresconfig>" + NEWLINE
            + "	<database>" + NEWLINE
            + "        <url>localhost</url>" + NEWLINE
            + "        <username>test</username>" + NEWLINE
            + "        <password>test</password>" + NEWLINE
            + "        <name>wres</name>" + NEWLINE
            + "        <port>5432</port>" + NEWLINE
            + "		<database_type>postgresql</database_type>" + NEWLINE
            + "        <max_idle_time>80</max_idle_time>" + NEWLINE
            + "        <!-- Warning: Default max connections is 100; approaching" + NEWLINE
            + "    that will cause deadlocks-->" + NEWLINE
            + "        <max_pool_size>25</max_pool_size>" + NEWLINE
            + "	</database>" + NEWLINE
            + "	<maximum_thread_count>50</maximum_thread_count>" + NEWLINE
            + "	<fetch_size>500</fetch_size>" + NEWLINE
            + "    <maximum_inserts>900</maximum_inserts>" + NEWLINE
            + "    <maximum_copies>200</maximum_copies>" + NEWLINE
            + "    <project_directory>projects</project_directory>" + NEWLINE
            + "    <should_log>false</should_log>" + NEWLINE
            + "    <in_development>true</in_development>" + NEWLINE
            + "</wresconfig>" + NEWLINE;

    private static final XMLInputFactory xmlFactory = XMLInputFactory.newFactory();

    private XMLStreamReader systemConfigXmlReader;
    private Reader systemConfigReader = new StringReader(SYS_CONFIG);
    private ComboPooledDataSource connectionPoolDataSource;

    @Mock
    private SystemSettings systemSettings;

    @Before
    public void setup() throws XMLStreamException, FileNotFoundException
    {
        connectionPoolDataSource = new ComboPooledDataSource();
        connectionPoolDataSource.setJdbcUrl("jdbc:h2:mem:db1;DB_CLOSE_DELAY=-1");
        connectionPoolDataSource.setUser("SA");
        connectionPoolDataSource.setPassword("");
        MockitoAnnotations.initMocks(this);
        // reader part is not working due to being in static portion?
        when(systemSettings.create_reader()).thenReturn(this.systemConfigXmlReader);
        // but this connection pool part might work...
        when(systemSettings.getConnectionPool()).thenReturn(this.connectionPoolDataSource);
        // only if we
        this.systemConfigXmlReader = xmlFactory.createXMLStreamReader(systemConfigReader);
    }

    here is the output
    /usr/lib/jvm/java-1.8.0-openjdk.x86_64/bin/java -ea -Didea.test.cyclic.buffer.size=1048576 -javaagent:/home/jesse/idea-IC-171.4424.56/lib/idea_rt.jar=42518:/home/jesse/idea-IC-171.4424.56/bin -Dfile.encoding=UTF-8 -classpath /home/jesse/idea-IC-171.4424.56/lib/idea_rt.jar:/home/jesse/idea-IC-171.4424.56/plugins/junit/lib/junit-rt.jar:/usr/lib/jvm/java-1.8.0-openjdk.x86_64/jre/lib/charsets.jar:/usr/lib/jvm/java-1.8.0-openjdk.x86_64/jre/lib/ext/cldrdata.jar:/usr/lib/jvm/java-1.8.0-openjdk.x86_64/jre/lib/ext/dnsns.jar:/usr/lib/jvm/java-1.8.0-openjdk.x86_64/jre/lib/ext/jaccess.jar:/usr/lib/jvm/java-1.8.0-openjdk.x86_64/jre/lib/ext/localedata.jar:/usr/lib/jvm/java-1.8.0-openjdk.x86_64/jre/lib/ext/nashorn.jar:/usr/lib/jvm/java-1.8.0-openjdk.x86_64/jre/lib/ext/sunec.jar:/usr/lib/jvm/java-1.8.0-openjdk.x86_64/jre/lib/ext/sunjce_provider.jar:/usr/lib/jvm/java-1.8.0-openjdk.x86_64/jre/lib/ext/sunpkcs11.jar:/usr/lib/jvm/java-1.8.0-openjdk.x86_64/jre/lib/ext/zipfs.jar:/usr/lib/jvm/java-1.8.0-openjdk.x86_64/jre/lib/jce.jar:/usr/lib/jvm/java-1.8.0-openjdk.x86_64/jre/lib/jsse.jar:/usr/lib/jvm/java-1.8.0-openjdk.x86_64/jre/lib/management-agent.jar:/usr/lib/jvm/java-1.8.0-openjdk.x86_64/jre/lib/resources.jar:/usr/lib/jvm/java-1.8.0-openjdk.x86_64/jre/lib/rt.jar:/media/sf_userprofile/code/wres2/wres-io/build/classes/test:/media/sf_userprofile/code/wres2/wres-io/build/classes/main:/media/sf_userprofile/code/wres2/utilities/build/classes/main:/media/sf_userprofile/code/wres2/utilities/build/resources/main:/media/sf_userprofile/code/wres2/wres-datamodel-api/build/classes/main:/media/sf_userprofile/code/wres2/wres-datamodel/build/classes/main:/home/jesse/.gradle/caches/modules-2/files-2.1/org.slf4j/slf4j-api/1.7.25/da76ca59f6a57ee3102f8f9bd9cee742973efa8a/slf4j-api-1.7.25.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/edu.ucar/cdm/4.6.8/e1655354f29952632c375e492c61e90805a96c5d/cdm-4.6.8.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/com.mchange/c3p0/0.9.5.2/5f86cb6130bc6e8475615ed82d5b5e6fb226a86a/c3p0-0.9.5.2.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/com.google.guava/guava-testlib/21.0/1ec77c45666cf17da76cd80725194148a8ffc440/guava-testlib-21.0.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/junit/junit/4.12/2973d150c0dc1fefe998f834810d68f278ea58ec/junit-4.12.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/org.mockito/mockito-core/2.8.9/1afb35b2d77d40567756c379e54c18da3574a96e/mockito-core-2.8.9.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/org.powermock/powermock-api-mockito2/1.7.0RC4/4d0278e9465831be3bbf877158ec0f49bc780bc1/powermock-api-mockito2-1.7.0RC4.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/com.google.guava/guava/22.0/3564ef3803de51fb0530a8377ec6100b33b0d073/guava-22.0.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/ch.qos.logback/logback-classic/1.2.3/7c4f3c474fb2c041d8028740440937705ebb473a/logback-classic-1.2.3.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/org.powermock/powermock-module-junit4/1.7.0RC4/8e36bdce3a7a8d9555d7e0771f3907561dbcb8fc/powermock-module-junit4-1.7.0RC4.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/tec.uom/uom-se/1.0.7/5ded85697178585b18ae5cccb421ab4c60e1d017/uom-se-1.0.7.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/com.h2database/h2/1.4.196/dd0034398d593aa3588c6773faac429bbd9aea0e/h2-1.4.196.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/systems.uom/systems-unicode/0.7/e5e2a1db43a0b46824df076674d58fe0f7dfdb78/systems-unicode-0.7.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/edu.ucar/udunits/4.6.8/2b779ee16815129941524eb462420acdaf1162ec/udunits-4.6.8.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/edu.ucar/httpservices/4.6.8/ed52d552907bea72fde563c4897b06ba5da184ec/httpservices-4.6.8.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/org.apache.httpcomponents/httpcore/4.4.4/b31526a230871fbe285fbcbe2813f9c0839ae9b0/httpcore-4.4.4.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/joda-time/joda-time/2.8.1/f5bfc718c95a7b1d3c371bb02a188a4df18361a9/joda-time-2.8.1.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/org.jdom/jdom2/2.0.4/4b65e55cc61b34bc634b25f0359d1242e4c519de/jdom2-2.0.4.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/net.jcip/jcip-annotations/1.0/afba4942caaeaf46aab0b976afd57cc7c181467e/jcip-annotations-1.0.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/org.quartz-scheduler/quartz/2.2.0/2eb16fce055d5f3c9d65420f6fc4efd3a079a3d8/quartz-2.2.0.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/com.google.protobuf/protobuf-java/2.5.0/a10732c76bfacdbd633a7eb0f7968b1059a65dfa/protobuf-java-2.5.0.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/com.beust/jcommander/1.35/47592e181b0bdbbeb63029e08c5e74f6803c4edd/jcommander-1.35.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/com.amazonaws/aws-java-sdk-s3/1.10.11/65822e3e1fa1ca41f2a47505b5162660e7d0cb24/aws-java-sdk-s3-1.10.11.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/com.mchange/mchange-commons-java/0.2.11/2a6a6c1fe25f28f5a073171956ce6250813467ef/mchange-commons-java-0.2.11.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/com.google.code.findbugs/jsr305/1.3.9/40719ea6961c0cb6afaeb6a921eaa1f6afd4cfdf/jsr305-1.3.9.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/net.bytebuddy/byte-buddy/1.6.14/871c3e49dc6183d0d361601c2f1d11abb1a6b48c/byte-buddy-1.6.14.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/net.bytebuddy/byte-buddy-agent/1.6.14/ba1e5ba3a84fb2fbf2f4de9138df19665eec4d59/byte-buddy-agent-1.6.14.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/org.objenesis/objenesis/2.5/612ecb799912ccf77cba9b3ed8c813da086076e9/objenesis-2.5.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/org.powermock/powermock-api-mockito-common/1.7.0RC4/17cec0fa1654ca5052986225bd579a461a34474b/powermock-api-mockito-common-1.7.0RC4.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/ch.qos.logback/logback-core/1.2.3/864344400c3d4d92dfeb0a305dc87d953677c03c/logback-core-1.2.3.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/javax.measure/unit-api/1.0/6b960260278588d7ff02fe376e5aad39a9c7440b/unit-api-1.0.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/tec.uom.lib/uom-lib-common/1.0.2/7159c464e682dd273902b3a3f32967a881e9193b/uom-lib-common-1.0.2.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/tec.units/unit-ri/1.0.3/87ec189ff2640c06aac00bbaf4a24a594a2b6c29/unit-ri-1.0.3.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/org.powermock/powermock-module-junit4-common/1.7.0RC4/1a81b98cb7da6ad785862f6aa5be427ef4c0c7b9/powermock-module-junit4-common-1.7.0RC4.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/org.apache.httpcomponents/httpclient/4.5.1/7e3cecc566df91338c6c67883b89ddd05a17db43/httpclient-4.5.1.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/org.apache.httpcomponents/httpmime/4.5.1/96823b9421ebb9f490dec837d9f96134e864e3a7/httpmime-4.5.1.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/com.amazonaws/aws-java-sdk-kms/1.10.11/6bf9ff2e3712dd498be91f6059434bd8d59297e6/aws-java-sdk-kms-1.10.11.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/com.amazonaws/aws-java-sdk-core/1.10.11/770b02ea2c5f4e43ea4ac4f0705234c25a3831c8/aws-java-sdk-core-1.10.11.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/org.powermock/powermock-api-support/1.7.0RC4/4c3d7fd0b8547230098a95292d5f2b082e059b72/powermock-api-support-1.7.0RC4.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/commons-logging/commons-logging/1.2/4bfc12adfe4842bf07b657f0369c4cb522955686/commons-logging-1.2.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/commons-codec/commons-codec/1.9/9ce04e34240f674bc72680f8b843b1457383161a/commons-codec-1.9.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/com.fasterxml.jackson.core/jackson-databind/2.5.3/c37875ff66127d93e5f672708cb2dcc14c8232ab/jackson-databind-2.5.3.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/org.powermock/powermock-reflect/1.7.0RC4/d1818db6df8fdfdcd81302b2866db24eaa58785a/powermock-reflect-1.7.0RC4.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/org.powermock/powermock-core/1.7.0RC4/44e49b7ad3019880590a7cb9ce918870cd70cdfb/powermock-core-1.7.0RC4.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/com.fasterxml.jackson.core/jackson-annotations/2.5.0/a2a55a3375bc1cef830ca426d68d2ea22961190e/jackson-annotations-2.5.0.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/com.fasterxml.jackson.core/jackson-core/2.5.3/a8b8a6dfc8a17890e4c7ff8aed810763d265b68b/jackson-core-2.5.3.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/org.javassist/javassist/3.21.0-GA/598244f595db5c5fb713731eddbb1c91a58d959b/javassist-3.21.0-GA.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/com.google.j2objc/j2objc-annotations/1.1/ed28ded51a8b1c6b112568def5f4b455e6809019/j2objc-annotations-1.1.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/org.codehaus.mojo/animal-sniffer-annotations/1.14/775b7e22fb10026eed3f86e8dc556dfafe35f2d5/animal-sniffer-annotations-1.14.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/org.hamcrest/hamcrest-core/1.3/42a25dc3219429f0e5d060061f71acb49bf010a0/hamcrest-core-1.3.jar:/home/jesse/.gradle/caches/modules-2/files-2.1/com.google.errorprone/error_prone_annotations/2.0.18/5f65affce1684999e2f4024983835efc3504012e/error_prone_annotations-2.0.18.jar com.intellij.rt.execution.junit.JUnitStarter -ideVersion5 wres.io.data.caching.SourceCacheTest,initSourceCache
11:25:44.899 [MLog-Init-Reporter] INFO com.mchange.v2.log.MLog - MLog clients using slf4j logging.
11:25:44.964 [MLog-Init-Reporter] DEBUG com.mchange.v2.log.MLog - Reading VM config for path list /com/mchange/v2/log/default-mchange-log.properties, /mchange-commons.properties, /c3p0.properties, hocon:/reference,/application,/c3p0,/, /mchange-log.properties, /
11:25:44.964 [MLog-Init-Reporter] DEBUG com.mchange.v2.log.MLog - The configuration file for resource identifier '/mchange-commons.properties' could not be found. Skipping.
11:25:44.964 [MLog-Init-Reporter] DEBUG com.mchange.v2.log.MLog - The configuration file for resource identifier '/c3p0.properties' could not be found. Skipping.
11:25:44.965 [MLog-Init-Reporter] DEBUG com.mchange.v2.log.MLog - The configuration file for resource identifier 'hocon:/reference,/application,/c3p0,/' could not be found. Skipping.
11:25:44.965 [MLog-Init-Reporter] DEBUG com.mchange.v2.log.MLog - The configuration file for resource identifier '/mchange-log.properties' could not be found. Skipping.
11:25:44.990 [main] DEBUG com.mchange.v2.cfg.MConfig - The configuration file for resource identifier '/mchange-commons.properties' could not be found. Skipping.
11:25:44.990 [main] DEBUG com.mchange.v2.cfg.MConfig - The configuration file for resource identifier '/mchange-log.properties' could not be found. Skipping.
11:25:44.990 [main] DEBUG com.mchange.v2.cfg.MConfig - The configuration file for resource identifier 'hocon:/reference,/application,/c3p0,/' could not be found. Skipping.
11:25:44.990 [main] DEBUG com.mchange.v2.cfg.MConfig - The configuration file for resource identifier '/c3p0.properties' could not be found. Skipping.
11:25:45.454 [main] INFO com.mchange.v2.c3p0.C3P0Registry - Initializing c3p0-0.9.5.2 [built 08-December-2015 22:06:04 -0800; debug? true; trace: 10]
11:25:45.585 [main] DEBUG com.mchange.v2.c3p0.management.DynamicPooledDataSourceManagerMBean - MBean: com.mchange.v2.c3p0:type=PooledDataSource,identityToken=z8kflt9o1bvsw2h1rtintq|7946e1f4,name=z8kflt9o1bvsw2h1rtintq|7946e1f4 registered.
11:25:45.933 [main] DEBUG com.mchange.v2.c3p0.management.DynamicPooledDataSourceManagerMBean - MBean: com.mchange.v2.c3p0:type=PooledDataSource,identityToken=z8kflt9o1bvsw2h1rtintq|7946e1f4,name=z8kflt9o1bvsw2h1rtintq|7946e1f4 unregistered, in order to be reregistered after update.
11:25:45.937 [main] DEBUG com.mchange.v2.c3p0.management.DynamicPooledDataSourceManagerMBean - MBean: com.mchange.v2.c3p0:type=PooledDataSource,identityToken=z8kflt9o1bvsw2h1rtintq|7946e1f4,name=z8kflt9o1bvsw2h1rtintq|7946e1f4 registered.
11:25:46.712 [main] ERROR wres.io.config.SystemSettings - Could not load system settings.
java.io.IOException: Could not parse file
	at wres.io.reading.XMLReader.parse(XMLReader.java:78)
	at wres.io.config.SystemSettings.<init>(SystemSettings.java:68)
	at wres.io.config.SystemSettings.<clinit>(SystemSettings.java:32)
	at sun.reflect.GeneratedSerializationConstructorAccessor2.newInstance(Unknown Source)
	at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
	at org.objenesis.instantiator.sun.SunReflectionFactoryInstantiator.newInstance(SunReflectionFactoryInstantiator.java:48)
	at org.objenesis.ObjenesisBase.newInstance(ObjenesisBase.java:73)
	at org.mockito.internal.creation.instance.ObjenesisInstantiator.newInstance(ObjenesisInstantiator.java:18)
	at org.powermock.api.mockito.repackaged.ClassImposterizer.createProxy(ClassImposterizer.java:146)
	at org.powermock.api.mockito.repackaged.ClassImposterizer.imposterise(ClassImposterizer.java:69)
	at org.powermock.api.mockito.repackaged.ClassImposterizer.imposterise(ClassImposterizer.java:60)
	at org.powermock.api.mockito.repackaged.CglibMockMaker.createMock(CglibMockMaker.java:27)
	at org.powermock.api.mockito.internal.mockmaker.PowerMockMaker.createMock(PowerMockMaker.java:47)
	at org.mockito.internal.util.MockUtil.createMock(MockUtil.java:35)
	at org.mockito.internal.MockitoCore.mock(MockitoCore.java:63)
	at org.mockito.Mockito.mock(Mockito.java:1729)
	at org.mockito.internal.configuration.MockAnnotationProcessor.process(MockAnnotationProcessor.java:33)
	at org.mockito.internal.configuration.MockAnnotationProcessor.process(MockAnnotationProcessor.java:16)
	at org.mockito.internal.configuration.IndependentAnnotationEngine.createMockFor(IndependentAnnotationEngine.java:38)
	at org.mockito.internal.configuration.IndependentAnnotationEngine.process(IndependentAnnotationEngine.java:62)
	at org.mockito.internal.configuration.InjectingAnnotationEngine.processIndependentAnnotations(InjectingAnnotationEngine.java:57)
	at org.mockito.internal.configuration.InjectingAnnotationEngine.process(InjectingAnnotationEngine.java:41)
	at org.mockito.MockitoAnnotations.initMocks(MockitoAnnotations.java:69)
	at wres.io.data.caching.SourceCacheTest.setup(SourceCacheTest.java:67)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:50)
	at org.junit.internal.runners.model.ReflectiveCallable.run(ReflectiveCallable.java:12)
	at org.junit.runners.model.FrameworkMethod.invokeExplosively(FrameworkMethod.java:47)
	at org.junit.internal.runners.statements.RunBefores.evaluate(RunBefores.java:24)
	at org.junit.runners.ParentRunner.runLeaf(ParentRunner.java:325)
	at org.junit.runners.BlockJUnit4ClassRunner.runChild(BlockJUnit4ClassRunner.java:78)
	at org.junit.runners.BlockJUnit4ClassRunner.runChild(BlockJUnit4ClassRunner.java:57)
	at org.junit.runners.ParentRunner$3.run(ParentRunner.java:290)
	at org.junit.runners.ParentRunner$1.schedule(ParentRunner.java:71)
	at org.junit.runners.ParentRunner.runChildren(ParentRunner.java:288)
	at org.junit.runners.ParentRunner.access$000(ParentRunner.java:58)
	at org.junit.runners.ParentRunner$2.evaluate(ParentRunner.java:268)
	at org.junit.runners.ParentRunner.run(ParentRunner.java:363)
	at org.junit.runner.JUnitCore.run(JUnitCore.java:137)
	at com.intellij.junit4.JUnit4IdeaTestRunner.startRunnerWithArgs(JUnit4IdeaTestRunner.java:68)
	at com.intellij.rt.execution.junit.IdeaTestRunner$Repeater.startRunnerWithArgs(IdeaTestRunner.java:51)
	at com.intellij.rt.execution.junit.JUnitStarter.prepareStreamsAndStart(JUnitStarter.java:242)
	at com.intellij.rt.execution.junit.JUnitStarter.main(JUnitStarter.java:70)
Caused by: java.io.FileNotFoundException: wresconfig.xml (No such file or directory)
	at java.io.FileInputStream.open0(Native Method)
	at java.io.FileInputStream.open(FileInputStream.java:195)
	at java.io.FileInputStream.<init>(FileInputStream.java:138)
	at java.io.FileInputStream.<init>(FileInputStream.java:93)
	at java.io.FileReader.<init>(FileReader.java:58)
	at wres.io.reading.XMLReader.create_reader(XMLReader.java:122)
	at wres.io.reading.XMLReader.parse(XMLReader.java:62)
	... 45 common frames omitted

java.lang.NullPointerException
	at wres.io.config.SystemSettings.getConnectionPool(SystemSettings.java:208)
	at wres.io.data.caching.SourceCacheTest.setup(SourceCacheTest.java:71)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:50)
	at org.junit.internal.runners.model.ReflectiveCallable.run(ReflectiveCallable.java:12)
	at org.junit.runners.model.FrameworkMethod.invokeExplosively(FrameworkMethod.java:47)
	at org.junit.internal.runners.statements.RunBefores.evaluate(RunBefores.java:24)
	at org.junit.runners.ParentRunner.runLeaf(ParentRunner.java:325)
	at org.junit.runners.BlockJUnit4ClassRunner.runChild(BlockJUnit4ClassRunner.java:78)
	at org.junit.runners.BlockJUnit4ClassRunner.runChild(BlockJUnit4ClassRunner.java:57)
	at org.junit.runners.ParentRunner$3.run(ParentRunner.java:290)
	at org.junit.runners.ParentRunner$1.schedule(ParentRunner.java:71)
	at org.junit.runners.ParentRunner.runChildren(ParentRunner.java:288)
	at org.junit.runners.ParentRunner.access$000(ParentRunner.java:58)
	at org.junit.runners.ParentRunner$2.evaluate(ParentRunner.java:268)
	at org.junit.runners.ParentRunner.run(ParentRunner.java:363)
	at org.junit.runner.JUnitCore.run(JUnitCore.java:137)
	at com.intellij.junit4.JUnit4IdeaTestRunner.startRunnerWithArgs(JUnit4IdeaTestRunner.java:68)
	at com.intellij.rt.execution.junit.IdeaTestRunner$Repeater.startRunnerWithArgs(IdeaTestRunner.java:51)
	at com.intellij.rt.execution.junit.JUnitStarter.prepareStreamsAndStart(JUnitStarter.java:242)
	at com.intellij.rt.execution.junit.JUnitStarter.main(JUnitStarter.java:70)


    */

    private EmbeddedPostgres pgInstance;
    private ComboPooledDataSource connectionPoolDataSource;
    private static final AtomicInteger port = new AtomicInteger(5555);

    @Before
    public void setup()
            throws SQLException, IOException, PropertyVetoException, IllegalAccessException
    {
        LOGGER.trace("setup began");
        // Create a temporary db instance, but keep the binaries in one place.
        pgInstance = new EmbeddedPostgres(V9_6);

        // Guarantee test isolation with a different port for different tests
        int portNumber = port.getAndIncrement();

        LOGGER.debug("Port number is {}", portNumber);
        String jdbcUrl = pgInstance.start(cachedRuntimeConfig(Paths.get(".postgres_testinstance")),
                "localhost",
                portNumber,
                "wrestest",
                "wres",
                "test",
                new ArrayList<>());

        // Create our own data source to connect to temp database
        connectionPoolDataSource = new ComboPooledDataSource();
        connectionPoolDataSource.setDriverClass("org.postgresql.Driver");
        connectionPoolDataSource.setAutoCommitOnClose(true);

        LOGGER.debug("JDBC url is {}", jdbcUrl);

        // Connect to the temporary database
        connectionPoolDataSource.setJdbcUrl(jdbcUrl);

        // Create the tables needed for this test
        try (Connection con = connectionPoolDataSource.getConnection();
             Statement s = con.createStatement())
        {
            s.execute(readStringFromFile("SQL/wres.Source.sql",
                    StandardCharsets.US_ASCII));
        }

        // Because SystemSettings is static, and parses XML in constructor,
        // and referred to elsewhere, need to use powermock to replace it
        // instead of a simpler mock:
        PowerMockito.mockStatic(SystemSettings.class);
        LOGGER.debug("setup using connectionPoolDataSource: {}", connectionPoolDataSource);
        PowerMockito.when(SystemSettings.getConnectionPool())
                .thenReturn(connectionPoolDataSource);

        // Need Database behavior to be actual methods except for getPool()
        PowerMockito.spy(Database.class);
        PowerMockito.when(Database.getPool()).thenReturn(connectionPoolDataSource);
        // Above was necessary for isolation, otherwise, during the single-jvm
        // run of two tests, the first connectionPoolDataSource ended up
        // being used during the second test, and of course that one was closed.

        LOGGER.trace("setup ended");
    }

    @Test
    public void getTwiceFromSourceCache()
            throws Exception // TODO: update when SourceCache throws checked exceptions
    {
        LOGGER.trace("getTwiceFromSourceCache began");

        final String path = "/this/is/just/a/test";
        final String time = "2017-06-16 11:13:00";

        Integer result = SourceCache.getSourceID(path, time);

        assertTrue("The id should be an integer greater than zero.",
                   result > 0);

        Integer result2 = SourceCache.getSourceID(path, time);

        assertEquals("Getting an id with the same path and time should yield the same result.",
                     result2, result);

        int countOfRows;
        try (Connection con = connectionPoolDataSource.getConnection();
                Statement statement = con.createStatement();
                ResultSet r = statement.executeQuery("SELECT COUNT(*) FROM wres.Source"))
        {
            r.next();
            countOfRows = r.getInt(1);
        }

        assertEquals("There should be only one row in the wres.Source table",
                     1, countOfRows);

        LOGGER.trace("getTwiceFromSourceCache ended");
    }

    @Test
    public void initializeCacheWithExistingData()
            throws Exception // TODO: update when SourceCache throws checked exceptions
    {
        LOGGER.trace("initializeCacheWithExistingData began");

        // Create one cache that inserts data to set us up for 2nd cache init.
        SourceCache sc = new SourceCache();
        sc.init();
        final String path = "/this/is/just/a/test";
        final String time = "2017-06-20 16:55:00";
        Integer firstId = sc.getID(path, time);

        // Initialize a second cache, it should find the same data already present
        SourceCache scTwo = new SourceCache();
        scTwo.init();
        Integer secondId = scTwo.getID(path, time);

        assertEquals("Second cache should find id in database from first cache",
                    firstId, secondId);

        LOGGER.trace("initializeCacheWithExistingData ended");
    }

    @After
    public void tearDown()
    {
        LOGGER.trace("tearDown began");

        connectionPoolDataSource.close();

        pgInstance.stop();

        LOGGER.trace("tearDown ended");
    }

    /**
     * Thanks, https://stackoverflow.com/questions/326390/how-do-i-create-a-java-string-from-the-contents-of-a-file#326440
     */
    private static String readStringFromFile(String path, Charset encoding)
            throws IOException
    {
        byte[] encodedBytes = Files.readAllBytes(Paths.get(path));
        return new String(encodedBytes, encoding);
    }
}
