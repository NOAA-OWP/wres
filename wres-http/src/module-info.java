/**
 * A worker wraps an instance of the core application as part of a web service deployment and performs evaluation work.
 */
module wres.http
{
    requires java.base;
    requires org.slf4j;
    requires java.logging;
    requires java.net.http;
    requires jdk.jfr;
    requires org.apache.commons.lang3;
    requires okhttp3;
    requires kotlin.stdlib;
    requires static lombok;
    exports wres.http;
}
