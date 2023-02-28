/**
 * A worker wraps an instance of the core application as part of a web service deployment and performs evaluation work.
 */
module wres.worker
{
    requires java.base;
    requires com.google.protobuf;
    requires com.rabbitmq.client;
    requires org.slf4j;
    requires wres.messages;
}
