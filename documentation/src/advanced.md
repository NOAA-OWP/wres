# Advanced Alterations of System Behavior

This section is intended to have additional information for users who wish to
change behaviors of the system that do not affect the functionality of inputs
being transformed into outputs. In other words, this section does not need to be
used in order to operate the system correctly.

## Logging behavior

As of 2017-09-05, the system uses the logback logging library implementation.

By default, the logback configuration file is:

        lib/conf/logback.xml

See <https://logback.qos.ch/manual/> for how to configure logback.

For example, if you wish to have one log file per execution, you can change the
FILE appender to use a byMillisecond timestamp, similar to what is described at
<https://logback.qos.ch/manual/appenders.html#uniquelyNamed>

Warning: if you change the appender to have one log file per execution, you will
need to set up external log file rotation with a tool such as logrotate, else
you will eventually fill up your disk.
