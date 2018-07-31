# Amplitude Event Configuration

This repository contains an 
[EventsToAmplitude](https://github.com/mozilla/telemetry-streaming/blob/master/src/main/scala/com/mozilla/telemetry/streaming/EventsToAmplitude.scala)
class which defines a job for taking in telemetry events, transforming them,
and publishing to Amplitude.

The particular filters and transformations applied are controlled via
a JSON configuration file in the top-level `config` directory of this repository.

## Related Documentation

- [Amplitude documentation](https://amplitude.zendesk.com/hc/en-us)
- [Amplitude HTTP API documentation](https://amplitude.zendesk.com/hc/en-us/articles/204771828-HTTP-API)
- [Mozilla internal Amplitude Onboarding Guide (Mana)](https://mana.mozilla.org/wiki/display/BIDW/Amplitude+Onboarding+Guideline)

## Destination Projects in Amplitude

We can define many "projects" in Amplitude to receive different sets of events.
Generally, one JSON configuration will correspond to one destination project.

The destination project is determined by which API key we use when making HTTP
calls to Amplitude. The job looks for the API key as an environment variable,
and it's generally the responsibility of the data engineer helping to deploy your
job to make sure the job is invoked with the right API key for your project.

## Meta events

The events that flow through your schema are determined by the source batch files
or Kafka topic. Generally, there's a one-to-one relation between entries
appearing in the `events` field of incoming pings and events presented to the job.

For main pings, though, we also inject a special "meta event" for each ping.
This event is prepended to the `events` list inside the ping and contains
some summary info about the subsession that the ping represents.
This event has a category of "meta" and a method of "session_split".
The event will be ignored by default. If you want to send this event to Amplitude,
you'll need to add an entry in your JSON configuration that matches the category
and method.

## Schema for JSON Configuration Files

The schema for JSON configuration files is defined in
[schemaFileSchema.json](https://github.com/mozilla/telemetry-streaming/blob/master/src/main/resources/schemas/schemaFileSchema.json).
We describe the meaning of each of the fields in the following subsections.

### source

`source` is a string like `"source": "telemetry"` defining which raw ping dataset
to use when the job runs in batch mode.
Available datasets are listed in [sources.json](https://console.aws.amazon.com/s3/object/net-mozaws-prod-us-west-2-pipeline-metadata/sources.json?region=us-east-1&tab=overview) in S3.
Note that each dataset is partitioned into files by a different set of fields
and performance for the job in batch mode can be greatly improved by configuring
efficient filters on those partitioning fields in the `filters` section of the config.

### filters

`filters` is a map of fieldNames to lists of acceptable values. Consider:

```json
"docType": [
  "main"
]
```

This filter will ensure that we only consider pings where the `docType` is `main`
(otherwise known as "main pings").
If we add additional entries to that list, they are treated as a logical OR, so we
would consider events with any of the listed docTypes.
In batch mode, filters will be applied both to the content of the pings and
to the partitioning fields. Configuring filters on partitioning fields of the source dataset
means that we're able to greatly reduce the set of files that need to be read for the
job to complete, greatly reducing runtime and cost.
You can also configure filters for fields within the ping that aren't used for partitioning
and in this case we can only apply the filter after the ping has been read from disk
and we have access to its contents.
For streaming mode, there is no concept of partitioning fields, so filters are always
applied based solely on the contents of the pings.

### eventGroups

`eventGroups` is where the transformation logic is defined that determines what
the events that make it to Amplitude look like. It has a more complex structure
than the other top-level fields.

Each group in this list has an `eventGroupName` and a list of events. The name of an
event in Amplitude will incorporate the group name like `eventGroupName - eventName`.

The next section describes the structure of each `event` entry.

### event

`name` determines the name of the event as seen in Amplitude.

`description` is a comment field. It doesn't get sent to Amplitude, but rather
exists as an opportunity for you to document the purpose of the event or any
additional caveats for future maintainters of the schema.

`sessionIdOffset` is an optional field created for the `devTools` job that gives
you a chance to add an offset to the session start time, which is what Amplitude uses for sessionId.
This allows projects where a Firefox session is insufficient to identify a usage stream to disambiguate multiple
sessions. For instance, in the devtools example, many users will open multiple devtools panes in the course of
a single Firefox session, sometimes concurrently. Devtools events contain an extra field which is the time in
milliseconds since the start of the Firefox session when that particular pane was opened, which is then added to the
Firefox session start time.

`amplitudeProperties` is an optional map of `eventPropertyName` to `sourceField`.
The values configured here will be added as `event_properties` when we call
Amplitude's [HTTP API](https://amplitude.zendesk.com/hc/en-us/articles/204771828-HTTP-API).
The `eventPropertyName` will be used as the key in `event_properties` while `sourceField`
is used to look up an appropriate value; see more on `sourceField` in a subsection below.

`userProperties` is an optional map very similar to `amplitudeProperties`, but
entries here will be set in the `user_properties` map we send to amplitude rather
than `event_properties`.

`schema` is a required field that defines which events match this entry.
Any event that does not have the `required` fields populated will not match that entry.
`category`, `method`, and `object` are the "coordinates" sent with every event
and allow you to define lists of values to consider. Values within a list are 
treated as a logical OR, 
but the various fields are combined via a logical AND; for example, if you configure both
`category` and `method` in a schema, an event must match both
one of the listed categories and one of the listed methods in order to match.

#### sourceField

`sessionIdOffset`, `amplitudeProperties`, and `userProperties` all accept
string values that are interpreted as looking up a field in the source event.

The following values will look up the corresponding top-level field of the event:

- timestamp
- category
- method
- object
- value

Events can also optionally have an `extra` object that's a map of key-value pairs.
To look up a key from `extra` named `foo`, use:

- extra.foo

Finally, you can inject a literal value that's not looked up from the event.
To inject the literal string "foo" as the value, use:

- literal.foo
