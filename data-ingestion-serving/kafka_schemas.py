
trip_update_schema = """
{
  "type": "record",
  "name": "TripMessageModel",
  "fields": [
    {
      "name": "timestamp_id",
      "type": "string"
    },
    {
      "name": "message",
      "type": "bytes"
    }
  ]
}
"""