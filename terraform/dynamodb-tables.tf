resource "aws_dynamodb_table" "anoobix-dynamodb-triptable" {
  name           = "trip-stream"
  billing_mode   = "PROVISIONED"
  read_capacity  = 10
  write_capacity = 5
  hash_key       = "uuid_value"

  attribute {
    name = "uuid_value"
    type = "S"
  }
}