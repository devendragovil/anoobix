# resource "aws_dynamodb_table" "anoobix-dynamodb" {
#   name           = "trip-stream"
#   billing_mode   = "PROVISIONED"
#   read_capacity  = 10
#   write_capacity = 5
#   hash_key       = "timestamp_id"

#   attribute {
#     name = "timestamp_id"
#     type = "S"
#   }
# }