resource "aws_s3_bucket" "anoobix-s3" {
  bucket = "anoobix"
  tags = {
    project = "noobix"
  }
}