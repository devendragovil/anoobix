resource "aws_vpc" "anoobix-vpc" {
  cidr_block           = "10.123.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    project = "anoobix"
  }
}

resource "aws_subnet" "anoobix-public-subnet" {
  vpc_id                  = aws_vpc.anoobix-vpc.id
  cidr_block              = "10.123.1.0/24"
  availability_zone       = "us-west-2a"

  tags = {
    project = "anoobix"
  }
}

resource "aws_internet_gateway" "anoobix-internet-gateway" {
  vpc_id = aws_vpc.anoobix-vpc.id

  tags = {
    project = "anoobix"
  }
}

resource "aws_route_table" "anoobix-public-route-table" {
  vpc_id = aws_vpc.anoobix-vpc.id

  tags = {
    project = "anoobix"
  }
}

resource "aws_route" "anoobix-default-route" {
  route_table_id         = aws_route_table.anoobix-public-route-table.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.anoobix-internet-gateway.id
}

resource "aws_route_table_association" "anoobix-public-assoc" {
  subnet_id      = aws_subnet.anoobix-public-subnet.id
  route_table_id = aws_route_table.anoobix-public-route-table.id
}

resource "aws_security_group" "anoobix-sg" {
  name        = "anoobix-sg"
  description = "anoobix security group"
  vpc_id      = aws_vpc.anoobix-vpc.id

  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    project = "anoobix"
  }
}