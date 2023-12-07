resource "aws_key_pair" "anoobix-ec2-key" {
  key_name   = "anoobix-ec2-key"
  public_key = file("~/.ssh/anoobix-ec2-key.pub")

  tags = {
    project = "anoobix"
  }
}

resource "aws_instance" "anoobix-ec2-instance" {
  instance_type = "t2.large"
  ami           = data.aws_ami.anoobix-ec2-ami.id
  private_ip    = "10.123.1.7"

  tags = {
    Name    = "anoobix-ec2-instance"
    project = "anoobix"
  }

  key_name               = aws_key_pair.anoobix-ec2-key.id
  vpc_security_group_ids = [aws_security_group.anoobix-sg.id]
  subnet_id              = aws_subnet.anoobix-public-subnet.id

  root_block_device {
    volume_size = 10
  }
}

resource "aws_eip" "anoobix-public-ip" {
  domain = "vpc"

  instance                  = aws_instance.anoobix-ec2-instance.id
  associate_with_private_ip = aws_instance.anoobix-ec2-instance.private_ip

  tags = {
    project = "anoobix"
  }
}

resource "aws_instance" "anoobix-ec2-instance-dataingestion" {
  instance_type = "t2.micro"
  ami           = data.aws_ami.anoobix-ec2-ami.id
  private_ip    = "10.123.1.4"

  tags = {
    Name    = "anoobix-ec2-instance-dataingestion"
    project = "anoobix"
  }

  key_name               = aws_key_pair.anoobix-ec2-key.id
  vpc_security_group_ids = [aws_security_group.anoobix-sg.id]
  subnet_id              = aws_subnet.anoobix-public-subnet.id

  root_block_device {
    volume_size = 10
  }
}

resource "aws_eip" "anoobix-public-ip-dataingestion" {
  domain = "vpc"

  instance                  = aws_instance.anoobix-ec2-instance-dataingestion.id
  associate_with_private_ip = aws_instance.anoobix-ec2-instance-dataingestion.private_ip

  tags = {
    project = "anoobix"
  }
}