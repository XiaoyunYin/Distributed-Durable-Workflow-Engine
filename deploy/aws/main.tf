locals {
  common_name = "${var.project_name}-dur049"
}

resource "aws_vpc" "campaign" {
  cidr_block           = "10.49.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "${local.common_name}-vpc" }
}

resource "aws_internet_gateway" "campaign" {
  vpc_id = aws_vpc.campaign.id
  tags   = { Name = "${local.common_name}-igw" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.campaign.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.campaign.id
  }

  tags = { Name = "${local.common_name}-public" }
}

resource "aws_subnet" "public" {
  count             = 2
  vpc_id            = aws_vpc.campaign.id
  cidr_block        = "10.49.${count.index + 1}.0/24"
  availability_zone = var.availability_zones[count.index]

  tags = { Name = "${local.common_name}-public-${count.index + 1}" }
}

resource "aws_route_table_association" "public" {
  count          = 2
  route_table_id = aws_route_table.public.id
  subnet_id      = aws_subnet.public[count.index].id
}

resource "aws_security_group" "app" {
  name        = "${local.common_name}-app"
  description = "Application host ingress for the bounded DUR-049 campaign"
  vpc_id      = aws_vpc.campaign.id

  dynamic "ingress" {
    for_each = var.admin_cidrs
    content {
      description = "Operator API access"
      protocol    = "tcp"
      from_port   = 8080
      to_port     = 8080
      cidr_blocks = [ingress.value]
    }
  }

  egress {
    description = "Outbound package and dependency access"
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.common_name}-app" }
}

resource "aws_security_group" "dependency" {
  name        = "${local.common_name}-dependency"
  description = "PostgreSQL and Kafka ingress only from campaign app hosts"
  vpc_id      = aws_vpc.campaign.id

  ingress {
    description     = "PostgreSQL from application hosts"
    protocol        = "tcp"
    from_port       = 5432
    to_port         = 5432
    security_groups = [aws_security_group.app.id]
  }

  ingress {
    description     = "Kafka from application hosts"
    protocol        = "tcp"
    from_port       = 9092
    to_port         = 9092
    security_groups = [aws_security_group.app.id]
  }

  dynamic "ingress" {
    for_each = var.admin_cidrs
    content {
      description = "Operator diagnostics"
      protocol    = "tcp"
      from_port   = 22
      to_port     = 22
      cidr_blocks = [ingress.value]
    }
  }

  egress {
    description = "Outbound package and repository access"
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.common_name}-dependency" }
}

resource "aws_iam_role" "ssm" {
  name = "${local.common_name}-ssm"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.ssm.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "ssm" {
  name = "${local.common_name}-ssm"
  role = aws_iam_role.ssm.name
}

resource "aws_instance" "dependency" {
  ami                         = var.ami_id
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.public[0].id
  vpc_security_group_ids      = [aws_security_group.dependency.id]
  associate_public_ip_address = true
  iam_instance_profile        = aws_iam_instance_profile.ssm.name
  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/cloud-init/dependency.sh.tftpl", {
    repo_url          = var.repo_url
    repo_ref          = var.repo_ref
    postgres_user     = var.postgres_user
    postgres_db       = var.postgres_db
    postgres_password = var.postgres_password
    postgres_image    = var.postgres_image
    kafka_image       = var.kafka_image
  })

  root_block_device {
    encrypted   = true
    volume_type = "gp3"
    volume_size = var.root_volume_size_gb
  }

  tags = { Name = "${local.common_name}-dependency" }
}

resource "aws_instance" "app" {
  count                       = 2
  ami                         = var.ami_id
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.public[count.index].id
  vpc_security_group_ids      = [aws_security_group.app.id]
  associate_public_ip_address = true
  iam_instance_profile        = aws_iam_instance_profile.ssm.name
  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/cloud-init/app.sh.tftpl", {
    repo_url              = var.repo_url
    repo_ref              = var.repo_ref
    runtime_role          = "scheduler-${count.index + 1}"
    worker_id             = "worker-${count.index + 1}"
    postgres_user         = var.postgres_user
    postgres_db           = var.postgres_db
    postgres_password     = var.postgres_password
    worker_slots          = var.worker_slots
    dependency_private_ip = aws_instance.dependency.private_ip
  })

  root_block_device {
    encrypted   = true
    volume_type = "gp3"
    volume_size = var.root_volume_size_gb
  }

  tags = { Name = "${local.common_name}-app-${count.index + 1}" }
}
