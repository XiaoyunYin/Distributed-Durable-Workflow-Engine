locals {
  common_name               = "${var.project_name}-dur049"
  postgres_secret_parameter = "/${local.common_name}/postgres-password"
  observer_secret_parameter = "/${local.common_name}/dur050-observer-password"
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

  dynamic "ingress" {
    for_each = aws_security_group.load_generator
    content {
      description     = "Private load-generator API traffic"
      protocol        = "tcp"
      from_port       = 8080
      to_port         = 8080
      security_groups = [ingress.value.id]
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
  description = "PostgreSQL/Kafka ingress from app hosts and PostgreSQL observer ingress from load generator"
  vpc_id      = aws_vpc.campaign.id

  ingress {
    description     = "PostgreSQL from application hosts"
    protocol        = "tcp"
    from_port       = 5432
    to_port         = 5432
    security_groups = [aws_security_group.app.id]
  }

  dynamic "ingress" {
    for_each = aws_security_group.load_generator
    content {
      description     = "Read-only DUR-050 observer from load-generator host"
      protocol        = "tcp"
      from_port       = 5432
      to_port         = 5432
      security_groups = [ingress.value.id]
    }
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

resource "aws_security_group" "load_generator" {
  count       = var.enable_dur050_load_generator ? 1 : 0
  name        = "${local.common_name}-load-generator"
  description = "Private DUR-050 load generator and read-only database observer"
  vpc_id      = aws_vpc.campaign.id

  egress {
    description = "Outbound package and campaign endpoint access"
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.common_name}-load-generator" }
}

resource "aws_ssm_parameter" "postgres_password" {
  name        = local.postgres_secret_parameter
  description = "Ephemeral DUR-049 PostgreSQL password; destroy with the campaign."
  type        = "SecureString"
  value       = var.postgres_password
  tier        = "Standard"

  tags = { Name = "${local.common_name}-postgres-password" }
}

resource "aws_ssm_parameter" "dur050_observer_password" {
  count       = var.enable_dur050_load_generator ? 1 : 0
  name        = local.observer_secret_parameter
  description = "Ephemeral DUR-050 read-only observer credential; destroy with the campaign."
  type        = "SecureString"
  value       = var.dur050_observer_password
  tier        = "Standard"

  tags = { Name = "${local.common_name}-dur050-observer-password" }
}

resource "aws_iam_policy" "postgres_secret_read" {
  name        = "${local.common_name}-postgres-secret-read"
  description = "Read only the DUR-049 PostgreSQL password from Parameter Store."
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ssm:GetParameter"]
      Resource = aws_ssm_parameter.postgres_password.arn
    }]
  })
}

resource "aws_iam_role_policy_attachment" "postgres_secret_read" {
  role       = aws_iam_role.ssm.name
  policy_arn = aws_iam_policy.postgres_secret_read.arn
}

resource "aws_iam_instance_profile" "ssm" {
  name = "${local.common_name}-ssm"
  role = aws_iam_role.ssm.name
}

resource "aws_iam_role" "dependency_ssm" {
  name               = "${local.common_name}-dependency-ssm"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "dependency_ssm_core" {
  role       = aws_iam_role.dependency_ssm.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_policy" "dependency_secret_read" {
  name        = "${local.common_name}-dependency-secret-read"
  description = "Read only the PostgreSQL bootstrap and DUR-050 observer secrets."
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ssm:GetParameter"]
      Resource = var.enable_dur050_load_generator ? [
        aws_ssm_parameter.postgres_password.arn,
        aws_ssm_parameter.dur050_observer_password[0].arn,
      ] : [aws_ssm_parameter.postgres_password.arn]
    }]
  })
}

resource "aws_iam_role_policy_attachment" "dependency_secret_read" {
  role       = aws_iam_role.dependency_ssm.name
  policy_arn = aws_iam_policy.dependency_secret_read.arn
}

resource "aws_iam_instance_profile" "dependency_ssm" {
  name = "${local.common_name}-dependency-ssm"
  role = aws_iam_role.dependency_ssm.name
}

resource "aws_iam_role" "load_generator_ssm" {
  count              = var.enable_dur050_load_generator ? 1 : 0
  name               = "${local.common_name}-load-generator-ssm"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "load_generator_ssm_core" {
  count      = var.enable_dur050_load_generator ? 1 : 0
  role       = aws_iam_role.load_generator_ssm[count.index].name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_policy" "load_generator_observer_read" {
  count       = var.enable_dur050_load_generator ? 1 : 0
  name        = "${local.common_name}-load-generator-observer-read"
  description = "Read only the DUR-050 observer credential from Parameter Store."
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ssm:GetParameter"]
      Resource = aws_ssm_parameter.dur050_observer_password[count.index].arn
    }]
  })
}

resource "aws_iam_role_policy_attachment" "load_generator_observer_read" {
  count      = var.enable_dur050_load_generator ? 1 : 0
  role       = aws_iam_role.load_generator_ssm[count.index].name
  policy_arn = aws_iam_policy.load_generator_observer_read[count.index].arn
}

resource "aws_iam_instance_profile" "load_generator_ssm" {
  count = var.enable_dur050_load_generator ? 1 : 0
  name  = "${local.common_name}-load-generator-ssm"
  role  = aws_iam_role.load_generator_ssm[count.index].name
}

resource "aws_instance" "dependency" {
  depends_on                  = [aws_iam_role_policy_attachment.dependency_secret_read, aws_iam_role_policy_attachment.dependency_ssm_core]
  ami                         = var.ami_id
  instance_type               = var.dependency_instance_type
  subnet_id                   = aws_subnet.public[0].id
  vpc_security_group_ids      = [aws_security_group.dependency.id]
  associate_public_ip_address = true
  iam_instance_profile        = aws_iam_instance_profile.dependency_ssm.name
  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/cloud-init/dependency.sh.tftpl", {
    repo_url                  = var.repo_url
    repo_ref                  = var.repo_ref
    aws_region                = var.aws_region
    postgres_user             = var.postgres_user
    postgres_db               = var.postgres_db
    postgres_secret_parameter = local.postgres_secret_parameter
    observer_secret_parameter = local.observer_secret_parameter
    enable_dur050_observer     = var.enable_dur050_load_generator
    postgres_image            = var.postgres_image
    kafka_image               = var.kafka_image
  })

  root_block_device {
    encrypted   = true
    volume_type = "gp3"
    volume_size = var.root_volume_size_gb
  }

  credit_specification { cpu_credits = "standard" }

  tags = { Name = "${local.common_name}-dependency" }
}

resource "aws_instance" "app" {
  depends_on                  = [aws_iam_role_policy_attachment.postgres_secret_read]
  count                       = 2
  ami                         = var.ami_id
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.public[count.index].id
  vpc_security_group_ids      = [aws_security_group.app.id]
  associate_public_ip_address = true
  iam_instance_profile        = aws_iam_instance_profile.ssm.name
  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/cloud-init/app.sh.tftpl", {
    repo_url                  = var.repo_url
    repo_ref                  = var.repo_ref
    aws_region                = var.aws_region
    runtime_role              = "scheduler-${count.index + 1}"
    worker_id                 = "worker-${count.index + 1}"
    postgres_user             = var.postgres_user
    postgres_db               = var.postgres_db
    postgres_secret_parameter = local.postgres_secret_parameter
    worker_slots              = var.worker_slots
    dependency_private_ip     = aws_instance.dependency.private_ip
  })

  root_block_device {
    encrypted   = true
    volume_type = "gp3"
    volume_size = var.root_volume_size_gb
  }

  credit_specification { cpu_credits = "standard" }

  tags = { Name = "${local.common_name}-app-${count.index + 1}" }
}

resource "aws_instance" "load_generator" {
  depends_on = [
    aws_iam_role_policy_attachment.load_generator_ssm_core,
    aws_iam_role_policy_attachment.load_generator_observer_read,
  ]
  count                       = var.enable_dur050_load_generator ? 1 : 0
  ami                         = var.ami_id
  instance_type               = var.load_generator_instance_type
  subnet_id                   = aws_subnet.public[0].id
  vpc_security_group_ids      = [aws_security_group.load_generator[count.index].id]
  associate_public_ip_address = true
  iam_instance_profile        = aws_iam_instance_profile.load_generator_ssm[count.index].name
  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/cloud-init/load-generator.sh.tftpl", {
    repo_url = var.repo_url
    repo_ref = var.repo_ref
  })

  root_block_device {
    encrypted   = true
    volume_type = "gp3"
    volume_size = var.root_volume_size_gb
  }

  tags = { Name = "${local.common_name}-load-generator" }
}
