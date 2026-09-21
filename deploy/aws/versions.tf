terraform {
  required_version = ">= 1.6.0, < 2.0.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Task        = "DUR-049"
      ExpiresAt   = var.expires_at
      ManagedBy   = "terraform"
      Environment = "portfolio-recovery"
    }
  }
}
