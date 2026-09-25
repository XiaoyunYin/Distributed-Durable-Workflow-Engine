variable "aws_region" {
  description = "AWS region for the bounded recovery campaign."
  type        = string
  default     = "us-west-1"
}

variable "availability_zones" {
  description = "Two explicitly selected AZs; keeping them as inputs avoids an unreviewed discovery query during apply."
  type        = list(string)
  default     = ["us-west-1a", "us-west-1c"]

  validation {
    condition     = length(var.availability_zones) == 2 && var.availability_zones[0] != var.availability_zones[1]
    error_message = "availability_zones must contain exactly two distinct zones."
  }
}

variable "project_name" {
  description = "Short resource name used in tags and names."
  type        = string
  default     = "durable-engine"
}

variable "campaign_slug" {
  description = "Lowercase campaign suffix used in resource names; defaults to the original DUR-049 topology name."
  type        = string
  default     = "dur049"

  validation {
    condition     = can(regex("^[a-z0-9]+(-[a-z0-9]+)*$", var.campaign_slug))
    error_message = "campaign_slug must contain lowercase letters, digits, and single hyphen separators."
  }

  validation {
    condition     = var.enable_dur050_load_generator == (var.campaign_slug == "dur050")
    error_message = "campaign_slug must be dur050 exactly when the DUR-050 load-generator profile is enabled."
  }
}

variable "task_id" {
  description = "Task attribution tag for resources created by this campaign."
  type        = string
  default     = "DUR-049"

  validation {
    condition     = can(regex("^DUR-[0-9]{3}$", var.task_id))
    error_message = "task_id must be a DUR-### task identifier."
  }

  validation {
    condition     = var.enable_dur050_load_generator == (var.task_id == "DUR-050")
    error_message = "task_id must be DUR-050 exactly when the DUR-050 load-generator profile is enabled."
  }
}

variable "environment_name" {
  description = "Environment attribution tag; defaults to the original recovery deployment."
  type        = string
  default     = "portfolio-recovery"

  validation {
    condition     = can(regex("^[a-z0-9]+(-[a-z0-9]+)*$", var.environment_name))
    error_message = "environment_name must contain lowercase letters, digits, and single hyphen separators."
  }

  validation {
    condition     = var.enable_dur050_load_generator == (var.environment_name == "portfolio-capacity")
    error_message = "environment_name must be portfolio-capacity exactly when the DUR-050 load-generator profile is enabled."
  }
}

variable "ami_id" {
  description = "Pinned Ubuntu AMI ID for the selected region. Resolve and record it before apply."
  type        = string

  validation {
    condition     = can(regex("^ami-[0-9a-f]+$", var.ami_id))
    error_message = "ami_id must be a concrete AWS AMI ID, not a floating image lookup."
  }
}

variable "repo_url" {
  description = "Public repository cloned by cloud-init at the exact repo_ref."
  type        = string
  default     = "https://github.com/XiaoyunYin/Distributed-Durable-Workflow-Engine.git"
}

variable "repo_ref" {
  description = "Immutable full 40-character lowercase commit SHA to deploy."
  type        = string

  validation {
    condition     = can(regex("^[0-9a-f]{40}$", var.repo_ref))
    error_message = "repo_ref must be a full 40-character lowercase commit SHA; short SHAs, tags, and moving branches are rejected."
  }
}

variable "expires_at" {
  description = "UTC expiration timestamp recorded on every resource tag, for example 2026-10-05T00:00:00Z."
  type        = string

  validation {
    condition     = can(formatdate("YYYY-MM-DD'T'hh:mm:ss'Z'", var.expires_at))
    error_message = "expires_at must be an RFC3339 UTC timestamp."
  }
}

variable "admin_cidrs" {
  description = "CIDRs allowed to reach the app health/API port. Keep this to the operator's current IP."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for cidr in var.admin_cidrs : can(cidrhost(cidr, 0))])
    error_message = "admin_cidrs must contain valid CIDR blocks."
  }
}

variable "instance_type" {
  description = "Small bounded instance type for the two application hosts."
  type        = string
  default     = "t3.micro"
}

variable "dependency_instance_type" {
  description = "Bounded instance type for the dependency host."
  type        = string
  default     = "t3.small"
}

variable "enable_dur050_load_generator" {
  description = "Create the separate DUR-050 load-generator/observer host. Keep disabled for DUR-049 recovery campaigns."
  type        = bool
  default     = false

  validation {
    condition = !var.enable_dur050_load_generator || (
      var.instance_type == "c7i.large" &&
      var.dependency_instance_type == "m7i.large" &&
      var.load_generator_instance_type == "c7i.large" &&
      var.root_volume_size_gb == 40
    )
    error_message = "The DUR-050 profile requires c7i.large app and load-generator hosts, an m7i.large dependency host, and 40-GiB root volumes."
  }
}

variable "load_generator_instance_type" {
  description = "DUR-050 load-generator instance type; excluded from plans unless enable_dur050_load_generator is true."
  type        = string
  default     = "c7i.large"
}

variable "root_volume_size_gb" {
  description = "Encrypted root volume size for each host."
  type        = number
  default     = 20

  validation {
    condition     = var.root_volume_size_gb >= 12 && var.root_volume_size_gb <= 40
    error_message = "root_volume_size_gb must stay between 12 and 40 for this bounded campaign."
  }
}

variable "postgres_user" {
  type    = string
  default = "durable"
}

variable "postgres_db" {
  type    = string
  default = "durable"
}

variable "postgres_password" {
  description = "Ephemeral alphanumeric password stored in an encrypted SSM parameter; never put it in user-data or commit it."
  type        = string
  sensitive   = true

  validation {
    condition     = can(regex("^[A-Za-z0-9]{20,64}$", var.postgres_password))
    error_message = "postgres_password must be 20-64 alphanumeric characters so the generated DATABASE_URL is unambiguous."
  }
}

variable "dur050_observer_password" {
  description = "Separate ephemeral SCRAM credential for the DUR-050 SELECT-only observer role, stored in encrypted SSM Parameter Store."
  type        = string
  sensitive   = true
  default     = ""

  validation {
    condition     = var.enable_dur050_load_generator ? can(regex("^[A-Za-z0-9]{24,64}$", var.dur050_observer_password)) : var.dur050_observer_password == ""
    error_message = "When the DUR-050 load generator is enabled, dur050_observer_password must be 24-64 alphanumeric characters; otherwise leave it empty."
  }
}

variable "postgres_image" {
  description = "Pinned PostgreSQL image digest."
  type        = string
  default     = "postgres:18.6-bookworm@sha256:1c59e2c3c818eaa0f0628f695b36e7c9e362d6b219b36a54a32df645cbd7e1af"
}

variable "kafka_image" {
  description = "Pinned Kafka image digest."
  type        = string
  default     = "apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837"
}

variable "worker_slots" {
  description = "Fixed worker capacity per application host for all measurements."
  type        = number
  default     = 2

  validation {
    condition     = var.worker_slots >= 1 && var.worker_slots <= 4
    error_message = "worker_slots must be between 1 and 4."
  }
}
