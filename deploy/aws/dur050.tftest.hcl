mock_provider "aws" {}

variables {
  ami_id                   = "ami-0123456789abcdef0"
  repo_ref                 = "0000000000000000000000000000000000000000"
  expires_at               = "2026-10-07T00:00:00Z"
  postgres_password        = "TestPostgresPassword123456"
  dur050_observer_password = "TestObserverPassword1234567890"
}

run "rejects_generator_with_dur049_attribution" {
  command = plan

  variables {
    enable_dur050_load_generator = true
    instance_type                = "c7i.large"
    dependency_instance_type     = "m7i.large"
    load_generator_instance_type = "c7i.large"
    root_volume_size_gb          = 40
  }

  expect_failures = [
    var.campaign_slug,
    var.task_id,
    var.environment_name,
  ]
}

run "rejects_generator_with_recovery_instance_sizes" {
  command = plan

  variables {
    campaign_slug                = "dur050"
    task_id                      = "DUR-050"
    environment_name             = "portfolio-capacity"
    enable_dur050_load_generator = true
  }

  expect_failures = [var.enable_dur050_load_generator]
}

run "accepts_complete_dur050_profile" {
  command = plan

  variables {
    campaign_slug                = "dur050"
    task_id                      = "DUR-050"
    environment_name             = "portfolio-capacity"
    enable_dur050_load_generator = true
    instance_type                = "c7i.large"
    dependency_instance_type     = "m7i.large"
    load_generator_instance_type = "c7i.large"
    root_volume_size_gb          = 40
  }

  assert {
    condition     = local.common_name == "durable-engine-dur050"
    error_message = "The DUR-050 profile must produce DUR-050 resource names."
  }

  assert {
    condition     = aws_instance.app[0].instance_type == "c7i.large" && aws_instance.app[1].instance_type == "c7i.large" && aws_instance.dependency.instance_type == "m7i.large" && aws_instance.load_generator[0].instance_type == "c7i.large"
    error_message = "The complete DUR-050 profile must plan its reviewed production-like host sizes."
  }

  assert {
    condition     = aws_security_group.app.name == "durable-engine-dur050-app" && aws_instance.app[0].tags["Name"] == "durable-engine-dur050-app-1" && aws_ssm_parameter.dur050_observer_password[0].name == "/durable-engine-dur050/dur050-observer-password"
    error_message = "The DUR-050 profile must use dur050 names for security groups, app instances, and the observer parameter."
  }

  assert {
    condition = strcontains(aws_instance.load_generator[0].user_data, "bash scripts/dur050-build-generator-binaries.sh") && can(regex(
      "(?s)export HOME=/root.*export GOPATH=/root/go.*export GOMODCACHE=/root/go/pkg/mod.*export GOCACHE=/root/.cache/go-build.*export GOFLAGS=-mod=readonly.*go build",
      file("${path.module}/../../scripts/dur050-build-generator-binaries.sh")
    ))
    error_message = "Rendered DUR-050 user data must call the build script, whose explicit HOME/cache exports precede every Go build."
  }
}

run "preserves_default_dur049_profile" {
  command = plan

  variables {
    enable_dur050_load_generator = false
    dur050_observer_password     = ""
  }

  assert {
    condition     = local.common_name == "durable-engine-dur049"
    error_message = "The default recovery profile must retain its DUR-049 resource names."
  }

  assert {
    condition     = length(aws_instance.load_generator) == 0
    error_message = "The default DUR-049 profile must not create a load-generator host."
  }
}
