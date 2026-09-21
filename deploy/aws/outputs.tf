output "dependency_instance_id" {
  description = "SSM target for the dependency host."
  value       = aws_instance.dependency.id
}

output "dependency_private_ip" {
  description = "Private dependency address used by application hosts."
  value       = aws_instance.dependency.private_ip
}

output "dependency_public_ip" {
  description = "Operator-only public address; dependency ports are SG-restricted."
  value       = aws_instance.dependency.public_ip
}

output "app_instance_ids" {
  description = "SSM targets for the two application hosts."
  value       = aws_instance.app[*].id
}

output "app_private_ips" {
  description = "Private addresses used for host-level fault confirmation."
  value       = aws_instance.app[*].private_ip
}

output "app_public_ips" {
  description = "Operator-only public addresses for health/API checks."
  value       = aws_instance.app[*].public_ip
}

output "resource_expiration" {
  description = "Expiration tag applied to all campaign resources."
  value       = var.expires_at
}
