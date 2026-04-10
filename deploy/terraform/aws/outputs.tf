output "bootstrap_private_ips" {
  value       = aws_instance.bootstrap[*].private_ip
  description = "Private IPs for the bootstrap/coherence seed nodes."
}

output "bootstrap_public_ips" {
  value       = aws_instance.bootstrap[*].public_ip
  description = "Public IPs for the bootstrap/coherence seed nodes."
}

output "validator_private_ips" {
  value       = aws_instance.validator[*].private_ip
  description = "Private IPs for the validator nodes."
}

output "validator_public_ips" {
  value       = aws_instance.validator[*].public_ip
  description = "Public IPs for the validator nodes."
}

output "reducer_private_ips" {
  value       = aws_instance.reducer[*].private_ip
  description = "Private IPs for the reducer nodes."
}

output "reducer_public_ips" {
  value       = aws_instance.reducer[*].public_ip
  description = "Public IPs for the reducer nodes."
}

output "trainer_private_ips" {
  value       = aws_instance.trainer[*].private_ip
  description = "Private IPs for the trainer nodes."
}

output "trainer_public_ips" {
  value       = aws_instance.trainer[*].public_ip
  description = "Public IPs for the trainer nodes."
}
