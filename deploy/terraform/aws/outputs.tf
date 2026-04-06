output "bootstrap_public_ips" {
  value       = aws_instance.bootstrap[*].public_ip
  description = "Public IPs for the bootstrap/coherence seed nodes."
}

output "validator_public_ips" {
  value       = aws_instance.validator[*].public_ip
  description = "Public IPs for the validator nodes."
}

output "trainer_public_ips" {
  value       = aws_instance.trainer[*].public_ip
  description = "Public IPs for the trainer nodes."
}
