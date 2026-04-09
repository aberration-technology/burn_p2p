output "bootstrap_public_ips" {
  value       = [for instance in google_compute_instance.bootstrap : instance.network_interface[0].access_config[0].nat_ip]
  description = "Public IPs for the bootstrap nodes."
}

output "validator_public_ips" {
  value       = [for instance in google_compute_instance.validator : instance.network_interface[0].access_config[0].nat_ip]
  description = "Public IPs for the validator nodes."
}

output "reducer_public_ips" {
  value       = [for instance in google_compute_instance.reducer : instance.network_interface[0].access_config[0].nat_ip]
  description = "Public IPs for the reducer nodes."
}

output "trainer_public_ips" {
  value       = [for instance in google_compute_instance.trainer : instance.network_interface[0].access_config[0].nat_ip]
  description = "Public IPs for the trainer nodes."
}
