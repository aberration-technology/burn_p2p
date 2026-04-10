output "bootstrap_private_ips" {
  value       = [for instance in google_compute_instance.bootstrap : instance.network_interface[0].network_ip]
  description = "Private IPs for the bootstrap nodes."
}

output "bootstrap_public_ips" {
  value       = [for instance in google_compute_instance.bootstrap : try(instance.network_interface[0].access_config[0].nat_ip, null)]
  description = "Public IPs for the bootstrap nodes."
}

output "validator_private_ips" {
  value       = [for instance in google_compute_instance.validator : instance.network_interface[0].network_ip]
  description = "Private IPs for the validator nodes."
}

output "validator_public_ips" {
  value       = [for instance in google_compute_instance.validator : try(instance.network_interface[0].access_config[0].nat_ip, null)]
  description = "Public IPs for the validator nodes."
}

output "reducer_private_ips" {
  value       = [for instance in google_compute_instance.reducer : instance.network_interface[0].network_ip]
  description = "Private IPs for the reducer nodes."
}

output "reducer_public_ips" {
  value       = [for instance in google_compute_instance.reducer : try(instance.network_interface[0].access_config[0].nat_ip, null)]
  description = "Public IPs for the reducer nodes."
}

output "trainer_private_ips" {
  value       = [for instance in google_compute_instance.trainer : instance.network_interface[0].network_ip]
  description = "Private IPs for the trainer nodes."
}

output "trainer_public_ips" {
  value       = [for instance in google_compute_instance.trainer : try(instance.network_interface[0].access_config[0].nat_ip, null)]
  description = "Public IPs for the trainer nodes."
}
