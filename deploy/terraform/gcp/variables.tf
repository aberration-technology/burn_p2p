variable "project_id" {
  type        = string
  description = "GCP project ID."
}

variable "region" {
  type        = string
  description = "GCP region."
  default     = "us-central1"
}

variable "zone" {
  type        = string
  description = "GCP zone."
  default     = "us-central1-a"
}

variable "name_prefix" {
  type        = string
  description = "Name prefix applied to resources."
  default     = "burn-p2p"
}

variable "network" {
  type        = string
  description = "VPC network name or self_link."
  default     = "default"
}

variable "subnetwork" {
  type        = string
  description = "Optional subnetwork name or self_link."
  default     = ""
}

variable "bootstrap_count" {
  type        = number
  description = "Number of bootstrap/coherence seed nodes."
  default     = 2
}

variable "validator_count" {
  type        = number
  description = "Number of validator nodes."
  default     = 1
}

variable "reducer_count" {
  type        = number
  description = "Number of reducer nodes."
  default     = 1
}

variable "trainer_count" {
  type        = number
  description = "Number of trainer nodes."
  default     = 0
}

variable "bootstrap_machine_type" {
  type        = string
  description = "Machine type for bootstrap nodes."
  default     = "e2-small"
}

variable "validator_machine_type" {
  type        = string
  description = "Machine type for validator nodes."
  default     = "e2-standard-2"
}

variable "reducer_machine_type" {
  type        = string
  description = "Machine type for reducer nodes."
  default     = "e2-standard-2"
}

variable "trainer_machine_type" {
  type        = string
  description = "Machine type for trainer nodes."
  default     = "n1-standard-4"
}

variable "trainer_accelerator_type" {
  type        = string
  description = "GPU accelerator type for trainer nodes."
  default     = "nvidia-tesla-t4"
}

variable "trainer_accelerator_count" {
  type        = number
  description = "Number of GPUs per trainer node."
  default     = 1
}

variable "bootstrap_public_ip_enabled" {
  type        = bool
  description = "Whether bootstrap nodes receive public IP addresses."
  default     = true
}

variable "validator_public_ip_enabled" {
  type        = bool
  description = "Whether validator nodes receive public IP addresses."
  default     = false
}

variable "reducer_public_ip_enabled" {
  type        = bool
  description = "Whether reducer nodes receive public IP addresses."
  default     = false
}

variable "trainer_public_ip_enabled" {
  type        = bool
  description = "Whether trainer nodes receive public IP addresses."
  default     = false
}

variable "internal_tcp_ports" {
  type        = list(string)
  description = "TCP ports opened between fleet members."
  default     = ["8787", "8788", "8789", "4001", "4002", "4003"]
}

variable "internal_udp_ports" {
  type        = list(string)
  description = "UDP ports opened between fleet members."
  default     = ["4001", "4002", "4003"]
}

variable "bootstrap_public_source_ranges" {
  type        = list(string)
  description = "Public ingress source ranges for bootstrap swarm ports."
  default     = ["0.0.0.0/0"]
}

variable "bootstrap_public_tcp_ports" {
  type        = list(string)
  description = "Public TCP ports exposed only on bootstrap nodes."
  default     = ["4001"]
}

variable "bootstrap_public_udp_ports" {
  type        = list(string)
  description = "Public UDP ports exposed only on bootstrap nodes."
  default     = ["4001"]
}

variable "ssh_source_ranges" {
  type        = list(string)
  description = "Optional SSH ingress source ranges for hosts that have public IPs."
  default     = []
}

variable "bootstrap_image" {
  type        = string
  description = "Container image for the bootstrap nodes."
}

variable "validator_image" {
  type        = string
  description = "Container image for the validator nodes."
}

variable "reducer_image" {
  type        = string
  description = "Container image for the reducer nodes."
}

variable "trainer_image" {
  type        = string
  description = "Container image for the trainer nodes."
  default     = ""
}

variable "bootstrap_container_command" {
  type        = string
  description = "Arguments passed to the bootstrap image entrypoint."
  default     = "/config/config.json"
}

variable "validator_container_command" {
  type        = string
  description = "Arguments passed to the validator image entrypoint."
  default     = "/config/config.json"
}

variable "reducer_container_command" {
  type        = string
  description = "Arguments passed to the reducer image entrypoint."
  default     = "/config/config.json"
}

variable "trainer_container_command" {
  type        = string
  description = "Arguments passed to the trainer image entrypoint."
  default     = ""
}

variable "bootstrap_config_json" {
  type        = string
  description = "Bootstrap config JSON written onto each bootstrap host."
}

variable "validator_config_json" {
  type        = string
  description = "Validator config JSON written onto each validator host."
}

variable "reducer_config_json" {
  type        = string
  description = "Reducer config JSON written onto each reducer host."
}

variable "trainer_config_json" {
  type        = string
  description = "Optional trainer config JSON written onto each trainer host."
  default     = ""
}

variable "container_environment" {
  type        = map(string)
  description = "Environment variables passed into every node container so config placeholders can resolve provider secrets, operator backends, and artifact publication targets."
  default     = {}
}
