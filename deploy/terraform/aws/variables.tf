variable "aws_region" {
  type        = string
  description = "AWS region for the deployment."
  default     = "us-east-1"
}

variable "name_prefix" {
  type        = string
  description = "Name prefix applied to instances and security groups."
  default     = "burn-p2p"
}

variable "subnet_id" {
  type        = string
  description = "Subnet to place the instances into."
}

variable "key_name" {
  type        = string
  description = "Optional EC2 key pair for SSH access."
  default     = ""
}

variable "bootstrap_count" {
  type        = number
  description = "Number of bootstrap/coherence seed nodes."
  default     = 2
}

variable "validator_count" {
  type        = number
  description = "Number of separate validator/authority nodes."
  default     = 1
}

variable "reducer_count" {
  type        = number
  description = "Number of separate reducer nodes."
  default     = 1
}

variable "trainer_count" {
  type        = number
  description = "Number of trainer nodes."
  default     = 0
}

variable "bootstrap_instance_type" {
  type        = string
  description = "EC2 instance type for bootstrap nodes."
  default     = "t3.small"
}

variable "validator_instance_type" {
  type        = string
  description = "EC2 instance type for validator nodes."
  default     = "t3.large"
}

variable "reducer_instance_type" {
  type        = string
  description = "EC2 instance type for reducer nodes."
  default     = "t3.large"
}

variable "trainer_instance_type" {
  type        = string
  description = "EC2 instance type for trainer nodes."
  default     = "g4dn.xlarge"
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

variable "ssh_cidrs" {
  type        = list(string)
  description = "Optional SSH ingress ranges for hosts that have public IPs."
  default     = []
}

variable "internal_tcp_ports" {
  type        = list(number)
  description = "TCP ports opened between fleet members."
  default     = [8787, 8788, 8789, 4001, 4002, 4003]
}

variable "internal_udp_ports" {
  type        = list(number)
  description = "UDP ports opened between fleet members."
  default     = [4001, 4002, 4003]
}

variable "bootstrap_public_cidrs" {
  type        = list(string)
  description = "Public ingress source ranges for bootstrap swarm ports."
  default     = ["0.0.0.0/0"]
}

variable "bootstrap_public_tcp_ports" {
  type        = list(number)
  description = "Public TCP ports exposed only on bootstrap nodes."
  default     = [4001]
}

variable "bootstrap_public_udp_ports" {
  type        = list(number)
  description = "Public UDP ports exposed only on bootstrap nodes."
  default     = [4001]
}

variable "bootstrap_image" {
  type        = string
  description = "Container image for the bootstrap/coherence seed nodes."
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
