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

variable "trainer_instance_type" {
  type        = string
  description = "EC2 instance type for trainer nodes."
  default     = "g4dn.xlarge"
}

variable "ssh_cidrs" {
  type        = list(string)
  description = "Optional SSH ingress ranges."
  default     = []
}

variable "allowed_tcp_ports" {
  type        = list(number)
  description = "TCP ports exposed on the instances."
  default     = [8787, 8788, 4001, 4002]
}

variable "allowed_udp_ports" {
  type        = list(number)
  description = "UDP ports exposed on the instances."
  default     = [4001, 4002]
}

variable "bootstrap_image" {
  type        = string
  description = "Container image for the bootstrap/coherence seed nodes."
}

variable "validator_image" {
  type        = string
  description = "Container image for the validator nodes."
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

variable "trainer_config_json" {
  type        = string
  description = "Optional trainer config JSON written onto each trainer host."
  default     = ""
}
