data "aws_subnet" "selected" {
  id = var.subnet_id
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

locals {
  common_tags = {
    Project   = "burn-p2p"
    ManagedBy = "terraform"
    Stack     = var.name_prefix
  }

  bootstrap_user_data = templatefile("${path.module}/user_data/node.sh.tftpl", {
    service_name = "${var.name_prefix}-bootstrap"
    image        = var.bootstrap_image
    command      = var.bootstrap_container_command
    config_json  = var.bootstrap_config_json
    gpu_enabled  = false
  })

  validator_user_data = templatefile("${path.module}/user_data/node.sh.tftpl", {
    service_name = "${var.name_prefix}-validator"
    image        = var.validator_image
    command      = var.validator_container_command
    config_json  = var.validator_config_json
    gpu_enabled  = false
  })

  reducer_user_data = templatefile("${path.module}/user_data/node.sh.tftpl", {
    service_name = "${var.name_prefix}-reducer"
    image        = var.reducer_image
    command      = var.reducer_container_command
    config_json  = var.reducer_config_json
    gpu_enabled  = false
  })

  trainer_user_data = templatefile("${path.module}/user_data/node.sh.tftpl", {
    service_name = "${var.name_prefix}-trainer"
    image        = var.trainer_image
    command      = var.trainer_container_command
    config_json  = var.trainer_config_json
    gpu_enabled  = true
  })
}

resource "aws_security_group" "burn_p2p" {
  name_prefix = "${var.name_prefix}-"
  description = "burn_p2p split fleet"
  vpc_id      = data.aws_subnet.selected.vpc_id

  tags = merge(local.common_tags, {
    Name = "${var.name_prefix}-sg"
  })
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.burn_p2p.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_vpc_security_group_ingress_rule" "tcp" {
  for_each          = toset([for port in var.allowed_tcp_ports : tostring(port)])
  security_group_id = aws_security_group.burn_p2p.id
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = tonumber(each.key)
  to_port           = tonumber(each.key)
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "udp" {
  for_each          = toset([for port in var.allowed_udp_ports : tostring(port)])
  security_group_id = aws_security_group.burn_p2p.id
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = tonumber(each.key)
  to_port           = tonumber(each.key)
  ip_protocol       = "udp"
}

resource "aws_vpc_security_group_ingress_rule" "ssh" {
  for_each          = toset(var.ssh_cidrs)
  security_group_id = aws_security_group.burn_p2p.id
  cidr_ipv4         = each.value
  from_port         = 22
  to_port           = 22
  ip_protocol       = "tcp"
}

resource "aws_instance" "bootstrap" {
  count                       = var.bootstrap_count
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.bootstrap_instance_type
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [aws_security_group.burn_p2p.id]
  key_name                    = var.key_name != "" ? var.key_name : null
  associate_public_ip_address = true
  user_data                   = local.bootstrap_user_data

  tags = merge(local.common_tags, {
    Name = format("%s-bootstrap-%02d", var.name_prefix, count.index + 1)
    Role = "bootstrap"
  })
}

resource "aws_instance" "validator" {
  count                       = var.validator_count
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.validator_instance_type
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [aws_security_group.burn_p2p.id]
  key_name                    = var.key_name != "" ? var.key_name : null
  associate_public_ip_address = true
  user_data                   = local.validator_user_data

  tags = merge(local.common_tags, {
    Name = format("%s-validator-%02d", var.name_prefix, count.index + 1)
    Role = "validator"
  })
}

resource "aws_instance" "reducer" {
  count                       = var.reducer_count
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.reducer_instance_type
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [aws_security_group.burn_p2p.id]
  key_name                    = var.key_name != "" ? var.key_name : null
  associate_public_ip_address = true
  user_data                   = local.reducer_user_data

  tags = merge(local.common_tags, {
    Name = format("%s-reducer-%02d", var.name_prefix, count.index + 1)
    Role = "reducer"
  })
}

resource "aws_instance" "trainer" {
  count                       = var.trainer_count
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.trainer_instance_type
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [aws_security_group.burn_p2p.id]
  key_name                    = var.key_name != "" ? var.key_name : null
  associate_public_ip_address = true
  user_data                   = local.trainer_user_data

  tags = merge(local.common_tags, {
    Name = format("%s-trainer-%02d", var.name_prefix, count.index + 1)
    Role = "trainer"
  })
}
