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

  bootstrap_public_tcp_rules = {
    for rule in flatten([
      for port in var.bootstrap_public_tcp_ports : [
        for cidr in var.bootstrap_public_cidrs : {
          key  = "tcp-${port}-${replace(cidr, "/", "-")}"
          port = port
          cidr = cidr
        }
      ]
    ]) : rule.key => rule
  }

  bootstrap_public_udp_rules = {
    for rule in flatten([
      for port in var.bootstrap_public_udp_ports : [
        for cidr in var.bootstrap_public_cidrs : {
          key  = "udp-${port}-${replace(cidr, "/", "-")}"
          port = port
          cidr = cidr
        }
      ]
    ]) : rule.key => rule
  }
}

resource "aws_security_group" "fleet_internal" {
  name_prefix = "${var.name_prefix}-internal-"
  description = "burn_p2p split fleet internal mesh"
  vpc_id      = data.aws_subnet.selected.vpc_id

  tags = merge(local.common_tags, {
    Name = "${var.name_prefix}-internal"
  })
}

resource "aws_security_group" "bootstrap_public" {
  name_prefix = "${var.name_prefix}-bootstrap-public-"
  description = "burn_p2p bootstrap public ingress"
  vpc_id      = data.aws_subnet.selected.vpc_id

  tags = merge(local.common_tags, {
    Name = "${var.name_prefix}-bootstrap-public"
  })
}

resource "aws_vpc_security_group_egress_rule" "fleet_all" {
  security_group_id = aws_security_group.fleet_internal.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_vpc_security_group_ingress_rule" "internal_tcp" {
  for_each                     = toset([for port in var.internal_tcp_ports : tostring(port)])
  security_group_id            = aws_security_group.fleet_internal.id
  referenced_security_group_id = aws_security_group.fleet_internal.id
  from_port                    = tonumber(each.key)
  to_port                      = tonumber(each.key)
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "internal_udp" {
  for_each                     = toset([for port in var.internal_udp_ports : tostring(port)])
  security_group_id            = aws_security_group.fleet_internal.id
  referenced_security_group_id = aws_security_group.fleet_internal.id
  from_port                    = tonumber(each.key)
  to_port                      = tonumber(each.key)
  ip_protocol                  = "udp"
}

resource "aws_vpc_security_group_ingress_rule" "bootstrap_public_tcp" {
  for_each          = local.bootstrap_public_tcp_rules
  security_group_id = aws_security_group.bootstrap_public.id
  cidr_ipv4         = each.value.cidr
  from_port         = each.value.port
  to_port           = each.value.port
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "bootstrap_public_udp" {
  for_each          = local.bootstrap_public_udp_rules
  security_group_id = aws_security_group.bootstrap_public.id
  cidr_ipv4         = each.value.cidr
  from_port         = each.value.port
  to_port           = each.value.port
  ip_protocol       = "udp"
}

resource "aws_vpc_security_group_ingress_rule" "ssh" {
  for_each          = toset(var.ssh_cidrs)
  security_group_id = aws_security_group.fleet_internal.id
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
  vpc_security_group_ids      = [aws_security_group.fleet_internal.id, aws_security_group.bootstrap_public.id]
  key_name                    = var.key_name != "" ? var.key_name : null
  associate_public_ip_address = var.bootstrap_public_ip_enabled
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
  vpc_security_group_ids      = [aws_security_group.fleet_internal.id]
  key_name                    = var.key_name != "" ? var.key_name : null
  associate_public_ip_address = var.validator_public_ip_enabled
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
  vpc_security_group_ids      = [aws_security_group.fleet_internal.id]
  key_name                    = var.key_name != "" ? var.key_name : null
  associate_public_ip_address = var.reducer_public_ip_enabled
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
  vpc_security_group_ids      = [aws_security_group.fleet_internal.id]
  key_name                    = var.key_name != "" ? var.key_name : null
  associate_public_ip_address = var.trainer_public_ip_enabled
  user_data                   = local.trainer_user_data

  tags = merge(local.common_tags, {
    Name = format("%s-trainer-%02d", var.name_prefix, count.index + 1)
    Role = "trainer"
  })
}
