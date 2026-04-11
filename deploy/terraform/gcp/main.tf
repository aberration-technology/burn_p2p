data "google_compute_image" "ubuntu" {
  family  = "ubuntu-2404-lts-amd64"
  project = "ubuntu-os-cloud"
}

locals {
  labels = {
    project = "burn-p2p"
    stack   = var.name_prefix
  }

  fleet_tag            = var.name_prefix
  bootstrap_public_tag = "${var.name_prefix}-bootstrap-public"
  container_environment_file = join("\n", [
    for key in sort(keys(var.container_environment)) : "${key}=${var.container_environment[key]}"
  ])
}

resource "google_compute_firewall" "fleet_internal_tcp" {
  name    = "${var.name_prefix}-internal-tcp"
  network = var.network

  allow {
    protocol = "tcp"
    ports    = var.internal_tcp_ports
  }

  source_tags = [local.fleet_tag]
  target_tags = [local.fleet_tag]
}

resource "google_compute_firewall" "fleet_internal_udp" {
  name    = "${var.name_prefix}-internal-udp"
  network = var.network

  allow {
    protocol = "udp"
    ports    = var.internal_udp_ports
  }

  source_tags = [local.fleet_tag]
  target_tags = [local.fleet_tag]
}

resource "google_compute_firewall" "bootstrap_public_tcp" {
  count   = length(var.bootstrap_public_tcp_ports) > 0 ? 1 : 0
  name    = "${var.name_prefix}-bootstrap-public-tcp"
  network = var.network

  allow {
    protocol = "tcp"
    ports    = var.bootstrap_public_tcp_ports
  }

  source_ranges = var.bootstrap_public_source_ranges
  target_tags   = [local.bootstrap_public_tag]
}

resource "google_compute_firewall" "bootstrap_public_udp" {
  count   = length(var.bootstrap_public_udp_ports) > 0 ? 1 : 0
  name    = "${var.name_prefix}-bootstrap-public-udp"
  network = var.network

  allow {
    protocol = "udp"
    ports    = var.bootstrap_public_udp_ports
  }

  source_ranges = var.bootstrap_public_source_ranges
  target_tags   = [local.bootstrap_public_tag]
}

resource "google_compute_firewall" "ssh" {
  count   = length(var.ssh_source_ranges) > 0 ? 1 : 0
  name    = "${var.name_prefix}-ssh"
  network = var.network

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = var.ssh_source_ranges
  target_tags   = [local.fleet_tag]
}

resource "google_compute_instance" "bootstrap" {
  count        = var.bootstrap_count
  name         = format("%s-bootstrap-%02d", var.name_prefix, count.index + 1)
  machine_type = var.bootstrap_machine_type
  zone         = var.zone
  tags         = compact([local.fleet_tag, var.bootstrap_public_ip_enabled ? local.bootstrap_public_tag : ""])
  labels       = merge(local.labels, { role = "bootstrap" })

  boot_disk {
    initialize_params {
      image = data.google_compute_image.ubuntu.self_link
    }
  }

  network_interface {
    network    = var.network
    subnetwork = var.subnetwork != "" ? var.subnetwork : null

    dynamic "access_config" {
      for_each = var.bootstrap_public_ip_enabled ? [1] : []
      content {}
    }
  }

  metadata_startup_script = templatefile("${path.module}/startup/node.sh.tftpl", {
    service_name      = "${var.name_prefix}-bootstrap"
    image             = var.bootstrap_image
    command           = var.bootstrap_container_command
    config_json       = var.bootstrap_config_json
    env_file_contents = local.container_environment_file
    gpu_enabled       = false
  })
}

resource "google_compute_instance" "validator" {
  count        = var.validator_count
  name         = format("%s-validator-%02d", var.name_prefix, count.index + 1)
  machine_type = var.validator_machine_type
  zone         = var.zone
  tags         = [local.fleet_tag]
  labels       = merge(local.labels, { role = "validator" })

  boot_disk {
    initialize_params {
      image = data.google_compute_image.ubuntu.self_link
    }
  }

  network_interface {
    network    = var.network
    subnetwork = var.subnetwork != "" ? var.subnetwork : null

    dynamic "access_config" {
      for_each = var.validator_public_ip_enabled ? [1] : []
      content {}
    }
  }

  metadata_startup_script = templatefile("${path.module}/startup/node.sh.tftpl", {
    service_name      = "${var.name_prefix}-validator"
    image             = var.validator_image
    command           = var.validator_container_command
    config_json       = var.validator_config_json
    env_file_contents = local.container_environment_file
    gpu_enabled       = false
  })
}

resource "google_compute_instance" "reducer" {
  count        = var.reducer_count
  name         = format("%s-reducer-%02d", var.name_prefix, count.index + 1)
  machine_type = var.reducer_machine_type
  zone         = var.zone
  tags         = [local.fleet_tag]
  labels       = merge(local.labels, { role = "reducer" })

  boot_disk {
    initialize_params {
      image = data.google_compute_image.ubuntu.self_link
    }
  }

  network_interface {
    network    = var.network
    subnetwork = var.subnetwork != "" ? var.subnetwork : null

    dynamic "access_config" {
      for_each = var.reducer_public_ip_enabled ? [1] : []
      content {}
    }
  }

  metadata_startup_script = templatefile("${path.module}/startup/node.sh.tftpl", {
    service_name      = "${var.name_prefix}-reducer"
    image             = var.reducer_image
    command           = var.reducer_container_command
    config_json       = var.reducer_config_json
    env_file_contents = local.container_environment_file
    gpu_enabled       = false
  })
}

resource "google_compute_instance" "trainer" {
  count        = var.trainer_count
  name         = format("%s-trainer-%02d", var.name_prefix, count.index + 1)
  machine_type = var.trainer_machine_type
  zone         = var.zone
  tags         = [local.fleet_tag]
  labels       = merge(local.labels, { role = "trainer" })

  boot_disk {
    initialize_params {
      image = data.google_compute_image.ubuntu.self_link
    }
  }

  network_interface {
    network    = var.network
    subnetwork = var.subnetwork != "" ? var.subnetwork : null

    dynamic "access_config" {
      for_each = var.trainer_public_ip_enabled ? [1] : []
      content {}
    }
  }

  guest_accelerator {
    type  = var.trainer_accelerator_type
    count = var.trainer_accelerator_count
  }

  scheduling {
    on_host_maintenance = "TERMINATE"
    automatic_restart   = true
  }

  metadata = {
    install-nvidia-driver = "true"
  }

  metadata_startup_script = templatefile("${path.module}/startup/node.sh.tftpl", {
    service_name      = "${var.name_prefix}-trainer"
    image             = var.trainer_image
    command           = var.trainer_container_command
    config_json       = var.trainer_config_json
    env_file_contents = local.container_environment_file
    gpu_enabled       = true
  })
}
